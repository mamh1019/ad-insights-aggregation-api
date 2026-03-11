#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from libs.http_client import HttpClient
from libs.utils import is_empty
from libs.utils.cli import log, start_task
from config.constants import CREATIVE_TYPE
from libs.utils.string import (
    make_hash_key,
    make_creative_asset_hash_key,
    make_creative_history_hash_key,
)
from libs.utils import date
from libs.utils.array import array_split
from libs.utils.video import extract_first_frame
from urllib.parse import urlencode
from datetime import datetime, timedelta

import os
import boto3
import pandas as pd


class Applovin(Advertiser):
    """
    https://support.applovin.com/hc/en-us/articles/115000784688-Reporting-API
    """

    def __init__(self):
        self.network_name = "applovin_int"
        self.api_key = settings.applovin_api_key

        self.applovin_api_url = "https://r.applovin.com/report"
        self.axon_api_url = "https://api.ads.axon.ai/manage/v1/creative_set/list"
        self.axon_api_key = settings.applovin_axon_api_key
        self.account_id = settings.applovin_account_id

        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "default"))
        self.s3 = session.resource("s3")
        self.s3_client = boto3.client("s3")
        self.cdn_bucket_id = os.environ.get("CDN_BUCKET_ID", "")
        self.cdn_bucket_key_prefix = "applovin/creatives"
        self.cdn_bucket = self.s3.Bucket(self.cdn_bucket_id)

    def report_cost(self):
        params = {
            "api_key": self.api_key,
            "start": self.start,
            "end": self.end,
            "format": "json",
            "not_zero": 1,
            "columns": "campaign_store_id,impressions,platform,day,cost,campaign_id_external,campaign,country,conversions,campaign_package_name,clicks",
            "report_type": "advertiser",
        }

        api_url = "https://r.applovin.com/report"
        result = HttpClient.get(api_url, params=params)

        if "code" in result and result["code"] == 200:
            reports = result["results"]
            for campaign in reports:
                if (
                    float(campaign["cost"]) <= 0
                    and int(campaign["impressions"]) <= 0
                    and int(campaign["clicks"]) <= 0
                ):
                    continue

                if is_empty(campaign["country"]):
                    continue

                log_date = campaign["day"].replace("-", "")
                app_id = campaign["campaign_store_id"]
                platform = self.suppose_platform(app_id, self.network_name)

                self.push(
                    log_date=log_date,
                    app_id=app_id,
                    platform=platform,
                    country=campaign["country"].upper(),
                    media_source=self.network_name,
                    campaign_id=campaign["campaign_id_external"],
                    campaign=campaign["campaign"],
                    cost=campaign["cost"],
                    impressions=campaign["impressions"],
                    clicks=campaign["clicks"],
                    conversions_type="INSTALL",
                    conversion_value=campaign["conversions"],
                )
        self.flush()

    ##############################################################################
    ## Creative
    ##############################################################################
    def report_creative(self):
        """
        campaign - creative_set_id is 1:N but we assume 1:1 (single video per campaign)
        Add playable asset if present
        """
        ## Step1. Fetch AppLovin campaign list - filter out inactive campaigns
        params = {
            "api_key": self.api_key,
            "start": self.start,
            "end": self.end,
            "format": "json",
            "columns": "creative_set_id,creative_set,impressions,platform,day,cost,country,campaign_id_external,campaign,conversions,campaign_package_name,clicks",
            "not_zero": 1,
            "report_type": "advertiser",
        }

        result = HttpClient.get(self.applovin_api_url, params=params, timeout=60)
        if "results" not in result:
            log(result)
            return
        df = pd.DataFrame(result["results"])
        if df.empty:
            log("no creative data")
            return

        log(f"get creative report data: {len(df)} rows")

        # Step2. TODO: Fetch applovin_campaign_ids from your data model (start, end)
        db_campaign_ids_df = pd.DataFrame(columns=["campaign_id"])
        df = df[df.campaign_id_external.isin(db_campaign_ids_df.campaign_id.tolist())]
        if df.empty:
            log("no cached campaign data")
            return

        creatives_ids = df.creative_set_id.unique()

        # Step3. Fetch creative list. Link to each campaign creative ID
        creatives_df = self.get_creative_sets(creatives_ids)
        creatives_df.rename(
            columns={"hashed_id": "creative_set_id", "name": "creative_set_name"},
            inplace=True,
        )
        creatives_df = creatives_df[["creative_set_id", "creative_set_name", "assets"]]
        df = df.merge(creatives_df, on="creative_set_id", how="left")

        # Step4. Normalize columns
        df.rename(
            columns={
                "day": "log_date",
                "campaign_package_name": "app_id",
                "campaign_id_external": "campaign_id",
                "campaign": "campaign_name",
                "creative_set_id": "adset_id",
                "creative_set_name": "adset_name",
            },
            inplace=True,
        )
        df["country"] = df["country"].str.upper()

        ad_network_creatives = []  # Full creative list
        assets_list = []  # Full asset list
        assets_info = []  # Video info collection

        for row in df.itertuples(index=False):
            thumbnail_uri = None
            if is_empty(row.assets):
                continue
            for asset in row.assets:
                if asset["type"] == "HOSTED_HTML":
                    creative_type = CREATIVE_TYPE.PLAYABLE
                elif "VID" in asset["type"]:
                    creative_type = CREATIVE_TYPE.VIDEO
                    if thumbnail_uri is None:
                        partition = int(asset["id"]) % 10000
                        thumbnail_uri = "{}/{}/{}/{}/{}".format(
                            self.cdn_bucket_key_prefix,
                            partition,
                            "video",
                            "thumbnail",
                            f"{asset['id']}.jpeg",
                        )
                        # Store for bulk processing
                        assets_info.append(
                            {
                                "uri": thumbnail_uri,
                                "video_url": asset["url"],
                            }
                        )
                else:
                    continue

                assets_list.append(
                    {
                        "adset_id": row.adset_id,
                        "creative_type": creative_type,
                        "creative_id": asset["id"],
                        "creative_name": asset["name"],
                        "creative_uri": asset["url"],
                        "hash_key": make_creative_asset_hash_key(
                            self.network_name, row.adset_id, asset["id"]
                        ),
                    }
                )

            log_date = row.log_date.replace("-", "")
            creative_history_hash_key = make_creative_history_hash_key(
                media_source=self.network_name,
                campaign_id=row.campaign_id,
                adset_id=row.adset_id,
                ad_id="",
                creative_id="",
            )
            # TODO: Fetch creative_start_date from your data model (hash_key)
            creative_start_date = None

            ad_network_creatives.append(
                {
                    "log_date": log_date,
                    "app_id": row.app_id,
                    "media_source": self.network_name,
                    "platform": row.platform,
                    "country": row.country,
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "adset_id": row.adset_id,
                    "adset_name": row.adset_name,
                    "ad_id": "",
                    "creative_id": "",
                    "creative_name": "",
                    "creative_type": "VIDEO",
                    "start_date": (
                        creative_start_date
                        if creative_start_date is not None
                        else log_date
                    ),
                    "creative_uri": "",
                    "creative_uri_expire_time": 0,
                    "thumbnail_uri": thumbnail_uri,
                    "impressions": row.impressions,
                    "clicks": row.clicks,
                    "installs": row.conversions,
                    "conversions": row.conversions,
                    "cost": row.cost,
                }
            )

        total_assets_num = 0
        new_assets_num = 0
        if len(assets_info) > 0:
            assets_df = pd.DataFrame(
                columns=["uri", "video_url"], data=assets_info, dtype=str
            ).drop_duplicates(subset=["uri", "video_url"])
            assets_df["hash_key"] = assets_df["uri"].apply(
                lambda u: make_hash_key(f"{self.cdn_bucket_id}:{u}")
            )
            total_assets_num = len(assets_df)

            # TODO: Fetch cached_cdn_uri from your data model (hash_keys)
            cached_cdn_uri_df = pd.DataFrame(columns=["uri"])
            new_cdn_assets_df = assets_df[~assets_df.uri.isin(cached_cdn_uri_df.uri)]
            new_assets_num = len(new_cdn_assets_df)
            process_num = 1
            if not new_cdn_assets_df.empty:
                for asset_info in new_cdn_assets_df.itertuples(index=False):
                    # AppLovin has no thumbnail; extract from video
                    tmp_path = "/tmp/thumbnail.jpg"
                    extract_first_frame(asset_info.video_url, tmp_path)
                    log(f"extract_first_frame: {asset_info.uri}")
                    self.load_to_s3_if_not_exists(tmp_path, asset_info.uri)
                    log(
                        f"s3 uploaded: {asset_info.uri} - {process_num} / {new_assets_num}"
                    )
                    process_num += 1
                    # Persist cached_cdn_uri to model

        # Performance metrics
        if len(ad_network_creatives) > 0:
            df = pd.DataFrame.from_records(ad_network_creatives)
            # Persist ad_cost_creatives to model
            log(f"creative insert {len(ad_network_creatives)} rows")

        # Performance metrics (store Assets linked to creative_set_id)
        if len(assets_list) > 0:
            df = pd.DataFrame.from_records(assets_list)
            df["ad_created"] = date.now()
            df["media_source"] = self.network_name
            # Persist ad_cost_creative_assets to model
            updated_rows = 0
            log(f"creative assets inserted {updated_rows} rows")

        log(f"new assets: {new_assets_num} of total assets: {total_assets_num} rows")

        # Per-asset metrics
        self.asset_report()

    def get_creative_sets(self, creative_set_ids: list) -> pd.DataFrame:
        max_page = 20
        creatives_set_ids_bulk = array_split(creative_set_ids, 100)
        res = []

        total_num = len(creative_set_ids)
        progress, task = start_task(
            total_num, description="Fetching creative sets", transient=False
        )

        with progress:
            current_process_num = 0
            progress.update(
                task,
                description=f"Fetching creative sets (0/{total_num})",
            )
            for _, creatives_set_ids in enumerate(creatives_set_ids_bulk, start=1):
                for page in range(1, max_page + 1):
                    params = {
                        "hashed_ids": ",".join(creatives_set_ids),
                        "page": page,
                        "size": 100,
                    }

                    response = self.axon_api_get(self.axon_api_url, params)

                    process_num = len(response)
                    current_process_num += process_num
                    progress.update(
                        task,
                        description=f"Fetching creative sets ({current_process_num}/{total_num})",
                    )
                    progress.advance(task, process_num)
                    if not response:
                        break

                    res += response

        return pd.DataFrame(res)

    def axon_api_get(self, url: str, params: dict) -> dict:
        headers = {"Authorization": self.axon_api_key}
        params["account_id"] = self.account_id
        response = HttpClient.get(url, params=params, headers=headers, timeout=60)
        return response

    def asset_report(self):
        try:
            yesterday = date.yesterday()
            params = {
                "api_key": self.api_key,
                "range": "yesterday",
                "columns": "asset_id,clicks,impressions,cost",
                "sort_day": "DESC",
                "limit": 10000,
                "format": "csv",
            }

            assets_df = HttpClient.csv(
                "https://r.applovin.com/assetReport?" + urlencode(params)
            )
            if not assets_df.empty:
                assets_df["log_date"] = yesterday
                # Persist applovin_assets to model

                check_last_date = (datetime.now() - timedelta(days=1)).strftime(
                    "%Y%m%d"
                )
                if assets_df[assets_df["log_date"] == check_last_date].empty:
                    pass

            log("assets %s rows will be inserted" % len(assets_df))

        except Exception as e:
            log(f"assets Failed: {e}")
