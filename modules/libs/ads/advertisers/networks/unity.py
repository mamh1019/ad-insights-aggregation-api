#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from libs.http_client import HttpClient
from urllib.parse import urlencode
from libs.utils.cli import log
from libs.utils.common import is_empty
from libs.utils import date
from libs.utils.string import (
    make_creative_asset_hash_key,
    make_hash_key,
    make_creative_history_hash_key,
)
from config.constants import CREATIVE_TYPE
from libs.utils.video import extract_first_frame

import os
import pandas as pd
import base64
import boto3


class Unity(Advertiser):
    def __init__(self):
        self.network_name = "unityads_int"
        self.api_key = settings.unityads_v2_api_key
        self.api_secret = settings.unityads_v2_api_secret
        self.organization_id = settings.unityads_organization_id
        self.stats_api_url = "https://services.api.unity.com/advertise/stats/v2/organizations/{}/reports/acquisitions".format(
            self.organization_id
        )
        self.marketing_api_base = (
            "https://services.api.unity.com/advertise/v1/organizations/{}/apps".format(
                self.organization_id
            )
        )
        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "default"))
        self.s3 = session.resource("s3")
        self.s3_client = boto3.client("s3")
        self.cdn_bucket_id = os.environ.get("CDN_BUCKET_ID", "")
        self.cdn_bucket_key_prefix = "unity/creatives"
        self.cdn_bucket = self.s3.Bucket(self.cdn_bucket_id)

    def _get_header(self) -> dict:
        return {"Authorization": "Basic {}:{}".format(self.api_key, self.api_secret)}

    def report_cost(self):
        header = self._get_header()
        params = {
            "start": "{}T00:00:00".format(self.start),
            "end": "{}T23:59:00".format(self.end),
            "metrics": "clicks,installs,spend,views",
            "breakdowns": "campaign,country,platform,targetGame",
            "scale": "day",
        }

        df = HttpClient.csv(
            self.stats_api_url + "?{}".format(urlencode(params)),
            headers=header,
            timeout=30,
        )
        df = df[df.spend > 0]
        if df.empty:
            return

        df.rename(columns={"views": "impressions"}, inplace=True)
        df.rename(columns={"target store id": "app_id"}, inplace=True)

        for _, report in df.iterrows():
            tmp = report["timestamp"].split("T")
            log_date = tmp[0].replace("-", "")
            platform = self.suppose_platform(report["app_id"], self.network_name)
            self.push(
                log_date=log_date,
                app_id=report["app_id"],
                platform=platform,
                country=report["country"],
                media_source=self.network_name,
                campaign_id=report["campaign id"],
                campaign=report["campaign name"],
                cost=report["spend"],
                impressions=report["impressions"],
                clicks=report["clicks"],
                conversions_type="INSTALL",
                conversion_value=report["installs"] if "installs" in report else 0,
            )

        self.flush()

    def _get_marketing_api_header(self) -> dict:
        raw = f"{self.api_key}:{self.api_secret}"
        encoded = base64.b64encode(raw.encode("utf-8")).decode("utf-8")
        return {
            "Authorization": "Basic {}".format(encoded),
        }

    def report_creative(self):
        """
        list creatives - https://services.docs.unity.com/advertise/#tag/Creatives
        """
        header = self._get_marketing_api_header()

        # Set Variables
        api_app_list = self.marketing_api_base
        api_creative_packs = self.marketing_api_base + "/{}/creative-packs"
        api_creatives = self.marketing_api_base + "/{}/creatives"

        header = self._get_header()
        params = {
            "start": "{}T00:00:00".format(self.start),
            "end": "{}T23:59:00".format(self.end),
            "metrics": "clicks,installs,spend,views",
            "breakdowns": "campaign,country,platform,targetGame,creativePack,creativePackType",
            "scale": "day",
        }
        df = HttpClient.csv(
            self.stats_api_url + "?{}".format(urlencode(params)),
            headers=header,
            timeout=30,
        )
        df = df[df.clicks > 0]
        if df.empty:
            return

        rename_map = {
            "timestamp": "log_date",
            "target store id": "bundle_id",
            "creative pack id": "creative_pack_id",
            "creative pack name": "creative_pack_name",
            "creative pack type": "creative_pack_type",
            "campaign id": "campaign_id",
            "campaign name": "campaign_name",
            "country": "country",
            "platform": "platform",
            "spend": "cost",
            "conversions": "installs",
            "views": "impressions",
            "clicks": "clicks",
        }
        df.rename(
            columns=rename_map,
            inplace=True,
        )
        df = df[rename_map.values()]
        df["creative_pack_type"] = df["creative_pack_type"].apply(
            self._get_creative_type
        )

        ## Step1. Call API to get Unity AppID (CampaignSetId). Filter to apps with cost in DB.
        # TODO: Fetch cost_app_info from your data model (start, end, network_name)
        cost_app_df = pd.DataFrame(columns=["store_id"])
        if cost_app_df.empty:
            return

        res = HttpClient.get(api_app_list, headers=header)
        unity_app_df = pd.DataFrame(res["results"])
        unity_app_df = unity_app_df[
            unity_app_df.storeId.isin(cost_app_df.store_id.tolist())
        ][["id", "name", "storeId", "store"]]
        unity_app_df["platform"] = unity_app_df["storeId"].apply(
            lambda x: self.suppose_platform(x, self.network_name)
        )
        unity_app_df.rename(
            columns={
                "id": "campaign_set_id",
                "name": "app_name",
                "storeId": "bundle_id",
            },
            inplace=True,
        )

        for app_info in unity_app_df.itertuples():
            campaign_set_id = app_info.campaign_set_id
            app_name = app_info.app_name
            bundle_id = app_info.bundle_id
            platform = app_info.platform

            log(
                f"[{app_name}][{self.start} ~ {self.end}] {platform} - start creative report processing"
            )

            # Performance-based DF
            stat_df = df[df.bundle_id == bundle_id]

            creative_pack_api_limit = 700
            params = {"limit": creative_pack_api_limit}
            res = HttpClient.get(
                api_creative_packs.format(campaign_set_id),
                headers=header,
                params=params,
            )
            if is_empty(res["results"]):
                continue

            if res["total"] > creative_pack_api_limit:
                pass

            log(
                f"[{app_name}][{self.start} ~ {self.end}] {platform} - get {len(res['results'])} creative packs"
            )

            creative_packs_df = pd.DataFrame(
                res["results"], columns=["id", "name", "type", "creativeIds"]
            )
            creative_packs_df = creative_packs_df.explode("creativeIds")
            creative_packs_df = creative_packs_df.rename(
                columns={
                    "id": "creative_pack_id",
                    "creativeIds": "creative_id",
                    "name": "creative_pack_name",
                    "type": "creative_pack_type",
                }
            )

            creatives_api_limit = 2000
            params = {"limit": creatives_api_limit}
            res = HttpClient.get(
                api_creatives.format(campaign_set_id), headers=header, params=params
            )
            if is_empty(res["results"]):
                continue

            if res["total"] > creatives_api_limit:
                pass

            creatives_df = pd.DataFrame(
                res["results"], columns=["id", "name", "type", "files"]
            )
            creatives_df = creatives_df.rename(
                columns={
                    "id": "creative_id",
                    "name": "creative_name",
                    "type": "creative_type",
                }
            )
            creatives_df = creatives_df.merge(
                creative_packs_df, on="creative_id", how="left"
            )
            creatives_df["creative_type"] = creatives_df["creative_type"].apply(
                self._get_creative_asset_type
            )

            ad_network_creatives = []  # Full creative list
            assets_list = []  # Full asset list
            video_asset_info = []  # Video info collection

            for stat_row in stat_df.itertuples():
                log_date = stat_row.log_date.replace("-", "")
                bundle_id = stat_row.bundle_id
                creative_pack_id = stat_row.creative_pack_id
                thumbnail_uri = None

                stat_creative_df = creatives_df[
                    creatives_df.creative_pack_id == creative_pack_id
                ]
                for creative_row in stat_creative_df.itertuples():
                    # Capture video info for thumbnail (mixed creatives: use first video only)
                    for file in creative_row.files:
                        if creative_row.creative_type == "VIDEO":
                            if thumbnail_uri is None:
                                thumbnail_uri = f"{self.cdn_bucket_key_prefix}/{creative_row.creative_id}.jpeg"
                                video_asset_info.append(
                                    {
                                        "uri": thumbnail_uri,
                                        "video_url": file["url"],
                                    }
                                )
                        elif creative_row.creative_type == "IMAGE":
                            # No sample to verify thumbnail extraction. Will check when image type appears.
                            pass

                        assets_list.append(
                            {
                                "adset_id": creative_row.creative_pack_id,
                                "creative_type": creative_row.creative_type,
                                "creative_id": creative_row.creative_id,
                                "creative_name": creative_row.creative_name,
                                "creative_uri": file["url"],
                                "hash_key": make_creative_asset_hash_key(
                                    self.network_name,
                                    creative_row.creative_pack_id,
                                    creative_row.creative_id,
                                ),
                            }
                        )

                creative_history_hash_key = make_creative_history_hash_key(
                    media_source=self.network_name,
                    campaign_id=stat_row.campaign_id,
                    adset_id=stat_row.creative_pack_id,
                    ad_id="",
                    creative_id="",
                )
                # TODO: Fetch creative_start_date from your data model (hash_key)
                creative_start_date = None

                ad_network_creatives.append(
                    {
                        "log_date": log_date,
                        "app_id": bundle_id,
                        "media_source": self.network_name,
                        "platform": platform,
                        "country": stat_row.country,
                        "campaign_id": stat_row.campaign_id,
                        "campaign_name": stat_row.campaign_name,
                        "adset_id": creative_pack_id,
                        "adset_name": stat_row.creative_pack_name,
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
                        "impressions": stat_row.impressions,
                        "clicks": stat_row.clicks,
                        "installs": stat_row.installs,
                        "conversions": stat_row.installs,
                        "cost": stat_row.cost,
                    }
                )

            total_assets_num = 0
            new_assets_num = 0
            if len(video_asset_info) > 0:
                assets_df = pd.DataFrame(
                    columns=["uri", "video_url"], data=video_asset_info, dtype=str
                ).drop_duplicates(subset=["uri", "video_url"])
                assets_df["hash_key"] = assets_df["uri"].apply(
                    lambda u: make_hash_key(f"{self.cdn_bucket_id}:{u}")
                )
                total_assets_num = len(assets_df)

                # TODO: Fetch cached_cdn_uri from your data model (hash_keys)
                cached_cdn_uri_df = pd.DataFrame(columns=["uri"])
                new_cdn_assets_df = assets_df[
                    ~assets_df.uri.isin(cached_cdn_uri_df.uri)
                ]
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
                tmp_df = pd.DataFrame.from_records(ad_network_creatives)
                # Persist ad_cost_creatives to model
                log(f"creative insert {len(ad_network_creatives)} rows")

            # Performance metrics (store Assets linked to creative_set_id)
            if len(assets_list) > 0:
                tmp_df = pd.DataFrame.from_records(assets_list)
                tmp_df["ad_created"] = date.now()
                tmp_df["media_source"] = self.network_name
                # Persist ad_cost_creative_assets to model
                updated_rows = 0
                log(f"creative assets inserted {updated_rows} rows")

            log(
                f"new assets: {new_assets_num} of total assets: {total_assets_num} rows"
            )

    def _get_creative_asset_type(self, creative_type: str) -> str:
        creative_type = creative_type.lower()
        if "playable" in creative_type:
            return CREATIVE_TYPE.PLAYABLE
        elif "video" in creative_type:
            return CREATIVE_TYPE.VIDEO
        else:
            return CREATIVE_TYPE.IMAGE

    def _get_creative_type(self, creative_type: str) -> str:
        # For mixed creatives (largest category), display as video
        creative_type = creative_type.lower()
        if "playable" in creative_type:
            return CREATIVE_TYPE.VIDEO
        elif "video" in creative_type:
            return CREATIVE_TYPE.VIDEO
        else:
            return CREATIVE_TYPE.IMAGE
