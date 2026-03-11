#!/usr/bin/env python
from collections import defaultdict
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from config.constants import CREATIVE_TYPE
from libs.http_client import HttpClient
from libs.utils.array import array_split
from libs.utils.cli import log
from datetime import datetime

import json
import os
import boto3
import pandas as pd
import time

from libs.utils.common import is_empty

MAX_API_RETRY = 3


class Tiktok(Advertiser):
    def __init__(self):
        self.network_name = "TikTok Ads"
        # https://ads.tiktok.com/marketing_api/docs?id=1715587780630529
        self.accounts_end_point = (
            "https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/"
        )
        self.end_point = (
            "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        )
        self.access_token = settings.tiktok_access_token
        self.app_id = settings.tiktok_app_id
        self.secret = settings.tiktok_app_secret
        self.headers = {
            "Access-Token": self.access_token,
            "Content-Type": "application/json",
        }

        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "default"))
        self.s3 = session.resource("s3")
        self.s3_client = boto3.client("s3")
        self.cdn_bucket_id = os.environ.get("CDN_BUCKET_ID", "")
        self.cdn_bucket_key_prefix = "tiktok/creatives"
        self.cdn_bucket = self.s3.Bucket(self.cdn_bucket_id)

    def _paging_check(self, tiktok_result):
        """TikTok paging not implemented; raise error when paging occurs"""
        data = tiktok_result["data"]
        if "page_info" not in data:
            raise Exception("invalid result")
        if int(data["page_info"]["total_page"]) > 1:
            raise Exception("too many page")

    def ad_accounts(self):
        # get ad accounts
        params = {"app_id": self.app_id, "secret": self.secret}
        result = HttpClient.get(
            self.accounts_end_point,
            params=params,
            headers=self.headers,
            timeout=20,
        )

        ad_accounts = []
        if result["code"] == 0 and result["message"] == "OK":
            accounts = result["data"]["list"]
            for account in accounts:
                # Skip agency-managed accounts if needed
                # Add custom filtering logic here if required
                ad_accounts.append(account["advertiser_id"])
        return ad_accounts

    def report_cost(self):
        ad_accounts = self.ad_accounts()
        if len(ad_accounts) == 0:
            return

        for advertiser_id in ad_accounts:
            # get campaign info
            params = {
                "advertiser_id": advertiser_id,
                "service_type": "AUCTION",
                "report_type": "AUDIENCE",
                "dimensions": '["platform", "adgroup_id"]',
                "metrics": '["spend", "impressions", "mobile_app_id", "campaign_id"]',
                "start_date": self.start,
                "end_date": self.end,
                "data_level": "AUCTION_ADGROUP",
                "page_size": 1000,
                "page": 1,
            }
            result = HttpClient.get(
                self.end_point,
                params=params,
                headers=self.headers,
                timeout=20,
            )

            self._paging_check(result)

            campaign_info = {}
            if result["code"] == 0 and result["message"] == "OK":
                rows = result["data"]["list"]
                for row in rows:
                    metrics = row["metrics"]
                    dimensions = row["dimensions"]

                    if metrics["campaign_id"] not in campaign_info:
                        campaign_info[metrics["campaign_id"]] = {
                            "store_id": metrics["mobile_app_id"],
                            "platform": dimensions["platform"],
                        }

            # get insight
            params = {
                "advertiser_id": advertiser_id,
                "report_type": "AUDIENCE",
                "dimensions": '["campaign_id", "stat_time_day", "country_code"]',
                "metrics": '["campaign_name", "spend", "impressions", "clicks", "conversion"]',
                "start_date": self.start,
                "end_date": self.end,
                "data_level": "AUCTION_CAMPAIGN",
                "page_size": 1000,
                "page": 1,
            }
            result = HttpClient.get(
                self.end_point,
                params=params,
                headers=self.headers,
                timeout=20,
            )

            if result["code"] == 0 and result["message"] == "OK":
                rows = result["data"]["list"]
                for row in rows:
                    metrics = row["metrics"]
                    dimensions = row["dimensions"]

                    spend = metrics["spend"]
                    if float(spend) <= 0:
                        continue
                    impressions = metrics["impressions"]
                    clicks = metrics["clicks"]
                    conversion = metrics["conversion"]
                    campaign_id = dimensions["campaign_id"]
                    campaign_name = metrics["campaign_name"]

                    if campaign_id not in campaign_info:
                        log(
                            "not exists campaign info ({}-{})".format(
                                advertiser_id, campaign_id
                            )
                        )
                        continue

                    store_id = campaign_info[campaign_id]["store_id"]
                    country = dimensions["country_code"]
                    log_date = (
                        dimensions["stat_time_day"].split(" ")[0].replace("-", "")
                    )
                    platform = self.suppose_platform(store_id, self.network_name)

                    self.push(
                        log_date=log_date,
                        app_id=store_id,
                        platform=platform,
                        country=country,
                        media_source=self.network_name,
                        campaign_id=campaign_id,
                        campaign=campaign_name,
                        cost=spend,
                        impressions=impressions,
                        clicks=clicks,
                        conversions_type="INSTALL",
                        conversion_value=conversion,
                    )

                self.flush()

    def report_creative(self):
        ad_accounts = self.ad_accounts()
        date_list = pd.date_range(start=self.start, end=self.end)

        for advertiser_id in ad_accounts:
            for date_object in date_list:
                date_cursor = date_object.strftime("%Y-%m-%d")
                ym = date_object.strftime("%Y%m")
                ad_info = defaultdict(dict)

                ad_date = date_object.strftime("%Y%m%d")
                # TODO: Fetch ad_cost_report from your data model (ad_date, media_source)
                daily_campaign_df = pd.DataFrame()
                if is_empty(daily_campaign_df):
                    continue

                params = {
                    "advertiser_id": str(advertiser_id),
                    "service_type": "AUCTION",  # https://ads.tiktok.com/marketing_api/docs?id=1751087777884161
                    "report_type": "BASIC",  # https://ads.tiktok.com/marketing_api/docs?id=1738864835805186
                    "data_level": "AUCTION_AD",  # https://ads.tiktok.com/marketing_api/docs?id=1715587780630529
                    "dimensions": json.dumps(  # https://ads.tiktok.com/marketing_api/docs?id=1707957217727489
                        [
                            "ad_id",
                            "country_code",
                        ]
                    ),
                    "metrics": json.dumps(
                        [
                            "spend",
                            "impressions",
                            "clicks",
                            "conversion",
                            "campaign_id",
                            "campaign_name",
                            "adgroup_id",
                            "adgroup_name",
                        ]
                    ),
                    "filtering": json.dumps(
                        [
                            {
                                "field_name": "ad_status",
                                "filter_type": "IN",
                                "filter_value": json.dumps(["STATUS_DELIVERY_OK"]),
                            },
                        ]
                    ),
                    "start_date": date_cursor,
                    "end_date": date_cursor,
                    "page_size": 1000,
                    "page": 1,
                }
                result = self._api_get(self.end_point, params=params)

                if "data" not in result:
                    continue
                if "list" not in result["data"]:
                    continue

                log(f"retrieved adgroup list ({len(result['data']['list'])})")

                ads_metrics = {}
                for audience_ad in result["data"]["list"]:
                    metrics = audience_ad["metrics"]
                    impressions = int(metrics["impressions"])
                    if impressions > 0:
                        dimensions = audience_ad["dimensions"]
                        if (
                            dimensions["country_code"] == "None"
                            or dimensions["country_code"] is None
                        ):
                            log(
                                f"country code is none ({advertiser_id} {date_cursor} {dimensions})"
                            )
                            continue

                        campaign_id = metrics["campaign_id"]
                        campaign_name = metrics["campaign_name"]
                        adgroup_id = metrics["adgroup_id"]
                        adgroup_name = metrics["adgroup_name"]
                        ad_id = dimensions["ad_id"]
                        country_code = dimensions["country_code"]

                        clicks = int(metrics["clicks"])
                        conversion = int(metrics["conversion"])
                        spend = float(metrics["spend"])

                        ad_cost_df = daily_campaign_df[
                            daily_campaign_df.campaign_id == campaign_id
                        ]
                        if ad_cost_df is None or ad_cost_df.empty:
                            continue

                        ad_cost = ad_cost_df.iloc[0]

                        bundle_id = ad_cost["app_id"]
                        platform = ad_cost["platform"]

                        ad_info[ad_id]["bundle_id"] = bundle_id
                        ad_info[ad_id]["platform"] = platform

                        ads_metrics[ad_id] = {
                            "advertiser_id": advertiser_id,
                            "campaign_id": campaign_id,
                            "campaign_name": campaign_name,
                            "adgroup_id": adgroup_id,
                            "adgroup_name": adgroup_name,
                            "ad_id": ad_id,
                            "country_code": country_code,
                            "platform": platform,
                            "metrics": {
                                "impressions": impressions,
                                "clicks": clicks,
                                "conversion": conversion,
                                "spend": spend,
                            },
                        }

                if len(ads_metrics) == 0:
                    continue

                log(f"made ads metrics ({len(ads_metrics)})")
                ad_ids = list(ads_metrics.keys())
                split_ad_ids = array_split(ad_ids, 80)
                for ad_id_parts in split_ad_ids:
                    video_info = self.creative_info(
                        advertiser_id, CREATIVE_TYPE.VIDEO, date_cursor, ad_id_parts
                    )
                    image_info = self.creative_info(
                        advertiser_id, CREATIVE_TYPE.IMAGE, date_cursor, ad_id_parts
                    )

                    creative_info = {}
                    if not is_empty(video_info):
                        creative_info.update(video_info)
                    if not is_empty(image_info):
                        creative_info.update(image_info)

                    for _, row in creative_info.items():
                        for ad_id in row["related_ad_ids"]:
                            if ad_id in ads_metrics:
                                ads_metrics[ad_id]["creative_info"] = row

                ad_network_creatives = []
                for ad_id, ad_metric in ads_metrics.items():
                    if "creative_info" not in ad_metric:
                        log("===================================================")
                        log(f"ad_id ({ad_id}) have not creative_info")
                        log(ad_metric)
                        log("===================================================")
                        continue

                    creative_info = ad_metric["creative_info"]
                    creative_type = creative_info["creative_type"]

                    match creative_type:
                        case CREATIVE_TYPE.VIDEO:
                            video = creative_info
                            video_id = creative_info["video_id"]
                            # TODO: Fetch ad_cost_creatives from your data model (video_id, network_name)
                            pre_creative = None
                            if (
                                pre_creative is not None
                                and pre_creative["creative_uri_expire_time"] > 0
                            ):
                                creative_uri = pre_creative["creative_uri"]
                                creative_uri_expire_time = pre_creative[
                                    "creative_uri_expire_time"
                                ]
                            else:
                                creative_uri = video["preview_url"]
                                creative_uri_expire_time = (
                                    int(datetime.now().timestamp()) - 1
                                )

                            video_cover_url = video["video_cover_url"]
                            video_name = video["file_name"]

                            bundle_id = ad_info[ad_id]["bundle_id"]
                            platform = ad_info[ad_id]["platform"]

                            campaign_id = ad_metric["campaign_id"]
                            campaign_name = ad_metric["campaign_name"]
                            adgroup_id = ad_metric["adgroup_id"]
                            adgroup_name = ad_metric["adgroup_name"]
                            country = ad_metric["country_code"]

                            impressions = ad_metric["metrics"]["impressions"]
                            clicks = ad_metric["metrics"]["clicks"]
                            installs = ad_metric["metrics"]["conversion"]

                            conversion = ad_metric["metrics"]["conversion"]
                            spend = ad_metric["metrics"]["spend"]

                            thumbnail_uri = "{}/{}/{}/{}/{}".format(
                                self.cdn_bucket_key_prefix,
                                ym,
                                "video",
                                "thumbnail",
                                f"{video_id}.jpeg",
                            )
                            self.load_to_s3_if_not_exists(
                                video_cover_url, thumbnail_uri
                            )

                            if (
                                pre_creative is not None
                                and "start_date" in pre_creative
                            ):
                                start_date = min(
                                    int(pre_creative["start_date"]),
                                    int(date_object.strftime("%Y%m%d")),
                                )
                            else:
                                start_date = date_object.strftime("%Y%m%d")

                            creative_uri = creative_uri.removeprefix(
                                "https://v16m-default.akamaized.net/"
                            )
                            ad_network_creatives.append(
                                {
                                    "log_date": date_object.strftime("%Y%m%d"),
                                    "app_id": bundle_id,
                                    "media_source": self.network_name,
                                    "platform": platform,
                                    "country": country,
                                    "campaign_id": campaign_id,
                                    "campaign_name": campaign_name,
                                    "adset_id": adgroup_id,
                                    "adset_name": adgroup_name,
                                    "ad_id": ad_id,
                                    "creative_id": video_id,
                                    "advertiser_id": advertiser_id,
                                    "creative_name": video_name,
                                    "creative_type": "VIDEO",
                                    "start_date": start_date,
                                    "creative_uri": creative_uri,
                                    "creative_uri_expire_time": creative_uri_expire_time,
                                    "thumbnail_uri": thumbnail_uri,
                                    "impressions": impressions,
                                    "clicks": clicks,
                                    "installs": installs,
                                    "conversions": conversion,
                                    "cost": spend,
                                }
                            )
                        case CREATIVE_TYPE.IMAGE:
                            image = creative_info
                            image_id = image["image_id"]
                            image_url = image[
                                "image_url"
                            ]  # valid for an hour and needs to be re-acquired after expiration
                            image_name = image["file_name"]
                            bundle_id = ad_info[ad_id]["bundle_id"]
                            platform = ad_info[ad_id]["platform"]

                            ad_metric = ads_metrics[ad_id]
                            campaign_id = ad_metric["campaign_id"]
                            campaign_name = ad_metric["campaign_name"]
                            adgroup_id = ad_metric["adgroup_id"]
                            adgroup_name = ad_metric["adgroup_name"]
                            country = ad_metric["country_code"]

                            impressions = ad_metric["metrics"]["impressions"]
                            clicks = ad_metric["metrics"]["clicks"]
                            installs = ad_metric["metrics"]["conversion"]

                            conversion = ad_metric["metrics"]["conversion"]
                            spend = ad_metric["metrics"]["spend"]

                            creative_uri = "{}/{}/{}/{}/{}".format(
                                self.cdn_bucket_key_prefix,
                                ym,
                                "image",
                                "resource",
                                f"{image_id}.jpeg",
                            )
                            self.load_to_s3_if_not_exists(image_url, creative_uri)

                            # TODO: Fetch ad_cost_creatives from your data model (image_id, network_name)
                            pre_creative = None
                            if (
                                pre_creative is not None
                                and "start_date" in pre_creative
                            ):
                                start_date = min(
                                    int(pre_creative["start_date"]),
                                    int(date_object.strftime("%Y%m%d")),
                                )
                            else:
                                start_date = date_object.strftime("%Y%m%d")

                            ad_network_creatives.append(
                                {
                                    "log_date": date_object.strftime("%Y%m%d"),
                                    "app_id": bundle_id,
                                    "media_source": self.network_name,
                                    "platform": platform,
                                    "country": country,
                                    "campaign_id": campaign_id,
                                    "campaign_name": campaign_name,
                                    "adset_id": adgroup_id,
                                    "adset_name": adgroup_name,
                                    "ad_id": ad_id,
                                    "creative_id": image_id,
                                    "advertiser_id": advertiser_id,
                                    "creative_name": image_name,
                                    "creative_type": "IMAGE",
                                    "start_date": start_date,
                                    "creative_uri": creative_uri,
                                    "creative_uri_expire_time": 0,
                                    "thumbnail_uri": creative_uri,
                                    "impressions": impressions,
                                    "clicks": clicks,
                                    "installs": installs,
                                    "conversions": conversion,
                                    "cost": spend,
                                }
                            )

                if not is_empty(ad_network_creatives):
                    df = pd.DataFrame.from_records(ad_network_creatives)
                    # Persist ad_cost_creatives to model

                log(f"insert {ad_date} {len(ad_network_creatives)} rows")

    def creative_info(self, advertiser_id, material_type, date_cursor, ad_ids: list):
        url = "https://business-api.tiktok.com/open_api/v1.3/creative/report/get/"
        params = {
            "advertiser_id": advertiser_id,
            "material_type": material_type,
            "start_date": date_cursor,
            "end_date": date_cursor,
            "info_fields": json.dumps(
                [
                    "country_code",
                    "material_id",
                    "video_id",
                    "image_id",
                    "page_thumbnail",
                    "related_ad_ids",
                ]
            ),
            "filtering": json.dumps({"ad_id": list(ad_ids)}),
            "page_size": 1000,
            "page": 1,
        }

        creatives = {}
        result = self._api_get(url=url, params=params)
        if "list" in result["data"]:
            for row in result["data"]["list"]:
                match material_type:
                    case CREATIVE_TYPE.VIDEO:
                        creative_key = row["info"]["video_id"]
                    case CREATIVE_TYPE.IMAGE:
                        creative_key = row["info"]["image_id"]
                    case _:
                        continue

                creatives[creative_key] = row["info"]
                creatives[creative_key]["creative_type"] = material_type
        else:
            log(
                {
                    "type": "TikTok Creative Issue",
                    "advertiser_id": advertiser_id,
                    "meterial_type": material_type,
                }
            )

        if is_empty(creatives):
            return {}

        creative_keys = list(creatives.keys())
        match material_type:
            case CREATIVE_TYPE.VIDEO:
                # VIDEO - https://ads.tiktok.com/marketing_api/docs?id=1740050161973250
                url = (
                    "https://business-api.tiktok.com/open_api/v1.3/file/video/ad/info/"
                )
                params = {
                    "advertiser_id": advertiser_id,
                    "video_ids": json.dumps(list(creative_keys)),
                }
                response = self._api_get(url=url, params=params)
                if response["code"] == 0:
                    videos = response["data"]["list"]
                    for video in videos:
                        creatives[video["video_id"]].update(video)
            case CREATIVE_TYPE.IMAGE:
                # IMAGE - https://ads.tiktok.com/marketing_api/docs?id=1740051721711618
                url = (
                    "https://business-api.tiktok.com/open_api/v1.3/file/image/ad/info/"
                )
                params = {
                    "advertiser_id": advertiser_id,
                    "image_ids": json.dumps(list(creative_keys)),
                }
                response = self._api_get(url=url, params=params)
                if response["code"] == 0:
                    images = response["data"]["list"]
                    for image in images:
                        creatives[image["image_id"]].update(image)
        return creatives

    def _api_get(self, url: str, params: dict, sleep: int = 10) -> dict:
        for idx in range(0, MAX_API_RETRY):
            try:
                log(f"call api - {url}")
                return HttpClient.get(url, params=params, headers=self.headers)
            except Exception as e:
                log(f"Tiktok Creative API errop - {url} {e} ({idx}/{MAX_API_RETRY})")
                time.sleep(sleep)
        return None
