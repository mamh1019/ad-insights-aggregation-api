#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from config.constants import PLATFORM, URL
from collections import defaultdict
from urllib.parse import urlencode, urlparse, parse_qs
from libs.utils.cli import log
from datetime import datetime
from libs.utils import is_empty

import os
import boto3
import requests
import json
import re

import pandas as pd
import math
import numpy as np
import time

FB_ACCESS_TOKEN = settings.facebook_system_token
FB_BUSINESS_ID = settings.facebook_business_id

# Web landing page store URL mapping for accounts using Meta Pixel
# Format: {account_id: {"ios": "https://apps.apple.com/...", "android": "https://play.google.com/..."}}
# Configure via environment variables or settings if needed
WEBLANDING_STORE_URL = {}


class FacebookBucException(Exception):
    pass


class Meta(Advertiser):
    graph_api_version = "v22.0"
    ad_account_list = []
    adset_list = []
    ad_fb_info_dic = {}
    network_name = "Facebook Ads"
    web_network_name = "metaweb_int"
    account = ""

    def __init__(self):
        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "default"))
        self.s3 = session.resource("s3")
        self.s3_client = boto3.client("s3")
        self.cdn_bucket_id = os.environ.get("CDN_BUCKET_ID", "")
        self.cdn_bucket_key_prefix = "facebook/creatives"
        self.cdn_bucket = self.s3.Bucket(self.cdn_bucket_id)
        self.fb_buc_threshold = 70

    class BatchRequest:
        """Facebook Graph API Batch Request"""

        MAX_REQUEST_NUM = 30
        request_queue = []

        @classmethod
        def add(cls, relative_url, method="GET"):
            cls.request_queue.append({"method": method, "relative_url": relative_url})

        @classmethod
        def send(cls, config=None):
            if config is None:
                config = {}
            result = []
            ## Split buffer into chunks of max 50 and merge results
            if not cls.request_queue:
                return []
            split_num = math.ceil(len(cls.request_queue) / cls.MAX_REQUEST_NUM)
            request_buffers = np.array_split(
                cls.request_queue, (split_num if split_num > 0 else 1)
            )

            # Common
            batch_params = {"access_token": FB_ACCESS_TOKEN, "include_headers": "false"}
            for request_buffer in request_buffers:
                log("request batch request ({})".format(len(request_buffer)))
                # Batch Argument
                batch_params["batch"] = json.dumps(request_buffer.tolist())

                response = requests.post(
                    "https://graph.facebook.com", data=batch_params, timeout=60
                )

                for row in response.json():
                    # Result list merging
                    body = json.loads(row["body"])
                    if "data" in body and body["data"]:
                        result += body["data"]
                        time.sleep(1)
                    elif "data" not in body and "error" not in body:
                        result.append(body)
                    elif "error" in body:
                        log(row["body"])
                        exit()

            # Buffer clear
            cls.request_queue.clear()
            return result

    def get_ad_account(self):
        url = "https://graph.facebook.com/{}/{}/owned_ad_accounts".format(
            self.graph_api_version, FB_BUSINESS_ID
        )
        querystring = {"access_token": FB_ACCESS_TOKEN, "limit": 10000}

        response = requests.get(url, params=querystring, timeout=10)
        if "X-Business-Use-Case-Usage" not in response.headers:
            pass

        tree = lambda: defaultdict(tree)
        result = tree()
        buc_usage = json.loads(response.headers["X-Business-Use-Case-Usage"])
        for account_id in buc_usage:
            account_usage = buc_usage[account_id][0]
            result[account_id]["call_count"] = account_usage["call_count"]
            result[account_id]["total_cputime"] = account_usage["total_cputime"]
            result[account_id]["total_time"] = account_usage["total_time"]
            result[account_id]["estimated_time_to_regain_access"] = account_usage[
                "estimated_time_to_regain_access"
            ]

        json_data = json.loads(response.text)

        for data in json_data["data"]:
            self.ad_account_list.append(data["id"])

        return result

    def load_ad_accounts(self, threshold=70):
        """
        https://developers.facebook.com/docs/graph-api/overview/rate-limiting/
        """
        self.fb_buc_threshold = threshold
        buc = self.get_ad_account()
        if len(buc) == 0:
            return

        for account_id in buc:
            account_usage = buc[account_id]

            call_count = account_usage["call_count"]
            if call_count >= self.fb_buc_threshold:
                raise FacebookBucException("Facebook BUC limit protection")

            total_cputime = account_usage["total_cputime"]
            if total_cputime >= self.fb_buc_threshold:
                raise FacebookBucException("Facebook BUC limit protection")

            total_time = account_usage["total_time"]
            if total_time >= self.fb_buc_threshold:
                raise FacebookBucException("Facebook BUC limit protection")

            estimated_time_to_regain_access = (
                account_usage["estimated_time_to_regain_access"] * 60
            )
            if estimated_time_to_regain_access > 0:
                raise FacebookBucException("Facebook BUC limit exception")
        return buc

    def get_ad_info(self):
        adsets_insight_api_url = "{}/{}/insights?{}"
        for ad_account in self.ad_account_list:
            insights_params = {
                "fields": "adset_id,clicks",
                "time_range": {"since": self.start, "until": self.end},
                "filtering": [
                    {"field": "clicks", "operator": "GREATER_THAN", "value": 0}
                ],  # Filter: clicks > 0
                "level": "adset",
                "limit": 50000,
            }
            api_url = adsets_insight_api_url.format(
                self.graph_api_version, ad_account, urlencode(insights_params)
            )
            self.BatchRequest.add(api_url)

        adset_data = self.BatchRequest.send()
        valid_adset_ids = [
            adset["adset_id"] for adset in adset_data if int(adset["clicks"]) > 0
        ]

        adsets_api_url = "{}/{}/adsets?{}"
        for ad_account in self.ad_account_list:
            params = {
                "fields": "name,optimization_goal,promoted_object,id,campaign{id,status},account_id",
                "time_range": {"since": self.start, "until": self.end},
                "filtering": [
                    {"field": "id", "operator": "IN", "value": valid_adset_ids}
                ],
                "limit": 50000,
            }

            api_url = adsets_api_url.format(
                self.graph_api_version, ad_account, urlencode(params)
            )
            self.BatchRequest.add(api_url)

        adsets_list = self.BatchRequest.send()
        self.adset_list = adsets_list
        ad_fb_info = []
        campaign_cache = []

        for adsets in adsets_list:
            # campaign cache
            status = "ACTIVE" if adsets["campaign"]["status"] == "ACTIVE" else "PAUSED"
            cache_tuple = (adsets["campaign"]["id"], status, "Facebook Ads")
            if cache_tuple not in campaign_cache:
                campaign_cache.append(
                    (adsets["campaign"]["id"], status, "Facebook Ads")
                )

            if "promoted_object" not in adsets:
                continue

            is_web_landing = False
            promoted_object = adsets["promoted_object"]
            if "object_store_url" not in promoted_object:
                # web landing (meta pixel)
                if (
                    adsets["account_id"] in WEBLANDING_STORE_URL.keys()
                    and "pixel_id" in promoted_object
                ):
                    if "ios" in adsets["name"].lower():
                        adsets["promoted_object"]["object_store_url"] = (
                            WEBLANDING_STORE_URL[adsets["account_id"]]["ios"]
                        )
                    else:
                        adsets["promoted_object"]["object_store_url"] = (
                            WEBLANDING_STORE_URL[adsets["account_id"]]["android"]
                        )
                    is_web_landing = True
                else:
                    continue
            object_store_url = promoted_object["object_store_url"]

            platform = None
            if "google.com" in object_store_url:
                platform = PLATFORM.ANDROID
            elif "apple.com" in object_store_url:
                platform = PLATFORM.IOS
            elif "amazon" in object_store_url:
                platform = PLATFORM.AMAZON
            else:
                continue

            app_id = ""

            if platform == PLATFORM.ANDROID:
                temp = urlparse(object_store_url)
                uri = parse_qs(temp.query)
                app_id = uri["id"][0]
            elif platform == PLATFORM.IOS:
                temp = object_store_url.split("/")
                app_id = temp[-1].replace("id", "")
            elif platform == PLATFORM.AMAZON:
                temp = object_store_url.split("/")
                app_id = temp[-1]

            conversion_type = "INSTALL"
            if "custom_event_type" in promoted_object:
                if promoted_object["custom_event_type"] == "PURCHASE":
                    conversion_type = "PURCHASE"

            self.ad_fb_info_dic[adsets["id"]] = {
                "adset_id": adsets["id"],
                "app_id": app_id,
                "adset_name": adsets["name"],
                "object_store_url": adsets["promoted_object"]["object_store_url"],
                "platform": platform,
                "conversions_type": conversion_type,
                "is_web_landing": is_web_landing,
            }

            ad_fb_info.append(
                [
                    str(adsets["id"]),
                    str(app_id),
                    str(adsets["name"]),
                    str(platform),
                    self.account,
                ]
            )

        if len(ad_fb_info) > 0:
            # Persist fb_ad_info to model
            pass

    def get_ad_cost(self):  # get ad_cost report)
        ad_accounts_map = []
        for adset_id, _ in self.ad_fb_info_dic.items():
            params = {
                "level": "adset",
                "fields": "campaign_id,campaign_name,adset_id,adset_name,spend,impressions,actions,account_id",
                "breakdowns": "country",
                "time_range": {"since": self.start, "until": self.end},
                "default_summary": "true",
                "time_increment": 1,
                "filtering": [
                    {
                        "field": "adset.impressions",
                        "operator": "GREATER_THAN",
                        "value": 0,
                    }
                ],
                "offset": 1,
                "limit": 50000,
            }
            insight_api_url = "{api_version}/{adset_id}/insights?{params}".format(
                api_version=self.graph_api_version,
                adset_id=adset_id,
                params=urlencode(params),
            )
            self.BatchRequest.add(insight_api_url)

        campaigns = self.BatchRequest.send()

        for campaign in campaigns:
            # May not exist
            if "actions" not in campaign:
                campaign["actions"] = []
            # Map list of actions to key-value dict
            actions = dict(
                list(
                    map(
                        lambda action: (action["action_type"], action["value"]),
                        filter(
                            lambda action: "action_type" in action, campaign["actions"]
                        ),
                    )
                )
            )
            # Must fill with 0 when key is missing, not skip
            actions["mobile_app_install"] = (
                actions["mobile_app_install"] if "mobile_app_install" in actions else 0
            )
            actions["link_click"] = (
                actions["link_click"] if "link_click" in actions else 0
            )
            actions["app_custom_event.fb_mobile_purchase"] = (
                actions["app_custom_event.fb_mobile_purchase"]
                if "app_custom_event.fb_mobile_purchase" in actions
                else 0
            )

            campaign["date_start"] = campaign["date_start"].replace("-", "")

            if campaign["adset_id"] not in self.ad_fb_info_dic:
                continue

            if campaign["country"] == "unknown":
                continue

            if "spend" not in campaign:
                continue

            if "impressions" not in campaign:
                continue

            if "link_click" not in actions:
                continue

            if (
                float(campaign["spend"]) <= 0
                and int(campaign["impressions"]) <= 0
                and int(actions["link_click"]) <= 0
            ):
                continue

            app_id = self.ad_fb_info_dic[campaign["adset_id"]]["app_id"]
            platform = self.suppose_platform(app_id, self.network_name)
            network_name = (
                self.network_name
                if not self.ad_fb_info_dic[campaign["adset_id"]]["is_web_landing"]
                else self.web_network_name
            )

            conversions_value = 0
            if (
                self.ad_fb_info_dic[campaign["adset_id"]]["conversions_type"]
                == "PURCHASE"
            ):
                conversions_value = (
                    actions["app_custom_event.fb_mobile_purchase"]
                    if "app_custom_event.fb_mobile_purchase" in actions
                    else 0
                )
            else:
                conversions_value = (
                    actions["mobile_app_install"]
                    if "mobile_app_install" in actions
                    else 0
                )

            self.push(
                log_date=campaign["date_start"],
                app_id=self.ad_fb_info_dic[campaign["adset_id"]]["app_id"],
                platform=platform,
                country=campaign["country"],
                media_source=network_name,
                campaign_id=campaign["campaign_id"],
                campaign=campaign["campaign_name"],
                adset_id=campaign["adset_id"],
                adset=campaign["adset_name"],
                cost=campaign["spend"],
                impressions=campaign["impressions"],
                clicks=actions["link_click"],
                conversions_type=self.ad_fb_info_dic[campaign["adset_id"]][
                    "conversions_type"
                ],
                conversion_value=conversions_value,
            )

            ad_accounts_map.append(
                {
                    "campaign_id": campaign["campaign_id"],
                    "account_id": campaign["account_id"],
                }
            )

        # TODO: self.flush()  # persist cost/conversion buffer to DB

    ##############################################################################
    ## Creative
    ##############################################################################
    def get_ad_creatives(self, date_object):
        # TODO: Fetch ad_cost_report from your data model (log_date, media_source, adset_id)
        ad_cost_report_df = pd.DataFrame(
            columns=["log_date", "app_id", "campaign_id", "adset_id", "platform"]
        )

        tree = lambda: defaultdict(tree)
        ym = date_object.strftime("%Y%m")

        campaign_tree = tree()
        platform_tree = tree()
        creative_tree = tree()

        df_adset = ad_cost_report_df.query(
            "log_date == {}".format(date_object.strftime("%Y%m%d"))
        )
        df_adset = df_adset[
            ["app_id", "campaign_id", "adset_id", "platform"]
        ]  # (Important) platform is at adset level
        df_adset = df_adset.drop_duplicates()

        log(f"{len(df_adset)} adsets founded")

        # WW campaigns may fail with "reduce the amount of data" when run in batch
        insight_ads = []
        for _, adset in df_adset.iterrows():
            campaign_id = adset["campaign_id"]
            adset_id = adset["adset_id"]
            platform = adset["platform"]

            platform_tree.setdefault(campaign_id, {})[adset_id] = platform

            # insight api - https://developers.facebook.com/docs/marketing-api/insights
            params = {
                "level": "ad",  # Represents the level of result
                "fields": ",".join(
                    [
                        "campaign_id",
                        "campaign_name",
                        "adset_id",
                        "adset_name",
                        "ad_id",
                        "ad_name",
                        "spend",
                        "impressions",
                        "clicks",
                        "actions",
                        # "conversions",
                    ]
                ),
                "breakdowns": "country",  # Platform is fixed at adset level, only country breakdown. Platform breakdown not supported.
                "filtering": [
                    {"field": "ad.impressions", "operator": "GREATER_THAN", "value": 0}
                ],
                "time_range": {
                    "since": date_object.strftime("%Y-%m-%d"),
                    "until": date_object.strftime("%Y-%m-%d"),
                },
                "time_increment": 1,
                "offset": 1,
                "limit": 500,
                "access_token": FB_ACCESS_TOKEN,
            }

            insight_api_url = "{api_version}/{adset_id}/insights?{params}".format(
                api_version=self.graph_api_version,
                adset_id=adset_id,
                params=urlencode(params),
            )
            api_call_limit = 5
            api_call_count = 0
            next_url = "https://graph.facebook.com/" + insight_api_url
            while next_url:
                result = requests.get(next_url, timeout=60).json()
                if "data" in result:
                    insight_ads.extend(result["data"])
                else:
                    exit(0)

                next_url = result.get("paging", {}).get("next")
                api_call_count += 1
                if api_call_count > api_call_limit:
                    exit(0)

        if len(insight_ads) <= 0:
            return

        # ad api - https://developers.facebook.com/docs/marketing-api/reference/adgroup/#----
        ad_id_list = []
        for insight in insight_ads:
            campaign_id = insight["campaign_id"]
            campaign_name = insight["campaign_name"]
            adset_id = insight["adset_id"]
            adset_name = insight["adset_name"]
            ad_id = insight["ad_id"]
            ad_name = insight["ad_name"]
            spend = insight["spend"]
            country = insight["country"]
            if country in ["unkn", "unknown"]:
                continue
            impressions = insight["impressions"]
            clicks = insight["clicks"]
            actions = insight["actions"] if "actions" in insight else None

            ad_id_list.append(ad_id)

            # Same ad may run in multiple countries
            campaign_tree[campaign_id][adset_id][ad_id][country] = {
                "campaign_name": campaign_name,
                "adset_name": adset_name,
                "ad_name": ad_name,
                "country": country,
                "impressions": impressions,
                "clicks": clicks,
                "spend": spend,
                "actions": actions,
            }

        ad_id_list = list(set(ad_id_list))
        for ad_id in ad_id_list:
            params = {
                "fields": ",".join(
                    [
                        "id",
                        "adset",
                        "campaign",
                        "creative",
                        # "status",
                    ]
                ),
                "access_token": FB_ACCESS_TOKEN,
            }

            ad_api_url = "{api_version}/{ad_id}?{params}".format(
                api_version=self.graph_api_version,
                ad_id=ad_id,
                params=urlencode(params),
            )
            self.BatchRequest.add(ad_api_url)

        log(f"request ad api queue size - {len(self.BatchRequest.request_queue)}")
        ad_creatives = self.BatchRequest.send()
        if len(ad_creatives) <= 0:
            return

        # creative api - https://developers.facebook.com/docs/marketing-api/reference/ad-creative#-
        for ad_creative in ad_creatives:
            campaign_id = ad_creative["campaign"]["id"]
            adset_id = ad_creative["adset"]["id"]
            ad_id = ad_creative["id"]
            creative_id = ad_creative["creative"]["id"]

            creative_tree[creative_id] = {
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "ad_id": ad_id,
                "countries": campaign_tree[campaign_id][adset_id][ad_id].keys(),
            }

            params = {
                "thumbnail_height": 1200,
                "thumbnail_width": 1200,
                "fields": ",".join(
                    [
                        "id",
                        "account_id",
                        "object_type",
                        "thumbnail_id",
                        "thumbnail_url",
                        "video_id",
                        "image_hash",
                        "image_url",
                        # "name",
                    ]
                ),
                "access_token": FB_ACCESS_TOKEN,
            }
            creative_api_url = "{api_version}/{creative_id}?{params}".format(
                api_version=self.graph_api_version,
                creative_id=creative_id,
                params=urlencode(params),
            )
            self.BatchRequest.add(creative_api_url)

        log(
            f"request creatives api queue size - {len(self.BatchRequest.request_queue)}"
        )
        creatives = self.BatchRequest.send()
        if len(creatives) <= 0:
            return

        # flush to db
        ad_network_creatives = []
        for creative in creatives:
            if "object_type" not in creative:
                continue

            creative_id = creative["id"]
            campaign_id = creative_tree[creative_id]["campaign_id"]
            adset_id = creative_tree[creative_id]["adset_id"]
            ad_id = creative_tree[creative_id]["ad_id"]
            countries = creative_tree[creative_id]["countries"]
            account_id = creative["account_id"]

            for country in countries:
                if country == "unknown":
                    continue

                ad = campaign_tree[campaign_id][adset_id][ad_id][country]
                campaign_name = ad["campaign_name"]
                adset_name = ad["adset_name"]
                ad_name = ad["ad_name"]
                creative_uri_expire_time = 0

                if is_empty(creative["thumbnail_url"]):
                    creative["thumbnail_url"] = URL.NO_IMAGE

                object_type = creative["object_type"]
                if object_type == "VIDEO":
                    thumbnail_id = creative["thumbnail_id"]
                    thumbnail_url = creative["thumbnail_url"]
                    creative_uri = creative["video_id"]

                    thumbnail_uri = "{}/{}/{}/{}/{}".format(
                        self.cdn_bucket_key_prefix,
                        ym,
                        "video",
                        "thumbnail",
                        f"{thumbnail_id}.jpeg",
                    )
                    self.load_to_s3_if_not_exists(thumbnail_url, thumbnail_uri)
                    creative_uri_expire_time = 0

                elif object_type == "SHARE":
                    creative_uri_expire_time = 0
                    thumbnail_url = creative["thumbnail_url"]
                    creative_uri = ""

                    is_share_ad_image_condition = "image_hash" in creative
                    is_share_ad_image_condition |= "_I_" in ad_name

                    is_share_ad_video_condition = is_share_ad_image_condition == False
                    is_share_ad_video_condition &= "_V_" in ad_name

                    if is_share_ad_image_condition:
                        object_type = "IMAGE"
                        origin_url = (
                            creative["image_url"]
                            if "image_url" in creative
                            else thumbnail_url
                        )
                        thumbnail_uri = "{}/{}/{}/{}/{}".format(
                            self.cdn_bucket_key_prefix,
                            ym,
                            "image",
                            "thumbnail",
                            f"{creative_id}.jpeg",
                        )
                        self.load_to_s3_if_not_exists(thumbnail_url, thumbnail_uri)

                        creative_uri = "{}/{}/{}/{}/{}".format(
                            self.cdn_bucket_key_prefix,
                            ym,
                            "image",
                            "resource",
                            f"{creative_id}.jpeg",
                        )
                        self.load_to_s3_if_not_exists(origin_url, creative_uri)
                    elif is_share_ad_video_condition:
                        object_type = "VIDEO"
                        thumbnail_url = creative["thumbnail_url"]

                        # TODO: Fetch ad_cost_creatives from your data model (creative_id, network_name, country)
                        pre_creative = None
                        if (
                            pre_creative is not None
                            and pre_creative.get("creative_uri_expire_time", 0) > 0
                        ):
                            creative_uri = pre_creative["creative_uri"]
                            creative_uri_expire_time = pre_creative[
                                "creative_uri_expire_time"
                            ]
                        else:
                            # Lazy load: don't call graph API upfront, set expire time for creative data fetch
                            creative_uri_expire_time = (
                                int(datetime.now().timestamp()) - 1
                            )

                        thumbnail_uri = "{}/{}/{}/{}/{}".format(
                            self.cdn_bucket_key_prefix,
                            ym,
                            "video",
                            "thumbnail",
                            f"{creative_id}.jpeg",
                        )
                        self.load_to_s3_if_not_exists(thumbnail_url, thumbnail_uri)

                if object_type not in ["IMAGE", "VIDEO"]:
                    continue

                if (
                    campaign_id in platform_tree
                    and adset_id in platform_tree[campaign_id]
                ):
                    platform = platform_tree[campaign_id][adset_id]
                elif "AOS" in campaign_name or "AOS" in adset_name:
                    platform = PLATFORM.ANDROID
                elif "iOS" in campaign_name or "iOS" in adset_name:
                    platform = PLATFORM.IOS
                else:
                    log(
                        {
                            "type": "Facebook Creative",
                            "contents": "not exist in platform_tree",
                            "campaign_id": campaign_id,
                            "campaign_name": campaign_name,
                            "adset_name": adset_name,
                            "ad_name": ad_name,
                        }
                    )
                    continue

                actions = ad["actions"]
                installs = 0
                if actions is not None and len(actions) > 0:
                    actions = dict(
                        list(
                            map(
                                lambda action: (action["action_type"], action["value"]),
                                filter(lambda action: "action_type" in action, actions),
                            )
                        )
                    )
                    installs = (
                        str(actions["mobile_app_install"])
                        if actions is not None and "mobile_app_install" in actions
                        else "0"
                    )

                # TODO: Fetch fb_ad_info from your data model (adset_id)
                ad_fb_info = {"app_id": ""}
                app_id = ad_fb_info["app_id"]

                if is_empty(app_id):
                    continue

                # TODO: Fetch ad_cost_creatives from your data model (creative_id, network_name, country)
                pre_creative = None
                if pre_creative is not None and "start_date" in pre_creative:
                    start_date = min(
                        int(pre_creative["start_date"]),
                        int(date_object.strftime("%Y%m%d")),
                    )
                else:
                    start_date = date_object.strftime("%Y%m%d")

                #
                ad_network_creatives.append(
                    {
                        "log_date": date_object.strftime("%Y%m%d"),
                        "app_id": app_id,
                        "media_source": self.network_name,
                        "platform": platform,
                        "country": country,
                        "campaign_id": campaign_id,
                        "campaign_name": campaign_name,
                        "adset_id": adset_id,
                        "adset_name": adset_name,
                        "ad_id": ad_id,
                        "creative_id": creative_id,
                        "creative_name": ad_name,
                        "creative_type": object_type,
                        "start_date": start_date,
                        "creative_uri": creative_uri,
                        "creative_uri_expire_time": creative_uri_expire_time,
                        "thumbnail_uri": thumbnail_uri,
                        "impressions": ad["impressions"],
                        "clicks": ad["clicks"],
                        "installs": installs,
                        "conversions": 0,
                        "cost": ad["spend"],
                    }
                )

        if len(ad_network_creatives) > 0:
            df = pd.DataFrame.from_records(ad_network_creatives)
            # Persist ad_cost_creatives to model
            pass

        log(f"insert {len(ad_network_creatives)} rows")

    ##############################################################################
    ## Runner
    ##############################################################################
    def report_cost(self):
        self.load_ad_accounts()
        self.get_ad_info()
        self.get_ad_cost()

    def report_creative(self):
        date_list = pd.date_range(start=self.start, end=self.end)
        date_list = date_list[::-1]

        self.load_ad_accounts(threshold=30)
        for date_obj in date_list:
            self.get_ad_creatives(date_object=date_obj)
