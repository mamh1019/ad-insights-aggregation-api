#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.settings import settings
from config.constants import AD_NETWORK_ID, PATH, PLATFORM, AD_FORMAT
from config.constants import PUBLISHER_PLATFORM, REPORT_TYPE
from libs.file_manager import FileManager
from libs.utils.date import str_to_timestamp, timestamp_to_datetime, now
from libs.http_client import HttpClient
from libs.utils.array import apply_dict_tree
from libs.utils.cli import log

import os
import time
import pandas as pd
import numpy as np
import json


class Facebook(Publisher):
    def __init__(self) -> None:
        self.ad_network_id = AD_NETWORK_ID.FACEBOOK
        self.graph_api_version = "v24.0"
        self.publisher_id = settings.facebook_business_id
        self.client_id = settings.facebook_client_id
        self.client_secret = settings.facebook_client_secret
        self.oauth_api_url = "https://graph.facebook.com/{}/oauth/access_token".format(
            self.graph_api_version
        )
        self.api_url = "https://graph.facebook.com/{}/{}/adnetworkanalytics".format(
            self.graph_api_version, self.publisher_id
        )
        self.token_path = os.path.join(PATH.CREDENTIAL_ROOT, "facebook_token.pickle")
        self._refresh_access_token()

    def _refresh_access_token(self):
        token_path = self.token_path
        if not FileManager.exists(token_path):
            raise Exception("Not found facebook token file")

        _now = str_to_timestamp(now("%Y-%m-%d %H:%M:%S"))
        token_info = FileManager.read_pickle(token_path)

        if _now > token_info["expire_time"] - (60 * 60 * 24):
            oauth_params = {
                "grant_type": "fb_exchange_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "fb_exchange_token": token_info["access_token"],
            }
            res = HttpClient.get(self.oauth_api_url, params=oauth_params)
            if "access_token" not in res:
                raise Exception("Invalid token refresh request")

            token_info["access_token"] = res["access_token"]
            token_info["expire_time"] = _now + res["expires_in"]
            token_info["expire_datetime"] = timestamp_to_datetime(
                token_info["expire_time"]
            ).strftime("%Y-%m-%d %H:%M:%S")
            FileManager.write_pickle(token_path, token_info)
            log(
                "token refresh completed. expired in {}".format(
                    token_info["expire_datetime"]
                )
            )

        self.access_token = token_info["access_token"]

    def get_cached_app_keys(self, log_date):
        # TODO: Fetch fb_audience_keys from your data model (by log_date)
        # Replace with your actual database/model query
        keys = []
        return keys

    def _get_params(self, start, end):
        ## Subtract one day
        import datetime

        end = pd.to_datetime(end) - datetime.timedelta(days=1)
        start = pd.to_datetime(start) - datetime.timedelta(days=1)
        return start, end

    def report_platform(self):
        start_date, end_date = self._get_params(self.start, self.end)
        date_range = pd.date_range(start=start_date, end=end_date)

        report = {}
        retries = 2

        for date_obj in date_range:
            start = date_obj.strftime("%Y-%m-%d")
            end = date_obj.strftime("%Y-%m-%d")

            params = {
                "access_token": self.access_token,
                "metrics": "['fb_ad_network_revenue','fb_ad_network_imp']",
                "breakdowns": "['platform','app']",
                "since": start,
                "until": end,
            }

            for count in range(retries):
                start_time = time.time()
                res = HttpClient.get(self.api_url, params=params)
                end_time = time.time()
                if "error" not in res:
                    break
                # https://developers.facebook.com/docs/marketing-api/error-reference/#examples-
                if count == retries - 1:
                    pass
                time.sleep(5)

            if "data" not in res:
                continue

            if "results" not in res["data"][0]:
                continue

            if len(res["data"][0]["results"]) <= 0:
                log("ADNetwork Facebook Exception #4 (Data Empty)")
                continue

            results = res["data"][0]["results"]

            for row in results:
                log_date = row["time"].split("T")[0].replace("-", "")
                app_key = None
                platform = None

                for breakdown in row["breakdowns"]:
                    if breakdown["key"] == "platform":
                        platform = breakdown["value"]
                    if breakdown["key"] == "app":
                        app_key = breakdown["value"]

                if platform == "unknown":
                    continue

                apply_dict_tree(report, [log_date, app_key, platform, "revenue"], 0)
                apply_dict_tree(report, [log_date, app_key, platform, "impressions"], 0)

                if row["metric"] == "fb_ad_network_revenue":
                    report[log_date][app_key][platform]["revenue"] += float(
                        row["value"]
                    )

                if row["metric"] == "fb_ad_network_imp":
                    report[log_date][app_key][platform]["impressions"] += float(
                        row["value"]
                    )

        for log_date, date_rows in report.items():
            for app_key, platform_rows in date_rows.items():
                for platform, metric_rows in platform_rows.items():
                    impressions = metric_rows["impressions"]
                    revenue = metric_rows["revenue"]

                    self.push(
                        REPORT_TYPE.PLATFORM,
                        self.ad_network_id,
                        log_date,
                        app_key,
                        "",
                        PUBLISHER_PLATFORM.standardization(platform),
                        impressions,
                        revenue,
                    )

        self.flush()

    def report_country(self):
        start_date, end_date = self._get_params(self.start, self.end)
        date_range = pd.date_range(start=start_date, end=end_date)
        retries = 2

        for date_obj in date_range:
            start = date_obj.strftime("%Y-%m-%d")
            end = start

            report = {}
            audience_keys = self.get_cached_app_keys(end)
            if len(audience_keys) <= 0:
                continue

            key_chunks = np.array_split(audience_keys, 5)

            for key_chunk in key_chunks:
                if len(key_chunk) <= 0:
                    continue

                key_list = "'" + "','".join(key_chunk) + "'"
                params = {
                    "access_token": self.access_token,
                    "metrics": "['fb_ad_network_revenue','fb_ad_network_imp']",
                    "breakdowns": "['platform','app','display_format','country']",
                    "filters": "[{'field':'app', 'operator':'in', 'values':["
                    + key_list
                    + "]}]",
                    "since": start,
                    "until": end,
                }

                for count in range(retries):
                    res = HttpClient.get(self.api_url, params=params)
                    if "error" not in res:
                        break
                    # https://developers.facebook.com/docs/marketing-api/error-reference/#examples-
                    if count == retries - 1:
                        pass
                    time.sleep(5)

                if "data" not in res:
                    continue

                if "results" not in res["data"][0]:
                    continue

                if len(res["data"][0]["results"]) <= 0:
                    log("ADNetwork Facebook Exception #4 (Data Empty)")
                    continue

                results = res["data"][0]["results"]
                for row in results:
                    log_date = row["time"].split("T")[0].replace("-", "")
                    app_key = None
                    platform = None
                    country = None
                    ad_format = None

                    for breakdown in row["breakdowns"]:
                        if breakdown["key"] == "platform":
                            platform = breakdown["value"]
                        if breakdown["key"] == "app":
                            app_key = breakdown["value"]
                        if breakdown["key"] == "country":
                            country = breakdown["value"]
                        if breakdown["key"] == "display_format":
                            ad_format = breakdown["value"]

                    if platform == "unknown":
                        continue

                    apply_dict_tree(
                        report,
                        [log_date, app_key, platform, country, ad_format, "revenue"],
                        0,
                    )
                    apply_dict_tree(
                        report,
                        [
                            log_date,
                            app_key,
                            platform,
                            country,
                            ad_format,
                            "impressions",
                        ],
                        0,
                    )

                    if row["metric"] == "fb_ad_network_revenue":
                        report[log_date][app_key][platform][country][ad_format][
                            "revenue"
                        ] += float(row["value"])

                    if row["metric"] == "fb_ad_network_imp":
                        report[log_date][app_key][platform][country][ad_format][
                            "impressions"
                        ] += float(row["value"])

            for log_date, date_rows in report.items():
                for app_key, platform_rows in date_rows.items():
                    for platform, country_rows in platform_rows.items():
                        for country, format_rows in country_rows.items():
                            for ad_format, metric_rows in format_rows.items():
                                impressions = metric_rows["impressions"]
                                revenue = metric_rows["revenue"]

                                sub_format = ad_format
                                ad_format = AD_FORMAT.standardization(ad_format)

                                if ad_format == AD_FORMAT.NONE:
                                    continue

                                self.push(
                                    REPORT_TYPE.COUNTRY,
                                    self.ad_network_id,
                                    log_date,
                                    app_key,
                                    "",
                                    platform,
                                    impressions,
                                    revenue,
                                    ad_format=ad_format,
                                    sub_format=sub_format,
                                    country=country,
                                )
            self.flush()
