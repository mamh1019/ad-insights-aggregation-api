#!/usr/bin/env python
from config.constants import AD_NETWORK_ID
from config.settings import settings
from libs.ads.publishers.publisher import Publisher
from datetime import timedelta
from libs.utils.common import is_empty
from libs.utils.cli import log
from config.constants import PUBLISHER_PLATFORM, REPORT_TYPE, AD_FORMAT
from libs.http_client import HttpClient
from datetime import datetime

import pandas as pd
import time


# https://apps.premiumads.net/reporting-api
class Premiumads(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.PREMIUMADS
        self.api_token = settings.premiumads_token
        self.base_url = "https://api.premiumads.net/v2/reports"

    def set_interval(self, start: str, end: str):
        self.start = start
        self.end = (pd.to_datetime(end) + timedelta(days=0)).strftime("%Y-%m-%d")

    def get_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.api_token),
        }

    def _check_result(self, _result: dict) -> bool:
        if is_empty(_result):
            log("data is empty")
            return False
        if "data" not in _result:
            log("data is empty")
            return False
        if len(_result["data"]) <= 0:
            return False
        return True

    def report_platform(self):
        params = {
            "from": self.start,
            "to": self.end,
            "dimensions": "date,app",
        }
        df = self._get_data(params)
        df = df[df.impressions > 0]
        if df.empty:
            return

        ## transform
        df["date"] = df["date"].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d").strftime("%Y%m%d")
        )

        for row in df.itertuples(index=False):
            log_date = row.date
            app_key = row.app_id
            app_name = row.app_name
            platform = PUBLISHER_PLATFORM.standardization(row.app_platform)
            revenue = row.revenue
            impressions = row.impressions

            self.push(
                report_type=REPORT_TYPE.PLATFORM,
                log_date=log_date,
                ad_network_id=self.ad_network_id,
                app_key=app_key,
                app_name=app_name,
                platform=platform,
                revenue=revenue,
                impressions=impressions,
            )

        ## load
        self.flush()

    def _get_data(self, params: dict) -> pd.DataFrame | None:
        try:
            max_retry = 5
            result = None
            for _ in range(max_retry):
                time.sleep(5)
                result = HttpClient.get(
                    self.base_url, params=params, headers=self.get_headers()
                )
                log(result)
                if result["status"] == "Success":
                    break

            if result["status"] == "Success" and not is_empty(result["download_url"]):
                df = HttpClient.csv_gz(result["download_url"])
                return df
            else:
                return None
        except Exception as e:
            log(e)
            return

    def report_country(self):
        params = {
            "from": self.start,
            "to": self.end,
            "dimensions": "date,app,country,zone",
        }

        df = self._get_data(params)
        df = df[
            (df.impressions > 0) & (~df["zone_type"].isna()) & (df["country"] != "ZZ")
        ]
        if df.empty:
            return

        df = (
            df.groupby(["date", "app_id", "app_platform", "country", "zone_type"])
            .agg({"revenue": "sum", "impressions": "sum"})
            .reset_index()
        )

        ## transform
        df["date"] = df["date"].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d").strftime("%Y%m%d")
        )
        for row in df.itertuples(index=False):
            log_date = row.date
            app_key = row.app_id
            app_name = ""
            country = row.country
            platform = PUBLISHER_PLATFORM.standardization(row.app_platform)
            revenue = row.revenue
            impressions = row.impressions
            ad_format = AD_FORMAT.standardization(row.zone_type)
            sub_format = None

            self.push(
                report_type=REPORT_TYPE.COUNTRY,
                log_date=log_date,
                ad_network_id=self.ad_network_id,
                app_key=app_key,
                app_name=app_name,
                platform=platform,
                revenue=revenue,
                impressions=impressions,
                country=country,
                ad_format=ad_format,
                sub_format=sub_format,
            )

        ## load
        self.flush()
