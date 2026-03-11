#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.constants import PUBLISHER_PLATFORM, REPORT_TYPE
from config.constants import AD_NETWORK_ID, AD_FORMAT
from config.settings import settings
from libs.utils.date import str_to_datetime

import time
import hashlib
import pandas as pd
import requests


class Bigo(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.BIGO
        self.api_token = settings.bigo_token
        self.api_id = settings.bigo_id
        self.api_url_base = "https://www.bigossp.com/open/report/media/v1.0"

    def get_headers(self, developer_id: str, token: str) -> str:
        ts_ms = int(time.time() * 1000)
        raw = f"{developer_id}-{ts_ms}-{token}".encode("utf-8")
        sha = hashlib.sha1(raw).hexdigest()
        sign = f"{sha}.{ts_ms}"

        headers = {
            "content-type": "application/json",
            "X-BIGO-DeveloperId": self.api_id,
            "X-BIGO-Sign": sign,
        }
        return headers

    def _get_ad_format(self, ad_type: str) -> AD_FORMAT:
        if ad_type == "1":
            return AD_FORMAT.NATIVE
        elif ad_type == "2":
            return AD_FORMAT.BANNER
        elif ad_type == "3":
            return AD_FORMAT.INTERSTITIAL
        elif ad_type == "4":
            return AD_FORMAT.REWARDED_VIDEO
        else:
            return AD_FORMAT.NONE

    def report_platform(self):
        headers = self.get_headers(self.api_id, self.api_token)
        payload = {
            "pageNo": 1,
            "pageSize": 10000,
            "startDate": self.start,
            "endDate": self.end,
            "indicators": ["adImprCnt", "eincome"],
            "aggregateType": 2,
            "breakDowns": ["pkgName", "appOs"],
        }

        response = requests.post(
            self.api_url_base, headers=headers, json=payload, timeout=120
        )
        if response.status_code != 200:
            raise Exception(f"Bigo API Error: {response.text}")

        data = response.json()
        result = data["result"]
        if int(result["total"]) > 10000:
            raise Exception("Bigo API Error: total row is over 10000")

        df = pd.DataFrame(result["list"])
        if df.empty:
            return

        df["platform"] = df["appOs"].apply(PUBLISHER_PLATFORM.standardization)
        df["impressions"] = df["sumAdImprCnt"].astype(int)
        df["revenue"] = df["sumEincome"].astype(float)
        df = df[df.impressions > 0]
        if df.empty:
            return

        df["log_date"] = df["aggregateTime"].apply(
            lambda x: str_to_datetime(x, "%Y-%m-%d").strftime("%Y%m%d")
        )
        df = df.groupby(["log_date", "pkgName", "platform"]).sum().reset_index()

        for row in df.itertuples(index=False):
            self.push(
                REPORT_TYPE.PLATFORM,
                self.ad_network_id,
                row.log_date,
                row.pkgName,
                "",
                row.platform,
                row.impressions,
                round(row.revenue, 6),
            )

        self.flush()

    def report_country(self):
        headers = self.get_headers(self.api_id, self.api_token)
        payload = {
            "pageNo": 1,
            "pageSize": 10000,
            "startDate": self.start,
            "endDate": self.end,
            "indicators": ["adImprCnt", "eincome"],
            "aggregateType": 2,
            "breakDowns": ["country", "pkgName", "appOs", "adType"],
        }

        response = requests.post(
            self.api_url_base, headers=headers, json=payload, timeout=120
        )
        if response.status_code != 200:
            raise Exception(f"Bigo API Error: {response.text}")

        data = response.json()
        result = data["result"]
        if int(result["total"]) > 10000:
            raise Exception("Bigo API Error: total row is over 10000")

        df = pd.DataFrame(result["list"])
        if df.empty:
            return

        df["platform"] = df["appOs"].apply(PUBLISHER_PLATFORM.standardization)
        df["ad_format"] = df["adType"].apply(self._get_ad_format)
        df["sub_format"] = ""

        df["impressions"] = df["sumAdImprCnt"].astype(int)
        df["revenue"] = df["sumEincome"].astype(float)
        df = df[df.impressions > 0]
        if df.empty:
            return

        df["log_date"] = df["aggregateTime"].apply(
            lambda x: str_to_datetime(x, "%Y-%m-%d").strftime("%Y%m%d")
        )
        df = (
            df.groupby(
                [
                    "log_date",
                    "pkgName",
                    "platform",
                    "country",
                    "ad_format",
                    "sub_format",
                ]
            )
            .sum()
            .reset_index()
        )

        for row in df.itertuples(index=False):
            if row.country is None:
                continue

            if row.ad_format == AD_FORMAT.NONE:
                continue

            self.push(
                REPORT_TYPE.COUNTRY,
                self.ad_network_id,
                row.log_date,
                row.pkgName,
                "",
                row.platform,
                row.impressions,
                round(row.revenue, 6),
                ad_format=row.ad_format,
                sub_format=row.sub_format,
                country=row.country,
            )

        self.flush()
