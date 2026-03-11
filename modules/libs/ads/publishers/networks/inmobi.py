#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.settings import settings
from config.constants import (
    AD_NETWORK_ID,
    PATH,
    REPORT_TYPE,
    PUBLISHER_PLATFORM,
    AD_FORMAT,
)
from libs.http_client import HttpClient
from libs.utils.date import timestamp
from libs.file_manager import FileManager
from libs.utils.cli import log
from libs.utils.common import is_empty

import libs.country as country_helper
import os
import pandas as pd


class Inmobi(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.INMOBI
        self.user_name = settings.inmobi_user_name
        self.secret_key = settings.inmobi_secret_key
        self.session_api_url = "https://api.inmobi.com/v1.0/generatesession/generate"
        self.report_api_url = "https://api.inmobi.com/v3.0/reporting/publisher"

    def _get_session_data(self):
        credential_path = os.path.join(PATH.CREDENTIAL_ROOT, "inmobi_secret.pickle")
        if not FileManager.exists(credential_path):
            session = {"SESSION_ID": None, "ACCOUNT_ID": None, "EXPIRE_TIME": 0}
            FileManager.write_pickle(credential_path, session)
        else:
            session = FileManager.read_pickle(credential_path)

        now = timestamp()

        if int(session["EXPIRE_TIME"]) < now:
            headers = {"userName": self.user_name, "secretKey": self.secret_key}
            result = HttpClient.get(self.session_api_url, headers=headers)
            if result["error"] is True:
                raise Exception("Inmobi API Error")

            session = {
                "SESSION_ID": result["respList"][0]["sessionId"],
                "ACCOUNT_ID": result["respList"][0]["accountId"],
                "EXPIRE_TIME": now + (3600 * 4),
            }
            FileManager.write_pickle(credential_path, session)
            log("refresh inmobi token")

        return session

    def _headers(self):
        session = self._get_session_data()

        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "userName": self.user_name,
            "secretKey": self.secret_key,
            "sessionId": session["SESSION_ID"],
            "accountId": session["ACCOUNT_ID"],
        }

    def _build_report_spec(self, metrics: list, group_by: list):
        return {
            "reportRequest": {
                "metrics": metrics,
                "timeFrame": self.start + ":" + self.end,
                "groupBy": group_by,
            }
        }

    def report_platform(self):
        headers = self._headers()
        params = self._build_report_spec(
            ["adImpressions", "earnings", "clicks"], ["date", "inmobiAppId", "platform"]
        )
        try:
            result = HttpClient.post(
                self.report_api_url, json_body=params, headers=headers
            )
        except Exception as _:
            log("Inmobi report api error")
            return

        if is_empty(result):
            log("Inmobi report api error")
            return

        if result["error"] is True:
            log("Inmobi report api error")
            return

        if "respList" in result:
            df = pd.DataFrame(result["respList"])
            df["log_date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")
            df.loc[df["platform"] == "Fire OS", "platform"] = "Android"

            df["platform"] = df["platform"].apply(PUBLISHER_PLATFORM.standardization)

            df.rename(columns={"inmobiAppId": "app_key"}, inplace=True)
            df.rename(columns={"earnings": "revenue"}, inplace=True)
            df.rename(columns={"adImpressions": "impressions"}, inplace=True)

            df = df[df["impressions"] > 0][
                ["log_date", "app_key", "platform", "revenue", "impressions", "clicks"]
            ]
            df = df.groupby(["log_date", "app_key", "platform"]).sum().reset_index()

            for _, row in df.iterrows():
                self.push(
                    REPORT_TYPE.PLATFORM,
                    self.ad_network_id,
                    row["log_date"],
                    row["app_key"],
                    "",
                    row["platform"],
                    row["impressions"],
                    round(row["revenue"], 6),
                )

            self.flush()

    def _paging_api(self, params):
        headers = self._headers()
        length = 5000
        params["reportRequest"]["length"] = length
        result = []
        for anchor in range(0, 10):
            offset = anchor * length
            params["reportRequest"]["offset"] = offset
            log(params)
            log("Inmobi paging api .. offset => {}".format(offset))
            resp = HttpClient.post(
                self.report_api_url, json_body=params, headers=headers
            )
            if "respList" not in resp:
                log("offset data empty")
                break
            if len(resp["respList"]) <= 0:
                log("offset data empty")
                break
            result += resp["respList"]
        return result

    def report_country(self):
        params = self._build_report_spec(
            ["adImpressions", "earnings", "clicks"],
            ["date", "inmobiAppId", "platform", "placement", "country"],
        )
        params["reportRequest"]["filterBy"] = [
            {"filterName": "adImpressions", "filterValue": 0, "comparator": ">"}
        ]

        try:
            result = self._paging_api(params)
        except Exception as _:
            log("Inmobi country report api error")
            return

        if is_empty(result):
            log("Inmobi report api error")
            return

        if len(result) <= 0:
            log("Inmobi country report api error")
            return

        df = pd.DataFrame(result)
        df["log_date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")
        df.loc[df["platform"] == "Fire OS", "platform"] = "Android"

        df.rename(columns={"inmobiAppId": "app_key"}, inplace=True)
        df.rename(columns={"earnings": "revenue"}, inplace=True)
        df.rename(columns={"adImpressions": "impressions"}, inplace=True)
        df.rename(columns={"placementName": "ad_format"}, inplace=True)

        df = df[df["impressions"] > 0][
            [
                "log_date",
                "app_key",
                "platform",
                "country",
                "ad_format",
                "revenue",
                "impressions",
                "clicks",
            ]
        ]

        df["platform"] = df["platform"].apply(PUBLISHER_PLATFORM.standardization)
        df["country"] = df["country"].apply(country_helper.find_by_name)
        df["sub_format"] = df["ad_format"]
        df["ad_format"] = df["ad_format"].apply(AD_FORMAT.standardization)

        df = df[df["impressions"] > 0][
            [
                "log_date",
                "app_key",
                "platform",
                "country",
                "ad_format",
                "sub_format",
                "revenue",
                "impressions",
                "clicks",
            ]
        ]
        df = (
            df.groupby(
                [
                    "log_date",
                    "app_key",
                    "platform",
                    "country",
                    "ad_format",
                    "sub_format",
                ]
            )
            .sum()
            .reset_index()
        )

        for _, row in df.iterrows():
            if row["country"] is None:
                continue

            self.push(
                REPORT_TYPE.COUNTRY,
                self.ad_network_id,
                row["log_date"],
                row["app_key"],
                "",
                row["platform"],
                row["impressions"],
                round(row["revenue"], 6),
                ad_format=row["ad_format"],
                sub_format=row["sub_format"],
                country=row["country"],
            )

        self.flush()
