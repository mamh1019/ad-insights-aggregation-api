#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.constants import AD_NETWORK_ID
from config.settings import settings
from requests.auth import HTTPBasicAuth
from libs.utils.date import str_to_datetime
from config.constants import AD_FORMAT
from config.constants import PUBLISHER_PLATFORM
from config.constants import REPORT_TYPE
from io import StringIO
from libs.utils.cli import log
from libs.utils import is_empty

import requests
import pandas as pd


class Bidmachine(Publisher):
    """For apps with Digital Turbine mediation, Fyber revenue is inconsistent. Use DT Exchange from mediation only.
    @link https://docs.bidmachine.io/docs/reporting-api
    """

    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.BIDMACHINE
        self.client_id = settings.bidmachine_client_id
        self.client_secret = settings.bidmachine_client_secret

        self.api_report_url = "https://api-eu.bidmachine.io/api/v1/report/ssp"

    def _get_auth(self):
        return HTTPBasicAuth(self.client_id, self.client_secret)

    def report_platform(self):
        cols = "date,app_bundle,platform,impressions,clicks,revenue"
        params = {
            "fields": cols,
            "start": self.start,
            "end": self.end,
            "format": "csv",
        }

        try:
            auth = self._get_auth()

            result = requests.get(
                self.api_report_url, auth=auth, params=params, timeout=10
            )
            if result.status_code != 200:
                log("Bidmachine API Error: ", result.status_code, result.text)
                return

            if is_empty(result.text):
                return
        except Exception as e:
            log("Bidmachine API Error: ", e)
            return

        df = pd.read_csv(StringIO(result.text), header=None)
        df.columns = cols.split(",")

        df["log_date"] = df["date"].apply(
            lambda x: str_to_datetime(x, "%Y-%m-%d").strftime("%Y%m%d")
        )
        df["platform"] = df["platform"].apply(PUBLISHER_PLATFORM.standardization)
        df = (
            df.groupby(
                [
                    "log_date",
                    "app_bundle",
                    "platform",
                ]
            )
            .sum()
            .reset_index()
        )

        for _, row in df.iterrows():
            self.push(
                REPORT_TYPE.PLATFORM,
                self.ad_network_id,
                row["log_date"],
                row["app_bundle"],
                "",
                row["platform"],
                row["impressions"],
                round(row["revenue"], 6),
            )
        self.flush()

    def report_country(self):
        cols = "date,app_bundle,platform,country,ad_type,impressions,clicks,revenue"
        params = {
            "fields": cols,
            "start": self.start,
            "end": self.end,
            "format": "csv",
        }

        try:
            auth = self._get_auth()
            result = requests.get(
                self.api_report_url, auth=auth, params=params, timeout=10
            )
            if result.status_code != 200:
                log("Bidmachine API Error: ", result.status_code, result.text)
                return

            if is_empty(result.text):
                return

        except Exception as e:
            log("Bidmachine API Error: ", e)
            return

        df = pd.read_csv(StringIO(result.text), header=None)
        df.columns = cols.split(",")

        df["log_date"] = df["date"].apply(
            lambda x: str_to_datetime(x, "%Y-%m-%d").strftime("%Y%m%d")
        )
        df["platform"] = df["platform"].apply(PUBLISHER_PLATFORM.standardization)
        df["country"] = df["country"].str.upper()
        df["sub_format"] = ""
        df["ad_format"] = df["ad_type"].apply(AD_FORMAT.standardization)
        df = (
            df.groupby(
                [
                    "log_date",
                    "app_bundle",
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
                row["app_bundle"],
                "",
                row["platform"],
                row["impressions"],
                round(row["revenue"], 6),
                ad_format=row["ad_format"],
                sub_format=row["sub_format"],
                country=row["country"],
            )
        self.flush()
