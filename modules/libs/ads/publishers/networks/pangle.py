#!/usr/bin/env python
import hashlib
import pandas as pd
from config.settings import settings
from config.constants import AD_NETWORK_ID, AD_FORMAT
from libs.ads.publishers.publisher import Publisher
from libs.utils.date import timestamp
from libs.http_client import HttpClient
from libs.query import Query
from libs.utils.cli import log


class Pangle(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.user_id = settings.pangle_user_id
        self.role_id = settings.pangle_role_id
        self.secure_key = settings.pangle_security_key

        self.ad_network_id = AD_NETWORK_ID.PANGLE
        self.version = "2.0"
        self.sign_type_md5 = "MD5"
        self.KEY_USER_ID = "user_id"
        self.KEY_ROLE_ID = "role_id"
        self.KEY_VERSION = "version"
        self.KEY_SIGN = "sign"
        self.KEY_SIGN_TYPE = "sign_type"
        self.PANGLE_HOST = "https://open-api.pangleglobal.com"

    def sign_gen(self, params):
        """Fetches sign .
        Args:
        params: a dict need to sign
        secure_key: string

        Returns:
        A dict. For example:

        {'url': 'a=1&sign_type=MD5&t=2&z=a&sign=7ff19ec1961d8c2b7c7b3845d974d22e',
        'sign': '7ff19ec1961d8c2b7c7b3845d974d22e'}
        """
        result = {
            "sign": "",
            "url": "",
        }
        if not isinstance(params, dict):
            log("invalid params: ", params)
            return result

        if self.user_id != "":
            params[self.KEY_USER_ID] = self.user_id

        if self.role_id != "":
            params[self.KEY_ROLE_ID] = self.role_id

        params[self.KEY_VERSION] = self.version
        params[self.KEY_SIGN_TYPE] = self.sign_type_md5

        param_orders = sorted(params.items(), key=lambda x: x[0], reverse=False)
        raw_str = ""
        for k, v in param_orders:
            if v == "":
                continue
            raw_str += str(k) + "=" + str(v) + "&"
            if len(raw_str) == 0:
                return ""
            sign_str = raw_str[0:-1] + self.secure_key

        sign = hashlib.md5(sign_str.encode()).hexdigest()
        result[self.KEY_SIGN] = sign
        result["url"] = raw_str + "sign=" + sign
        return result

    def get_signed_url(self, params):
        return self.sign_gen(params).get("url", "")

    def get_media_rt_income(self, params):
        result = self.get_signed_url(params)
        if result == "":
            return ""
        return self.PANGLE_HOST + "/union_pangle/open/api/rt/income?" + result

    def report_platform(self):
        pass

    def report_country(self):
        if self.start < "2024-10-01":
            self.start = "2024-10-01"

        if self.end < self.start:
            self.end = self.start

        log("Pangle v2 API - {} - {}".format(self.start, self.end))

        date_range = pd.date_range(start=self.start, end=self.end)
        for date_obj in date_range:
            log_date = date_obj.strftime("%Y-%m-%d")
            params = {
                "currency": "usd",
                "time_zone": 0,
                "date": log_date,
                "timestamp": str(timestamp()),
            }

            api_url = self.get_media_rt_income(params)
            response = HttpClient.get(api_url, timeout=10)

            if "Code" in response and response["Code"] == "100":
                data = response["Data"]
                new_data = []
                for date, records in data.items():
                    for record in records:
                        record["log_date"] = pd.to_datetime(str(date)).strftime(
                            "%Y%m%d"
                        )
                        new_data.append(record)

                df = pd.DataFrame(new_data)
                df.rename(
                    columns={
                        "app_id": "app_key",
                        "ad_slot_type": "sub_format",
                        "click": "clicks",
                        "show": "impressions",
                        "os": "platform",
                        "region": "country",
                    },
                    inplace=True,
                )
                df = df[df.sub_format.isin([2, 4, 5, 6, 9])]
                df = df[df.clicks > 0]
                if df.empty:
                    continue

                df["country"] = df["country"].str.upper()
                df["ad_network_id"] = self.ad_network_id
                df.loc[df.sub_format == 4, "format"] = AD_FORMAT.INTERSTITIAL
                df.loc[df.sub_format == 9, "format"] = AD_FORMAT.INTERSTITIAL
                df.loc[df.sub_format == 6, "format"] = AD_FORMAT.INTERSTITIAL
                df.loc[df.sub_format == 5, "format"] = AD_FORMAT.REWARDED_VIDEO
                df.loc[df.sub_format == 2, "format"] = AD_FORMAT.BANNER

                df = df[
                    [
                        "log_date",
                        "app_key",
                        "platform",
                        "country",
                        "format",
                        "sub_format",
                        "revenue",
                        "impressions",
                        "clicks",
                        "ad_network_id",
                    ]
                ]

                df = (
                    df.groupby(
                        [
                            "log_date",
                            "app_key",
                            "platform",
                            "country",
                            "format",
                            "sub_format",
                        ]
                    )
                    .sum()
                    .reset_index()
                ).round(2)

                sql = Query.build_insert_stmt(
                    "ad_network_report_country",
                    df.columns,
                    dup_update_columns=["revenue", "impressions", "clicks"],
                )
                # Persist ad_network_report_country to model
                log(f"Pangle Country {len(df)} Inserted")

                df = df[
                    [
                        "log_date",
                        "app_key",
                        "platform",
                        "revenue",
                        "impressions",
                        "clicks",
                        "ad_network_id",
                    ]
                ]

                df = (
                    df.groupby(["log_date", "app_key", "platform", "ad_network_id"])
                    .sum()
                    .reset_index()
                    .round(2)
                )

                sql = Query.build_insert_stmt(
                    "ad_network_report",
                    df.columns,
                    dup_update_columns=["revenue", "impressions", "clicks"],
                )
                # Persist ad_network_report to model
                log(f"Pangle Platform {len(df)} Inserted")
