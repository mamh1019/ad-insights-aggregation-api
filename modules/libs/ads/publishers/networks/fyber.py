#!/usr/bin/env python
import time
import pandas as pd
from config.constants import AD_NETWORK_ID, AD_FORMAT, PUBLISHER_PLATFORM, REPORT_TYPE
from config.settings import settings
from libs.ads.publishers.publisher import Publisher
from libs.http_client import HttpClient
from libs.utils import date
from libs.utils.cli import log


class Fyber(Publisher):
    """For apps with Digital Turbine mediation, Fyber revenue is inconsistent. Use DT Exchange from mediation only.
    @link https://console.fyber.com/inventory/publisher?startDate=2023-10-30&endDate=2023-10-30
    @link https://developer.digitalturbine.com/hc/en-us/articles/360010079438-FairBid-Reporting-API
    """

    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.FYBER
        self.client_id = settings.fyber_oauth_client_id
        self.client_secret = settings.fyber_oauth_client_secret

        self.api_token_url = "https://reporting.fyber.com/auth/v1/token"
        self.api_report_url = "https://reporting.fyber.com/api/v1/report?format=csv"
        self.api_headers = {"Content-Type": "application/json"}

    def _get_access_token(self):
        params = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        result = HttpClient.post(
            self.api_token_url,
            headers=self.api_headers,
            json_body=params,
            timeout=60,
        )

        if "accessToken" not in result:
            return None

        return result["accessToken"]

    def report_platform(self):
        pass

    def report_country(self):
        # This mediation data arrives at 06:00 today for yesterday. Adjust date to avoid errors.
        tmp_e = int(self.end.replace("-", ""))
        yesterday = int(date.yesterday())
        now = pd.to_datetime(date.now())
        hour = date.hour(now)

        if tmp_e >= yesterday:
            if hour <= 7:
                self.end = date.sub_days(now, 2, "%Y-%m-%d")
            else:
                self.end = date.sub_days(now, 1, "%Y-%m-%d")

        if self.start > self.end:
            self.start = self.end

        access_token = self._get_access_token()
        if access_token is None:
            log("not exists accestoken")
            return

        params = {
            "source": "mediation",
            "dateRange": {
                "start": self.start,
                "end": self.end,
            },
            "metrics": ["Clicks", "Revenue (USD)", "Impressions"],
            "splits": [
                "Date",
                "Fyber App ID",
                "Country",
                "Demand Source Type Name",
                "Device OS",
                "Placement Type",
            ],
            "filters": [
                {"dimension": "Demand Source Type Name", "values": ["Programmatic"]},
            ],
        }
        log(params)
        report_headers = self.api_headers
        report_headers.update({"Authorization": "Bearer {}".format(access_token)})

        data = HttpClient.post(
            self.api_report_url,
            headers=report_headers,
            json_body=params,
            timeout=60,
        )

        if "url" in data:
            log(f"report url {data['url']}")
            df = pd.DataFrame()
            for _ in range(0, 5):
                time.sleep(7)
                log(f"fyber(digital turbine) calling..{_}")
                df = HttpClient.csv(data["url"], timeout=10)

            if df.empty:
                log("data empty")
                return

            df.rename(columns={"Date": "log_date"}, inplace=True)
            df.rename(columns={"Fyber App ID": "app_id"}, inplace=True)
            df.rename(columns={"Device OS": "platform"}, inplace=True)
            df.rename(columns={"Country": "country"}, inplace=True)
            df.rename(columns={"Placement Type": "format"}, inplace=True)
            df.rename(columns={"Clicks": "clicks"}, inplace=True)
            df.rename(columns={"Impressions": "impressions"}, inplace=True)
            df.rename(columns={"Revenue (USD)": "revenue"}, inplace=True)

            # May receive invalid values
            if "clicks" not in df:
                log("invalid data")
                log(df)
                return

            df["clicks"] = df["clicks"].astype(float)
            df["impressions"] = df["impressions"].astype(float)
            df["revenue"] = df["revenue"].astype(float)
            df["log_date"] = df["log_date"].str.replace("-", "")

            platform_df = (
                df.groupby(["log_date", "app_id", "platform"])
                .agg({"impressions": "sum", "clicks": "sum", "revenue": "sum"})
                .reset_index()
            ).round(4)

            for _, row in platform_df.iterrows():
                self.push(
                    report_type=REPORT_TYPE.PLATFORM,
                    log_date=row["log_date"],
                    ad_network_id=self.ad_network_id,
                    app_key=row["app_id"],
                    app_name="",
                    platform=PUBLISHER_PLATFORM.standardization(row["platform"]),
                    revenue=row["revenue"],
                    impressions=row["impressions"],
                )

            country_df = (
                df.groupby(["log_date", "app_id", "platform", "country", "format"])
                .agg({"impressions": "sum", "clicks": "sum", "revenue": "sum"})
                .reset_index()
            ).round(4)

            for _, row in country_df.iterrows():
                self.push(
                    report_type=REPORT_TYPE.COUNTRY,
                    log_date=row["log_date"],
                    ad_network_id=self.ad_network_id,
                    app_key=row["app_id"],
                    app_name="",
                    platform=PUBLISHER_PLATFORM.standardization(row["platform"]),
                    revenue=row["revenue"],
                    impressions=row["impressions"],
                    country=row["country"],
                    ad_format=AD_FORMAT.standardization(row["format"]),
                    sub_format="",
                )

            self.flush()

        else:
            log("Digital Turbine API error")
