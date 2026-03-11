#!/usr/bin/env python
import pandas as pd

from libs.ads.publishers.publisher import Publisher
from config.settings import settings
from config.constants import AD_NETWORK_ID
from config.constants import REPORT_TYPE
from config.constants import PUBLISHER_PLATFORM
from config.constants import AD_FORMAT
from libs.http_client import HttpClient


# https://docs.unity3d.com/Packages/com.unity.ads@3.2/manual/MonetizationResourcesStatistics.html#using-the-monetization-stats-api
class Unity(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.UNITY
        self.api_key = settings.unity_pub_api_key
        self.organization_id = settings.unity_pub_organization_id
        self.api_url = "https://monetization.api.unity.com/stats/v1/operate/organizations/{}".format(
            self.organization_id
        )

    def _get_headers(self):
        return {
            "Authorization": "Token {}".format(self.api_key),
            "Accept": "application/json",
        }

    def report_platform(self):
        params = {
            "apiKey": self.api_key,
            "fields": "start_count,revenue_sum",
            "start": self.start,
            "end": self.end,
            "groupBy": "game,platform",
            "scale": "day",
        }

        result = HttpClient.get(
            self.api_url, headers=self._get_headers(), params=params, timeout=10
        )
        if len(result) <= 0:
            return

        df = pd.DataFrame(result)
        df.rename(
            columns={
                "timestamp": "log_date",
                "source_game_id": "app_key",
                "source_name": "app_name",
                "revenue_sum": "revenue",
                "start_count": "impressions",
            },
            inplace=True,
        )
        df["log_date"] = pd.to_datetime(df["log_date"]).dt.strftime("%Y%m%d")
        df = df[df.impressions > 0]
        df["platform"] = df["platform"].apply(PUBLISHER_PLATFORM.standardization)
        df = (
            df.groupby(["log_date", "app_key", "platform", "app_name"])
            .agg({"impressions": "sum", "revenue": "sum"})
            .reset_index()
        )

        for row in df.itertuples(index=False):
            log_date = row.log_date
            app_key = row.app_key
            app_name = row.app_name
            platform = row.platform
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

    def report_country(self):
        params = {
            "apiKey": self.api_key,
            "fields": "start_count,revenue_sum",
            "start": self.start,
            "end": self.end,
            "groupBy": "placement,platform,country,game",
            "scale": "day",
        }

        result = HttpClient.get(
            self.api_url, headers=self._get_headers(), params=params, timeout=10
        )
        if len(result) <= 0:
            return

        df = pd.DataFrame(result)
        df.rename(
            columns={
                "timestamp": "log_date",
                "source_game_id": "app_key",
                "source_name": "app_name",
                "placement": "ad_format",
                "revenue_sum": "revenue",
                "start_count": "impressions",
            },
            inplace=True,
        )
        df = df[(df.ad_format != "") & (~df.ad_format.isna())]
        df["log_date"] = pd.to_datetime(df["log_date"]).dt.strftime("%Y%m%d")
        df["ad_format"] = df["ad_format"].apply(AD_FORMAT.standardization)
        df["platform"] = df["platform"].apply(PUBLISHER_PLATFORM.standardization)

        df = df[df.impressions > 0]
        df = df[df.ad_format != AD_FORMAT.NONE]
        df = (
            df.groupby(
                ["log_date", "app_key", "platform", "country", "ad_format", "app_name"]
            )
            .agg({"impressions": "sum", "revenue": "sum"})
            .reset_index()
        )
        df["sub_format"] = None

        for row in df.itertuples(index=False):
            log_date = row.log_date
            app_key = row.app_key
            app_name = row.app_name
            country = row.country
            platform = row.platform
            revenue = row.revenue
            impressions = row.impressions
            ad_format = row.ad_format
            sub_format = row.sub_format

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
