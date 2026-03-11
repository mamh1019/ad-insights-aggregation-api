#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.constants import AD_NETWORK_ID
from config.settings import settings
from config.constants import AD_FORMAT, PUBLISHER_PLATFORM, REPORT_TYPE
from libs.http_client import HttpClient


class Applovin(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.APPLOVIN
        self.api_key = settings.applovin_api_key
        self.base_url = "https://r.applovin.com/report"

    def report_platform(self):
        ## extract
        columns = [
            "day",
            "impressions",
            "revenue",
            # "country",
            # "ad_type",
            # "size",
            "application",
            "package_name",
            # "placement",
            "platform",
        ]

        params = {
            "api_key": self.api_key,
            "start": self.start,
            "end": self.end,
            "columns": ",".join(columns),
            "report_type": "publisher",
            "format": "csv",
        }

        df = HttpClient.csv(self.base_url, params=params)
        if df.empty:
            return

        ## transform
        df["Revenue"] = df["Revenue"].apply(lambda x: float(x.replace("$", "")))
        df = df.groupby(
            ["Day", "Package Name", "Application", "Platform"], as_index=False
        ).agg({"Impressions": "sum", "Revenue": "sum"})
        df.rename(
            columns={
                "Day": "log_date",
                "Package Name": "app_key",
                "Platform": "platform",
                "Revenue": "revenue",
                "Impressions": "impressions",
                "Application": "app_name",
            },
            inplace=True,
        )

        for row in df.itertuples(index=False):
            log_date = row.log_date
            log_date = log_date.replace("-", "")
            app_key = row.app_key
            app_name = row.app_name
            platform = PUBLISHER_PLATFORM.standardization(row.platform)
            revenue = row.revenue
            impressions = row.impressions

            self.push(
                REPORT_TYPE.PLATFORM,
                ad_network_id=self.ad_network_id,
                log_date=log_date,
                app_key=app_key,
                app_name=app_name,
                platform=platform,
                revenue=revenue,
                impressions=impressions,
            )

        ## load
        self.flush()

    def report_country(self):

        ## extract
        columns = [
            "day",
            "impressions",
            "revenue",
            "country",
            "ad_type",
            "size",
            "application",
            "package_name",
            # "placement",
            "platform",
        ]

        params = {
            "api_key": self.api_key,
            "start": self.start,
            "end": self.end,
            "columns": ",".join(columns),
            "report_type": "publisher",
            "format": "csv",
        }

        df = HttpClient.csv(self.base_url, params=params)
        if df.empty:
            return

        ## transform
        df["Revenue"] = df["Revenue"].apply(lambda x: float(x.replace("$", "")))

        df = df.groupby(
            [
                "Day",
                "Package Name",
                "Application",
                "Platform",
                "Country",
                "Ad_type",
                "Size",
            ],
            as_index=False,
        ).agg({"Impressions": "sum", "Revenue": "sum"})

        df.rename(
            columns={
                "Day": "log_date",
                "Package Name": "app_key",
                "Platform": "platform",
                "Revenue": "revenue",
                "Impressions": "impressions",
                "Application": "app_name",
                "Country": "country",
                "Ad_type": "ad_type",
                "Size": "size",
            },
            inplace=True,
        )

        for row in df.itertuples(index=False):
            log_date = row.log_date
            log_date = log_date.replace("-", "")
            app_key = row.app_key
            app_name = row.app_name
            platform = PUBLISHER_PLATFORM.standardization(row.platform)
            revenue = row.revenue
            impressions = row.impressions
            country = row.country
            country = country.upper() if str(country) == country else "NaN"

            # https://support.applovin.com/hc/en-us/articles/4403932193037
            # https://support.applovin.com/hc/en-us/articles/228046827
            def adjustAdTypeBySize(ad_type: str, size: str) -> str:
                if size == "INTER":
                    if ad_type == "REWARD":
                        return AD_FORMAT.REWARDED_VIDEO
                    elif ad_type == "APPOPEN":
                        return AD_FORMAT.APP_OPEN
                    return AD_FORMAT.INTERSTITIAL

                if size == "LEADER" or size == "MREC" or size == "BANNER":
                    return AD_FORMAT.BANNER

                return ad_type

            ad_type = row.ad_type
            size = row.size
            ad_format = AD_FORMAT.standardization(adjustAdTypeBySize(ad_type, size))
            sub_format = ad_type + "_" + size

            self.push(
                REPORT_TYPE.COUNTRY,
                ad_network_id=self.ad_network_id,
                log_date=log_date,
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
