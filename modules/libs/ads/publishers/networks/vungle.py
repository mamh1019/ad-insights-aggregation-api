#!/usr/bin/env python
from config.constants import AD_NETWORK_ID, PUBLISHER_PLATFORM, REPORT_TYPE, AD_FORMAT
from config.settings import settings
from libs.ads.publishers.publisher import Publisher
from libs.http_client import HttpClient


class Vungle(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.VUNGLE
        self.api_key = settings.vungle_api_key
        self.base_url = "https://report.api.vungle.com/ext/pub/reports/performance"

    def report_platform(self):
        ## extract
        headers = {
            "Accept": "application/json",
            # "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.api_key),
            "Vungle-Version": "1",
        }

        params = {
            "startDate": self.start,
            "endDate": self.end,
            "dimensions": ",".join(["date", "application", "platform"]),
            "aggregates": ",".join(["revenue", "impressions"]),
        }

        res = HttpClient.get(self.base_url, params=params, headers=headers)

        ## transform
        for row in res:
            dt = row["date"].replace("-", "")
            app_key = row["application id"]
            app_name = row["application name"]
            country = "ALL"
            platform = row["platform"]
            platform = PUBLISHER_PLATFORM.standardization(platform)
            impressions = row["impressions"]
            revenue = row["revenue"]

            self.push(
                report_type=REPORT_TYPE.PLATFORM,
                log_date=dt,
                ad_network_id=self.ad_network_id,
                app_key=app_key,
                app_name=app_name,
                country=country,
                platform=platform,
                revenue=revenue,
                impressions=impressions,
            )

        ## load
        self.flush()

    def report_country(self):
        ## extract
        headers = {
            "Accept": "application/json",
            # "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.api_key),
            "Vungle-Version": "1",
        }

        params = {
            "startDate": self.start,
            "endDate": self.end,
            "dimensions": ",".join(
                ["date", "application", "country", "platform", "adType", "incentivized"]
            ),
            "aggregates": ",".join(["revenue", "impressions"]),
        }

        res = HttpClient.get(self.base_url, params=params, headers=headers)

        ## transform
        for row in res:
            dt = row["date"].replace("-", "")
            app_key = row["application id"]
            app_name = row["application name"]
            country = row["country"]
            platform = row["platform"]
            platform = PUBLISHER_PLATFORM.standardization(platform)
            impressions = row["impressions"]
            revenue = row["revenue"]

            if row["adType"] == "banner":
                ad_format = AD_FORMAT.BANNER
            elif row["incentivized"] is True and row["adType"] == "video":
                ad_format = AD_FORMAT.REWARDED_VIDEO
            elif row["incentivized"] is False and row["adType"] == "video":
                ad_format = AD_FORMAT.INTERSTITIAL
            elif row["adType"] == "mrec":
                ad_format = AD_FORMAT.BANNER
            else:
                print(row)
                raise Exception("Invalid adType or incentivized")

            self.push(
                report_type=REPORT_TYPE.COUNTRY,
                log_date=dt,
                ad_network_id=self.ad_network_id,
                app_key=app_key,
                app_name=app_name,
                country=country,
                platform=platform,
                revenue=revenue,
                impressions=impressions,
                ad_format=ad_format,
                sub_format=None,
            )

        ## load
        self.flush()
