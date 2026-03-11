#!/usr/bin/env python
from config.constants import AD_NETWORK_ID
from config.constants import AD_FORMAT
from config.constants import PUBLISHER_PLATFORM
from config.constants import REPORT_TYPE
from libs.ads.publishers.publisher import Publisher
from libs.utils import is_empty, apply_dict_tree
from config.settings import settings
from libs.http_client import HttpClient


# https://answers.chartboost.com/en-us/child_article/analytics-methods
class Chartboost(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.CHARTBOOST
        self.user_id = settings.chartboost_user_id
        self.sign = settings.chartboost_sign

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def report_platform(self):

        ## extract
        params = {
            "dateMin": self.start,
            "dateMax": self.end,
            "userId": self.user_id,
            "userSignature": self.sign,
        }

        # App analytics
        res = HttpClient.get(
            "https://analytics.chartboost.com/v3/metrics/app", params=params
        )

        ## transform
        for row in res:
            if type(row) != dict:
                continue
            if "appId" not in row:
                continue

            app_key = row["appId"]
            app_name = row["app"]

            if app_key + app_name == "":
                continue

            dt = row["dt"].replace("-", "")
            revenue = row["moneyEarned"]
            impressions = row["impressionsDelivered"]
            platform = row["platform"]  # iOS, Google Play, or Amazon
            platform = PUBLISHER_PLATFORM.standardization(platform)

            self.push(
                report_type=REPORT_TYPE.PLATFORM,
                log_date=dt,
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
        ## extract
        params = {
            "dateMin": self.start,
            "dateMax": self.end,
            "userId": self.user_id,
            "userSignature": self.sign,
        }

        # App analytics grouped by country
        res = HttpClient.get(
            "https://analytics.chartboost.com/v3/metrics/appcountry", params=params
        )

        ## transform
        breakdowns_revenue_acc = {}
        breakdowns_impressions_acc = {}
        key_set = set()
        key_delimeter = "@"

        for row in res:
            if type(row) != dict:
                continue
            if "appId" not in row:
                continue
            if is_empty(row["adType"]):
                continue

            dt = row["dt"]
            app_id = row["appId"]
            app = row["app"]
            country_code = row["countryCode"]  # Two-letter country code, or “unknown”
            revenue = row["moneyEarned"]
            impressions = row["impressionsDelivered"]
            platform = row["platform"]  # iOS, Google Play, or Amazon
            platform = PUBLISHER_PLATFORM.standardization(platform)
            ad_format = AD_FORMAT.standardization(row["adType"])
            sub_format = row["adType"]

            breakdowns = [
                dt,
                app_id,
                app,
                platform,
                country_code,
                ad_format,
                sub_format,
            ]
            key = key_delimeter.join(breakdowns)
            key_set.add(key)

            apply_dict_tree(breakdowns_revenue_acc, [key], 0.0)
            apply_dict_tree(breakdowns_impressions_acc, [key], 0)

            breakdowns_revenue_acc[key] += float(revenue)
            breakdowns_impressions_acc[key] += impressions

        for key in key_set:
            key_list = key.split(key_delimeter)

            dt = key_list[0].replace("-", "")
            app_key = key_list[1]
            app_name = key_list[2]
            platform = key_list[3]
            country_code = key_list[4]
            ad_format = key_list[5]
            sub_format = key_list[6]
            revenue = breakdowns_revenue_acc[key]
            impressions = breakdowns_impressions_acc[key]

            self.push(
                report_type=REPORT_TYPE.COUNTRY,
                log_date=dt,
                ad_network_id=self.ad_network_id,
                app_key=app_key,
                app_name=app_name,
                platform=platform,
                country=country_code,
                ad_format=ad_format,
                sub_format=sub_format,
                revenue=revenue,
                impressions=impressions,
            )

        ## load
        self.flush()
