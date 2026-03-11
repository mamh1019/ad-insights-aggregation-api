#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.settings import settings
from config.constants import PUBLISHER_PLATFORM, REPORT_TYPE, AD_FORMAT, AD_NETWORK_ID
from libs.http_client import HttpClient
from libs.utils.array import apply_dict_tree
import requests


class Ironsource(Publisher):
    def __init__(self):
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.IRONSOURCE

        self.secret_key = settings.ironsource_secret_key
        self.refresh_token = settings.ironsource_refresh_token

        self.auth_url = "https://platform.ironsrc.com/partners/publisher/auth"
        self.base_url = "https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats"

    def get_auth_header(self):
        # https://developers.is.com/ironsource-mobile/air/authentication/#step-1
        headers = {"secretkey": self.secret_key, "refreshToken": self.refresh_token}
        result = requests.get(self.auth_url, headers=headers, timeout=10)
        return {"Authorization": "Bearer {}".format(result.text.replace('"', ""))}

    def report_platform(self):
        params = {
            "startDate": self.start,
            "endDate": self.end,
            "breakdowns": ",".join(["date", "app", "platform"]),
            "metrics": ",".join(["revenue", "impressions"]),
        }

        res = HttpClient.get(
            self.base_url, params=params, headers=self.get_auth_header()
        )

        ## transform
        breakdowns_revenue_acc = {}
        breakdowns_impressions_acc = {}
        key_set = set()
        key_delimeter = "@"

        for row in res:
            dt = row["date"]
            app_key = row["appKey"]
            platform = row["platform"]
            platform = PUBLISHER_PLATFORM.standardization(platform)
            app_name = row["appName"]

            data = row["data"][0]
            revenue = data["revenue"]
            impressions = data["impressions"]

            breakdowns = [dt, app_key, app_name, platform]
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
            revenue = breakdowns_revenue_acc[key]
            impressions = breakdowns_impressions_acc[key]

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
        params = {
            "startDate": self.start,
            "endDate": self.end,
            "breakdowns": ",".join(["date", "app", "country", "platform"]),
            "metrics": ",".join(["revenue", "impressions"]),
        }

        res = HttpClient.get(
            self.base_url, params=params, headers=self.get_auth_header()
        )

        ## transform
        breakdowns_revenue_acc = {}
        breakdowns_impressions_acc = {}
        key_set = set()
        key_delimeter = "@"

        for row in res:
            dt = row["date"]
            app_key = row["appKey"]
            platform = row["platform"]
            platform = PUBLISHER_PLATFORM.standardization(platform)
            app_name = row["appName"]
            ad_units = row["adUnits"]
            ad_format = AD_FORMAT.standardization(ad_units)
            sub_format = ad_units

            for data in row["data"]:
                revenue = data["revenue"]
                impressions = data["impressions"]
                country = data["countryCode"]

                breakdowns = [
                    dt,
                    app_key,
                    app_name,
                    platform,
                    country,
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
                revenue=revenue,
                impressions=impressions,
                country=country_code,
                ad_format=ad_format,
                sub_format=sub_format,
            )

        ## load
        self.flush()
