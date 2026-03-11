#!/usr/bin/env python
from config.constants import AD_NETWORK_ID, PUBLISHER_PLATFORM
from config.constants import AD_FORMAT
from config.constants import REPORT_TYPE
from libs.ads.publishers.publisher import Publisher
from config.settings import settings
from libs.http_client import HttpClient
import pandas as pd
from libs.utils.array import apply_dict_tree


class Tapjoy(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.TABJOY
        self.api_key = settings.tapjoy_api_key
        self.base_url = "https://api.tapjoy.com/%s"
        self.auth_url = self.base_url % "v1/oauth2/token"
        self.report_url = self.base_url % "v2/publisher/reports?"

        self.response = {}

    def _get_token(self):
        headers = {
            "Accept": "application/json",
            "Authorization": "Basic {}".format(self.api_key),
        }

        res = HttpClient.post(self.auth_url, headers=headers)
        token = res["access_token"]
        return token

    def report_platform(self):
        pass

    def report_country(self):
        div_mirco = 1000000
        platform_key_set = set()
        country_key_set = set()
        key_delimeter = "@"
        platform_revenue_acc = {}
        platform_impressions_acc = {}
        country_revenue_acc = {}
        country_impressions_acc = {}

        access_token = self._get_token()
        headers = {
            "Authorization": "Bearer {}".format(access_token),
            "Accept": "application/json; */*",
        }

        date_list = pd.date_range(start=self.start, end=self.end)[::-1]
        for date in date_list:
            start_date = date.strftime("%Y-%m-%dT00:00:00Z")
            end_date = date.strftime("%Y-%m-%dT23:59:59Z")
            log_date = date.strftime("%Y%m%d")

            graph_qry = """
                query {
                    publisher{
                        apps(first:500){
                            edges{
                                node{
                                    id,name,platform,
                                    placements {
                                        name
                                        contents {
                                            type
                                        }
                                        insights(timeRange: {from:"%s",until: "%s"}) {
                                            timestamps,
                                            reports { 
                                                impressions
                                                earnings
                                                country
                                            }
                                        }
                                    }
                                }
                            }
                            pageInfo {
                                endCursor
                                hasNextPage
                            }
                        }
                    }
                }
            """ % (
                start_date,
                end_date,
            )
            api_url = "https://api.tapjoy.com/graphql"
            result = HttpClient.post(
                api_url, headers=headers, json_body={"query": graph_qry}, timeout=60
            )
            reports = result

            if "data" not in reports:
                return
            if "publisher" not in reports["data"]:
                return
            if "apps" not in reports["data"]["publisher"]:
                return
            if "edges" not in reports["data"]["publisher"]["apps"]:
                return

            apps_edges = reports["data"]["publisher"]["apps"]["edges"]
            for edge in apps_edges:
                node = edge["node"]
                app_key = node["id"]
                platform = PUBLISHER_PLATFORM.standardization(node["platform"])
                placements = node["placements"]
                for placement in placements:
                    format_name = (
                        placement["contents"][0]["type"]
                        if len(placement["contents"]) > 0
                        else placement["name"]
                    )
                    ad_format = AD_FORMAT.standardization(format_name)
                    reports = placement["insights"]["reports"]
                    for report in reports:
                        impressions = report["impressions"][0]
                        if int(impressions) <= 0:
                            continue

                        revenue = float(report["earnings"][0]) / div_mirco
                        country_code = report["country"]

                        platform_key = key_delimeter.join([log_date, app_key, platform])
                        platform_key_set.add(platform_key)
                        apply_dict_tree(platform_revenue_acc, [platform_key], 0.0)
                        apply_dict_tree(platform_impressions_acc, [platform_key], 0)
                        platform_revenue_acc[platform_key] += float(revenue)
                        platform_impressions_acc[platform_key] += int(impressions)

                        country_key = key_delimeter.join(
                            [log_date, app_key, platform, country_code, ad_format]
                        )
                        country_key_set.add(country_key)
                        apply_dict_tree(country_revenue_acc, [country_key], 0.0)
                        apply_dict_tree(country_impressions_acc, [country_key], 0)
                        country_revenue_acc[country_key] += float(revenue)
                        country_impressions_acc[country_key] += int(impressions)

        if len(platform_key_set) > 0:
            for key_set in platform_key_set:
                (log_date, app_key, platform) = key_set.split(key_delimeter)
                impressions = platform_impressions_acc[key_set]
                revenue = platform_revenue_acc[key_set]

                self.push(
                    report_type=REPORT_TYPE.PLATFORM,
                    log_date=log_date,
                    ad_network_id=self.ad_network_id,
                    app_key=app_key,
                    app_name="",
                    platform=platform,
                    revenue=revenue,
                    impressions=impressions,
                )

        if len(country_key_set) > 0:
            for key_set in country_key_set:
                (log_date, app_key, platform, country_code, ad_format) = key_set.split(
                    key_delimeter
                )
                impressions = country_impressions_acc[key_set]
                revenue = country_revenue_acc[key_set]

                self.push(
                    report_type=REPORT_TYPE.COUNTRY,
                    log_date=log_date,
                    ad_network_id=self.ad_network_id,
                    app_key=app_key,
                    app_name="",
                    platform=platform,
                    country=country_code,
                    ad_format=ad_format,
                    sub_format=ad_format,
                    revenue=revenue,
                    impressions=impressions,
                )

        ## load
        self.flush()
