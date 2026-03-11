#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from libs.http_client import HttpClient
from datetime import datetime
from iso3166 import countries
from libs.utils.cli import log

import time

# List of account IDs to exclude from reporting
# Configure via environment variables or settings if needed
EXCEPT_ACCOUNTS = []


class Moloco(Advertiser):
    def __init__(self):
        self.network_name = "moloco"
        self.workplace_ids = []

        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Moloco-Cloud-Api-Version": "v1.1",
        }

    def refresh_token(self, workplace_id):
        """Issue access token and set in auth header"""
        url = "https://api.moloco.cloud/cm/v1/auth/tokens"
        params = {
            "workplace_id": workplace_id,
            "email": settings.moloco_id,
            "password": settings.moloco_pw,
            "api_key": settings.moloco_api_key,
        }
        response = HttpClient.post(
            url, headers=self.headers, json_body=params, timeout=30
        )
        self.headers["Authorization"] = "Bearer {}".format(response["token"])

    def get_ad_accounts(self):
        url = "https://api.moloco.cloud/cm/v1/ad-accounts"
        response = HttpClient.get(url, headers=self.headers, timeout=30)
        if "ad_accounts" not in response:
            return []

        accounts = []
        for row in response["ad_accounts"]:
            if row["id"] in EXCEPT_ACCOUNTS:
                continue
            accounts.append(row["id"])
        return accounts

    def get_products(self, ad_account):
        url = "https://api.moloco.cloud/cm/v1/products"
        params = {"ad_account_id": ad_account}
        response = HttpClient.get(url, headers=self.headers, params=params, timeout=30)
        product_map = {}

        if "products" in response:
            for product in response["products"]:
                product_map[product["id"]] = product["app"]["bundle_id"]

        return product_map

    def report_cost(self):
        for workspace_id in self.workplace_ids:
            self.refresh_token(workspace_id)
            ad_accounts = self.get_ad_accounts()

            report_api_url = "https://api.moloco.cloud/cm/v1/reports"
            for ad_account in ad_accounts:
                product_map = self.get_products(ad_account)

                params = {
                    "ad_account_id": ad_account,
                    "date_range": {"start": self.start, "end": self.end},
                    "dimensions": ["DATE", "APP_OR_SITE", "CAMPAIGN"],
                }
                response = HttpClient.post(
                    report_api_url,
                    headers=self.headers,
                    json_body=params,
                    timeout=30,
                )
                if "id" not in response:
                    continue

                status_id = response["id"]
                status_params = {"report_id": status_id}

                status_api_url = (
                    "https://api.moloco.cloud/cm/v1/reports/{}/status".format(status_id)
                )
                result_csv_url = None
                for idx in range(0, 10):
                    time.sleep(15)
                    response = HttpClient.get(
                        status_api_url,
                        headers=self.headers,
                        params=status_params,
                        timeout=30,
                    )
                    if response["status"] == "READY":
                        result_csv_url = response["location_csv"]
                        log("Moloco API Ready...({})".format(result_csv_url))
                        break
                    else:
                        log("Moloco API Wating...({}-{})".format(workspace_id, idx))

                if result_csv_url is not None:
                    result_df = HttpClient.csv(result_csv_url)
                    for _, report in result_df.iterrows():
                        log_date = datetime.strptime(
                            report["Date"], "%Y-%m-%d"
                        ).strftime("%Y%m%d")

                        if report["App_ID"] not in product_map:
                            continue
                        app_key = product_map[report["App_ID"]]
                        country_map = countries.get(report["Campaign_Country"])
                        country = country_map.alpha2
                        platform = self.suppose_platform(app_key, self.network_name)

                        self.push(
                            log_date=log_date,
                            app_id=app_key,
                            platform=platform,
                            country=country,
                            media_source=self.network_name,
                            campaign_id=report["Campaign_ID"],
                            campaign=report["Campaign_Title"],
                            cost=float(report["Spend"]),
                            impressions=int(report["Impressions"]),
                            clicks=int(report["Clicks"]),
                            conversions_type="INSTALL",
                            conversion_value=int(report["Installs"]),
                        )

            self.flush()
