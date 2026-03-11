#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from libs.http_client import HttpClient
from urllib.parse import urlencode

import os
import requests


class Mistplay(Advertiser):
    def __init__(self):
        self.network_name = "mistplay_int"

        ## API Auth Config
        self.account_id = os.environ.get("MISTPLAY_ACCOUNT_ID", "")
        self.secret = settings.mistplay_secret
        self.api_token = settings.mistplay_token

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

    def report_cost(self):
        params = {
            "account_id": self.account_id,
            "secret": self.secret,
            "api_token": self.api_token,
            "start_date": self.start,
            "end_date": self.end,
        }
        api_url = "https://tp.mistplay.com/report/custom?{}".format(urlencode(params))

        result = requests.get(api_url, timeout=100).json()

        if "data" not in result:
            return
        reports = result["data"]["report"]

        for report in reports:
            log_date = report["date"].replace("-", "")
            platform = self.suppose_platform(report["store_id"], self.network_name)

            if (
                int(report["cost"]) <= 0
                and int(report["impressions"]) <= 0
                and int(report["clicks"]) <= 0
            ):
                continue

            self.push(
                log_date=log_date,
                app_id=report["store_id"],
                platform=platform,
                country=report["country_code"],
                media_source=self.network_name,
                campaign_id=report["campaign_id"],
                campaign=report["campaign_name"],
                cost=report["cost"],
                impressions=report["impressions"],
                clicks=report["clicks"],
                conversions_type="INSTALL",
                conversion_value=report["installs"] if "installs" in report else 0,
            )

        self.flush()
