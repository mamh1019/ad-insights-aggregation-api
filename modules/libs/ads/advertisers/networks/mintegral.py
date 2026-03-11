#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from libs.http_client import HttpClient
from datetime import datetime

import os
import hashlib
import requests
from urllib.parse import urlencode


class Mintegral(Advertiser):
    def __init__(self):
        self.network_name = "mintegral_int"
        self.user_name = os.environ.get("MINTEGRAL_USER_NAME", "")
        self.api_key = settings.mintegral_api_key

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
        def md5(text):
            return hashlib.md5(str(text).encode("utf-8")).hexdigest()

        timestamp = str(int(datetime.now().timestamp()))
        token = md5(self.api_key + md5(timestamp))

        params = {
            "username": self.user_name,
            "token": token,
            "timestamp": timestamp,
            "start_date": self.start,
            "end_date": self.end,
            "dimension": "location",
            "page": 1,
            "per_page": 9999,
            "utc": "+0",
        }
        api_url = "https://ss-api.mintegral.com/api/v1/reports/data?{}".format(
            urlencode(params)
        )

        result = requests.get(api_url, timeout=60)
        campaigns = result.json()

        if "code" in campaigns and campaigns["code"] == 200:
            reports = campaigns["data"]

            for report in reports:
                app_id = report["package_name"]
                platform = report["platform"]
                campaign_id = report["uuid"]

                if platform == "ios" and app_id.startswith("id"):
                    app_id = str(app_id[2:])

                log_date = report["date"].replace("-", "")
                campaign_name = report["offer_name"]
                spend = report["spend"]
                impressions = report["impression"]
                clicks = report["click"]
                installs = report["install"]

                country = report["location"].upper()

                platform = self.suppose_platform(app_id, self.network_name)

                self.push(
                    log_date=log_date,
                    app_id=app_id,
                    platform=platform,
                    country=country,
                    media_source=self.network_name,
                    campaign_id=campaign_id,
                    campaign=campaign_name,
                    cost=spend,
                    impressions=impressions,
                    clicks=clicks,
                    conversions_type="INSTALL",
                    conversion_value=installs,
                )

            self.flush()
