#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from config.constants import PATH
from libs.file_manager import FileManager
from requests.auth import HTTPBasicAuth
from libs.utils import is_empty
from libs.http_client import HttpClient

import os
import requests
import pandas as pd
import numpy as np


class Reddit(Advertiser):
    def __init__(self):
        self.network_name = "reddit_int"
        self.redirect_url = os.environ.get("REDDIT_OAUTH_CALLBACK_URL", "")
        self.app_id = settings.reddit_app_id
        self.app_secret = settings.reddit_app_secret
        self.business_id = settings.reddit_business_id
        self.token_path = os.path.join(PATH.CREDENTIAL_ROOT, "reddit_auth.pickle")
        self.access_token = self.get_access_token()

    def get_access_token(self):
        auth_data = FileManager.read_pickle(self.token_path)
        headers = {
            "User-Agent": "ad-insights-aggregation-api/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": auth_data["refresh_token"],
        }
        response = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=HTTPBasicAuth(self.app_id, self.app_secret),
            data=data,
            headers=headers,
            timeout=10,
        )
        auth_data = response.json()
        FileManager.write_pickle(self.token_path, auth_data)
        return auth_data["access_token"]

    def get_ad_accounts(self):
        url = f"https://ads-api.reddit.com/api/v3/businesses/{self.business_id}/ad_accounts"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }
        response = requests.get(url, headers=headers, timeout=10)
        return list(map(lambda x: x["id"], response.json()["data"]))

    def report_cost(self):
        start = self.start
        end = (pd.to_datetime(self.end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        ad_accounts = self.get_ad_accounts()
        if len(ad_accounts) == 0:
            return

        for ad_account in ad_accounts:
            api_url = (
                f"https://ads-api.reddit.com/api/v3/ad_accounts/{ad_account}/reports"
            )
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
            }
            params = {
                "data": {
                    "breakdowns": ["DATE", "CAMPAIGN_ID", "COUNTRY"],
                    "fields": [
                        "DATE",
                        "SPEND",
                        "CLICKS",
                        "IMPRESSIONS",
                        "APP_INSTALL_INSTALL_COUNT",
                    ],
                    "starts_at": f"{start}T00:00:00Z",
                    "ends_at": f"{end}T00:00:00Z",
                    "time_zone_id": "UTC",
                }
            }
            res = HttpClient.post(
                api_url, headers=headers, timeout=20, json_body=params
            )
            if "data" not in res:
                continue
            if "metrics" not in res["data"]:
                continue
            if is_empty(res["data"]["metrics"]):
                continue

            df = pd.DataFrame(res["data"]["metrics"])
            df.rename(columns={"app_install_install_count": "installs"}, inplace=True)

            api_url = (
                f"https://ads-api.reddit.com/api/v3/ad_accounts/{ad_account}/campaigns"
            )
            res = HttpClient.get(api_url, headers=headers, timeout=20)
            campaign_df = pd.DataFrame(res["data"])
            campaign_df.rename(
                columns={"id": "campaign_id", "name": "campaign_name"}, inplace=True
            )

            df = pd.merge(
                df,
                campaign_df[["campaign_id", "campaign_name", "app_id"]],
                on="campaign_id",
                how="left",
            )
            df["platform"] = np.select(
                [
                    df["campaign_name"].str.contains("ios", case=False, na=False),
                    df["campaign_name"].str.contains(
                        "android|aos", case=False, na=False
                    ),
                ],
                ["ios", "android"],
                default="unknown",
            )
            check_platform = df[df.platform == "unknown"]
            if not check_platform.empty:
                df = df[df.platform != "unknown"]

            df["spend"] = df["spend"] / 1_000_000

            for _, report in df.iterrows():
                self.push(
                    log_date=report["date"].replace("-", ""),
                    app_id=report["app_id"],
                    platform=report["platform"],
                    country=report["country"],
                    media_source="reddit_int",
                    campaign_id=report["campaign_id"],
                    campaign=report["campaign_name"],
                    cost=report["spend"],
                    impressions=report["impressions"],
                    clicks=report["clicks"],
                    conversions_type="INSTALL",
                    conversion_value=report["installs"] if "installs" in report else 0,
                )

            self.flush()
