#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from libs.http_client import HttpClient
from config.constants import PLATFORM
from libs.utils.cli import log


class Apple(Advertiser):
    def __init__(self):
        super().__init__()

        self.network_name = "iOS Search Ads"
        self.client_id = None
        self.client_secret = None
        self.api_version = "v5"
        self.access_token = None
        # List of organization IDs to exclude from reporting
        # Configure via environment variables or settings if needed
        self.ban_accounts = []

        self.credentials = [
            {
                "client_id": settings.apple_biz_client_id,
                "client_secret": settings.apple_biz_client_secret,
            },
            {
                "client_id": settings.apple_bm_client_id,
                "client_secret": settings.apple_bm_client_secret,
            },
        ]

    def _refresh_access_token(self, client_id, client_secret):
        """Default expiry 1hr; no need to cache"""
        url = "https://appleid.apple.com/auth/oauth2/token"
        params = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "searchadsorg",
        }

        # Request headers
        headers = {
            "Host": "appleid.apple.com",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # Send POST request
        response = HttpClient.post(url, params=params, headers=headers, timeout=10)
        if "access_token" not in response:
            log(response)
        return response["access_token"]

    def _get_headers(self, access_token, org_id=None):
        headers = {"Authorization": "Bearer {}".format(access_token)}
        if org_id is not None:
            headers["Content-Type"] = "application/json"
            headers["X-AP-Context"] = "orgId={}".format(org_id)
        return headers

    def _get_ad_accounts(self, access_token):
        url = "https://api.searchads.apple.com/api/{}/acls".format(self.api_version)
        headers = self._get_headers(access_token)
        response = HttpClient.get(url, headers=headers, timeout=10)

        accounts = []
        for row in response["data"]:
            org_id = row["orgId"]
            if org_id in self.ban_accounts:
                continue
            accounts.append(org_id)

        return accounts

    def report_cost(self):
        for credential in self.credentials:
            access_token = self._refresh_access_token(
                credential["client_id"], credential["client_secret"]
            )
            accounts = self._get_ad_accounts(access_token)

            campaign_api_url = (
                "https://api.searchads.apple.com/api/{}/reports/campaigns".format(
                    self.api_version
                )
            )
            params = {
                "startTime": self.start,
                "endTime": self.end,
                "selector": {
                    "orderBy": [{"field": "countryOrRegion", "sortOrder": "ASCENDING"}],
                    "pagination": {"offset": 0, "limit": 1000},
                    "conditions": [
                        {
                            "field": "displayStatus",
                            "operator": "IN",
                            "values": ["RUNNING"],
                        }
                    ],
                },
                "groupBy": ["countryOrRegion"],
                "timeZone": "UTC",
                "granularity": "DAILY",
                "returnRecordsWithNoMetrics": True,
                "returnRowTotals": True,
                "returnGrandTotals": True,
            }

            for account in accounts:
                log("{} processing...".format(account))
                headers = self._get_headers(access_token, org_id=account)
                response = HttpClient.post(
                    campaign_api_url,
                    json_body=params,
                    headers=headers,
                    timeout=30,
                )

                if "error" in response and response["error"] is not None:
                    if (
                        "errors" in response["error"]
                        and len(response["error"]["errors"]) > 0
                    ):
                        log("ASA error reported..")
                        log(response)
                        continue

                reports = response["data"]["reportingDataResponse"]["row"]
                if len(reports) <= 0:
                    continue

                for daily_report in reports:
                    meta = daily_report["metadata"]
                    campaign_id = meta["campaignId"]
                    campaign_name = meta["campaignName"]
                    country = meta["countryOrRegion"]
                    app_id = meta["app"]["adamId"]
                    platform = PLATFORM.IOS

                    granularity = daily_report["granularity"]
                    for row in granularity:
                        if "impressions" not in row:
                            continue
                        impressions = row["impressions"]
                        if int(impressions) <= 0:
                            continue
                        log_date = row["date"].replace("-", "")
                        clicks = row["taps"]
                        cost = row["localSpend"]["amount"]

                        if "totalInstalls" in row:
                            installs = row["totalInstalls"]
                        elif "installs" in row:
                            installs = row["installs"]
                        else:
                            installs = 0

                        self.push(
                            log_date=log_date,
                            app_id=app_id,
                            platform=platform,
                            country=country,
                            media_source=self.network_name,
                            campaign_id=campaign_id,
                            campaign=campaign_name,
                            cost=cost,
                            impressions=impressions,
                            clicks=clicks,
                            conversions_type="INSTALL",
                            conversion_value=int(installs),
                        )

            self.flush()
