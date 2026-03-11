#!/usr/bin/env python
from iso3166 import countries
from datetime import datetime
from libs.ads.publishers.publisher import Publisher
from config.constants import (
    AD_NETWORK_ID,
    AD_FORMAT,
    REPORT_TYPE,
    PUBLISHER_PLATFORM,
)
from config.settings import settings
from libs.http_client import HttpClient
from libs.utils.cli import log


class Moloco(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.MOLOCO
        self.base_url = "https://sdkpubapi.moloco.com/api/adcloud/publisher/v1"
        self.auth_url = f"{self.base_url}/auth/tokens"
        self.report_url = f"{self.base_url}/sdk/summary"

    def _get_access_token(self):

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        data = {
            "email": settings.moloco_pub_email,
            "password": settings.moloco_pub_pw,
            "workplace_id": "",
        }
        response = HttpClient.post(
            self.auth_url, headers=headers, json_body=data, timeout=30
        )
        return response["token"]

    def _report_request(self, params: dict):
        params["publisher_id"] = settings.moloco_pub_id
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._get_access_token()}",
        }
        response = HttpClient.post(
            self.report_url, headers=headers, json_body=params, timeout=30
        )
        return response

    def report_platform(self):
        params = {
            "date_range": {"start": self.start, "end": self.end},
            "dimensions": [
                "UTC_DATE",
                "PUBLISHER_APP_STORE_ID",
                "PUBLISHER_APP_ID",
                "DEVICE_OS",
            ],
            "metrics": ["IMPRESSIONS", "REVENUE"],
        }

        response = self._report_request(params)
        if "rows" not in response:
            return

        for row in response["rows"]:
            log_date = datetime.strptime(
                row["utc_date"], "%Y-%m-%d %H:%M:%S %z UTC"
            ).strftime("%Y%m%d")
            if "app_store_id" not in row["app"]:
                continue
            app_key = row["app"]["app_store_id"]
            if "os" not in row["device"]:
                log("os not exists")
                log(row)
                continue
            platform = PUBLISHER_PLATFORM.standardization(row["device"]["os"])

            metrics = row["metric"]
            revenue = float(metrics["revenue"]) if "revenue" in metrics else 0
            impressions = int(metrics["impressions"]) if "impressions" in metrics else 0
            if impressions <= 0 and revenue <= 0:
                continue

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
        self.flush()

    def report_country(self):
        params = {
            "date_range": {"start": self.start, "end": self.end},
            "dimensions": [
                "UTC_DATE",
                "PUBLISHER_APP_STORE_ID",
                "PUBLISHER_APP_ID",
                "DEVICE_OS",
                "GEO_COUNTRY",
                "AD_UNIT_TITLE",
                "AD_UNIT_ID",
            ],
            "metrics": ["IMPRESSIONS", "REVENUE"],
        }

        response = self._report_request(params)
        if "rows" not in response:
            return

        for row in response["rows"]:
            log_date = datetime.strptime(
                row["utc_date"], "%Y-%m-%d %H:%M:%S %z UTC"
            ).strftime("%Y%m%d")
            if "app_store_id" not in row["app"]:
                continue
            app_key = row["app"]["app_store_id"]

            if "os" not in row["device"]:
                log("os not exists")
                log(row)
                continue

            platform = PUBLISHER_PLATFORM.standardization(row["device"]["os"])

            metrics = row["metric"]
            revenue = float(metrics["revenue"]) if "revenue" in metrics else 0
            impressions = int(metrics["impressions"]) if "impressions" in metrics else 0
            ad_format = AD_FORMAT.standardization(row["ad_unit"]["ad_unit_title"])
            try:
                country = countries.get(row["geo"]["country"]).alpha2
                if country == "ZZ":
                    continue
            except Exception as e:
                log(e)
                continue

            if impressions <= 0 and revenue <= 0:
                continue

            self.push(
                report_type=REPORT_TYPE.COUNTRY,
                ad_network_id=self.ad_network_id,
                log_date=log_date,
                app_key=app_key,
                app_name="",
                platform=platform,
                revenue=revenue,
                impressions=impressions,
                country=country,
                ad_format=ad_format,
                sub_format="",
            )
        self.flush()
