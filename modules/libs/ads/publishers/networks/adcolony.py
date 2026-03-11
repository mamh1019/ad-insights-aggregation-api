#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.settings import settings
from config.constants import AD_NETWORK_ID, PUBLISHER_PLATFORM
from libs.ads.publishers.publisher import REPORT_TYPE
from datetime import datetime

from libs.http_client import HttpClient


class Adcolony(Publisher):
    def __init__(self):
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.ADCOLONY
        self.api_key = settings.adcolony_api_key
        self.api_url = "http://clients-api.adcolony.com/api/v2/publisher_summary"

    def _build_parmas(self, date_group: str, group_by: str = None):
        start = datetime.strptime(self.start, "%Y-%m-%d").strftime("%m%d%Y")
        end = datetime.strptime(self.end, "%Y-%m-%d").strftime("%m%d%Y")
        _params = {
            "user_credentials": self.api_key,
            "date": start,
            "end_date": end,
            "date_group": date_group,
        }
        if group_by is not None:
            _params["group_by"] = group_by
        return _params

    def report_platform(self):
        params = self._build_parmas(date_group="day")
        result = HttpClient.get(self.api_url, params=params)

        if "status" in result and "success" != result["status"]:
            raise Exception("Adcolony internal server error")

        reports = result["results"]

        for row in reports:
            self.push(
                REPORT_TYPE.PLATFORM,
                self.ad_network_id,
                row["date"].replace("-", ""),
                row["app_id"],
                row["app_name"],
                PUBLISHER_PLATFORM.standardization(row["platform"]),
                row["impressions"],
                row["earnings"],
            )

        self.flush()

    def report_country(self):
        pass
