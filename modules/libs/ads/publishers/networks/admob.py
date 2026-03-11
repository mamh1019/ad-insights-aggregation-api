#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.constants import AD_NETWORK_ID, AD_FORMAT
from config.constants import PUBLISHER_PLATFORM, REPORT_TYPE
from libs.google_oauth import GoogleOAuth
from config.settings import settings
from config.constants import PATH
from libs.http_client import HttpClient
from libs.utils.common import is_empty
from libs.utils.cli import log

import os


class Admob(Publisher):
    def __init__(self):
        self.ad_network_id = AD_NETWORK_ID.ADMOB
        self.publisher_id = settings.admob_publisher_id
        self.token = GoogleOAuth.get_credentials(
            token_path=os.path.join(PATH.CREDENTIAL_ROOT, "admob_token.pickle"),
        ).token

    def _build_report_spec(self, dimensions, metrics, filters=None):
        (s_year, s_month, s_day) = self.start.split("-")
        (e_year, e_month, e_day) = self.end.split("-")

        payload = {
            "date_range": {
                "start_date": {"year": s_year, "month": s_month, "day": s_day},
                "end_date": {"year": e_year, "month": e_month, "day": e_day},
            },
            "dimensions": dimensions,
            "metrics": metrics,
        }

        if filters is not None:
            payload["dimensionFilters"] = filters

        return {"reportSpec": payload}

    def _network_report(self, report_spec: dict = None) -> dict:
        api_url = "https://admob.googleapis.com/v1/accounts/{}/networkReport:generate?access_token={}".format(
            self.publisher_id, self.token
        )
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        return HttpClient.post(api_url, json_body=report_spec, headers=headers)

    def _mediation_report(self, report_spec: dict = None) -> dict:
        api_url = "https://admob.googleapis.com/v1/accounts/{}/mediationReport:generate?access_token={}".format(
            self.publisher_id, self.token
        )
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        return HttpClient.post(api_url, json_body=report_spec, headers=headers)

    def _app_keys(self) -> list:
        report_spec = self._build_report_spec(["APP"], ["IMPRESSIONS"])
        result = self._network_report(report_spec)
        app_keys = []

        for row in result:
            if "row" not in row:
                continue
            data = row["row"]
            _app_key = data["dimensionValues"]["APP"]["value"]
            if is_empty(_app_key) or _app_key in app_keys:
                continue
            app_keys.append(_app_key)

        return app_keys

    def report_platform(self):
        app_keys = self._app_keys()

        filters = [
            {"dimension": "APP", "matchesAny": {"values": app_keys}},
        ]
        report_spec = self._build_report_spec(
            ["DATE", "APP", "PLATFORM"],
            ["CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS"],
            filters=filters,
        )

        log("admob report platform call")
        result = self._network_report(report_spec)
        log("admob report platform end ({})".format(len(result)))

        for row in result:
            if "row" not in row:
                continue

            data = row["row"]
            dimensions = data["dimensionValues"]
            metrics = data["metricValues"]

            log_date = dimensions["DATE"]["value"]
            app_key = dimensions["APP"]["value"]
            app_name = dimensions["APP"]["displayLabel"]
            platform = PUBLISHER_PLATFORM.standardization(
                dimensions["PLATFORM"]["value"]
            )
            impressions = metrics["IMPRESSIONS"]["integerValue"]
            revenue = int(metrics["ESTIMATED_EARNINGS"]["microsValue"]) / 1000000

            self.push(
                REPORT_TYPE.PLATFORM,
                self.ad_network_id,
                log_date,
                app_key,
                app_name,
                platform,
                impressions,
                revenue,
            )

        # save
        self.flush()

    def report_country(self):
        app_keys = self._app_keys()

        filters = [
            {"dimension": "APP", "matchesAny": {"values": app_keys}},
        ]
        report_spec = self._build_report_spec(
            ["DATE", "APP", "PLATFORM", "COUNTRY", "FORMAT"],
            ["CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS"],
            filters=filters,
        )

        log("admob report country call")
        result = self._network_report(report_spec)
        log("admob report country end ({})".format(len(result)))

        for row in result:
            if "row" not in row:
                continue

            data = row["row"]
            dimensions = data["dimensionValues"]
            metrics = data["metricValues"]

            log_date = dimensions["DATE"]["value"]
            app_key = dimensions["APP"]["value"]
            app_name = dimensions["APP"]["displayLabel"]
            platform = PUBLISHER_PLATFORM.standardization(
                dimensions["PLATFORM"]["value"]
            )
            impressions = metrics["IMPRESSIONS"]["integerValue"]
            revenue = int(metrics["ESTIMATED_EARNINGS"]["microsValue"]) / 1000000
            ad_format = AD_FORMAT.standardization(dimensions["FORMAT"]["value"])
            sub_format = dimensions["FORMAT"]["value"]
            country = (
                dimensions["COUNTRY"]["value"]
                if "value" in dimensions["COUNTRY"]
                else "ZZ"
            )

            self.push(
                REPORT_TYPE.COUNTRY,
                self.ad_network_id,
                log_date,
                app_key,
                app_name,
                platform,
                impressions,
                revenue,
                ad_format=ad_format,
                sub_format=sub_format,
                country=country,
            )

        # save
        self.flush()
