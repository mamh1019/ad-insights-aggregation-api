#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.constants import PUBLISHER_PLATFORM
from config.constants import AD_NETWORK_ID
from config.settings import settings
from config.constants import AD_FORMAT
from libs.utils.cli import log

import pandas as pd
import requests

from libs.country import find_by_name
from libs.query import Query


class Yandex(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.YANDEX
        self.oauth_url = "https://oauth.yandex.com/token"
        self.api_url = "https://partner2.yandex.ru/api/statistics2/get.json?"
        self.oauth_token = settings.yandex_oauth_token

    def report_platform(self):
        pass

    def report_country(self):
        params = {
            "period": [self.start, self.end],
            "dimension_field": ["date|day", "geo|country"],
            "entity_field": ["domain", "os", "block_type"],
            "field": [
                "impressions",
                "clicks",
                "partner_wo_nds",
            ],
            "lang": ["en"],
            "currency": ["USD"],
            "stat_type": ["main"],
        }

        header = {
            "Authorization": "OAuth {}".format(self.oauth_token),
        }

        query_string = "&".join(
            f"{key}={value}" for key, values in params.items() for value in values
        )

        report_url = f"{self.api_url}{query_string}"
        result = requests.get(report_url, headers=header, timeout=60).json()
        if "errors" in result:
            for error in result["errors"]:
                break

        if "points" not in result.get("data", {}):
            log("Yandex data is empty")
            return

        report_data = []
        points = result["data"]["points"]
        for point in points:
            measures = point["measures"][0]
            impressions = measures.get("impressions", 0)
            revenue = measures.get("partner_wo_nds", 0)
            if impressions == 0:
                continue

            dimensions = point["dimensions"]
            log_date = dimensions["date"][0].replace("-", "")
            app_key = dimensions["domain"]
            platform = PUBLISHER_PLATFORM.standardization(dimensions["os"])
            if dimensions["geo"] == "Undefined":
                continue
            country = find_by_name(dimensions["geo"])
            ad_format = AD_FORMAT.standardization(dimensions["block_type"])

            report_data.append(
                {
                    "log_date": log_date,
                    "app_key": app_key,
                    "platform": platform,
                    "country": country,
                    "format": ad_format,
                    "impressions": impressions,
                    "revenue": revenue,
                }
            )

        df = pd.DataFrame(report_data)
        if df.empty:
            return

        df["sub_format"] = ""
        df["clicks"] = 0
        df["ad_network_id"] = self.ad_network_id
        df = df[
            [
                "log_date",
                "app_key",
                "platform",
                "country",
                "format",
                "sub_format",
                "revenue",
                "impressions",
                "clicks",
                "ad_network_id",
            ]
        ]

        df = (
            df.groupby(
                [
                    "log_date",
                    "app_key",
                    "platform",
                    "country",
                    "format",
                    "sub_format",
                    "ad_network_id",
                ]
            )
            .sum()
            .reset_index()
        ).round(2)

        sql = Query.build_insert_stmt(
            "ad_network_report_country",
            df.columns,
            dup_update_columns=["revenue", "impressions", "clicks"],
        )
        # Persist ad_network_report_country to model
        log(f"Yandex Country {len(df)} Inserted")

        df = df[
            [
                "log_date",
                "app_key",
                "platform",
                "revenue",
                "impressions",
                "clicks",
                "ad_network_id",
            ]
        ]

        df = (
            df.groupby(["log_date", "app_key", "platform", "ad_network_id"])
            .sum()
            .reset_index()
            .round(2)
        )

        sql = Query.build_insert_stmt(
            "ad_network_report",
            df.columns,
            dup_update_columns=["revenue", "impressions", "clicks"],
        )
        # Persist ad_network_report to model
        log(f"Yandex Platform {len(df)} Inserted")
