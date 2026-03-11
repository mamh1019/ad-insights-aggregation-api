#!/usr/bin/env python
from abc import ABCMeta, abstractmethod
from typing import List, Dict
from config.constants import CONVERSION_TYPE
from urllib.parse import urlparse

import botocore
import requests


class Advertiser(metaclass=ABCMeta):
    # Extract date range
    start: str = None
    end: str = None

    s3 = None

    # Data buffer
    buffer_cost_rows: List[Dict] = []
    buffer_conversion_rows: List[Dict] = []

    # Network name for BI
    network_name: str = None

    def set_interval(self, start: str, end: str):
        self.start = start
        self.end = end

    @abstractmethod
    def report_cost(self):
        pass

    def report_creative(self):
        pass

    @staticmethod
    def suppose_platform(bundle_id: str, media_source: str):
        bundle_id = str(bundle_id)
        platform = "unknown"

        if bundle_id.isdigit():
            # ios or onestore
            if bundle_id.startswith("00") and media_source == "moloco":
                platform = "onestore"
            else:
                platform = "ios"
        elif "_ios" in bundle_id:  # applovin except
            platform = "ios"
        elif "_android" in bundle_id:  # applovin except
            platform = "android"
        else:
            platform = "android"

        if platform == "unknown":
            raise Exception(f"unknown platform: {bundle_id}")

        return platform

    def report(self):
        pass

    def creative(self):
        pass

    def push(
        self,
        log_date: str,
        app_id: str,
        platform: str,
        country: str,
        media_source: str,
        campaign_id: str,
        campaign: str,
        cost: float = 0.0,
        impressions: int = 0,
        clicks: int = 0,
        conversions_type: str = CONVERSION_TYPE.INSTALL,
        conversion_value: float = 0.0,
        *,
        adset_id: str = "",
        adset: str = "",
    ):
        if len(country) != 2:
            return

        if float(cost) <= 0:
            return

        self.buffer_cost_rows.append(
            {
                "log_date": log_date,
                "app_id": app_id,
                "platform": platform,
                "country": country,
                "media_source": media_source,
                "campaign_id": campaign_id,
                "campaign": campaign,
                "adset_id": adset_id,
                "adset": adset,
                "cost": cost,
                "impressions": impressions,
                "clicks": clicks,
                "conversions_type": conversions_type,
            }
        )
        self.buffer_conversion_rows.append(
            {
                "log_date": log_date,
                "app_id": app_id,
                "platform": platform,
                "country": country,
                "media_source": media_source,
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "conversions_type": conversions_type,
                "value": conversion_value,
            }
        )

    def flush(self):
        """
        Persist buffered data to database.
        Persist buffer_cost_rows, buffer_conversion_rows to model.
        """
        if len(self.buffer_cost_rows) > 0:
            self.buffer_cost_rows.clear()
            self.buffer_conversion_rows.clear()

    def load_to_s3_if_not_exists(self, src, key):
        try:
            self.s3.Object(self.cdn_bucket_id, key).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                parsed = urlparse(src)
                if parsed.scheme in ("http", "https"):
                    # Download from remote URL
                    response = requests.get(src, timeout=60)
                    body = response.content
                else:
                    # Open local file
                    with open(src, "rb") as f:
                        body = f.read()

                self.s3_client.put_object(Bucket=self.cdn_bucket_id, Key=key, Body=body)
