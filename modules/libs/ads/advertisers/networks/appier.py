#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.settings import settings
from libs.http_client import HttpClient
from libs.utils import clean_json_string, is_empty

import pandas as pd
import json


class Appier(Advertiser):
    def __init__(self):
        super().__init__()

        self.network_name = "Appier"
        self.api_url = "https://mmp.appier.org/inventory_report"
        self.api_key = settings.appier_api_key

    def report_cost(self):
        """Platform must be derived from app_id. Many records have unknown platform."""
        date_range = pd.date_range(start=self.start, end=self.end)
        for date_obj in date_range:
            api_log_date = date_obj.strftime("%Y-%m-%d")
            params = {
                "access_token": self.api_key,
                "start_date": api_log_date,
                "end_date": api_log_date,
                "timezone": 0,
            }
            result = HttpClient.get(self.api_url, params=params)

            try:
                json_str = json.dumps(result)
                split_json_str = (
                    json_str.replace("][", "],[").replace(",,", ",").replace(",]", "]")
                )
                split_json_str = clean_json_string(split_json_str)
                json_list = json.loads(f"[{split_json_str}]")
            except Exception as _:
                continue

            data = [item for sublist in json_list for item in sublist]
            if is_empty(data):
                return

            df = pd.DataFrame(data)
            df["platform"] = df["app_id"].apply(
                lambda x: "android" if x.startswith("com.") else "ios"
            )
            df["country"] = df["geo"].str.upper()
            df = (
                df.groupby(
                    [
                        "date",
                        "app_id",
                        "platform",
                        "country",
                        "campaign_id",
                        "campaign_name",
                    ]
                )
                .agg(
                    {
                        "impressions": "sum",
                        "clicks": "sum",
                        "installs": "sum",
                        "cost": "sum",
                    }
                )
                .reset_index()
            )

            for row in df.itertuples():
                self.push(
                    log_date=row.date.replace("-", ""),
                    app_id=row.app_id,
                    platform=row.platform,
                    country=row.country,
                    media_source=self.network_name,
                    campaign_id=row.campaign_id.replace("-", ""),
                    campaign=row.campaign_name,
                    cost=row.cost,
                    impressions=row.impressions,
                    clicks=row.clicks,
                    conversions_type="INSTALL",
                    conversion_value=int(row.installs),
                )

        self.flush()
