#!/usr/bin/env python
import hashlib
import traceback
import json
from libs.ads.publishers.publisher import Publisher
from config.constants import (
    AD_NETWORK_ID,
    PUBLISHER_PLATFORM,
    AD_FORMAT,
    REPORT_TYPE,
)
from config.settings import settings
from libs.utils.date import timestamp
from libs.utils.cli import log
from libs.http_client import HttpClient


class Mintegral(Publisher):
    def __init__(self) -> None:
        super().__init__()

        self.ad_network_id = AD_NETWORK_ID.MINTEGRAL
        self.key = settings.mintegral_key
        self.secret_key = settings.mintegral_secret_key
        self.api_url = "https://api.mintegral.com/reporting/data?{}&sign={}"

    def _build_api_url(self, group_by: list, page: int = 0):
        start = self.start.replace("-", "")
        end = self.end.replace("-", "")
        group_by = "%2C".join(group_by)
        now = timestamp()

        # alpha order
        if page <= 0:
            query_string = (
                f"end={end}&group_by={group_by}&skey={self.key}&start={start}"
                f"&time={now}&timezone=0"
            )
        else:
            query_string = (
                f"end={end}&group_by={group_by}&limit=1000&page={page}"
                f"&skey={self.key}&start={start}&time={now}&timezone=0"
            )

        md5_hash = hashlib.md5()
        md5_hash.update(query_string.encode("utf-8"))

        secret_md5_hash = hashlib.md5()
        hash_head = (md5_hash.hexdigest() + self.secret_key).encode("utf-8")
        secret_md5_hash.update(hash_head)
        sign_key = secret_md5_hash.hexdigest()

        return self.api_url.format(query_string, sign_key)

    def report_platform(self):
        api_url = self._build_api_url(["date", "app_id", "platform"])
        try:
            result = HttpClient.get(api_url)
        except Exception as e:
            message = "Mintegral Platform Reporter Failed\n" + str(e)
            traceback.print_exc()
            log(message)
            return

        if "code" not in result:
            log("Mintegral report api error")
            log(result)
            return

        if result["code"] != "ok":
            raise Exception("Mintegral report api error")

        for row in result["data"]["lists"]:
            self.push(
                REPORT_TYPE.PLATFORM,
                self.ad_network_id,
                row["date"],
                row["app_id"],
                row["app_name"],
                PUBLISHER_PLATFORM.standardization(row["platform"]),
                row["impression"],
                round(row["est_revenue"], 6),
            )

        self.flush()

    def report_country(self):
        api_url = self._build_api_url(
            ["date", "app_id", "platform", "country", "unit_id"], page=1
        )
        try:
            result = HttpClient.get(api_url)
        except Exception as e:
            message = "Mintegral Country Reporter Failed\n" + str(e)
            traceback.print_exc()
            log(message)
            return

        if result["code"] != "ok":
            raise Exception(json.dumps(result))

        for row in result["data"]["lists"]:
            self._push_country_row(row)

        total_pages = int(result["data"]["total_page"])
        if total_pages > 1:
            for page in range(1, total_pages + 1):
                api_url = self._build_api_url(
                    ["date", "app_id", "platform", "country", "unit_id"], page=page
                )
                result = HttpClient.get(api_url)

                if "code" not in result:
                    log("Mintegral report api error")
                    log(result)
                    return

                if result["code"] != "ok":
                    raise Exception("Mintegral report api error")

                for row in result["data"]["lists"]:
                    self._push_country_row(row)

        self.flush()

    def _push_country_row(self, row):
        ad_format = AD_FORMAT.standardization(row["ad_format"])
        if ad_format != AD_FORMAT.NONE:
            self.push(
                REPORT_TYPE.COUNTRY,
                self.ad_network_id,
                row["date"],
                row["app_id"],
                row["app_name"],
                PUBLISHER_PLATFORM.standardization(row["platform"]),
                row["impression"],
                round(row["est_revenue"], 6),
                ad_format=AD_FORMAT.standardization(row["ad_format"]),
                sub_format=row["ad_format"],
                country=row["country"],
            )
