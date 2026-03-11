#!/usr/bin/env python
from libs.ads.publishers.publisher import Publisher
from config.constants import AD_NETWORK_ID, AD_FORMAT
from config.settings import settings
from libs.country import find_by_name
from config.constants import REPORT_TYPE
from datetime import datetime, timedelta
from pytz import timezone

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import io
import boto3
import pandas as pd


class Amazon(Publisher):
    def __init__(self) -> None:
        self.ad_network_id = AD_NETWORK_ID.AMAZON
        # APS policy: S3 bucket access key expires in 90 days. Rotate every 60-90 days
        self.client_access = settings.publisher_amazon_access
        self.client_secret = settings.publisher_amazon_secret
        self.aps_stream_s3_up_to_days = (
            4
            + 1  # https://ams.amazon.com/webpublisher/uam/docs/aps-mobile/aps-reporting-mobile
        )
        self.aps_stream_s3_df = pd.DataFrame()

        # https://ams.amazon.com/webpublisher/analytics/custom_reports
        # stream s3.
        pub_uuid = "xxx"
        stream_id = "xxx"
        version_id = "xxx"

        self.s3_bucket_id = "aps-reporting-mobile"
        self.s3_bucket_home = f"aps-download-publisher-{pub_uuid}"
        self.s3_report_dir = (
            f"{self.s3_bucket_home}/streamId={stream_id}/versionId={version_id}"
        )

        # self.session = boto3.Session(profile_name="ams")
        self.session = boto3.Session(
            aws_access_key_id=self.client_access,
            aws_secret_access_key=self.client_secret,
        )
        self.s3 = self.session.resource(service_name="s3")
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_id)
        self.s3_client = self.session.client(service_name="s3")

    def download_object(self, key) -> pd.DataFrame:
        remote_obj = self.s3_client.get_object(Bucket=self.s3_bucket_id, Key=key)
        df = pd.read_csv(io.BytesIO(remote_obj["Body"].read()))
        return df

    def download_parallel_multithreading(self, keys: list):
        """
        # S3 API calls for schedule: 5 days (stream S3 retention) * 24 (hourly dirs)
        # Use multithreading due to network latency
        ref) https://www.learnaws.org/2022/10/12/boto3-download-multiple-files-s3/
        """

        # Dispatch work tasks with our s3_client
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_key = {
                executor.submit(self.download_object, key): key for key in keys
            }

            for future in futures.as_completed(future_to_key):
                key = future_to_key[future]
                exception = future.exception()

                if not exception:
                    yield key, future.result()
                else:
                    yield key, exception

    def extract_report(self) -> pd.DataFrame:
        result = pd.DataFrame()
        if len(self.aps_stream_s3_df.index) > 0:
            return self.aps_stream_s3_df

        start_date = datetime.today()
        start_date -= timedelta(days=self.aps_stream_s3_up_to_days)
        end_date = datetime.today()
        date_list = pd.date_range(start=start_date, end=end_date)

        for date_object in date_list:
            log_date = date_object.strftime("%Y%m%d")

            # UTC to PST because report timezone is PST
            temp_time = datetime.strptime(f"{log_date}", "%Y%m%d")
            utc_timezone = timezone("UTC")
            utc_time = utc_timezone.localize(temp_time, is_dst=None)
            pst_time = utc_time.astimezone(timezone("US/Pacific"))

            pst_date = pst_time.strftime("%Y%m%d")
            objs = list(
                self.s3_bucket.objects.filter(
                    Prefix=f"{self.s3_report_dir}/date={pst_date}/"
                )
            )

            keys = [obj.key for obj in objs]
            for key, df in self.download_parallel_multithreading(keys):
                # for obj in objs:
                #     key = obj.key
                #     remote_obj = self.s3_client.get_object(Bucket=self.s3_bucket_id, Key=key)
                #     df = pd.read_csv(io.BytesIO(remote_obj["Body"].read()))
                if isinstance(df, pd.DataFrame) == False:
                    continue

                df = df[~df["Device OS"].isna()]
                if df.empty:
                    continue

                df.rename(
                    columns={
                        "App Name": "app_name",  #
                        "App Store ID": "bundle_id",  # 1501654819
                        "Browser Family": "brower_family",  # Chromium
                        "Country": "country",  # US
                        "Device OS": "platform",  # iOS
                        "Device Type": "device_type",  # Phone
                        "Inventory Format": "inventory_format",  # Outstream
                        "Inventory Type": "inventory_type",  # Web
                        "Slot Size": "slot_size",  # 320x480
                        "Slot UUID": "slot_uuid",  # f8fd6518-5b58-41ee-872b-d00c41ce864a
                        "Advertiser Domain": "advertiser_domain",  # example.com
                        "Creative Category": "creative_category",  # IAB22
                        "Payment Relationship": "payment_relationship",  # UAM
                        "Price Point": "price_point",  # 0.4
                        "Request Media Type": "request_media_type",  # Video
                        "Response Media Type": "response_media_type",  # Video
                        "Floor Id": "floor_id",  #
                        "Floor Name": "floor name",  #
                        "Floor Type": "floor type",  # Pricepoint
                        "Impressions": "impressions",  # 1
                        "Earnings": "earnings",  # 0.01
                        "CPM": "cpm",  # 7.4
                    },
                    inplace=True,
                )

                df["platform"] = df["platform"].str.lower()
                df.loc[df["platform"] == "fireos", "platform"] = "amazon"

                # report pst time to utc
                split = key.split("/")
                pst_date = split[3].split("=")[1]
                pst_hour = split[4].split("=")[1]

                temp_time = datetime.strptime(f"{pst_date} {pst_hour}", "%Y%m%d %H")
                pst_timezone = timezone("US/Pacific")
                pst_time = pst_timezone.localize(temp_time, is_dst=False)
                utc_time = pst_time.astimezone(timezone("UTC"))

                df["log_date"] = utc_time.strftime("%Y%m%d")
                result = pd.concat([result, df])

        self.aps_stream_s3_df = result
        return self.aps_stream_s3_df

    def report_platform(self):
        super().report_platform()

        # extract
        df = self.extract_report()
        if isinstance(df, pd.DataFrame) == False:
            return
        if df.empty == True:
            return

        # transform
        df = (
            df.query("impressions > 0")
            .query("(platform != 'Unknown') & (platform != 'unknown')")
            .groupby(by=["log_date", "bundle_id", "platform"], as_index=False)
            .agg({"impressions": "sum", "earnings": "sum"})
        )

        # loading
        for _, row in df.iterrows():
            # TODO: Fetch app_info from your data model (bundle_id -> app_id, app_name)
            # Replace with your actual database/model query
            app_info = {"app_id": "", "app_name": ""}
            app_name = app_info["app_name"]
            platform = row["platform"]

            # TODO: Fetch app_ad_network_keys from your data model (app_id, ad_network_id, platform)
            # Replace with your actual database/model query
            ad_network_keys = []
            if len(ad_network_keys) == 0:
                continue
            if len(ad_network_keys) > 1:
                continue

            ad_network_key = ad_network_keys[0]
            app_key = ad_network_key["key"]

            self.push(
                REPORT_TYPE.PLATFORM,
                self.ad_network_id,
                row["log_date"],
                app_key,
                app_name,
                platform,
                row["impressions"],
                round(row["earnings"], 6),
            )

        self.flush()

    def report_country(self):
        df = self.extract_report()
        if isinstance(df, pd.DataFrame) == False:
            return
        if df.empty == True:
            return

        # transform
        df = df.query("impressions > 0").query(
            "(platform != 'Unknown') & (platform != 'unknown')"
        )
        df = df.groupby(
            by=["log_date", "bundle_id", "platform", "country", "response_media_type"],
            as_index=False,
        ).agg({"impressions": "sum", "earnings": "sum"})
        df["country"] = df["country"].apply(find_by_name)
        df = df.groupby(
            by=["log_date", "bundle_id", "platform", "country", "response_media_type"],
            as_index=False,
        ).agg({"impressions": "sum", "earnings": "sum"})

        # loading
        cached_app_info = {}
        for _, row in df.iterrows():
            if row["bundle_id"] in cached_app_info:
                app_info = cached_app_info[row["bundle_id"]]
            else:
                # TODO: Fetch app_info from your data model (bundle_id -> app_id, app_name)
                # Replace with your actual database/model query
                app_info = {"app_id": "", "app_name": ""}
                cached_app_info[row["bundle_id"]] = app_info

            app_name = app_info["app_name"]
            platform = row["platform"]
            country = row["country"]
            if country is None:
                continue
            ad_format = row["response_media_type"]

            # TODO: Fetch app_ad_network_keys from your data model (app_id, ad_network_id, platform)
            # Replace with your actual database/model query
            ad_network_keys = []
            if len(ad_network_keys) == 0:
                continue
            if len(ad_network_keys) > 1:
                continue

            ad_network_key = ad_network_keys[0]
            app_key = ad_network_key["key"]

            sub_format = ad_format
            ad_format = AD_FORMAT.standardization(ad_format)
            if ad_format is AD_FORMAT.NONE and sub_format == "Video":
                ad_format = AD_FORMAT.REWARDED_VIDEO

            self.push(
                REPORT_TYPE.COUNTRY,
                self.ad_network_id,
                row["log_date"],
                app_key,
                app_name,
                platform,
                row["impressions"],
                round(row["earnings"], 6),
                ad_format=ad_format,
                sub_format=sub_format,
                country=country,
            )

        self.flush()
