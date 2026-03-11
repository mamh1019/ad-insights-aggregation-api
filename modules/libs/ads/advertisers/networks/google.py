#!/usr/bin/env python
from libs.ads.advertisers.advertiser import Advertiser
from config.constants import PATH
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from libs.utils import is_empty
from libs.utils.cli import log
from datetime import datetime

import boto3
import os
import pandas as pd
import botocore
import requests
from PIL import Image
from io import BytesIO
import io

# version dependency
from google.ads.googleads.v21.enums.types import AppCampaignBiddingStrategyGoalTypeEnum
from google.ads.googleads.v21.enums.types import AssetTypeEnum


class Google(Advertiser):
    def __init__(self):
        credential_path = os.path.join(PATH.CREDENTIAL_ROOT, "google-ads.yaml")
        self.googleads_client = GoogleAdsClient.load_from_storage(
            credential_path, version="v21"
        )
        self.googleads_service = self.googleads_client.get_service("GoogleAdsService")

        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "default"))
        self.s3 = session.resource("s3")
        self.s3_client = boto3.client("s3")
        self.cdn_bucket_id = os.environ.get("CDN_BUCKET_ID", "")
        self.cdn_bucket_key_prefix = "google/creatives"
        self.cdn_bucket = self.s3.Bucket(self.cdn_bucket_id)

        self.ga_service = self.googleads_client.get_service("GoogleAdsService")
        self.network_name = "googleadwords_int"

        self.campaigns = {}
        self.country_df: pd.DataFrame = None

    def get_google_accounts(self):
        try:
            query = """
                SELECT 
                    customer_client.id, customer_client.manager 
                FROM 
                    customer_client 
                WHERE 
                    customer_client.manager = FALSE AND customer_client.status = 'ENABLED'
            """
            mcc_customer_id = os.environ.get("GOOGLE_ADS_MCC_CUSTOMER_ID", "")
            if not mcc_customer_id:
                raise ValueError("GOOGLE_ADS_MCC_CUSTOMER_ID env var required")
            results = self.ga_service.search(customer_id=mcc_customer_id, query=query)

            accounts = []
            for row in results:
                accounts.append(row.customer_client.id)

            return accounts
        except GoogleAdsException as ex:
            log(
                'Request with ID "%s" failed with status "%s" and includes the '
                "following errors:" % (ex.request_id, ex.error.code().name)
            )
            for error in ex.failure.errors:
                log('\tError with message "%s".' % error.message)
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        log("\t\tOn field: %s" % field_path_element.field_name)
            raise Exception("Google Ads API Error") from GoogleAdsException

    def get_google_campaigns(self, accounts):
        try:
            if len(self.campaigns) > 0:
                return self.campaigns

            query = (
                "SELECT campaign.id, campaign.status FROM campaign ORDER BY campaign.id"
            )

            for account in accounts:
                account_id = str(account)
                results = self.ga_service.search(customer_id=account_id, query=query)
                self.campaigns[account_id] = []

                for row in results:
                    if row.campaign.status != 4:
                        self.campaigns[account_id].append(row.campaign.id)

            return self.campaigns
        except GoogleAdsException as ex:
            log(
                'Request with ID "%s" failed with status "%s" and includes the '
                "following errors:" % (ex.request_id, ex.error.code().name)
            )
            for error in ex.failure.errors:
                log('\tError with message "%s".' % error.message)
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        log("\t\tOn field: %s" % field_path_element.field_name)
            raise Exception("Google Ads API Error") from GoogleAdsException

    def get_cost_reports(self, campaigns):
        try:
            reports = []
            criterions = []

            for account in campaigns.keys():
                # Skip when no campaigns exist
                if len(campaigns[account]) == 0:
                    continue

                query = """
                    SELECT
                        campaign.id,
                        campaign.name,
                        campaign.status,
                        campaign.app_campaign_setting.app_id,
                        campaign.app_campaign_setting.app_store,
                        campaign.app_campaign_setting.bidding_strategy_goal_type,
                        metrics.clicks,
                        metrics.conversions,
                        metrics.conversions_value,
                        metrics.cost_micros,
                        metrics.impressions,
                        segments.date,
                        user_location_view.country_criterion_id
                    FROM
                        user_location_view
                    WHERE
                        segments.date >= '%s'
                        and segments.date <= '%s'
                        and campaign.id IN (%s)
                """ % (
                    self.start,
                    self.end,
                    ",".join(map(str, campaigns[account])),
                )

                results = self.ga_service.search(customer_id=account, query=query)
                for row in results:
                    criterion_id = row.user_location_view.country_criterion_id
                    cost = (
                        round(row.metrics.cost_micros / 1000000, 3)
                        if row.metrics.cost_micros
                        else 0
                    )
                    log_date = row.segments.date.replace("-", "", 2)
                    reports.append(
                        {
                            "resource_name": row.user_location_view.resource_name,
                            "log_date": log_date,
                            "app_id": row.campaign.app_campaign_setting.app_id,
                            "media_source": self.network_name,
                            "campaign_id": row.campaign.id,
                            "campaign": row.campaign.name,
                            "cost": cost,
                            "installs": (
                                row.metrics.conversions
                                if row.metrics.conversions
                                else 0
                            ),
                            "purchase": 0,
                            "impressions": (
                                row.metrics.impressions
                                if row.metrics.impressions
                                else 0
                            ),
                            "clicks": row.metrics.clicks if row.metrics.clicks else 0,
                            "criterion_id": criterion_id,
                            "conversions_type": (
                                "INSTALL"
                                if row.campaign.app_campaign_setting.bidding_strategy_goal_type
                                == AppCampaignBiddingStrategyGoalTypeEnum.AppCampaignBiddingStrategyGoalType.OPTIMIZE_INSTALLS_TARGET_INSTALL_COST
                                else "PURCHASE"
                            ),
                        }
                    )

                    criterions.append(str(criterion_id))

                # Fetch installs separately and merge
                query = """
                    SELECT
                        campaign.id,
                        metrics.conversions,
                        segments.date,
                        segments.conversion_action_category
                    FROM
                        user_location_view
                    WHERE
                        segments.date >= '%s'
                        and segments.date <= '%s'
                        and campaign.id IN (%s)
                """ % (
                    self.start,
                    self.end,
                    ",".join(map(str, campaigns[account])),
                )

                results = self.ga_service.search(customer_id=account, query=query)

                for row in results:
                    for item in reports:
                        if item[
                            "resource_name"
                        ] == row.user_location_view.resource_name and item[
                            "log_date"
                        ] == row.segments.date.replace(
                            "-", "", 2
                        ):
                            conversions_value = row.metrics.conversions
                            # https://developers.google.com/google-ads/api/reference/rpc/v12/AppCampaignBiddingStrategyGoalTypeEnum.AppCampaignBiddingStrategyGoalType
                            if (
                                row.segments.conversion_action_category
                                == AppCampaignBiddingStrategyGoalTypeEnum.AppCampaignBiddingStrategyGoalType.OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST
                            ):
                                item["purchase"] = conversions_value
                                item["installs"] = 0
                            else:
                                item["install"] = conversions_value
                                item["installs"] = 0
                            break

            # No data case
            if len(criterions) <= 0:
                return False

            report_df = pd.DataFrame(reports).fillna(0)
            report_df["country"] = "ZZ"

            # Country code lookup query
            criterions_set = "'" + "','".join(set(criterions)) + "'"
            geo_query = (
                "SELECT geo_target_constant.id, geo_target_constant.country_code FROM geo_target_constant WHERE geo_target_constant.id IN ( %s )"
                % criterions_set
            )
            mcc_customer_id = os.environ.get("GOOGLE_ADS_MCC_CUSTOMER_ID", "")
            if not mcc_customer_id:
                raise ValueError("GOOGLE_ADS_MCC_CUSTOMER_ID env var required")
            geo_results = self.ga_service.search(
                customer_id=mcc_customer_id, query=geo_query
            )

            # Add country code to row
            for row in geo_results:
                criterion_id = row.geo_target_constant.id
                country_code = row.geo_target_constant.country_code
                report_df.loc[report_df.criterion_id == criterion_id, "country"] = (
                    country_code
                )

            report_df = report_df[report_df.country != "ZZ"]
            report_df = (
                report_df.groupby(
                    [
                        "log_date",
                        "app_id",
                        "country",
                        "media_source",
                        "campaign_id",
                        "campaign",
                        "conversions_type",
                    ]
                )
                .sum()
                .reset_index()
            )

            for _, report in report_df.iterrows():
                if is_empty(report["app_id"]):
                    # web landing campaign
                    continue

                platform = self.suppose_platform(
                    report["app_id"], report["media_source"]
                )
                conversions_value = report["purchase"] if "purchase" in report else 0

                self.push(
                    log_date=report["log_date"],
                    app_id=report["app_id"],
                    platform=platform,
                    country=report["country"],
                    media_source=report["media_source"],
                    campaign_id=report["campaign_id"],
                    campaign=report["campaign"],
                    cost=report["cost"],
                    impressions=report["impressions"],
                    clicks=report["clicks"],
                    conversions_type=report["conversions_type"],
                    conversion_value=conversions_value,
                )

            self.flush()
        except GoogleAdsException as ex:
            log(
                'Request with ID "%s" failed with status "%s" and includes the '
                "following errors:" % (ex.request_id, ex.error.code().name)
            )
            for error in ex.failure.errors:
                log('\tError with message "%s".' % error.message)
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        log("\t\tOn field: %s" % field_path_element.field_name)
            raise Exception("Google Ads API Error") from GoogleAdsException

    def report_cost(self):
        accounts = self.get_google_accounts()
        campaigns = self.get_google_campaigns(accounts)
        self.get_cost_reports(campaigns)

    ##############################################################################
    ## Creative
    ##############################################################################
    def update_country(self, df) -> pd.DataFrame:
        """
        # Google creative has no country info; fill from ad_cost_report
        """
        import numpy as np

        if isinstance(self.country_df, pd.DataFrame) and len(self.country_df.index) > 0:
            return self.country_df

        # TODO: Fetch app_store_keys from your data model (app_id -> app_name mapping)
        app_list = []
        bundle_id_dict = {}
        for app_info in app_list:
            if app_info["aos_store_id"] is not None:
                bundle_id_dict[app_info["aos_store_id"]] = app_info["app_name"]
            if app_info["ios_store_id"] is not None:
                bundle_id_dict[app_info["ios_store_id"]] = app_info["app_name"]

        df["app_name"] = df["app_id"].apply(lambda x: bundle_id_dict.get(x, ""))

        start = df["log_date"].min()
        end = df["log_date"].max()
        media_sources = list(pd.unique(df["media_source"]))

        # TODO: Fetch ad_cost_report from your data model (start, end, media_sources)
        ad_cost_report_df = pd.DataFrame()
        ad_cost_report_dict = {}
        for _, row in ad_cost_report_df.iterrows():
            key = "{}_{}_{}_{}".format(
                row["platform"],
                row["media_source"],
                row["campaign_id"],
                row["campaign"],
            )
            if key not in ad_cost_report_dict:
                ad_cost_report_dict[key] = []
            if row["country"] in row["campaign"]:
                ad_cost_report_dict[key].append(row["country"])
            else:
                ad_cost_report_dict[key].append("WW")

        for key in ad_cost_report_dict:
            ad_cost_report_dict[key] = list(set(ad_cost_report_dict[key]))
            if len(ad_cost_report_dict[key]) == 1:
                ad_cost_report_dict[key] = ad_cost_report_dict[key][0]
            else:
                ad_cost_report_dict[key] = "WW"

        df["country_tmp"] = df.apply(
            lambda x: "{}_{}_{}_{}".format(
                x["platform"], x["media_source"], x["campaign_id"], x["campaign_name"]
            ),
            axis=1,
        )

        df["country"] = df["country_tmp"].apply(
            lambda x: ad_cost_report_dict[x] if x in ad_cost_report_dict else "ZZ"
        )
        df["country"] = np.where(
            df.campaign_name.str.contains("_WW_"), "WW", df.country
        )
        df["country"] = np.where(
            df.campaign_name.str.contains("\+"), "WW", df.country
        )  # UK_GA_AOS_SG+HK+MO_AIO_230203
        df["country"] = np.where(
            df.campaign_name.str.contains("/"), "WW", df.country
        )  # ZJ_GA_AOS_GB/CA_ACI_230203
        df["country"] = np.where(
            df.campaign_name.str.contains("_Page_Like_"), "WW", df.country
        )
        df.drop(columns=["country_tmp"], inplace=True)

        # Mapping failures above. ad_cost_report data may not have arrived yet.
        df_tmp = df.query("country == 'ZZ'")
        if isinstance(df_tmp, pd.DataFrame) and len(df_tmp.index) > 0:
            max_date = df_tmp["log_date"].max()
            today = datetime.now().strftime("%Y%m%d")
            if int(today) - int(max_date) >= 2:  # Wait at least 2 days
                campaign_ids = df_tmp[["campaign_id"]].drop_duplicates().values.tolist()
                log(
                    {
                        "type": "Google Creative",
                        "contents": "not exist campaign in ad_cost_report",
                        "df_max_date": max_date,
                        "today": today,
                        "campaign_ids": campaign_ids,
                    }
                )
        return df.query("country != 'ZZ'")

    def get_ad_group_asset(self, accounts):
        date_list = pd.date_range(start=self.start, end=self.end)
        for date_object in date_list:
            ad_network_creatives = []
            for account in accounts:
                ad_network_creatives += self.extract_creative(account, date_object)

            if len(ad_network_creatives) > 0:
                df = pd.DataFrame.from_records(ad_network_creatives)
                df = self.update_country(df)
                # Persist ad_cost_creatives to model

                log(
                    f"[{date_object.strftime('%Y%m%d')}] creatives insert {len(df)} rows"
                )

    def extract_creative(self, account, date_object) -> list:
        date_cursor = date_object.strftime("%Y-%m-%d")
        ym = date_object.strftime("%Y%m")

        request = self.googleads_client.get_type("SearchGoogleAdsRequest")
        request.customer_id = str(account)
        request.query = """
            SELECT 
                ad_group_ad_asset_view.asset, 
                ad_group_ad_asset_view.enabled, 
                campaign.app_campaign_setting.app_id,
                campaign.app_campaign_setting.bidding_strategy_goal_type,
                campaign.id, 
                campaign.name, 
                campaign.status, 
                ad_group.id,
                ad_group.name,
                asset.call_asset.country_code, 
                asset.id, 
                asset.name,
                asset.image_asset.full_size.url, 
                asset.resource_name, 
                asset.type, 
                asset.youtube_video_asset.youtube_video_id, 
                asset.youtube_video_asset.youtube_video_title, 
                metrics.impressions, 
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.biddable_app_install_conversions
            FROM ad_group_ad_asset_view 
            WHERE segments.date = '{date_cursor}' AND metrics.impressions > 0 AND asset.type IN ('IMAGE','YOUTUBE_VIDEO') 
        """.format(
            date_cursor=date_cursor
        )

        ad_network_creatives = []

        try:
            iterator = self.googleads_service.search(request)
            for row in iterator:
                # Minimum unit adset can fetch (ad level)
                campaign = row.campaign
                ad_group = row.ad_group
                asset = row.asset
                metrics = row.metrics

                #
                app_id = campaign.app_campaign_setting.app_id
                campaign_id = campaign.id
                campaign_name = campaign.name
                ad_group_id = ad_group.id
                ad_group_name = ad_group.name
                asset_id = asset.id
                asset_type = asset.type_

                if asset_type == AssetTypeEnum.AssetType.IMAGE:
                    asset_type = "IMAGE"
                    asset_name = asset.name
                    resource_url = asset.image_asset.full_size.url

                    resource_uri = "{}/{}/{}/{}/{}".format(
                        self.cdn_bucket_key_prefix,
                        ym,
                        "image",
                        "resource",
                        f"{asset_id}.jpeg",
                    )
                    self.load_to_s3_if_not_exists(resource_url, resource_uri)

                    #
                    thumbnail_uri = "{}/{}/{}/{}/{}".format(
                        self.cdn_bucket_key_prefix,
                        ym,
                        "image",
                        "thumbnail",
                        f"{asset_id}.jpeg",
                    )

                    try:
                        self.s3.Object(self.cdn_bucket_id, thumbnail_uri).load()
                    except botocore.exceptions.ClientError as e:
                        if e.response["Error"]["Code"] == "404":
                            thumbnail = requests.get(
                                resource_url, stream=True, timeout=10
                            )
                            thumbnail_image = Image.open(BytesIO(thumbnail.content))
                            pil_image = thumbnail_image.resize((64, 64))
                            if pil_image.mode == "RGBA":
                                pil_image = pil_image.convert("RGB")
                            in_mem_file = io.BytesIO()
                            pil_image.save(in_mem_file, format="jpeg")
                            in_mem_file.seek(0)

                            self.s3_client.upload_fileobj(
                                in_mem_file,
                                self.cdn_bucket_id,
                                thumbnail_uri,
                                ExtraArgs={"ACL": "private"},
                            )

                elif asset_type == AssetTypeEnum.AssetType.YOUTUBE_VIDEO:
                    asset_type = "VIDEO"
                    asset_name = asset.youtube_video_asset.youtube_video_title
                    asset_id = asset.youtube_video_asset.youtube_video_id
                    resource_uri = "watch?v={video_id}".format(
                        video_id=row.asset.youtube_video_asset.youtube_video_id
                    )
                    thumbnail_uri = "vi/{video_id}/default.jpg".format(
                        video_id=asset.youtube_video_asset.youtube_video_id
                    )
                else:
                    continue

                clicks = metrics.clicks
                impressions = metrics.impressions
                conversions = metrics.conversions
                installs = (
                    metrics.biddable_app_install_conversions
                )  # https://developers.google.com/google-ads/api/fields/v10/customer#metrics.biddable_app_install_conversions
                cost = round(metrics.cost_micros / 1000000, 3)

                platform = self.suppose_platform(app_id, self.network_name)

                # TODO: Fetch ad_cost_creatives from your data model (asset_id, network_name)
                pre_creative = None
                if pre_creative is not None:
                    start_date = min(
                        int(date_object.strftime("%Y%m%d")),
                        int(pre_creative["start_date"]),
                    )
                else:
                    start_date = date_object.strftime("%Y%m%d")

                ad_network_creatives.append(
                    {
                        "log_date": date_object.strftime("%Y%m%d"),
                        "app_id": app_id,
                        "media_source": self.network_name,
                        "platform": platform,
                        "country": "ZZ",
                        "campaign_id": campaign_id,
                        "campaign_name": campaign_name,
                        "adset_id": ad_group_id,
                        "adset_name": ad_group_name,
                        "ad_id": "",
                        "creative_id": asset_id,
                        "creative_type": asset_type,
                        "creative_name": asset_name,
                        "start_date": start_date,
                        "creative_uri": resource_uri,
                        "creative_uri_expire_time": 0,
                        "thumbnail_uri": thumbnail_uri,
                        "impressions": impressions,
                        "cost": cost,
                        "clicks": clicks,
                        "installs": installs,
                        "conversions": conversions,
                    }
                )

        except GoogleAdsException as ex:
            log(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" and includes the following errors:'
            )
            for error in ex.failure.errors:
                log(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        log(f"\t\tOn field: {field_path_element.field_name}")

        return ad_network_creatives

    def report_creative(self):
        accounts = self.get_google_accounts()
        self.get_ad_group_asset(accounts)
