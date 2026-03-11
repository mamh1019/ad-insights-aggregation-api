#!/usr/bin/env python
from enum import Enum
import os


class PATH:
    # project src
    APP_ROOT = os.path.dirname(os.path.realpath(__file__)) + "/../../"
    # credentials
    CREDENTIAL_ROOT = os.path.join(APP_ROOT, "credentials")


class COMMON:
    CDN_DOMAIN = "https://xxx.cloudfront.net"


class DB_TYPE(str, Enum):
    MYSQL = "mysql"
    POSTGRES = "postgres"


class DATACENTER:
    AWS_PROFILE_NAME = os.environ.get("AWS_PROFILE", "default")
    DW_ROOT_DIR = os.environ.get("AWS_S3_ROOT_DIR", "")

    class CREDENTIALS:
        # S3 and Athena configurations should be provided via environment variables
        # or configured through aws_athena_helper DataCenter class
        S3 = None  # Configure via aws_athena_helper or environment variables
        ATHENA = None  # Configure via aws_athena_helper or environment variables
        pass


class CREATIVE_TYPE:
    PLAYABLE = "PLAYABLE"
    VIDEO = "VIDEO"
    IMAGE = "IMAGE"


class CONVERSION_TYPE:
    INSTALL = "INSTALL"
    PURCHASE = "PURCHASE"


class URL:
    NO_IMAGE = "https://xxx.cloudfront.net/noimage.png"


class PLATFORM:
    ANDROID = "android"
    IOS = "ios"
    AMAZON = "amazon"
    ONESTORE = "onestore"
    FACEBOOK = "f"


class REPORT_TYPE(str, Enum):
    PLATFORM = "report_platform"
    COUNTRY = "report_country"


class AD_FORMAT:
    INTERSTITIAL = "Interstitial"
    REWARDED_VIDEO = "Rewarded Video"
    BANNER = "Banner"
    NATIVE = "Native"
    OFFERWALL = "Offerwall"
    APP_OPEN = "App Open"
    NONE = "None"

    @staticmethod
    def standardization(origin: str):
        format_key = origin.lower()
        ad_format = AD_FORMAT.NONE

        if "interstitial" in format_key:
            ad_format = AD_FORMAT.INTERSTITIAL
        elif format_key in ["app_open", "appopen", "app open"]:
            ad_format = AD_FORMAT.APP_OPEN
        elif "offerwall" in format_key:
            ad_format = AD_FORMAT.OFFERWALL
        elif "directplay" in format_key:
            ad_format = AD_FORMAT.REWARDED_VIDEO
        elif "unityads_int" in format_key:
            ad_format = AD_FORMAT.INTERSTITIAL
        elif "adcolony_int" in format_key:
            ad_format = AD_FORMAT.INTERSTITIAL
        elif "mopub_int" in format_key:
            ad_format = AD_FORMAT.INTERSTITIAL
        elif "reward" in format_key:
            ad_format = AD_FORMAT.REWARDED_VIDEO
        elif "unityads_ba" in format_key:
            ad_format = AD_FORMAT.BANNER
        elif "banner" in format_key:
            ad_format = AD_FORMAT.BANNER
        elif "rv" in format_key:
            ad_format = AD_FORMAT.REWARDED_VIDEO
        elif "rw" in format_key:
            ad_format = AD_FORMAT.REWARDED_VIDEO
        elif "inter" in format_key:
            ad_format = AD_FORMAT.INTERSTITIAL
        elif "native" in format_key:
            ad_format = AD_FORMAT.NATIVE
        elif "is" in format_key:
            ad_format = AD_FORMAT.INTERSTITIAL
        elif "video" in format_key and "reward" in format_key:
            # Unity: "video" is interstitial. Avoid overlap with reward video.
            ad_format = AD_FORMAT.INTERSTITIAL
        elif "rectangle" in format_key:
            # medium rectangle -> mrec banner
            ad_format = AD_FORMAT.BANNER
        elif "mrec" in format_key:
            ad_format = AD_FORMAT.BANNER
        elif "android_max_inmobi_bidding" in format_key:
            ad_format = AD_FORMAT.BANNER
        elif "video" in format_key:
            ad_format = AD_FORMAT.REWARDED_VIDEO
        elif "fullscreen" in format_key:
            ad_format = AD_FORMAT.INTERSTITIAL
        elif "fullscreen_rewarded" in format_key:
            ad_format = AD_FORMAT.REWARDED_VIDEO

        return ad_format


class AD_NETWORK_ID:
    # @deprecated ADMOB = 1
    FACEBOOK = 2
    IRONSOURCE = 3
    UNITY = 4
    VUNGLE = 5
    CHARTBOOST = 6
    ADCOLONY = 7
    MOPUB = 8
    ADSENSE = 9
    APPLOVIN = 10
    LIFESTREET = 11
    INMOBI = 12
    AMAZON = 13
    MINTEGRAL = 17
    ADMOB = 18
    ADMOB_OPENBIDDING = {}
    FYBER = 30
    TABJOY = 31
    PANGLE = 32
    SMAATO = 33
    REKLAMUP = 39
    PREMIUMADS = 40
    YANDEX = 41
    DIGITAL_TURBINE = 42
    MOLOCO = 43
    LINE = 44
    BIGO = 45
    BIDMACHINE = 46


class PUBLISHER_PLATFORM:
    @staticmethod
    def standardization(origin: str):
        ret = "unknown"
        origin = origin.lower()
        if "google" in origin or "aos" in origin or "android" in origin:
            ret = "android"
        elif "iphone" in origin or "ios" in origin or "ipad" in origin:
            ret = "ios"
        elif (
            "aws" in origin
            or "amazon" in origin
            or "amz" in origin
            or "fireos" in origin
        ):
            ret = "amazon"
        elif "f" in origin:
            ret = "f"
        return ret
