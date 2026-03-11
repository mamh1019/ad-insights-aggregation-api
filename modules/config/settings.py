#!/usr/bin/env python
from __future__ import annotations
from pydantic_settings import BaseSettings, SettingsConfigDict
from config.schemas import ENV_PATH


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_PATH, extra="ignore")

    ##############################################################################
    ## ADNetwork API KeysKeys
    ##############################################################################
    adcolony_api_key: str
    appier_api_key: str
    apple_biz_client_id: str
    apple_biz_client_secret: str
    apple_bm_client_id: str
    apple_bm_client_secret: str
    applovin_api_key: str
    applovin_client_id: str
    applovin_client_secret: str
    applovin_account_id: str
    applovin_axon_api_key: str
    appsflyer_api_key: str
    chartboost_sign: str
    chartboost_user: str
    facebook_system_token: str
    facebook_business_id: str
    moloco_id: str
    moloco_pw: str
    moloco_api_key: str
    reddit_app_id: str
    reddit_app_secret: str
    reddit_business_id: str
    tiktok_access_token: str
    tiktok_app_id: str
    tiktok_app_secret: str
    unityads_v2_api_key: str
    unityads_v2_api_secret: str
    unityads_organization_id: str

    adcolony_api_key: str
    admob_publisher_id: str
    publisher_amazon_access: str
    publisher_amazon_secret: str
    bigo_token: str
    bigo_id: str
    facebook_client_id: str
    facebook_client_secret: str
    moloco_pub_email: str
    moloco_pub_pw: str
    moloco_pub_id: str
    unity_pub_api_key: str
    unity_pub_organization_id: str
    pangle_user_id: str
    pangle_role_id: str
    pangle_security_key: str
    pangle_api_key: str
    mintegral_key: str
    mintegral_secret_key: str
    mintegral_api_key: str
    ironsource_secret_key: str
    ironsource_refresh_token: str
    ironsource_auth: str
    inmobi_user_name: str
    inmobi_secret_key: str
    chartboost_user_id: str
    chartboost_sign: str
    fyber_oauth_consumer_key: str
    fyber_oauth_consumer_secret: str
    fyber_oauth_client_id: str
    fyber_oauth_client_secret: str
    tapjoy_api_key: str
    premiumads_token: str
    vungle_api_key: str
    onegram_applovin_api_key: str
    bidmachine_client_id: str
    bidmachine_client_secret: str
    mistplay_secret: str
    mistplay_token: str
    yandex_oauth_token: str


settings = Settings()
