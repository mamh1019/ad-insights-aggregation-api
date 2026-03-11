# Ad Manager CLI v2.1.4

Ads Controller CLI for managing **Advertiser** and **Publisher**
reports.\
This tool helps automate reporting tasks such as cost, creatives, and
revenue by platform/country.

> **Note:** Some features have been removed from this codebase. Use this as a reference for the overall process only.

------------------------------------------------------------------------

## 📌 Features

-   Dynamic discovery of **Advertiser** and **Publisher** modules
-   Configurable **include/exclude** filters for providers
-   Interval handling with smart defaults
-   Reporting:
    -   Advertiser cost reports
    -   Advertiser creatives reports
    -   Publisher platform revenue reports
    -   Publisher country revenue reports
-   Global exclude lists and tail prioritization

------------------------------------------------------------------------

## 🌐 Supported Networks

### Advertisers (11 networks)

-   **Appier** - Cost and creatives reporting
-   **Apple Search Ads** - Cost reporting
-   **AppLovin** - Cost and creatives reporting
-   **Google Ads** - Cost and creatives reporting
-   **Meta (Facebook Ads)** - Cost and creatives reporting
-   **Mintegral** - Cost reporting
-   **Mistplay** - Cost reporting
-   **Moloco** - Cost reporting
-   **Reddit Ads** - Cost reporting
-   **TikTok Ads** - Cost and creatives reporting
-   **Unity Ads** - Cost and creatives reporting

### Publishers (20 networks)

-   **AdColony** - Platform and country revenue reports
-   **AdMob** - Platform and country revenue reports
-   **Amazon** - Platform and country revenue reports
-   **AppLovin** - Platform and country revenue reports
-   **BidMachine** - Platform and country revenue reports
-   **Bigo** - Platform and country revenue reports
-   **Chartboost** - Platform and country revenue reports
-   **Facebook** - Platform and country revenue reports
-   **Fyber** - Platform and country revenue reports
-   **InMobi** - Platform and country revenue reports
-   **IronSource** - Platform and country revenue reports
-   **Mintegral** - Platform and country revenue reports
-   **Moloco** - Platform and country revenue reports
-   **Pangle** - Platform and country revenue reports
-   **PremiumAds** - Platform and country revenue reports
-   **Tapjoy** - Platform and country revenue reports
-   **Unity** - Platform and country revenue reports
-   **Vungle** - Platform and country revenue reports
-   **Yandex** - Platform and country revenue reports

------------------------------------------------------------------------

## ⚙️ Installation

Clone the repository and install dependencies:

``` bash
git clone <repo-url>
cd <repo-folder>
pip install -r requirements.txt
```

Run CLI:

``` bash
python adsctl.py --help
```

------------------------------------------------------------------------

## 🚀 Usage

### Global help

``` bash
python ad_manager.py --help
```

### Advertiser Commands

-   **List advertisers**

``` bash
python ad_manager.py advertiser list
```

-   **Cost report**

``` bash
python ad_manager.py advertiser cost --provider Tiktok --start 2025-09-01 --end 2025-09-02
```

-   **Creatives report**

``` bash
python ad_manager.py advertiser creatives --provider Moloco --list
```

### Publisher Commands

-   **List publishers**

``` bash
python ad_manager.py publisher list
```

-   **Platform report**

``` bash
python ad_manager.py publisher platform --provider Unity --start 2025-09-01 --end 2025-09-02
```

-   **Country report**

``` bash
python ad_manager.py publisher country --exclude Unity
```

------------------------------------------------------------------------

## 🕒 Interval Rules

If no `--start` or `--end` is provided, the default interval is chosen
based on cron schedule:

-   **02:00** → interval = today-2 \~ today-3\
-   **10:00** → interval = today-4 \~ today-5\
-   **14:00** → interval = today-6 \~ today-7\
-   **18:00** → interval = today-8 \~ today-9\
-   Otherwise → last 1-day interval

If only `--start` or `--end` is provided, the other is auto-adjusted to
create a 1-day window.

------------------------------------------------------------------------

## ⚠️ Error Handling

-   Failures trigger logs with stack trace.
-   Excluded providers are listed separately when using `list` commands.

------------------------------------------------------------------------

## 🧩 Project Structure

    ad_manager.py
    libs/
      ├─ ads/
      │   ├─ advertisers/
      │   │   ├─ advertiser.py
      │   │   └─ networks/...
      │   └─ publishers/
      │       ├─ publisher.py
      │       └─ networks/...
      └─ utils/
          ├─ date.py
          ├─ string.py
          ├─ cli.py
          └─ discovery.py

------------------------------------------------------------------------

## 📖 References

------------------------------------------------------------------------
