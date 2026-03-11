#!/usr/bin/env python
from abc import ABCMeta, abstractmethod
from typing import Dict, List
from config.constants import REPORT_TYPE
from libs.utils.common import is_empty


class Publisher(metaclass=ABCMeta):
    # Extract date range
    start: str = None
    end: str = None

    # Data buffer
    _buffer: Dict[REPORT_TYPE, List[Dict]] = {
        REPORT_TYPE.PLATFORM: [],
        REPORT_TYPE.COUNTRY: [],
    }

    def set_interval(self, start: str, end: str):
        self.start = start
        self.end = end

    @abstractmethod
    def report_platform(self):
        pass

    @abstractmethod
    def report_country(self):
        pass

    def push(
        self,
        report_type: REPORT_TYPE,
        ad_network_id: int,
        log_date,
        app_key,
        app_name,
        platform,
        impressions,
        revenue,
        ad_format=None,
        sub_format=None,
        country=None,
    ):
        app_name = ""  # Omit for size
        report = {}
        if report_type == REPORT_TYPE.PLATFORM:
            report = {
                "log_date": log_date,
                "app_key": app_key,
                "app_name": app_name,
                "platform": platform,
                "impressions": impressions,
                "revenue": revenue,
                "ad_network_id": ad_network_id,
            }
        elif report_type == REPORT_TYPE.COUNTRY:
            report = {
                "log_date": log_date,
                "app_key": app_key,
                "app_name": app_name,
                "platform": platform,
                "impressions": impressions,
                "revenue": revenue,
                "format": ad_format,
                "sub_format": sub_format,
                "country": country,
                "ad_network_id": ad_network_id,
            }

        if not is_empty(report):
            self._buffer[report_type].append(report)

    def flush(self, report_type: REPORT_TYPE = None):
        # Persist ad_network_report to model (report_type, buffer)
        if report_type is None:
            if not is_empty(self._buffer[REPORT_TYPE.PLATFORM]):
                self._buffer[REPORT_TYPE.PLATFORM] = []

            if not is_empty(self._buffer[REPORT_TYPE.COUNTRY]):
                self._buffer[REPORT_TYPE.COUNTRY] = []
        else:
            if not is_empty(self._buffer.get(report_type, [])):
                self._buffer[report_type] = []
