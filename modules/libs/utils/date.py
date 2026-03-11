#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Optional
from datetime import datetime, timedelta, timezone


def now(fmt: str = "%Y%m%d") -> str:
    return datetime.now(timezone.utc).strftime(fmt)


def yesterday(fmt: str = "%Y%m%d") -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime(fmt)


def add_days(origin: datetime, days: int = 0, fmt: str = "%Y%m%d") -> str:
    return (origin + timedelta(days=days)).strftime(fmt)


def sub_days(origin: datetime, days: int = 0, fmt: str = "%Y%m%d") -> str:
    return (origin - timedelta(days=days)).strftime(fmt)


def days_between(start: str, end: str, fmt: str = "%Y%m%d") -> int:
    s = datetime.strptime(start, fmt).replace(tzinfo=timezone.utc)
    e = datetime.strptime(end, fmt).replace(tzinfo=timezone.utc)
    return abs((e - s).days)


def str_to_datetime(date_str: str, fmt: str = "%Y%m%d") -> datetime:
    return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)


def datetime_to_str(dt: datetime, fmt: str = "%Y%m%d") -> str:
    return dt.astimezone(timezone.utc).strftime(fmt)


def timestamp() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def hour(dt: Optional[datetime] = None) -> int:
    dt = dt or datetime.now(timezone.utc)
    return dt.hour


def str_to_timestamp(date_str: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> int:
    return int(
        datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc).timestamp()
    )


def timestamp_to_datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def norm_date(d: Optional[str]) -> Optional[str]:
    if d is None:
        return None
    dlow = d.lower()
    if dlow in ("today", "now"):
        return datetime.today().strftime("%Y-%m-%d")
    return datetime.strptime(d, "%Y-%m-%d").strftime("%Y-%m-%d")
