#!/usr/bin/env python
import math
import socket
import pandas as pd

from typing import Any, Mapping


def is_empty(obj: Any) -> bool:
    if obj is None:
        return True

    if isinstance(obj, pd.DataFrame):
        return obj.empty

    if isinstance(obj, pd.Series):
        return obj.empty

    if isinstance(obj, str):
        return obj.strip() == ""

    if isinstance(obj, float) and math.isnan(obj):
        return True

    if isinstance(obj, Mapping):
        return len(obj) == 0
    if isinstance(obj, (list, tuple, set, frozenset)):
        return len(obj) == 0

    return False


def my_ip() -> str:
    return socket.gethostbyname(socket.gethostname())
