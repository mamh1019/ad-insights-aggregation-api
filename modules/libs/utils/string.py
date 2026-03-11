#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Optional

import hashlib
import base64
import json
import re
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlparse, urlunparse, urlencode as _urlencode


def expand_list(vals: Optional[List[str]]) -> List[str]:
    """
    ['A,B', 'C'] -> ['A','B','C']
    Trim whitespace, dedupe while preserving order.
    """
    if not vals:
        return []
    out: List[str] = []
    for v in vals:
        out.extend([x.strip() for x in v.split(",") if x.strip()])
    seen = set()
    uniq: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def make_hash_key(key: str) -> str:
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def make_creative_asset_hash_key(media_source: str, adset_id: str, creative_id: str):
    return make_hash_key(f"{media_source}:{adset_id}:{creative_id}")


def make_creative_creation_hash_key(media_source: str, creative_name: str):
    return make_hash_key(f"{media_source}:{creative_name}")


def make_creative_history_hash_key(
    media_source,
    campaign_id,
    adset_id,
    ad_id,
    creative_id,
):
    return make_hash_key(
        ":".join(
            str(s).strip()
            for s in (
                media_source,
                campaign_id,
                adset_id,
                ad_id,
                creative_id,
            )
        )
    )


def add_slash(text: str) -> str:
    """Escape single quotes."""
    return text.replace("'", "\\'")


def base64_encode(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def raw_encode(text: str):
    import urllib

    return urllib.parse.quote_plus(str(text))


def is_json(text: Any) -> bool:
    try:
        if isinstance(text, (bytes, bytearray)):
            text = text.decode("utf-8", errors="strict")
        json.loads(text)
        return True
    except json.JSONDecodeError:
        return False
    except Exception:
        return False


def clean_json_string(json_str):
    json_str = re.sub(r",\s*}", "}", json_str)
    json_str = re.sub(r",\s*]", "]", json_str)
    json_str = re.sub(r",+", ",", json_str)
    return json_str


def urlencode(url: str, params: Mapping[str, Any]) -> str:
    parsed = urlparse(url)
    current = dict(parse_qsl(parsed.query, keep_blank_values=True))
    current.update({k: "" if v is None else v for k, v in params.items()})
    new_query = _urlencode(current, doseq=True)
    return urlunparse(parsed._replace(query=new_query))
