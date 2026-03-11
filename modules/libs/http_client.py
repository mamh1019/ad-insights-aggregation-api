#!/usr/bin/env python
from io import BytesIO
from typing import Any, Dict, Optional

import pandas as pd
import requests

DEFAULT_TIMEOUT = 60


class HttpClient:
    @staticmethod
    def _request(
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
        allow_redirects: bool = True,
    ) -> Optional[requests.Response]:
        try:
            return requests.request(
                method=method.upper(),
                url=url,
                params=params,
                headers=headers,
                data=data,
                json=json_body,
                timeout=timeout or DEFAULT_TIMEOUT,
                allow_redirects=allow_redirects,
            )
        except requests.RequestException as ex:
            print(f"[HttpClient] RequestException: {ex}")
            return None

    @staticmethod
    def get(
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
        timeout: int | None = None,
        allow_redirects: bool = True,
    ) -> Any:
        try:
            res = HttpClient._request(
                "GET",
                url,
                params=params,
                headers=headers,
                timeout=timeout,
                allow_redirects=allow_redirects,
            )
            if res is None:
                return {"error": res.text}
            if not res.ok:
                print(f"[HttpClient] GET non-OK: {res.status_code} {res.text[:200]}")
                return {"error": res.text}
            try:
                return res.json()
            except Exception as ex:
                print(
                    f"[HttpClient] GET JSON parse error: {ex} | body: {res.text[:200]}"
                )
                return {"error": res.text}
        except Exception as ex:
            print(f"[HttpClient] GET unexpected error: {ex} | body: {res.text}")
            return {"error": res.text}

    @staticmethod
    def post(
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
        json_body: Dict[str, Any] | None = None,
        form_body: Dict[str, Any] | None = None,
        timeout: int | None = None,
        allow_redirects: bool = True,
    ) -> Any:
        try:
            res = HttpClient._request(
                "POST",
                url,
                params=params,
                headers=headers,
                data=form_body,
                json_body=json_body,
                timeout=timeout,
                allow_redirects=allow_redirects,
            )
            if res is None:
                return {"error": res.text}

            quota = res.headers.get("X-Rate-Limit-Quota")
            remaining = res.headers.get("X-Rate-Limit-Remaining")
            reset = res.headers.get("X-Rate-Limit-Reset")
            if any([quota, remaining, reset]):
                print(
                    f"[HttpClient] RateLimit → Quota:{quota}, Remaining:{remaining}, Reset:{reset}"
                )

            if not res.ok:
                print(f"[HttpClient] POST non-OK: {res.status_code} {res.text[:200]}")
                return {"error": res.text}
            try:
                return res.json()
            except Exception as ex:
                print(
                    f"[HttpClient] POST JSON parse error: {ex} | body: {res.text[:200]}"
                )
                return {"error": res.text}
        except Exception as ex:
            print(f"[HttpClient] POST unexpected error: {ex}")
            return {"error": res.text}

    @staticmethod
    def csv(  # type: ignore
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
        timeout: int | None = None,
        allow_redirects: bool = True,
    ) -> pd.DataFrame:
        try:
            res = HttpClient._request(
                "GET",
                url,
                params=params,
                headers=headers,
                timeout=timeout,
                allow_redirects=allow_redirects,
            )
            if res is None:
                return pd.DataFrame()
            if not res.ok:
                print(f"[HttpClient] CSV non-OK: {res.status_code} {res.text[:200]}")
                return pd.DataFrame()
            try:
                return pd.read_csv(BytesIO(res.content), index_col=False)
            except Exception as ex:
                print(
                    f"[HttpClient] CSV parse error: {ex} | body(head): {res.text[:200]}"
                )
                return pd.DataFrame()
        except Exception as ex:
            print(f"[HttpClient] CSV unexpected error: {ex}")
            return pd.DataFrame()

    @staticmethod
    def csv_gz(  # type: ignore
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
        timeout: int | None = None,
        allow_redirects: bool = True,
        **read_csv_kwargs,
    ) -> pd.DataFrame:
        """
        Download *.csv.gz and return as DataFrame
        """
        try:
            res = HttpClient._request(
                "GET",
                url,
                params=params,
                headers=headers,
                timeout=timeout,
                allow_redirects=allow_redirects,
            )
            if res is None:
                return pd.DataFrame()

            if not res.ok:
                print(f"[HttpClient] CSV_GZ non-OK: {res.status_code} {res.text[:200]}")
                return pd.DataFrame()

            try:
                return pd.read_csv(
                    BytesIO(res.content),
                    compression="gzip",
                    index_col=False,
                    **read_csv_kwargs,
                )
            except Exception as ex:
                print(
                    f"[HttpClient] CSV_GZ parse error: {ex} | body(head): {res.content[:200]}"
                )
                return pd.DataFrame()

        except Exception as ex:
            print(f"[HttpClient] CSV_GZ unexpected error: {ex}")
            return pd.DataFrame()
