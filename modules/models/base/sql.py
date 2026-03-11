#!/usr/bin/env python
from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence, List, Dict, Union
from sqlalchemy import create_engine, text, NullPool
from sqlalchemy.engine import Engine, URL
from sqlalchemy.sql.elements import TextClause

from config.schemas import DatabaseSettings
from config.constants import DB_TYPE

import pandas as pd


class SQL:
    def __init__(
        self, cfg: DatabaseSettings, db_type: DB_TYPE, *, connect_timeout: int = 10
    ):
        self.cfg = cfg
        self.db_type = db_type
        self.engine: Engine = create_engine(
            self._build_url(cfg, db_type, connect_timeout),
            poolclass=NullPool,
            future=True,
        )

    def _build_url(
        self, cfg: DatabaseSettings, db_type: DB_TYPE, connect_timeout: int
    ) -> URL:
        if db_type == DB_TYPE.MYSQL:
            return URL.create(
                "mysql+pymysql",
                username=cfg.user,
                password=cfg.password,
                host=cfg.host,
                database=cfg.name,
                query={"charset": "utf8mb4", "connect_timeout": str(connect_timeout)},
            )
        elif db_type == DB_TYPE.POSTGRES:
            return URL.create(
                "postgresql+psycopg2",
                username=cfg.user,
                password=cfg.password,
                host=cfg.host,
                database=cfg.name,
                query={"connect_timeout": str(connect_timeout)},
            )

    def commit(
        self,
        sql: Union[str, TextClause],
        params: Optional[Mapping[str, Any] | Sequence[Any]] = None,
    ) -> bool:
        """
        Auto-commit after DML execution
        :param sql: SQL to execute (str or SQLAlchemy TextClause)
        :param params: Bind parameters (dict or sequence)
        """
        with self.engine.begin() as conn:
            if isinstance(sql, str):
                conn.execute(text(sql), params or {})
            else:
                conn.execute(sql, params or {})
        return True

    def insert_batch(
        self,
        sql: str,
        params: Sequence[Sequence[Any] | Mapping[str, Any]],
        *,
        raw: bool = False,
        chunk_size: int = 2000,
    ) -> None:
        if params is None or len(params) == 0:
            return

        if raw:
            first = params[0]
            from collections.abc import Mapping as _Mapping, Sequence as Seq

            if isinstance(first, _Mapping):
                pass
            elif isinstance(first, Seq) and not isinstance(first, tuple):
                params = [tuple(p) for p in params]

        with self.engine.begin() as conn:
            if len(params) <= chunk_size:
                if raw:
                    conn.exec_driver_sql(sql, params)
                else:
                    conn.execute(text(sql), params)
                return

            for i in range(0, len(params), chunk_size):
                chunk = params[i : i + chunk_size]
                if raw:
                    conn.exec_driver_sql(sql, chunk)
                else:
                    conn.execute(text(sql), chunk)

    def fetchall(
        self,
        sql: str,
        params: Optional[Mapping[str, Any] | Sequence[Any]] = None,
    ) -> List[Dict[str, Any]]:
        try:
            df = pd.read_sql_query(text(sql), self.engine, params=params)
        except Exception:
            df = pd.read_sql_query(sql, self.engine, params=params)

        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient="records")

    def fetchone(
        self,
        sql: str,
        params: Optional[Mapping[str, Any] | Sequence[Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        rows = self.fetchall(sql, params=params)
        return rows[0] if rows else None

    def dataframe(
        self,
        sql: str,
        params: Optional[Mapping[str, Any] | Sequence[Any]] = None,
    ) -> pd.DataFrame:
        try:
            return pd.read_sql_query(text(sql), self.engine, params=params)
        except Exception:
            return pd.read_sql_query(sql, self.engine, params=params)

    def dispose(self) -> None:
        self.engine.dispose()
