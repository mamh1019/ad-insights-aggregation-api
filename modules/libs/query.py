#!/usr/bin/env python
from __future__ import annotations
from typing import List, Optional, Sequence, Literal
from config.constants import DB_TYPE


class Query:
    @staticmethod
    def _quote_ident(ident: str, db_type: DB_TYPE) -> str:
        if db_type == DB_TYPE.POSTGRES:
            return f'"{ident}"'
        return f"`{ident}`"  # MySQL

    @staticmethod
    def _join_idents(idents: Sequence[str], db_type: DB_TYPE) -> str:
        return ",".join(Query._quote_ident(i, db_type) for i in idents)

    @staticmethod
    def _placeholders(
        columns: Sequence[str],
        style: Literal["named", "percent"] = "named",
    ) -> str:
        """
        style="named"   -> :col1,:col2 (SQLAlchemy text binding)
        style="percent" -> %s,%s      (driver positional binding)
        """
        if style == "percent":
            return ",".join(["%s"] * len(columns))
        # named
        return ",".join(f":{c}" for c in columns)

    @staticmethod
    def build_insert_stmt(
        table: str,
        columns: List[str],
        dup_update_columns: Optional[List[str]] = None,
        ignore: bool = False,
        *,
        db_type: DB_TYPE = DB_TYPE.MYSQL,
        conflict_target: Optional[Sequence[str]] = None,  # PG ON CONFLICT (col,...)
        conflict_constraint: Optional[str] = None,  # PG ON CONFLICT ON CONSTRAINT name
        placeholder_style: Literal["named", "percent"] = "named",
    ) -> str:
        """
        Build INSERT / INSERT IGNORE / UPSERT SQL string

        placeholder_style:
          - "named"   -> VALUES(:col1,:col2,...)  (recommended with SQLAlchemy text())
          - "percent" -> VALUES(%s,%s,...)        (for driver executemany)

        MySQL:
          - no dup_update_columns, ignore=False -> INSERT
          - no dup_update_columns, ignore=True  -> INSERT IGNORE
          - dup_update_columns present          -> ON DUPLICATE KEY UPDATE col = VALUES(col)

        PostgreSQL:
          - no dup_update_columns, ignore=False -> INSERT
          - else -> ON CONFLICT required
              * ignore=True & no dup_update_columns -> DO NOTHING
              * dup_update_columns present -> DO UPDATE SET col = EXCLUDED.col
        """
        dup_update_columns = dup_update_columns or []

        q_table = Query._quote_ident(table, db_type)
        cols_sql = Query._join_idents(columns, db_type)
        placeholders = Query._placeholders(columns, placeholder_style)

        parts: List[str] = ["INSERT"]

        if db_type == DB_TYPE.MYSQL:
            if ignore and not dup_update_columns:
                parts.append("IGNORE")

            parts += ["INTO", q_table, f"({cols_sql})", "VALUES", f"({placeholders})"]

            if dup_update_columns:
                set_sql = ",".join(
                    f"{Query._quote_ident(c, db_type)} = VALUES({Query._quote_ident(c, db_type)})"
                    for c in dup_update_columns
                )
                parts += ["ON DUPLICATE KEY UPDATE", set_sql]

            return " ".join(parts)

        # PostgreSQL
        parts += ["INTO", q_table, f"({cols_sql})", "VALUES", f"({placeholders})"]

        if not dup_update_columns and not ignore:
            return " ".join(parts)

        # ON CONFLICT (ignore=True or upsert)
        if conflict_constraint:
            conflict_sql = f"ON CONFLICT ON CONSTRAINT {Query._quote_ident(conflict_constraint, db_type)}"
        else:
            if not conflict_target:
                raise ValueError(
                    "PostgreSQL ignore=True or upsert requires "
                    "conflict_target (column list) or conflict_constraint."
                )
            conflict_cols = Query._join_idents(conflict_target, db_type)
            conflict_sql = f"ON CONFLICT ({conflict_cols})"

        if ignore and not dup_update_columns:
            parts += [conflict_sql, "DO NOTHING"]
            return " ".join(parts)

        set_sql = ",".join(
            f"{Query._quote_ident(c, db_type)} = EXCLUDED.{Query._quote_ident(c, db_type)}"
            for c in dup_update_columns
        )
        parts += [conflict_sql, "DO UPDATE SET", set_sql]
        return " ".join(parts)

    @staticmethod
    def build_update_stmt(
        table: str,
        set_columns: List[str],
        where_columns: Optional[List[str]] = None,
        ignore: bool = False,
        *,
        db_type: DB_TYPE = DB_TYPE.MYSQL,
        placeholder_style: Literal["named", "percent"] = "named",
    ) -> str:
        """
        Build UPDATE SQL string

        Example (placeholder_style="named"):
          UPDATE my_table
             SET col1 = :col1,
                 col2 = :col2
           WHERE id = :id

        Example (placeholder_style="percent"):
          UPDATE my_table
             SET col1 = %s,
                 col2 = %s
           WHERE id = %s

        MySQL:
          - ignore=True -> "UPDATE IGNORE ..."
        PostgreSQL:
          - ignore option ignored (not supported)
        """
        if not set_columns:
            raise ValueError(
                "set_columns is empty. At least 1 column required."
            )

        q_table = Query._quote_ident(table, db_type)

        # Placeholder generator
        if placeholder_style == "named":

            def ph(col: str) -> str:
                return f":{col}"

        elif placeholder_style == "percent":

            def ph(_: str) -> str:
                return "%s"

        else:
            raise ValueError(f"Unsupported placeholder_style: {placeholder_style}")

        # SET clause
        set_sql = ", ".join(
            f"{Query._quote_ident(c, db_type)} = {ph(c)}" for c in set_columns
        )

        parts: List[str] = ["UPDATE"]

        # MySQL UPDATE IGNORE
        if db_type == DB_TYPE.MYSQL and ignore:
            parts.append("IGNORE")

        parts.append(q_table)
        parts += ["SET", set_sql]

        # WHERE clause (when present)
        if where_columns:
            where_sql = " AND ".join(
                f"{Query._quote_ident(c, db_type)} = {ph(c)}" for c in where_columns
            )
            parts += ["WHERE", where_sql]

        return " ".join(parts)

    @staticmethod
    def build_in_clause(values: list, placeholder: str = "%s") -> tuple[str, list]:
        """
        Example: sql, params = Query.build_in_clause([1,2,3])
            cursor.execute(f"SELECT ... WHERE col IN {sql}", params)

        placeholder:
          - MySQL / psycopg2: "%s"
          - SQLite: "?"
        """
        vals = list(values or [])
        if not vals:
            # Safe handling: empty list always yields false
            return "(SELECT 1 WHERE 0)", []
        return "(" + ",".join([placeholder] * len(vals)) + ")", vals

    @staticmethod
    def format_in_values_literal(values: list) -> str:
        vals = list(values or [])
        if not vals:
            return "(SELECT 1 WHERE 0)"

        def q(v):
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                return str(v)
            s = str(v).replace("'", "''")
            return f"'{s}'"

        return "(" + ",".join(q(v) for v in vals) + ")"
