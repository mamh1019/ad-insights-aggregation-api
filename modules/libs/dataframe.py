#!/usr/bin/env python
from __future__ import annotations
from typing import Iterable, List, Sequence, Union, Optional, Dict
from pandas import DataFrame


Key = Union[str, Sequence[str]]


def update(
    df1: DataFrame,
    df2: DataFrame,
    key: Key,
    update_columns: List[str],
    fill_na_val=0,
    how: str = "left",
) -> DataFrame:
    """
    Join df1 with df2 on key, update update_columns from df2 values.
    - Safely overwrite even if df1 has same column names (create suffix column then replace)
    - Fill NaN from failed matches with fill_na_val
    - how: 'left' | 'inner' | 'right' | 'outer' (same as pandas merge)

    Example:
        update(df1, df2, key="id", update_columns=["name", "age"], fill_na_val=None)
    """
    # Guard: return as-is if no columns to update in df2
    cols_present = [c for c in update_columns if c in df2.columns]
    if not cols_present:
        return df1.copy()

    # Extract only join columns from df2 (minimize memory)
    keep_cols = list(
        {
            *(key if isinstance(key, Sequence) and not isinstance(key, str) else [key]),
            *cols_present,
        }
    )
    df2_trim = df2.loc[:, keep_cols].copy()

    # Merge with suffix to avoid column collision
    merged = df1.merge(df2_trim, how=how, on=key, suffixes=("", "__r"))

    # Replace: use df2 value if present, else keep df1 original
    for col in cols_present:
        rcol = f"{col}__r"
        if rcol in merged:
            merged[col] = merged[rcol].combine_first(merged[col])
            merged.drop(columns=[rcol], inplace=True)

    # Fill NaN for requested columns (only existing columns)
    existed = [c for c in update_columns if c in merged.columns]
    if existed:
        merged[existed] = merged[existed].fillna(fill_na_val)

    return merged


def fetch_row(df: DataFrame) -> Optional[Dict]:
    """
    Return row as dict when exactly 1 row. Otherwise None.
    (Fixed return type inconsistency from original DataFrame)
    """
    if len(df) == 1:
        return df.iloc[0].to_dict()
    return None


def drop(df: DataFrame, cols: Iterable[str]) -> DataFrame:
    """Safely drop only existing columns."""
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return df
    return df.drop(columns=cols)


def subtract(df1: DataFrame, df2: DataFrame, key: Key) -> DataFrame:
    """
    Set difference: df1 - df2 (rows in df1 not in df2, by key)
    """
    return (
        df1.merge(df2, on=key, how="left", indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns=["_merge"])
    )


def intersect(df1: DataFrame, df2: DataFrame, key: Key) -> DataFrame:
    """
    Set intersection: df1 ∩ df2 (rows in both, by key)
    """
    return df1.merge(df2, on=key, how="inner")
