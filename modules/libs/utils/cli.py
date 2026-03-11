#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Any
from rich.console import Console
from rich.table import Table
from datetime import datetime
from dataclasses import is_dataclass, asdict
from rich.text import Text
from rich.pretty import Pretty
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    SpinnerColumn,
)

_console = Console(force_terminal=True)


def print_table(title: str, rows: List[str], col_name: str):
    t = Table(title=title)
    t.add_column("#", style="cyan", justify="right")
    t.add_column(col_name, style="white")
    for i, n in enumerate(rows, 1):
        t.add_row(str(i), n)
    _console.print(t)


def _to_rich_arg(x: Any):
    if isinstance(x, str):
        return x
    try:
        from pydantic import BaseModel  # optional import

        if isinstance(x, BaseModel):
            return Pretty(x.model_dump())
    except Exception:
        pass
    if is_dataclass(x):
        return Pretty(asdict(x))
    if isinstance(x, (dict, list, tuple, set)):
        return Pretty(x)
    return Pretty(x)


def log(*msgs: Any, style: str = "bright_white"):
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts = Text(f"[{ts_str}]", style="cyan")
    parts = [ts] + [_to_rich_arg(m) for m in msgs]
    _console.print(*parts, style=style)


def console():
    return _console


def create_progress(*, transient: bool = False) -> Progress:
    """
    Create Progress instance connected to shared console (_console).
    transient=True: progress bar disappears after completion.
    """
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),  # 0% • 50/100
        MofNCompleteColumn(),  # (50 of 100)
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=_console,
        transient=transient,
    )


def start_task(total: int, description: str = "Working...", *, transient: bool = False):
    """
    Helper to prepare progress bar and task together.
    Usage:
        progress, task_id = start_task(total, "Fetching")
        with progress:
            ...
            progress.advance(task_id)
    """
    progress = create_progress(transient=transient)
    task_id = progress.add_task(description, total=total)
    return progress, task_id
