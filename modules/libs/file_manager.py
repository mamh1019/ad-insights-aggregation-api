#!/usr/bin/env python
# -*- coding: utf-8 -*-
# file_manager.py

from __future__ import annotations
from pathlib import Path
from typing import Any, Optional
import json
import os
import pickle
import tempfile


class FileManager:
    # ---------- Path helpers ----------
    @staticmethod
    def exists(path: str | Path) -> bool:
        return Path(path).exists()

    @staticmethod
    def is_dir(path: str | Path) -> bool:
        return Path(path).is_dir()

    @staticmethod
    def ensure_dir(path: str | Path) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def remove(path: str | Path) -> None:
        Path(path).unlink(missing_ok=True)

    # ---------- Text ----------
    @staticmethod
    def write_text(path: str | Path, data: str, *, encoding: str = "utf-8") -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", encoding=encoding, newline="", dir=p.parent, delete=False
        ) as tmp:
            tmp.write(data)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, p)

    @staticmethod
    def append_text(path: str | Path, data: str, *, encoding: str = "utf-8") -> None:
        """Append if exists, create if not."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding=encoding, newline="") as f:
            f.write(data)

    @staticmethod
    def read_text(path: str | Path, *, encoding: str = "utf-8") -> str:
        return Path(path).read_text(encoding=encoding)

    @staticmethod
    def write_bytes(path: str | Path, data: bytes) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("wb", dir=p.parent, delete=False) as tmp:
            tmp.write(data)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, p)

    @staticmethod
    def read_bytes(path: str | Path) -> bytes:
        return Path(path).read_bytes()

    @staticmethod
    def write_json(path: str | Path, obj: Any, *, indent: Optional[int] = 2) -> None:
        FileManager.write_text(path, json.dumps(obj, ensure_ascii=False, indent=indent))

    @staticmethod
    def read_json(path: str | Path) -> Any:
        return json.loads(FileManager.read_text(path))

    @staticmethod
    def write_pickle(
        path: str | Path, obj: Any, *, protocol: int = pickle.HIGHEST_PROTOCOL
    ) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("wb", dir=p.parent, delete=False) as tmp:
            pickle.dump(obj, tmp, protocol=protocol)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, p)

    @staticmethod
    def read_pickle(path: str | Path) -> Any:
        with Path(path).open("rb") as fr:
            return pickle.load(fr)
