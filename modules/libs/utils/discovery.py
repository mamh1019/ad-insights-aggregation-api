#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations
import importlib
import inspect
import pkgutil
from typing import Dict, Type


def discover_subclasses(package, base_cls, name_attr: str = "name") -> Dict[str, Type]:
    """
    Auto-discover base_cls subclasses in package submodules.
    key = class name_attr (or class name if absent)
    Note: scanned modules must have no side effects on import.
    """
    found: Dict[str, Type] = {}
    for m in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
        mod = importlib.import_module(m.name)
        for _, obj in inspect.getmembers(mod, inspect.isclass):
            if issubclass(obj, base_cls) and obj is not base_cls:
                key = getattr(obj, name_attr, obj.__name__)
                found[key] = obj
    return found
