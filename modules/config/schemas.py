#!/usr/bin/env python
from __future__ import annotations
from typing import Type, Any, Dict, Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic.fields import PydanticUndefined
from typing import Union, get_origin, get_args
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / ".env"


# ---- Schemas ----
class BasicAuthSettings(BaseModel):
    host: str
    user: str
    password: str
    port: Optional[int] = None


class DatabaseSettings(BasicAuthSettings):
    name: str
    engine: str


class BasicNoAuthSettings(BaseModel):
    host: str
    password: Optional[str] = None
    port: int


# ----- Functions -----
def _make_optional(tp):
    origin = get_origin(tp)
    if origin is Union:
        args = get_args(tp)
        if type(None) in args:
            return tp  # Already Optional
        return Optional[tp]  # type: ignore
    return Optional[tp]  # type: ignore


def load_with_prefix(
    schema: Type[BaseModel],
    prefix: str,
    *,
    case_sensitive: bool = False,
    allow_missing: bool = False,
) -> Optional[BaseModel]:
    annotations: Dict[str, Any] = {}
    namespace: Dict[str, Any] = {}

    for name, f in schema.model_fields.items():
        annotations[name] = _make_optional(f.annotation)
        env_candidates = [
            f"{prefix}{name}",
            f"{prefix}{name}".upper(),
            f"{prefix}{name}".lower(),
            name,
            name.upper(),
            name.lower(),
        ]
        namespace[name] = Field(None, env=env_candidates)

    namespace["__annotations__"] = annotations
    namespace["model_config"] = SettingsConfigDict(
        env_file=ENV_PATH,
        extra="ignore",
        case_sensitive=case_sensitive,
        env_prefix=prefix,
    )

    Loader = type(f"{schema.__name__}Loader", (BaseSettings,), namespace)
    loaded = Loader()
    loaded_env: Dict[str, Any] = loaded.model_dump(exclude_none=True)

    defaults: Dict[str, Any] = {
        n: fld.default
        for n, fld in schema.model_fields.items()
        if fld.default is not PydanticUndefined
    }

    data = {**defaults, **loaded_env}

    if not data:
        if allow_missing:
            return None
        return schema()

    return schema(**data)
