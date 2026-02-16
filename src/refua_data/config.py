"""Configuration helpers for refua-data."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_CACHE_ENV = "REFUA_DATA_HOME"


def default_cache_root() -> Path:
    """Resolve the cache root directory.

    Preference order:
    1. `REFUA_DATA_HOME`
    2. `~/.cache/refua-data`
    """
    env = os.environ.get(DEFAULT_CACHE_ENV)
    if env:
        return Path(env).expanduser().resolve()
    return Path.home().joinpath(".cache", "refua-data").resolve()
