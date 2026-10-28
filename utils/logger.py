"""Centralized logging for every pipeline module."""

from __future__ import annotations

import logging
import sys
from typing import Optional

from config.settings import LOG_LEVEL, PROJECT_ROOT

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Return a module logger. Handlers are attached once on the root logger."""
    root = logging.getLogger("financial_pipeline")
    if not root.handlers:
        root.setLevel(getattr(logging, (level or LOG_LEVEL), logging.INFO))
        formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(formatter)
        root.addHandler(stream)

        log_dir = PROJECT_ROOT / "logs"
        log_dir.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
        root.propagate = False

    logger = root.getChild(name)
    return logger
