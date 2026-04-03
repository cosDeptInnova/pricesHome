from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

_LOG_CONFIGURED = False


def configure_logging(log_dir: str | Path = "data/output/logs", level: int = logging.INFO) -> Path:
    global _LOG_CONFIGURED

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logfile = log_path / f"homebuy_ai_{run_id}.log"

    if not _LOG_CONFIGURED:
        root = logging.getLogger()
        root.setLevel(level)
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)

        file_handler = logging.FileHandler(logfile, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
        _LOG_CONFIGURED = True

    return logfile


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
