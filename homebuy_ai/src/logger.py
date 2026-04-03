from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_LOG_CONFIGURED = False


class JsonEventLogger:
    """Escribe eventos estructurados JSONL para trazabilidad del pipeline."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, payload: dict[str, Any] | None = None, level: str = "INFO") -> None:
        entry = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "event": event,
            "payload": payload or {},
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


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
