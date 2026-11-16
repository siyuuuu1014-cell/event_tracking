import json
import logging
import os
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "service": "analytics-api",
            "action": record.getMessage(),
        }
        for name in ("requestId", "route", "method", "resultCode", "durationMs", "errorType"):
            if hasattr(record, name):
                entry[name] = getattr(record, name)
        return json.dumps(entry, ensure_ascii=False)


def get_logger(log_dir: Path) -> logging.Logger:
    log_dir = log_dir.resolve()
    logger = logging.getLogger(f"ipaklink.analytics.api.{log_dir}")
    if not logger.handlers:
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            log_dir / f"api-{os.getpid()}.log",
            maxBytes=256 * 1024 * 1024,
            backupCount=4,
            encoding="utf-8",
        )
        handler.namer = lambda path: f"{path}.log"
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger

