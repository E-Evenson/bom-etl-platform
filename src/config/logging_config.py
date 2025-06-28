from datetime import datetime, timezone
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path

from config.config import (
    LOG_DIR,
    LOG_LEVEL,
    LOG_FORMAT,
    DEBUG,
    LOG_TO_FILE,
)


class UTCFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        utc_dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return utc_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def configure_logging():
    formatter = UTCFormatter(LOG_FORMAT)

    env = os.getenv("ENV", "production")

    LOG_DIR.mkdir(exist_ok=True)

    handlers: list = []

    terminal_handler = logging.StreamHandler()
    terminal_handler.setLevel(LOG_LEVEL)
    terminal_handler.setFormatter(formatter)
    handlers.append(terminal_handler)

    if LOG_TO_FILE:
        log_file = Path(LOG_DIR / "app.log")
        # rotate logs every 10 minutes if in DEBUG mode, daily in production
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="M" if DEBUG else "midnight",
            interval=10 if DEBUG else 1,
            backupCount=5,
        )
        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(
        level=LOG_LEVEL,
        handlers=handlers,
    )

    logging.getLogger().info(
        f"Logging configured | Debug Mode: {DEBUG} | Environment {env}"
    )

    return
