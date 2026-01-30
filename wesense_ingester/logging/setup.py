"""
Logging setup for WeSense ingesters.

Provides colored console output, rotating file handler, and an optional
future-timestamp logger for nodes with incorrect RTC.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional


class ColoredFormatter(logging.Formatter):
    """Log formatter with ANSI colors based on log level."""

    COLORS = {
        logging.DEBUG: "",             # No color
        logging.INFO: "\033[96m",      # Cyan
        logging.WARNING: "\033[93m",   # Yellow
        logging.ERROR: "\033[91m",     # Red
        logging.CRITICAL: "\033[91m",  # Red
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        formatted = super().format(record)
        color = self.COLORS.get(record.levelno, "")
        if color:
            return f"{color}{formatted}{self.RESET}"
        return formatted


def setup_logging(
    name: str,
    log_dir: str = "logs",
    level: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    enable_future_timestamp_log: bool = False,
) -> logging.Logger:
    """
    Set up logging with colored console output and rotating file handler.

    Args:
        name: Logger name (typically the ingester name).
        log_dir: Directory for log files.
        level: Log level string (DEBUG, INFO, etc.). Defaults to LOG_LEVEL
               env var, then DEBUG.
        max_bytes: Max size per log file before rotation.
        backup_count: Number of rotated log files to keep.
        enable_future_timestamp_log: If True, create a dedicated logger
            for future timestamp warnings.

    Returns:
        Configured logger instance.
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "DEBUG").upper()

    log_level = getattr(logging, level, logging.DEBUG)

    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid adding duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    fmt = "%(asctime)s | %(levelname)s | %(message)s"

    # Rotating file handler
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f"{name}.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(file_handler)

    # Colored console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(ColoredFormatter(fmt))
    logger.addHandler(console_handler)

    # Future timestamp logger (optional)
    if enable_future_timestamp_log:
        ft_logger = logging.getLogger(f"{name}.future_timestamps")
        ft_logger.setLevel(logging.WARNING)
        ft_handler = RotatingFileHandler(
            os.path.join(log_dir, "future_timestamps.log"),
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        ft_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
        ft_logger.addHandler(ft_handler)

    return logger
