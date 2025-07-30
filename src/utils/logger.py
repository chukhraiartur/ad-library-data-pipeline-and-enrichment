"""
Logging utilities for the ad data pipeline.

This module provides centralized logging configuration and management
for the entire pipeline, ensuring consistent log formatting and output
across all components.
"""

import logging
import sys
from pathlib import Path


def setup_logging(level: int = logging.INFO, log_file: str | None = None) -> None:
    """
    Configure logging for the entire pipeline with consistent formatting.

    Sets up a centralized logging configuration that can output to both
    console and file (if specified). Creates necessary directories for
    log files and ensures proper handler management.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               Defaults to INFO for production use.
        log_file: Optional path to log file. If provided, logs will be
                 written to both console and file. If None, logs only
                 to console.

    Example:
        >>> setup_logging(logging.DEBUG, "logs/pipeline.log")
        >>> logger = get_logger("my_module")
        >>> logger.info("Pipeline started")
    """
    # Create logs directory if log file is specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

    # Configure logging with consistent format
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file) if log_file else logging.NullHandler(),
        ],
        force=True,  # Override existing configuration
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.

    Returns a configured logger that inherits the logging setup
    from setup_logging(). The logger name helps identify the source
    of log messages in the output.

    Args:
        name: Name of the logger, typically the module name.
              Used for identifying log message sources.

    Returns:
        Configured logger instance ready for use.

    Example:
        >>> logger = get_logger("extract.fetch_ads")
        >>> logger.info("Starting data extraction")
        >>> logger.error("API request failed", exc_info=True)
    """
    return logging.getLogger(name)
