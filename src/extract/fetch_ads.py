"""
Main data extraction module for the ad processing pipeline.

This module orchestrates the extraction of advertisement data from various
sources (mock data or Facebook Ads API) and saves it to the bronze layer
for further processing. It handles different extraction modes and provides
comprehensive logging and error handling.
"""

import os
from datetime import datetime
from typing import Any, Literal

from dotenv import load_dotenv
from typing_extensions import assert_never

from src.extract.modes.api_mode import fetch_api_ads
from src.extract.modes.mock_mode import fetch_mock_ads
from src.utils.logger import get_logger

# Load environment variables at module import
load_dotenv()

logger = get_logger(__name__)

# Type alias for supported extraction modes
ExtractionMode = Literal["mock", "api"]


def fetch_ads_data(
    mode: ExtractionMode = "mock", output_path: str | None = None, **context: Any
) -> str:
    """
    Extract advertisement data from specified source and save to bronze layer.

    Orchestrates the data extraction process by delegating to appropriate
    extraction mode (mock or API). Handles environment variable loading,
    access token validation, and comprehensive error handling with logging.

    Args:
        mode: Extraction mode to use:
              - "mock": Generate synthetic ad data for testing
              - "api": Fetch real data from Facebook Ads API
        output_path: File path where extracted data will be saved.
                    If None, generates timestamped filename.
                    Should point to bronze layer directory.

    Raises:
        ValueError: If mode is invalid or ACCESS_TOKEN is missing for API mode.
        RuntimeError: If data extraction fails for any reason.

    Example:
        >>> fetch_ads_data("mock")
        >>> fetch_ads_data("api", "data/bronze/custom_ads.jsonl")
    """
    # Generate timestamped filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/bronze/ads_raw_{timestamp}.jsonl"

    logger.info(f"Starting data extraction in mode: {mode}")

    try:
        if mode == "mock":
            _extract_mock_data(output_path)
        elif mode == "api":
            _extract_api_data(output_path)
        else:
            assert_never(mode)

    except Exception as e:
        logger.error(f"Failed to extract data in mode {mode}: {str(e)}")
        raise RuntimeError(f"Data extraction failed: {str(e)}") from e

    # Return output path for XCom
    return output_path


def _extract_mock_data(output_path: str) -> None:
    """
    Extract mock advertisement data for testing purposes.

    Delegates to mock mode extraction and logs the process.

    Args:
        output_path: File path where mock data will be saved.
    """
    logger.debug("Using mock data mode")
    fetch_mock_ads(output_path)
    logger.info(f"Successfully extracted mock data to {output_path}")


def _extract_api_data(output_path: str) -> None:
    """
    Extract real advertisement data from Facebook Ads API.

    Validates access token and delegates to API mode extraction.

    Args:
        output_path: File path where API data will be saved.

    Raises:
        ValueError: If ACCESS_TOKEN environment variable is not set.
    """
    logger.debug("Using API data mode")

    access_token = os.getenv("ACCESS_TOKEN")
    if not access_token:
        logger.error("ACCESS_TOKEN not found in environment variables")
        raise ValueError("ACCESS_TOKEN environment variable is required for API mode")

    fetch_api_ads(output_path, access_token)
    logger.info(f"Successfully extracted API data to {output_path}")


def _handle_invalid_mode(mode: str) -> None:
    """
    Handle invalid extraction mode with appropriate error logging.

    Args:
        mode: The invalid mode that was provided.

    Raises:
        ValueError: Always raised with descriptive error message.
    """
    logger.error(f"Unknown extraction mode: {mode}")
    raise ValueError(f"Unknown mode: {mode}. Supported modes: 'mock', 'api'")
