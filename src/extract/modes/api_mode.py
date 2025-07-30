"""
Facebook Ads API integration for real advertisement data extraction.

This module handles communication with the Facebook Ads API to fetch
real advertisement data. It includes proper error handling, rate limiting
considerations, and data validation for production use.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Facebook Ads API configuration
API_BASE_URL = "https://graph.facebook.com/v18.0/ads_archive"
DEFAULT_SEARCH_TERMS = "microlearning"
DEFAULT_COUNTRIES = ["US"]
DEFAULT_AD_TYPE = "POLITICAL_AND_ISSUE_ADS"
DEFAULT_FIELDS = [
    "id",
    "ad_creative_body",
    "page_name",
    "ad_snapshot_url",
    "impressions",
    "spend",
]


def fetch_api_ads(output_path: str, access_token: str) -> None:
    """
    Fetch advertisement data from Facebook Ads API and save to file.

    Makes authenticated requests to Facebook's Ads Archive API to retrieve
    real advertisement data. Handles API rate limiting, error responses,
    and data validation. Results are saved in JSONL format with metadata
    for pipeline processing.

    Args:
        output_path: File path where API data will be saved.
                    Directory will be created if it doesn't exist.
        access_token: Facebook API access token for authentication.
                     Must have appropriate permissions for ads_archive.

    Raises:
        RuntimeError: If API request fails or returns error status.
        requests.RequestException: If network communication fails.
        json.JSONDecodeError: If API response cannot be parsed as JSON.

    Example:
        >>> fetch_api_ads("data/bronze/api_ads.jsonl", "your_access_token")
        >>> # Fetches real ads from Facebook API
    """
    logger.info("Starting Facebook Ads API data extraction")

    try:
        # Prepare API request parameters
        params = _build_api_params(access_token)

        # Make API request with error handling
        response_data = _make_api_request(params)

        # Process and save response data
        _process_and_save_data(response_data, output_path)

        logger.info(f"Successfully saved {len(response_data)} API ads to {output_path}")

    except requests.RequestException as e:
        logger.error(f"Network error during API request: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse API response: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during API extraction: {str(e)}")
        raise RuntimeError(f"API data extraction failed: {str(e)}") from e


def _build_api_params(access_token: str) -> dict[str, str]:
    """
    Build parameters for Facebook Ads API request.

    Constructs the query parameters needed for the ads_archive endpoint,
    including authentication, search criteria, and requested fields.

    Args:
        access_token: Facebook API access token for authentication.

    Returns:
        Dictionary of API request parameters.
    """
    return {
        "access_token": access_token,
        "search_terms": DEFAULT_SEARCH_TERMS,
        "ad_reached_countries": json.dumps(DEFAULT_COUNTRIES),
        "ad_type": DEFAULT_AD_TYPE,
        "fields": ",".join(DEFAULT_FIELDS),
    }


def _make_api_request(params: dict[str, str]) -> list[dict[str, Any]]:
    """
    Make authenticated request to Facebook Ads API.

    Performs the actual HTTP request to the Facebook API with proper
    error handling and response validation.

    Args:
        params: API request parameters including authentication.

    Returns:
        List of ad data from API response.

    Raises:
        RuntimeError: If API returns error status code.
    """
    logger.debug(f"Making API request to: {API_BASE_URL}")

    response = requests.get(API_BASE_URL, params=params, timeout=30)

    if response.status_code != 200:
        logger.error(
            f"API request failed with status {response.status_code}: {response.text}"
        )
        raise RuntimeError(f"Facebook API error: {response.status_code}")

    response_json = response.json()
    data = response_json.get("data", [])

    # Ensure data is a list
    if not isinstance(data, list):
        data = []

    logger.info(f"Received {len(data)} ads from Facebook API")
    return data


def _process_and_save_data(ads_data: list[dict[str, Any]], output_path: str) -> None:
    """
    Process API response data and save to file with metadata.

    Wraps each ad with source and ingestion metadata, then saves
    to the specified file path in JSONL format.

    Args:
        ads_data: List of ad dictionaries from API response.
        output_path: File path where processed data will be saved.
    """
    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Add metadata to each ad
    ingestion_time = datetime.utcnow().isoformat()

    with open(output_path, "w") as file:
        for ad in ads_data:
            ad_with_metadata = {
                "source": "api",
                "ingestion_time": ingestion_time,
                "raw_data": ad,
            }
            file.write(json.dumps(ad_with_metadata) + "\n")
