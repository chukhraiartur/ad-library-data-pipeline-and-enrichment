"""
Mock data generation for ad processing pipeline testing.

This module generates synthetic advertisement data that mimics the structure
and characteristics of real Facebook Ads API responses. It's used for testing
and development when real API access is not available or desired.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Predefined content templates for generating realistic mock data
AD_TITLES = [
    "Boost your microlearning today!",
    "Master a new skill in 5 minutes",
    "Learn smarter, not harder",
    "Microlearning for busy people",
    "Upgrade your brain",
]

AD_BODIES = [
    "This ad teaches you microlearning techniques.",
    "Daily micro lessons to improve focus.",
    "Become better every day with microlearning.",
    "Microlearning is the future of education.",
    "Quick tips, big impact with microlearning.",
]

MEDIA_COMBINATIONS = [["image"], ["video"], ["image", "video"], []]


def fetch_mock_ads(output_path: str, count: int = 50) -> None:
    """
    Generate synthetic advertisement data and save to specified file path.

    Creates realistic mock ad data with randomized content, timing, and
    performance metrics. Each ad includes metadata for tracking source
    and ingestion time. Data is saved in JSONL format for efficient
    processing by downstream pipeline stages.

    Args:
        output_path: File path where mock data will be saved.
                    Directory will be created if it doesn't exist.
        count: Number of mock ads to generate. Defaults to 50 for
               comprehensive testing scenarios.

    Raises:
        RuntimeError: If data generation or file writing fails.

    Example:
        >>> fetch_mock_ads("data/bronze/mock_ads.jsonl", 100)
        >>> # Generates 100 mock ads with realistic data
    """
    logger.info(f"Generating {count} mock ads")

    try:
        # Ensure output directory exists
        _ensure_output_directory(output_path)

        # Generate mock ads with consistent base time
        base_time = datetime.utcnow()
        ingestion_time = base_time.isoformat()
        logger.debug(f"Creating mock ads with base time: {base_time}")

        ads = [_generate_mock_ad(i, base_time, ingestion_time) for i in range(count)]

        # Write ads to file in JSONL format
        _write_ads_to_file(ads, output_path)

        logger.info(f"Successfully generated {len(ads)} mock ads to {output_path}")

    except Exception as e:
        logger.error(f"Failed to generate mock ads: {str(e)}")
        raise RuntimeError(f"Mock data generation failed: {str(e)}") from e


def _generate_mock_ad(
    index: int, base_time: datetime, ingestion_time: str
) -> dict[str, Any]:
    """
    Generate a single mock advertisement with realistic data.

    Creates an ad with randomized content, timing, and performance metrics
    that closely mimics real Facebook ad data structure.

    Args:
        index: Sequential index for generating unique ad IDs.
        base_time: Base timestamp for calculating ad delivery times.
        ingestion_time: ISO format timestamp when data was ingested.

    Returns:
        Dictionary containing ad data with metadata wrapper.
    """
    # Generate realistic timing data
    start_time = base_time - timedelta(days=random.randint(1, 30))
    stop_time = start_time + timedelta(days=random.randint(1, 5))

    # Generate duration in minutes and convert to hours/minutes format
    duration_minutes = random.randint(30, 600)
    hours = duration_minutes // 60
    minutes = duration_minutes % 60

    # Create raw ad data
    raw_ad = {
        "ad_id": f"mock_{index}",
        "page_id": f"page_{1000 + index}",
        "page_name": f"Mock Page {index}",
        "ad_creative_body": random.choice(AD_BODIES),
        "ad_creative_link_title": random.choice(AD_TITLES),
        "ad_delivery_start_time": start_time.isoformat(),
        "ad_delivery_stop_time": stop_time.isoformat(),
        "ad_snapshot_url": f"https://facebook.com/ads/snapshot/mock_{index}",
        "currency": "USD",
        "spend": round(random.uniform(5, 500), 2),
        "impressions": random.randint(1000, 50000),
        "ad_reached_countries": ["US"],
        "ad_text": f"This is a test ad #{index} with great features and microlearning tricks",
        "active": f"Active for {hours} hrs {minutes} mins",
        "media": random.choice(MEDIA_COMBINATIONS),
        "country": "US",
    }

    # Wrap with metadata for pipeline processing
    return {"source": "mock", "ingestion_time": ingestion_time, "raw_data": raw_ad}


def _ensure_output_directory(output_path: str) -> None:
    """
    Ensure the output directory exists, creating it if necessary.

    Args:
        output_path: File path where data will be written.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)


def _write_ads_to_file(ads: list[dict[str, Any]], output_path: str) -> None:
    """
    Write list of ads to file in JSONL format.

    Args:
        ads: List of ad dictionaries to write.
        output_path: File path where data will be written.
    """
    with open(output_path, "w") as file:
        for ad in ads:
            file.write(json.dumps(ad) + "\n")
