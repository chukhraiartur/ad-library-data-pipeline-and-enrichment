"""
Ad ranking and scoring utilities for the pipeline.

This module provides functions for calculating ad scores and ranking
ads based on various criteria such as duration and media type.
The scoring system helps identify the most effective advertisements.
"""

from typing import Any


def proxy_score(ad: dict[str, Any]) -> float:
    """
    Calculate a proxy score for an advertisement based on duration and media type.

    Computes a weighted score that combines ad duration with media type
    multipliers. This scoring system favors longer ads and richer media
    content (video > image > none) to identify potentially more engaging
    advertisements.

    Args:
        ad: Dictionary containing ad data with keys:
            - duration_hours: Duration in hours (float)
            - media_type: Media classification ("image-only", "video-only", "both", "none")

    Returns:
        Calculated score as float. Higher scores indicate potentially
        more effective ads based on duration and media richness.

    Examples:
        >>> ad = {"duration_hours": 2.0, "media_type": "image-only"}
        >>> proxy_score(ad)
        2.0
        >>> ad = {"duration_hours": 3.0, "media_type": "video-only"}
        >>> proxy_score(ad)
        3.3
        >>> ad = {"duration_hours": 1.5, "media_type": "both"}
        >>> proxy_score(ad)
        1.8
    """
    # Get base duration, defaulting to 0 if not present
    base_duration = float(ad.get("duration_hours", 0.0))

    # Get media type multiplier
    media_type = str(ad.get("media_type", "none"))
    media_multipliers = {
        "both": 1.2,  # Highest multiplier for mixed media
        "video-only": 1.1,  # Video content gets slight boost
        "image-only": 1.0,  # Base multiplier for images
        "none": 0.5,  # Penalty for no media content
    }

    multiplier = media_multipliers.get(media_type, 1.0)
    return base_duration * multiplier


def top_10_ads(ads_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Rank ads by score and return the top 10 highest-scoring advertisements.

    Sorts the provided list of ads by their proxy scores in descending
    order and returns the top 10 results. This function helps identify
    the most promising advertisements for further analysis or optimization.

    Args:
        ads_list: List of ad dictionaries, each containing ad data
                 that can be processed by proxy_score().

    Returns:
        List of top 10 ads sorted by score (highest first).
        If fewer than 10 ads provided, returns all ads sorted.
        If empty list provided, returns empty list.

    Examples:
        >>> ads = [
        ...     {"duration_hours": 1.0, "media_type": "image-only"},
        ...     {"duration_hours": 2.0, "media_type": "video-only"},
        ...     {"duration_hours": 3.0, "media_type": "both"}
        ... ]
        >>> top_10_ads(ads)
        [{"duration_hours": 3.0, "media_type": "both"}, ...]
    """
    if not ads_list:
        return []

    # Sort ads by proxy score in descending order and take top 10
    sorted_ads = sorted(ads_list, key=proxy_score, reverse=True)
    return sorted_ads[:10]
