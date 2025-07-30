"""
Data enrichment utilities for ad processing pipeline.

This module provides functions for enriching raw ad data with derived
information such as duration parsing, media type classification, and
language detection. These utilities transform basic ad information
into more structured and analyzable data.
"""

import re

from langdetect import LangDetectException, detect


def parse_duration(text: str | None) -> float:
    """
    Parse duration information from text and convert to hours.

    Extracts hours and minutes from text strings like "Active for 2 hrs 30 mins"
    and converts them to decimal hours. Handles various edge cases and invalid
    inputs gracefully.

    Args:
        text: Text containing duration information. Expected format:
              "Active for X hrs Y mins" or similar variations.
              Can be None or empty string.

    Returns:
        Duration in decimal hours. Returns 0.0 for invalid or empty input.

    Examples:
        >>> parse_duration("Active for 2 hrs 30 mins")
        2.5
        >>> parse_duration("Active for 45 mins")
        0.75
        >>> parse_duration("")
        0.0
        >>> parse_duration(None)
        0.0
    """
    if not text:
        return 0.0

    hours = minutes = 0

    # Extract hours using regex pattern
    hours_match = re.search(r"(\d+)\s*hrs?", text, re.IGNORECASE)
    if hours_match:
        hours = int(hours_match.group(1))

    # Extract minutes using regex pattern
    minutes_match = re.search(r"(\d+)\s*mins?", text, re.IGNORECASE)
    if minutes_match:
        minutes = int(minutes_match.group(1))

    # Round to 2 decimal places for cleaner output
    return round(hours + minutes / 60.0, 2)


def get_media_type(media_list: list[str] | None) -> str:
    """
    Classify media content type based on available media items.

    Analyzes a list of media types and categorizes them into predefined
    categories: image-only, video-only, both, or none. This classification
    helps in scoring and ranking ads based on their media richness.

    Args:
        media_list: List of media type strings. Expected values:
                   "image", "video", or other media types.
                   Can be None or empty list.

    Returns:
        Media classification string: "image-only", "video-only", "both", or "none".

    Examples:
        >>> get_media_type(["image", "video"])
        "both"
        >>> get_media_type(["image"])
        "image-only"
        >>> get_media_type([])
        "none"
        >>> get_media_type(None)
        "none"
    """
    if not media_list:
        return "none"

    # Convert to set for efficient lookup and remove duplicates
    media_set = set(media_list)

    # Check for both image and video content
    if "image" in media_set and "video" in media_set:
        return "both"
    elif "image" in media_set:
        return "image-only"
    elif "video" in media_set:
        return "video-only"

    return "none"


def detect_language(text: str) -> str:
    """
    Detect the primary language of text content.

    Uses language detection library to identify the most likely language
    of the provided text. Handles edge cases and detection failures
    gracefully by returning "unknown" for problematic inputs.

    Args:
        text: Text content to analyze for language detection.
              Should be non-empty string for best results.

    Returns:
        ISO 639-1 language code (e.g., "en", "uk", "fr") or "unknown"
        if detection fails or input is invalid.

    Examples:
        >>> detect_language("Hello world")
        "en"
        >>> detect_language("Привіт світ")
        "uk"
        >>> detect_language("")
        "unknown"
        >>> detect_language("12345")
        "unknown"
    """
    if not text or not text.strip():
        return "unknown"

    try:
        return str(detect(text))
    except LangDetectException:
        return "unknown"
    except Exception:
        return "unknown"
