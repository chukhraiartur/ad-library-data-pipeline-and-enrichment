"""
Data normalization module for the ad processing pipeline.

This module transforms raw advertisement data from the bronze layer into
a standardized format in the silver layer. It handles different data sources
(mock and API) and applies consistent field mapping and validation using
Pydantic schemas.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.schemas.ads import AdNormalizedSchema
from src.utils.json_encoder import DateTimeEncoder
from src.utils.logger import get_logger

logger = get_logger(__name__)


def normalize_ads(
    input_path: str = "data/bronze/ads_raw.jsonl",
    output_path: str | None = None,
    **context: Any,
) -> str:
    """
    Normalize raw advertisement data from bronze to silver layer.

    Reads raw ad data from the bronze layer and transforms it into a
    standardized format using Pydantic schemas. Handles different data
    sources (mock and API) with appropriate field mapping and validation.
    Processes data line by line for memory efficiency and provides
    detailed logging of processing statistics.

    Args:
        input_path: Path to raw data file in bronze layer (JSONL format).
        output_path: Path where normalized data will be saved in silver layer.

    Raises:
        FileNotFoundError: If input file doesn't exist.
        RuntimeError: If normalization process fails.

    Example:
        >>> normalize_ads("data/bronze/raw_ads.jsonl", "data/silver/normalized_ads.jsonl")
        >>> # Transforms raw data into standardized format
    """
    # Generate timestamped filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/silver/ads_normalized_{timestamp}.jsonl"

    logger.info(f"Starting data normalization from {input_path} to {output_path}")

    try:
        # Ensure output directory exists
        _ensure_output_directory(output_path)

        # Process raw data and collect statistics
        normalized_data, stats = _process_raw_data(input_path)

        # Write normalized data to output file
        _write_normalized_data(normalized_data, output_path)

        # Log completion statistics
        _log_processing_stats(stats, output_path)

    except FileNotFoundError:
        logger.error(f"Input file not found: {input_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to normalize data: {str(e)}")
        raise RuntimeError(f"Data normalization failed: {str(e)}") from e

    # Return output path for XCom
    return output_path


def _process_raw_data(input_path: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Process raw data file and return normalized records with statistics.

    Reads the input file line by line, normalizes each record based on
    its source type, and collects processing statistics.

    Args:
        input_path: Path to raw data file.

    Returns:
        Tuple containing:
        - List of normalized ad dictionaries
        - Dictionary with processing statistics (processed, skipped)
    """
    normalized_data = []
    stats = {"processed": 0, "skipped": 0}

    with open(input_path) as file:
        for line_num, line in enumerate(file, 1):
            try:
                raw_record = json.loads(line)
                normalized_record = _normalize_single_record(raw_record, line_num)

                if normalized_record:
                    normalized_data.append(normalized_record)
                    stats["processed"] += 1
                else:
                    stats["skipped"] += 1

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON at line {line_num}: {str(e)}")
                stats["skipped"] += 1
            except Exception as e:
                logger.error(f"Failed to normalize record at line {line_num}: {str(e)}")
                stats["skipped"] += 1

    return normalized_data, stats


def _normalize_single_record(
    raw_record: dict[str, Any], line_num: int
) -> dict[str, Any] | None:
    """
    Normalize a single raw record based on its source type.

    Applies source-specific field mapping and validation to transform
    raw data into standardized format.

    Args:
        raw_record: Raw record dictionary from input file.
        line_num: Line number for logging purposes.

    Returns:
        Normalized record dictionary or None if processing failed.
    """
    raw_data = raw_record.get("raw_data", {})
    source = raw_record.get("source", "unknown")

    try:
        if source == "mock":
            logger.debug(f"Processing mock data record {line_num}")
            return _normalize_mock_record(raw_data)
        elif source == "api":
            logger.debug(f"Processing API data record {line_num}")
            return _normalize_api_record(raw_data)
        else:
            logger.warning(f"Skipping record {line_num} with unknown source: {source}")
            return None

    except Exception as e:
        logger.error(
            f"Failed to normalize {source} record at line {line_num}: {str(e)}"
        )
        return None


def _normalize_mock_record(raw_data: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize mock data record using direct field mapping.

    Args:
        raw_data: Raw data dictionary from mock source.

    Returns:
        Normalized record dictionary.
    """
    normalized = AdNormalizedSchema(
        ad_id=raw_data.get("ad_id", ""),
        ad_text=raw_data.get("ad_text", ""),
        active=raw_data.get("active"),
        media=raw_data.get("media", []),
        country=raw_data.get("country", ""),
    )
    return normalized.model_dump()


def _normalize_api_record(raw_data: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize API data record using field mapping for Facebook API.

    Args:
        raw_data: Raw data dictionary from Facebook API.

    Returns:
        Normalized record dictionary.
    """
    normalized = AdNormalizedSchema(
        ad_id=raw_data.get("id", ""),
        ad_text=raw_data.get("ad_creative_body", ""),
        active=None,  # API doesn't provide this field
        media=[],  # API doesn't provide this field
        country="US",  # Default for API data
    )
    return normalized.model_dump()


def _ensure_output_directory(output_path: str) -> None:
    """
    Ensure the output directory exists, creating it if necessary.

    Args:
        output_path: File path where normalized data will be written.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)


def _write_normalized_data(
    normalized_data: list[dict[str, Any]], output_path: str
) -> None:
    """
    Write normalized data to output file in JSONL format.

    Args:
        normalized_data: List of normalized ad dictionaries.
        output_path: File path where data will be written.
    """
    with open(output_path, "w") as file:
        for item in normalized_data:
            file.write(json.dumps(item, cls=DateTimeEncoder) + "\n")


def _log_processing_stats(stats: dict[str, int], output_path: str) -> None:
    """
    Log processing statistics and completion message.

    Args:
        stats: Dictionary containing processing statistics.
        output_path: Path where data was saved.
    """
    logger.info(
        f"Normalization completed: {stats['processed']} records processed, "
        f"{stats['skipped']} skipped"
    )
    logger.info(f"Normalized data saved to {output_path}")
