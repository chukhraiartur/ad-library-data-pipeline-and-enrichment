"""
Data enrichment module for the ad processing pipeline.

This module transforms normalized advertisement data from the silver layer into
enriched data in the gold layer. It adds derived fields such as duration parsing,
media type classification, and language detection to provide more analytical
insights for downstream processing.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.schemas.ads import AdEnrichedSchema, AdNormalizedSchema
from src.utils.enricher import detect_language, get_media_type, parse_duration
from src.utils.json_encoder import DateTimeEncoder
from src.utils.logger import get_logger

logger = get_logger(__name__)


def enrich_ads(
    input_path: str = "data/silver/ads_normalized.jsonl",
    output_path: str | None = None,
    **context: Any,
) -> str:
    """
    Enrich normalized advertisement data from silver to gold layer.

    Reads normalized ad data from the silver layer and enhances it with
    derived fields including parsed duration, media type classification,
    and language detection. This enrichment process adds analytical value
    to the data for better insights and ranking capabilities.

    Args:
        input_path: Path to normalized data file in silver layer (JSONL format).
        output_path: Path where enriched data will be saved in gold layer.

    Raises:
        FileNotFoundError: If input file doesn't exist.
        RuntimeError: If enrichment process fails.

    Example:
        >>> enrich_ads("data/silver/normalized_ads.jsonl", "data/gold/enriched_ads.jsonl")
        >>> # Adds derived fields for better analytics
    """
    # Generate timestamped filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/gold/ads_enriched_{timestamp}.jsonl"

    logger.info(f"Starting data enrichment from {input_path} to {output_path}")

    try:
        # Ensure output directory exists
        _ensure_output_directory(output_path)

        # Process normalized data and collect statistics
        enriched_data, stats = _process_normalized_data(input_path)

        # Write enriched data to output file
        _write_enriched_data(enriched_data, output_path)

        # Log completion statistics
        _log_processing_stats(stats, output_path)

    except FileNotFoundError:
        logger.error(f"Input file not found: {input_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to enrich data: {str(e)}")
        raise RuntimeError(f"Data enrichment failed: {str(e)}") from e

    # Return output path for XCom
    return output_path


def _process_normalized_data(
    input_path: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Process normalized data file and return enriched records with statistics.

    Reads the input file line by line, enriches each record with derived
    fields, and collects processing statistics.

    Args:
        input_path: Path to normalized data file.

    Returns:
        Tuple containing:
        - List of enriched ad dictionaries
        - Dictionary with processing statistics (processed, errors)
    """
    enriched_data = []
    stats = {"processed": 0, "errors": 0}

    with open(input_path) as file:
        for line_num, line in enumerate(file, 1):
            try:
                normalized_ad = AdNormalizedSchema(**json.loads(line))
                enriched_record = _enrich_single_record(normalized_ad, line_num)

                if enriched_record:
                    enriched_data.append(enriched_record)
                    stats["processed"] += 1
                else:
                    stats["errors"] += 1

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON at line {line_num}: {str(e)}")
                stats["errors"] += 1
            except Exception as e:
                logger.error(f"Failed to enrich record at line {line_num}: {str(e)}")
                stats["errors"] += 1

    return enriched_data, stats


def _enrich_single_record(
    normalized_ad: AdNormalizedSchema, line_num: int
) -> dict[str, Any]:
    """
    Enrich a single normalized record with derived fields.

    Applies enrichment functions to add duration parsing, media type
    classification, and language detection to the normalized data.

    Args:
        normalized_ad: Normalized ad schema object.
        line_num: Line number for logging purposes.

    Returns:
        Enriched record dictionary.
    """
    logger.debug(f"Enriching record {line_num}: {normalized_ad.ad_id}")

    enriched_ad = AdEnrichedSchema(
        ad_id=normalized_ad.ad_id,
        ad_text=normalized_ad.ad_text,
        active=normalized_ad.active,
        media=normalized_ad.media,
        country=normalized_ad.country,
        duration_hours=parse_duration(normalized_ad.active),
        media_type=get_media_type(normalized_ad.media),
        language=detect_language(normalized_ad.ad_text),
    )

    return enriched_ad.model_dump()


def _ensure_output_directory(output_path: str) -> None:
    """
    Ensure the output directory exists, creating it if necessary.

    Args:
        output_path: File path where enriched data will be written.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)


def _write_enriched_data(enriched_data: list[dict[str, Any]], output_path: str) -> None:
    """
    Write enriched data to output file in JSONL format.

    Args:
        enriched_data: List of enriched ad dictionaries.
        output_path: File path where data will be written.
    """
    with open(output_path, "w") as file:
        for item in enriched_data:
            file.write(json.dumps(item, cls=DateTimeEncoder) + "\n")


def _log_processing_stats(stats: dict[str, int], output_path: str) -> None:
    """
    Log processing statistics and completion message.

    Args:
        stats: Dictionary containing processing statistics.
        output_path: Path where data was saved.
    """
    logger.info(
        f"Enrichment completed: {stats['processed']} records processed, "
        f"{stats['errors']} errors"
    )
    logger.info(f"Enriched data saved to {output_path}")
