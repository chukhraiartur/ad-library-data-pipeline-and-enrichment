"""
Ad ranking module for the final analytics layer of the pipeline.

This module processes enriched advertisement data from the gold layer and
applies ranking algorithms to identify the top-performing ads. It generates
a CSV file with the top 10 ads based on calculated scores for further
analysis and reporting.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.schemas.ads import AdEnrichedSchema
from src.utils.logger import get_logger
from src.utils.ranker import top_10_ads

logger = get_logger(__name__)


def rank_ads(
    input_path: str = "data/gold/ads_enriched.jsonl",
    output_csv: str | None = None,
    **context: Any,
) -> str:
    """
    Rank enriched advertisements and save top 10 to CSV file.

    Reads enriched ad data from the gold layer, applies scoring algorithms
    to rank ads by effectiveness, and saves the top 10 performing ads to
    a CSV file for further analysis. This represents the final analytics
    layer of the data pipeline.

    Args:
        input_path: Path to enriched data file in gold layer (JSONL format).
        output_csv: Path where top 10 ranked ads will be saved as CSV.

    Raises:
        FileNotFoundError: If input file doesn't exist.
        RuntimeError: If ranking process fails.

    Example:
        >>> rank_ads("data/gold/enriched_ads.jsonl", "data/gold/top10_ads.csv")
        >>> # Generates CSV with top 10 performing ads
    """
    # Generate timestamped filename if not provided
    if output_csv is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_csv = f"data/gold/top10_ads_{timestamp}.csv"

    logger.info(f"Starting ad ranking from {input_path} to {output_csv}")

    try:
        # Load and validate enriched ads
        ads_data, stats = _load_enriched_ads(input_path)

        if not ads_data:
            logger.warning("No valid ads found for ranking")
            return output_csv

        # Apply ranking algorithm
        logger.info(f"Ranking {len(ads_data)} ads")
        top_ads = top_10_ads(ads_data)

        # Save results to CSV
        _save_ranking_results(top_ads, output_csv)

        # Log completion statistics
        _log_processing_stats(stats, output_csv)

    except FileNotFoundError:
        logger.error(f"Input file not found: {input_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to rank ads: {str(e)}")
        raise RuntimeError(f"Ad ranking failed: {str(e)}") from e

    # Return output path for XCom
    return output_csv


def _load_enriched_ads(input_path: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Load enriched ads from file and return with processing statistics.

    Reads the input file line by line, validates each record using
    Pydantic schemas, and collects processing statistics.

    Args:
        input_path: Path to enriched data file.

    Returns:
        Tuple containing:
        - List of validated ad dictionaries
        - Dictionary with processing statistics (processed, errors)
    """
    ads_data = []
    stats = {"processed": 0, "errors": 0}

    with open(input_path) as file:
        for line_num, line in enumerate(file, 1):
            try:
                ad = AdEnrichedSchema(**json.loads(line))
                ads_data.append(ad.model_dump())
                stats["processed"] += 1

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON at line {line_num}: {str(e)}")
                stats["errors"] += 1
            except Exception as e:
                logger.error(f"Failed to process record at line {line_num}: {str(e)}")
                stats["errors"] += 1

    return ads_data, stats


def _save_ranking_results(top_ads: list[dict[str, Any]], output_csv: str) -> None:
    """
    Save top ranked ads to CSV file.

    Converts the list of top ads to a pandas DataFrame and saves
    it as a CSV file for easy analysis and reporting.

    Args:
        top_ads: List of top 10 ad dictionaries.
        output_csv: File path where CSV will be saved.
    """
    # Ensure output directory exists
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)

    # Convert to DataFrame and save as CSV
    df = pd.DataFrame(top_ads)
    df.to_csv(output_csv, index=False)


def _log_processing_stats(stats: dict[str, int], output_csv: str) -> None:
    """
    Log processing statistics and completion message.

    Args:
        stats: Dictionary containing processing statistics.
        output_csv: Path where results were saved.
    """
    logger.info(
        f"Ranking completed: {stats['processed']} records processed, "
        f"{stats['errors']} errors"
    )
    logger.info(f"Top 10 ads saved to {output_csv}")
