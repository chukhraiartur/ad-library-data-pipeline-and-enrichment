#!/usr/bin/env python3
"""
Local pipeline runner for advertisement data processing.

This script provides a command-line interface to run the complete ETL pipeline
locally without requiring Apache Airflow. It orchestrates all pipeline stages:
extract -> normalize -> enrich -> rank, with comprehensive logging and error handling.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Literal

from src.extract.fetch_ads import fetch_ads_data
from src.normalize.normalize_ads import normalize_ads
from src.enrich.enrich_ads import enrich_ads
from src.rank.rank_ads import rank_ads
from src.utils.logger import setup_logging, get_logger

# Type alias for supported extraction modes
ExtractionMode = Literal["mock", "api"]

# Pipeline configuration constants
DEFAULT_LOG_FILE = "logs/local_pipeline.log"


def run_pipeline(mode: ExtractionMode = "mock", log_file: str = DEFAULT_LOG_FILE) -> None:
    """
    Execute the complete advertisement data processing pipeline.
    
    Orchestrates all pipeline stages in sequence: data extraction (bronze layer),
    normalization (silver layer), enrichment (gold layer), and ranking (analytics).
    Provides comprehensive logging and error handling throughout the process.
    
    Args:
        mode: Data extraction mode - "mock" for synthetic data or "api" for real data.
        log_file: Path to log file for pipeline execution logs.
    
    Raises:
        SystemExit: If pipeline execution fails with exit code 1.
    
    Example:
        >>> run_pipeline("mock", "logs/test_pipeline.log")
        >>> # Executes complete pipeline with mock data
    """
    # Initialize logging system
    setup_logging(level=logging.INFO, log_file=log_file)
    logger = get_logger(__name__)
    
    # Log pipeline start with configuration
    _log_pipeline_start(logger, mode)
    
    try:
        # Execute pipeline stages sequentially
        bronze_path = _execute_extraction_stage(logger, mode)
        silver_path = _execute_normalization_stage(logger, bronze_path)
        gold_path = _execute_enrichment_stage(logger, silver_path)
        top10_path = _execute_ranking_stage(logger, gold_path)
        
        # Log successful completion
        _log_pipeline_success(logger, top10_path)
        
    except Exception as e:
        _log_pipeline_failure(logger, str(e))
        sys.exit(1)


def _execute_extraction_stage(logger, mode: ExtractionMode) -> str:
    """
    Execute data extraction stage (Bronze layer).
    
    Args:
        logger: Logger instance for stage logging.
        mode: Extraction mode to use.
        
    Returns:
        Path to the extracted data file.
    """
    logger.info("Step 1: Extracting raw data (Bronze layer)")
    return fetch_ads_data(mode=mode)


def _execute_normalization_stage(logger, input_path: str) -> str:
    """
    Execute data normalization stage (Silver layer).
    
    Args:
        logger: Logger instance for stage logging.
        input_path: Path to input data file.
        
    Returns:
        Path to the normalized data file.
    """
    logger.info("Step 2: Normalizing data (Silver layer)")
    return normalize_ads(input_path=input_path)


def _execute_enrichment_stage(logger, input_path: str) -> str:
    """
    Execute data enrichment stage (Gold layer).
    
    Args:
        logger: Logger instance for stage logging.
        input_path: Path to input data file.
        
    Returns:
        Path to the enriched data file.
    """
    logger.info("Step 3: Enriching data (Gold layer)")
    return enrich_ads(input_path=input_path)


def _execute_ranking_stage(logger, input_path: str) -> str:
    """
    Execute ad ranking stage (Analytics layer).
    
    Args:
        logger: Logger instance for stage logging.
        input_path: Path to input data file.
        
    Returns:
        Path to the ranking results file.
    """
    logger.info("Step 4: Ranking top ads")
    return rank_ads(input_path=input_path)


def _log_pipeline_start(logger, mode: ExtractionMode) -> None:
    """
    Log pipeline start with configuration details.
    
    Args:
        logger: Logger instance for pipeline logging.
        mode: Extraction mode being used.
    """
    logger.info("=" * 50)
    logger.info("Starting local ad data pipeline")
    logger.info(f"Mode: {mode}")
    logger.info("=" * 50)


def _log_pipeline_success(logger, output_path: str) -> None:
    """
    Log successful pipeline completion with output file locations.
    
    Args:
        logger: Logger instance for pipeline logging.
        output_path: Path to the final output file.
    """
    logger.info("=" * 50)
    logger.info("Pipeline completed successfully!")
    logger.info(f"Results saved to: {output_path}")
    logger.info("=" * 50)


def _log_pipeline_failure(logger, error_message: str) -> None:
    """
    Log pipeline failure with error details.
    
    Args:
        logger: Logger instance for pipeline logging.
        error_message: Error message to log.
    """
    logger.error(f"Pipeline failed: {error_message}")
    logger.error("Check logs for detailed error information")


def _create_argument_parser() -> argparse.ArgumentParser:
    """
    Create and configure command-line argument parser.
    
    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        description="Run advertisement data pipeline locally",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_local_pipeline.py --mode mock
  python run_local_pipeline.py --mode api --log-file logs/api_pipeline.log
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["mock", "api"],
        default="mock",
        help="Data extraction mode (default: mock)"
    )
    parser.add_argument(
        "--log-file",
        default=DEFAULT_LOG_FILE,
        help=f"Path to log file (default: {DEFAULT_LOG_FILE})"
    )
    
    return parser


def _ensure_log_directory(log_file: str) -> None:
    """
    Ensure the log directory exists, creating it if necessary.
    
    Args:
        log_file: Path to log file.
    """
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    # Parse command-line arguments
    parser = _create_argument_parser()
    args = parser.parse_args()
    
    # Ensure log directory exists
    _ensure_log_directory(args.log_file)
    
    # Execute pipeline with parsed arguments
    run_pipeline(mode=args.mode, log_file=args.log_file)