"""
Common test fixtures and configuration for the ad data pipeline tests.
"""

import tempfile

import pytest


@pytest.fixture
def temp_dir() -> str:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_mock_data() -> list[dict]:
    """Sample mock data for testing."""
    return [
        {
            "source": "mock",
            "ingestion_time": "2024-01-01T12:00:00Z",
            "raw_data": {
                "ad_id": "test_1",
                "ad_text": "Hello World!",
                "active": "Active for 2 hrs 30 mins",
                "media": ["image", "video"],
                "country": "US",
            },
        },
        {
            "source": "mock",
            "ingestion_time": "2024-01-01T12:00:00Z",
            "raw_data": {
                "ad_id": "test_2",
                "ad_text": "Привіт Світ!",
                "active": "Active for 1 hr",
                "media": ["image"],
                "country": "CA",
            },
        },
    ]


@pytest.fixture
def sample_api_data() -> list[dict]:
    """Sample API data for testing."""
    return [
        {
            "source": "api",
            "ingestion_time": "2024-01-01T12:00:00Z",
            "raw_data": {
                "id": "api_123",
                "ad_creative_body": "API test advertisement",
                "page_name": "Test Page",
            },
        }
    ]


@pytest.fixture
def sample_normalized_data() -> list[dict]:
    """Sample normalized data for testing."""
    return [
        {
            "ad_id": "test_1",
            "ad_text": "Hello World!",
            "active": "Active for 2 hrs 30 mins",
            "media": ["image", "video"],
            "country": "US",
        },
        {
            "ad_id": "test_2",
            "ad_text": "Привіт Світ!",
            "active": "Active for 1 hr",
            "media": ["image"],
            "country": "CA",
        },
    ]


@pytest.fixture
def sample_enriched_data() -> list[dict]:
    """Sample enriched data for testing."""
    return [
        {
            "ad_id": "test_1",
            "ad_text": "Hello World!",
            "active": "Active for 2 hrs 30 mins",
            "media": ["image", "video"],
            "country": "US",
            "duration_hours": 2.5,
            "media_type": "both",
            "language": "en",
        },
        {
            "ad_id": "test_2",
            "ad_text": "Привіт Світ!",
            "active": "Active for 1 hr",
            "media": ["image"],
            "country": "CA",
            "duration_hours": 1.0,
            "media_type": "image-only",
            "language": "uk",
        },
    ]


@pytest.fixture
def create_temp_file():
    """Create a temporary file with given content."""

    def _create_temp_file(content: str | list, suffix: str = ".jsonl") -> str:
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=suffix) as f:
            if isinstance(content, list):
                import json

                for item in content:
                    f.write(json.dumps(item) + "\n")
            else:
                f.write(content)
            return f.name

    return _create_temp_file


@pytest.fixture(autouse=True)
def setup_logging() -> None:
    """Setup logging for tests."""
    import logging

    from src.utils.logger import setup_logging

    setup_logging(level=logging.WARNING)  # Reduce log noise during tests
