import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from src.extract.fetch_ads import fetch_ads_data
from src.extract.modes.mock_mode import fetch_mock_ads


def test_fetch_mock_ads_creates_file() -> None:
    """Test that mock ads are created and saved to file"""
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".jsonl"
    ) as tmp_file:
        output_path = tmp_file.name

    try:
        fetch_mock_ads(output_path, count=5)

        # Check if file exists and contains data
        assert os.path.exists(output_path)

        with open(output_path) as f:
            lines = f.readlines()
            assert len(lines) == 5

            # Check structure of first record
            first_record = json.loads(lines[0])
            assert "source" in first_record
            assert "ingestion_time" in first_record
            assert "raw_data" in first_record
            assert first_record["source"] == "mock"

            # Check raw_data structure
            raw_data = first_record["raw_data"]
            assert "ad_id" in raw_data
            assert "ad_text" in raw_data
            assert "media" in raw_data
            assert "country" in raw_data

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


def test_fetch_mock_ads_custom_count() -> None:
    """Test mock ads with custom count"""
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".jsonl"
    ) as tmp_file:
        output_path = tmp_file.name

    try:
        fetch_mock_ads(output_path, count=10)

        with open(output_path) as f:
            lines = f.readlines()
            assert len(lines) == 10

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


@patch("src.extract.fetch_ads.fetch_mock_ads")
def test_fetch_ads_data_mock_mode(mock_fetch_mock) -> None:
    """Test fetch_ads_data with mock mode"""
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".jsonl"
    ) as tmp_file:
        output_path = tmp_file.name

    try:
        fetch_ads_data(mode="mock", output_path=output_path)

        # Verify mock function was called
        mock_fetch_mock.assert_called_once_with(output_path)

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


@patch("src.extract.fetch_ads.fetch_api_ads")
@patch("src.extract.fetch_ads.os.getenv")
def test_fetch_ads_data_api_mode(mock_getenv, mock_fetch_api) -> None:
    """Test fetch_ads_data with API mode"""
    mock_getenv.return_value = "test_token"

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".jsonl"
    ) as tmp_file:
        output_path = tmp_file.name

    try:
        fetch_ads_data(mode="api", output_path=output_path)

        # Verify API function was called
        mock_fetch_api.assert_called_once_with(output_path, "test_token")

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


@patch("src.extract.fetch_ads.os.getenv")
def test_fetch_ads_data_api_mode_no_token(mock_getenv) -> None:
    """Test fetch_ads_data with API mode but no token"""
    mock_getenv.return_value = None

    with pytest.raises(
        RuntimeError,
        match="Data extraction failed: ACCESS_TOKEN environment variable is required for API mode",
    ):
        fetch_ads_data(mode="api")


def test_fetch_ads_data_invalid_mode() -> None:
    """Test fetch_ads_data with invalid mode"""
    with pytest.raises(
        RuntimeError, match="Data extraction failed: Expected code to be unreachable, but got: 'invalid_mode'"
    ):
        fetch_ads_data(mode="invalid_mode")


@patch("src.extract.modes.api_mode.requests.get")
def test_fetch_api_ads_success(mock_get) -> None:
    """Test successful API call"""
    # Mock successful response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [
            {"id": "ad_123", "ad_creative_body": "Test ad", "page_name": "Test Page"}
        ]
    }
    mock_get.return_value = mock_response

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".jsonl"
    ) as tmp_file:
        output_path = tmp_file.name

    try:
        from src.extract.modes.api_mode import fetch_api_ads

        fetch_api_ads(output_path, "test_token")

        # Check if file was created
        assert os.path.exists(output_path)

        with open(output_path) as f:
            lines = f.readlines()
            assert len(lines) == 1

            record = json.loads(lines[0])
            assert record["source"] == "api"
            assert "ingestion_time" in record
            assert "raw_data" in record

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


@patch("src.extract.modes.api_mode.requests.get")
def test_fetch_api_ads_error(mock_get) -> None:
    """Test API call with error response"""
    # Mock error response
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.text = "Unauthorized"
    mock_get.return_value = mock_response

    with pytest.raises(RuntimeError, match="Facebook API error: 401"):
        from src.extract.modes.api_mode import fetch_api_ads

        fetch_api_ads("test_path", "test_token")
