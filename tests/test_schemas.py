from src.schemas.ads import AdEnrichedSchema, AdNormalizedSchema


def test_valid_normalized_schema() -> None:
    """Test valid AdNormalizedSchema creation"""
    data = {
        "ad_id": "test_123",
        "ad_text": "Test advertisement",
        "active": "Active for 2 hrs 30 mins",
        "media": ["image", "video"],
        "country": "US",
    }
    schema = AdNormalizedSchema(**data)
    assert schema.ad_id == "test_123"
    assert schema.ad_text == "Test advertisement"
    assert schema.media == ["image", "video"]
    assert schema.country == "US"
    assert schema.normalized_at is not None


def test_normalized_schema_with_optional_fields() -> None:
    """Test AdNormalizedSchema with optional fields"""
    data = {
        "ad_id": "test_123",
        "ad_text": "Test advertisement",
        "active": None,
        "media": [],
        "country": "US",
    }
    schema = AdNormalizedSchema(**data)
    assert schema.active is None
    assert schema.media == []


def test_valid_enriched_schema() -> None:
    """Test valid AdEnrichedSchema creation"""
    data = {
        "ad_id": "test_123",
        "ad_text": "Test advertisement",
        "active": "Active for 2 hrs 30 mins",
        "media": ["image"],
        "country": "US",
        "duration_hours": 2.5,
        "media_type": "image-only",
        "language": "en",
    }
    schema = AdEnrichedSchema(**data)
    assert schema.duration_hours == 2.5
    assert schema.media_type == "image-only"
    assert schema.language == "en"
    assert schema.enriched_at is not None


def test_enriched_schema_default_language() -> None:
    """Test AdEnrichedSchema with default language"""
    data = {
        "ad_id": "test_123",
        "ad_text": "Test advertisement",
        "active": None,
        "media": [],
        "country": "US",
        "duration_hours": 0.0,
        "media_type": "none",
    }
    schema = AdEnrichedSchema(**data)
    assert schema.language == "unknown"
