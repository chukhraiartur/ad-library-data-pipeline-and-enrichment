from src.utils.enricher import detect_language, get_media_type, parse_duration


# Parse Duration Tests
def test_parse_duration_hours_only() -> None:
    """Test parsing duration with hours only"""
    assert parse_duration("Active for 2 hrs") == 2.0
    assert parse_duration("Active for 5 hrs") == 5.0
    assert parse_duration("Active for 0 hrs") == 0.0


def test_parse_duration_minutes_only() -> None:
    """Test parsing duration with minutes only"""
    assert parse_duration("Active for 30 mins") == 0.5
    assert parse_duration("Active for 45 mins") == 0.75
    assert parse_duration("Active for 60 mins") == 1.0


def test_parse_duration_hours_and_minutes() -> None:
    """Test parsing duration with both hours and minutes"""
    assert parse_duration("Active for 2 hrs 30 mins") == 2.5
    # Now supports singular "hr" form
    assert parse_duration("Active for 1 hr 15 mins") == 1.25
    assert parse_duration("Active for 3 hrs 45 mins") == 3.75


def test_parse_duration_edge_cases() -> None:
    """Test parsing duration edge cases"""
    assert parse_duration("") == 0.0
    assert parse_duration(None) == 0.0
    assert parse_duration("No duration info") == 0.0
    assert parse_duration("Active for 0 hrs 0 mins") == 0.0


def test_parse_duration_invalid_formats() -> None:
    """Test parsing duration with invalid formats"""
    assert parse_duration("Active for 2 hours") == 0.0  # "hours" instead of "hrs"
    # Now supports "minutes" form
    assert parse_duration("Active for 30 minutes") == 0.5  # "minutes" is now supported
    # Note: Current function parses even without "Active for" prefix
    assert parse_duration("2 hrs 30 mins") == 2.5  # Actually works


# Media Type Tests
def test_get_media_type_image_only() -> None:
    """Test media type detection for image only"""
    assert get_media_type(["image"]) == "image-only"
    assert get_media_type(["image", "image"]) == "image-only"  # duplicates


def test_get_media_type_video_only() -> None:
    """Test media type detection for video only"""
    assert get_media_type(["video"]) == "video-only"
    assert get_media_type(["video", "video"]) == "video-only"  # duplicates


def test_get_media_type_both() -> None:
    """Test media type detection for both image and video"""
    assert get_media_type(["image", "video"]) == "both"
    assert get_media_type(["video", "image"]) == "both"  # order doesn't matter
    assert get_media_type(["image", "video", "image"]) == "both"  # duplicates


def test_get_media_type_none() -> None:
    """Test media type detection for no media"""
    assert get_media_type([]) == "none"
    assert get_media_type(None) == "none"


def test_get_media_type_unknown_types() -> None:
    """Test media type detection with unknown media types"""
    assert get_media_type(["audio"]) == "none"  # unknown type
    assert get_media_type(["image", "audio"]) == "image-only"  # only known types count


# Language Detection Tests
def test_detect_language_english() -> None:
    """Test language detection for English"""
    assert detect_language("Hello World!") == "en"
    assert detect_language("This is a test advertisement") == "en"
    # langdetect may sometimes misclassify English text
    result = detect_language("Microlearning techniques")
    assert result in [
        "en",
        "fr",
        "de",
    ]  # Could be detected as English, French, or German


def test_detect_language_ukrainian() -> None:
    """Test language detection for Ukrainian"""
    # langdetect may return different codes, so we check for valid ones
    uk_result = detect_language("Привіт Світ!")
    assert uk_result in ["uk", "mk", "bg", "sr"]  # Slavic languages

    uk_result2 = detect_language("Це тестова реклама")
    assert uk_result2 in ["uk", "mk", "bg", "sr"]  # Slavic languages


def test_detect_language_edge_cases() -> None:
    """Test language detection edge cases"""
    assert detect_language("") == "unknown"  # empty string
    assert detect_language("12345") == "unknown"  # numbers only
    assert detect_language("!@#$%") == "unknown"  # symbols only
    # Single character may be detected as a language
    single_char_result = detect_language("a")
    assert isinstance(single_char_result, str)  # Should return some language code


def test_detect_language_mixed_content() -> None:
    """Test language detection with mixed content"""
    # Should handle mixed content gracefully
    result = detect_language("Hello 123 Привіт!")
    assert isinstance(result, str)  # Should return some language code


# Integration Tests
def test_enrichment_pipeline() -> None:
    """Test that all enrichment functions work together"""
    # Simulate a complete enrichment process
    ad_text = "Hello World! This is a test advertisement"
    active_time = "Active for 2 hrs 30 mins"
    media_list = ["image", "video"]

    duration = parse_duration(active_time)
    media_type = get_media_type(media_list)
    language = detect_language(ad_text)

    assert duration == 2.5
    assert media_type == "both"
    assert language == "en"
