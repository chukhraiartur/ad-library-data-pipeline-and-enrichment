from src.utils.ranker import proxy_score, top_10_ads


def test_proxy_score_basic() -> None:
    """Test basic proxy score calculation"""
    ad = {"duration_hours": 2.0, "media_type": "image-only"}
    score = proxy_score(ad)
    assert score == 2.0  # 2.0 * 1.0 (image-only multiplier)


def test_proxy_score_video() -> None:
    """Test proxy score with video media type"""
    ad = {"duration_hours": 3.0, "media_type": "video-only"}
    score = proxy_score(ad)
    # Use approximate comparison for float precision issues
    assert abs(score - 3.3) < 0.001  # 3.0 * 1.1 (video-only multiplier)


def test_proxy_score_both_media() -> None:
    """Test proxy score with both image and video"""
    ad = {"duration_hours": 1.5, "media_type": "both"}
    score = proxy_score(ad)
    # Use approximate comparison for float precision issues
    assert abs(score - 1.8) < 0.001  # 1.5 * 1.2 (both multiplier)


def test_proxy_score_no_media() -> None:
    """Test proxy score with no media"""
    ad = {"duration_hours": 4.0, "media_type": "none"}
    score = proxy_score(ad)
    assert score == 2.0  # 4.0 * 0.5 (none multiplier)


def test_proxy_score_missing_fields() -> None:
    """Test proxy score with missing fields"""
    ad = {}
    score = proxy_score(ad)
    assert score == 0.0  # 0.0 * 1.0 (default values)


def test_top_10_ads_less_than_10() -> None:
    """Test top 10 with less than 10 ads"""
    ads = [
        {"duration_hours": 1.0, "media_type": "image-only"},
        {"duration_hours": 2.0, "media_type": "video-only"},
        {"duration_hours": 3.0, "media_type": "both"},
    ]
    result = top_10_ads(ads)
    assert len(result) == 3
    # Should be sorted by score descending
    assert result[0]["duration_hours"] == 3.0  # Highest score


def test_top_10_ads_more_than_10() -> None:
    """Test top 10 with more than 10 ads"""
    ads = [{"duration_hours": i, "media_type": "image-only"} for i in range(1, 15)]
    result = top_10_ads(ads)
    assert len(result) == 10
    # Should be sorted by score descending
    assert result[0]["duration_hours"] == 14  # Highest score
    assert result[-1]["duration_hours"] == 5  # 10th highest


def test_top_10_ads_empty_list() -> None:
    """Test top 10 with empty list"""
    result = top_10_ads([])
    assert result == []
