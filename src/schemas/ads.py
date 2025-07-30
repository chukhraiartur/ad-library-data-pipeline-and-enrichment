"""
Pydantic data schemas for advertisement data pipeline layers.

This module defines the data structures used throughout the pipeline,
following the Bronze/Silver/Gold architecture pattern. Each schema
represents a different layer of data processing with increasing
structure and enrichment.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class AdNormalizedSchema(BaseModel):
    """
    Silver layer schema for normalized advertisement data.

    Represents standardized advertisement data after normalization
    from raw sources. This layer provides consistent field structure
    across different data sources (mock and API).

    Attributes:
        ad_id: Unique identifier for the advertisement.
        ad_text: Main text content of the advertisement.
        active: Optional duration information in text format.
        media: List of media types associated with the ad.
        country: Country code where the ad was targeted.
    """

    ad_id: str = Field(..., description="Unique advertisement identifier")
    ad_text: str = Field(..., description="Main advertisement text content")
    active: str | None = Field(None, description="Duration information in text format")
    media: list[str] = Field(
        default=[], description="List of media types (image, video, etc.)"
    )
    country: str = Field(..., description="Target country code")
    normalized_at: datetime = Field(
        default_factory=datetime.now, description="Timestamp when data was normalized"
    )


class AdEnrichedSchema(BaseModel):
    """
    Gold layer schema for enriched advertisement data.

    Extends the normalized schema with derived analytical fields
    including parsed duration, media type classification, and
    language detection. This layer provides the richest data
    structure for analysis and ranking.

    Attributes:
        ad_id: Unique identifier for the advertisement.
        ad_text: Main text content of the advertisement.
        active: Optional duration information in text format.
        media: List of media types associated with the ad.
        country: Country code where the ad was targeted.
        duration_hours: Parsed duration in decimal hours.
        media_type: Classified media type (image-only, video-only, both, none).
        language: Detected language code or "unknown".
    """

    ad_id: str = Field(..., description="Unique advertisement identifier")
    ad_text: str = Field(..., description="Main advertisement text content")
    active: str | None = Field(None, description="Duration information in text format")
    media: list[str] = Field(
        default=[], description="List of media types (image, video, etc.)"
    )
    country: str = Field(..., description="Target country code")
    duration_hours: float = Field(..., description="Parsed duration in decimal hours")
    media_type: str = Field(
        ..., description="Classified media type (image-only, video-only, both, none)"
    )
    language: str | None = Field(
        default="unknown", description="Detected language code"
    )
    enriched_at: datetime = Field(
        default_factory=datetime.now, description="Timestamp when data was enriched"
    )
