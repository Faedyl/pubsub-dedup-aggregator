"""
Event Models dan Schemas
"""

from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID


class EventPayload(BaseModel):
    """Event payload - flexible dictionary"""
    
    class Config:
        extra = "allow"


class Event(BaseModel):
    """Event model untuk Pub-Sub aggregator"""
    
    topic: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Topic name"
    )
    event_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique event identifier"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="ISO8601 timestamp"
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Event source/publisher name"
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Event data/payload"
    )
    
    @field_validator("topic")
    @classmethod
    def validate_topic(cls, v):
        if not v or not v.strip():
            raise ValueError("Topic cannot be empty")
        return v.lower().strip()

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v):
        if not v or not v.strip():
            raise ValueError("Event ID cannot be empty")
        return v.strip()

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v):
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError("Invalid ISO8601 timestamp format")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "topic": "app-logs",
                "event_id": "evt-001",
                "timestamp": "2024-04-24T12:00:00Z",
                "source": "service-a",
                "payload": {
                    "level": "INFO",
                    "message": "Application started"
                }
            }
        }


class PublishRequest(BaseModel):
    """Request model untuk /publish endpoint"""
    
    events: list[Event] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="List of events to publish"
    )


class PublishResponse(BaseModel):
    """Response model untuk /publish endpoint"""
    
    status: str
    received: int
    processed: int
    duplicates_detected: int
    timestamp: datetime


class EventResponse(BaseModel):
    """Response model untuk /events endpoint"""
    
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]
    processed_at: datetime


class StatsResponse(BaseModel):
    """Response model untuk /stats endpoint"""
    
    received: int = Field(description="Total events received")
    unique_processed: int = Field(description="Unique events processed")
    duplicate_dropped: int = Field(description="Duplicate events dropped")
    topics: list[str] = Field(description="List of topics")
    uptime_seconds: int = Field(description="Uptime in seconds")
    timestamp: datetime = Field(description="Stats timestamp")


class HealthResponse(BaseModel):
    """Response model untuk /health endpoint"""
    
    status: str
    timestamp: datetime
