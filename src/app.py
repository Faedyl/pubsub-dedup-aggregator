"""
FastAPI Application Factory
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os
from typing import Optional, Dict, Any
import asyncio
from .models import Event, PublishRequest, PublishResponse, EventResponse, StatsResponse
from .dedup_store import DedupStore
from .consumer import IdempotentConsumer
from .utils import EventMetrics, format_timestamp

logger = logging.getLogger(__name__)


# Global state
_consumer: Optional[IdempotentConsumer] = None
_dedup_store: Optional[DedupStore] = None
_metrics: Optional[EventMetrics] = None
_worker_task: Optional[asyncio.Task] = None


def create_app(db_path: Optional[str] = None) -> FastAPI:
    """Create and configure FastAPI application"""

    global _consumer, _dedup_store, _metrics

    if db_path is None:
        db_path = os.getenv("DATABASE_PATH", "/app/data/dedup.db")

    _dedup_store = DedupStore(db_path=db_path)
    _consumer = IdempotentConsumer(_dedup_store)
    _metrics = EventMetrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("Application startup...")
        await _consumer.start()
        logger.info("Consumer started successfully")
        try:
            yield
        finally:
            logger.info("Application shutdown...")
            await _consumer.stop()
            _dedup_store.close()
            logger.info("Application shutdown complete")

    app = FastAPI(
        title="Pub-Sub Log Aggregator",
        description="Pub-Sub Log Aggregator dengan Idempotent Consumer dan Deduplication",
        version="1.0.0",
        lifespan=lifespan,
    )
    
    # ============================================================
    # Health Check
    # ============================================================
    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {
            "status": "healthy",
            "timestamp": format_timestamp()
        }
    
    # ============================================================
    # Publish Events
    # ============================================================
    @app.post("/publish")
    async def publish_events(request: PublishRequest):
        """
        Publish single atau batch event
        
        Request format:
        {
            "events": [
                {
                    "topic": "string",
                    "event_id": "string-unik",
                    "timestamp": "ISO8601",
                    "source": "string",
                    "payload": {...}
                }
            ]
        }
        """
        try:
            received_count = len(request.events)
            processed_count = 0
            duplicates_detected = 0
            
            for event in request.events:
                _metrics.on_received()
                
                # Check if duplicate
                is_duplicate = await _dedup_store.exists(event.topic, event.event_id)
                
                if is_duplicate:
                    duplicates_detected += 1
                    _metrics.on_duplicate()
                    logger.warning(
                        f"Duplicate detected: topic={event.topic}, "
                        f"event_id={event.event_id}, source={event.source}"
                    )
                else:
                    # Enqueue for processing
                    await _consumer.enqueue_event(event)
                    processed_count += 1
                    _metrics.on_processed()
            
            return PublishResponse(
                status="success",
                received=received_count,
                processed=processed_count,
                duplicates_detected=duplicates_detected,
                timestamp=datetime.utcnow()
            )
            
        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Error publishing events: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # ============================================================
    # Get Events
    # ============================================================
    @app.get("/events")
    async def get_events(
        topic: Optional[str] = Query(None, description="Filter by topic"),
        limit: int = Query(100, ge=1, le=1000)
    ):
        """
        Get list of processed unique events
        
        Query params:
        - topic: Filter by topic (optional)
        - limit: Maximum number of events (default: 100, max: 1000)
        """
        try:
            # Get events from dedup store
            entries = await _dedup_store.get_all_entries(topic=topic)
            
            # Convert to response format
            events = []
            for entry in entries:
                # Get full event info from consumer cache if available
                dedup_key = (entry['topic'], entry['event_id'])
                processed_event = _consumer.processed_events.get(dedup_key)
                
                if processed_event:
                    event_data = processed_event['event']
                    response = EventResponse(
                        topic=event_data['topic'],
                        event_id=event_data['event_id'],
                        timestamp=event_data['timestamp'],
                        source=event_data['source'],
                        payload=event_data['payload'],
                        processed_at=processed_event['processed_at']
                    )
                    events.append(response)
            
            return {
                "status": "success",
                "events": events[:limit],
                "count": len(events),
                "timestamp": format_timestamp()
            }
            
        except Exception as e:
            logger.error(f"Error getting events: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # ============================================================
    # Get Statistics
    # ============================================================
    @app.get("/stats")
    async def get_stats():
        """
        Get system statistics
        """
        try:
            topics = await _dedup_store.get_topics()
            stats = _metrics.get_stats()
            
            return StatsResponse(
                received=stats['received'],
                unique_processed=stats['unique_processed'],
                duplicate_dropped=stats['duplicate_dropped'],
                topics=topics,
                uptime_seconds=stats['uptime_seconds'],
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # ============================================================
    # Error Handlers
    # ============================================================
    @app.exception_handler(ValueError)
    async def value_error_handler(request, exc):
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": str(exc),
                "timestamp": format_timestamp()
            }
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request, exc):
        logger.error(f"Unhandled exception: {str(exc)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Internal server error",
                "timestamp": format_timestamp()
            }
        )
    
    return app
