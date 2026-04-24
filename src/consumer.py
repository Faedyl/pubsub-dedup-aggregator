"""
Consumer Logic - Idempotent Event Processing
"""

import asyncio
import logging
from typing import Optional, Set, Tuple
from datetime import datetime
from .models import Event
from .dedup_store import DedupStore

logger = logging.getLogger(__name__)


class IdempotentConsumer:
    """
    Idempotent consumer yang memproses event hanya sekali
    meskipun event duplikat diterima berkali-kali.
    """
    
    def __init__(self, dedup_store: DedupStore):
        """
        Initialize consumer
        
        Args:
            dedup_store: DedupStore instance untuk tracking event yang sudah diproses
        """
        self.dedup_store = dedup_store
        self.processed_events: dict = {}  # In-memory cache
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.running = False
        
    async def start(self):
        """Start consumer worker"""
        logger.info("Consumer starting...")
        self.running = True
        # Start background worker
        asyncio.create_task(self._worker())
    
    async def stop(self):
        """Stop consumer gracefully"""
        logger.info("Consumer stopping...")
        self.running = False
    
    async def _worker(self):
        """Background worker - process events from queue"""
        while self.running:
            try:
                # Get event from queue dengan timeout
                try:
                    event = self.event_queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.1)
                    continue
                
                # Process event
                await self.process_event(event)
                self.event_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in consumer worker: {str(e)}")
                await asyncio.sleep(1)
    
    async def enqueue_event(self, event: Event):
        """
        Enqueue event untuk diproses
        
        Args:
            event: Event object
        """
        await self.event_queue.put(event)
    
    async def process_event(self, event: Event) -> dict:
        """
        Process event dengan deduplication
        
        Logic:
        1. Generate key (topic, event_id)
        2. Check apakah sudah diproses sebelumnya
        3. Jika belum: process dan simpan ke dedup store
        4. Jika sudah: skip dan log sebagai duplicate
        
        Args:
            event: Event object
            
        Returns:
            dict: Status processing result
        """
        dedup_key = (event.topic, event.event_id)
        
        # Check dedup store
        is_duplicate = await self.dedup_store.exists(event.topic, event.event_id)
        
        if is_duplicate:
            logger.warning(f"Duplicate detected: topic={event.topic}, event_id={event.event_id}")
            return {
                "status": "skipped",
                "reason": "duplicate",
                "dedup_key": dedup_key
            }
        
        try:
            # Process event (business logic)
            result = await self._handle_event(event)
            
            # Mark as processed in dedup store
            await self.dedup_store.mark_processed(
                event.topic,
                event.event_id,
                event.timestamp
            )
            
            # Cache in memory
            self.processed_events[dedup_key] = {
                "event": event.dict(),
                "processed_at": datetime.utcnow(),
                "status": "processed"
            }
            
            logger.info(f"Event processed: topic={event.topic}, event_id={event.event_id}")
            
            return {
                "status": "processed",
                "dedup_key": dedup_key,
                "result": result
            }
            
        except Exception as e:
            logger.error(f"Error processing event {dedup_key}: {str(e)}")
            return {
                "status": "error",
                "reason": str(e),
                "dedup_key": dedup_key
            }
    
    async def _handle_event(self, event: Event) -> dict:
        """
        Aplikasi bisnis logic untuk memproses event
        
        Dalam implementasi nyata, ini akan melakukan:
        - Transformasi data
        - Penyimpanan ke database
        - Pemicu action/workflow
        - etc.
        
        Args:
            event: Event object
            
        Returns:
            dict: Processing result
        """
        # TODO: Implementasi business logic
        # Contoh sederhana: validasi dan return
        
        await asyncio.sleep(0.01)  # Simulate processing
        
        return {
            "message": f"Event from {event.source} processed",
            "payload_keys": list(event.payload.keys()) if event.payload else []
        }
    
    def get_processed_count(self) -> int:
        """Get total unique events processed"""
        return len(self.processed_events)
    
    def get_processed_events(self, topic: Optional[str] = None, limit: int = 100) -> list:
        """
        Get list of processed events
        
        Args:
            topic: Filter by topic (optional)
            limit: Maximum results
            
        Returns:
            list: List of processed events
        """
        events = list(self.processed_events.values())
        
        if topic:
            events = [e for e in events if e["event"]["topic"] == topic]
        
        return events[:limit]
    
    def get_topics(self) -> Set[str]:
        """Get all topics that have events"""
        return {e["event"]["topic"] for e in self.processed_events.values()}
