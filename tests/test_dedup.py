"""
Unit Tests untuk Deduplication Logic
"""

import pytest
import asyncio
from datetime import datetime
from src.dedup_store import DedupStore
from src.consumer import IdempotentConsumer
from src.models import Event
import tempfile
import os


@pytest.fixture
def temp_db():
    """Create temporary database for testing"""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def dedup_store(temp_db):
    """Create dedup store instance"""
    store = DedupStore(db_path=temp_db)
    yield store
    store.close()


@pytest.fixture
def consumer(dedup_store):
    """Create consumer instance"""
    consumer = IdempotentConsumer(dedup_store=dedup_store)
    yield consumer


class TestDedupStore:
    """Test deduplication store"""
    
    def test_dedup_store_init(self, temp_db):
        """Test dedup store initialization"""
        store = DedupStore(db_path=temp_db)
        assert os.path.exists(temp_db)
        store.close()
    
    @pytest.mark.asyncio
    async def test_mark_and_check_processed(self, dedup_store):
        """Test marking event as processed and checking existence"""
        topic = "test-topic"
        event_id = "evt-001"
        timestamp = datetime.utcnow()
        
        # Initially should not exist
        exists = await dedup_store.exists(topic, event_id)
        assert not exists
        
        # Mark as processed
        await dedup_store.mark_processed(topic, event_id, timestamp)
        
        # Now should exist
        exists = await dedup_store.exists(topic, event_id)
        assert exists
    
    @pytest.mark.asyncio
    async def test_duplicate_detection(self, dedup_store):
        """Test detection of duplicate events"""
        topic = "logs"
        event_id = "evt-001"
        timestamp = datetime.utcnow()
        
        # First insert
        await dedup_store.mark_processed(topic, event_id, timestamp)
        
        # Should be detected as existing
        exists = await dedup_store.exists(topic, event_id)
        assert exists
    
    @pytest.mark.asyncio
    async def test_different_topics_same_event_id(self, dedup_store):
        """Test that same event_id on different topics are treated separately"""
        event_id = "evt-001"
        timestamp = datetime.utcnow()
        
        # Insert on topic1
        await dedup_store.mark_processed("topic1", event_id, timestamp)
        
        # Should not exist on topic2
        exists_topic2 = await dedup_store.exists("topic2", event_id)
        assert not exists_topic2
        
        # Should exist on topic1
        exists_topic1 = await dedup_store.exists("topic1", event_id)
        assert exists_topic1
    
    @pytest.mark.asyncio
    async def test_get_processed_count(self, dedup_store):
        """Test getting count of processed events"""
        timestamp = datetime.utcnow()
        
        count = await dedup_store.get_processed_count()
        assert count == 0
        
        # Add events
        await dedup_store.mark_processed("topic1", "evt-001", timestamp)
        await dedup_store.mark_processed("topic1", "evt-002", timestamp)
        await dedup_store.mark_processed("topic2", "evt-001", timestamp)
        
        count = await dedup_store.get_processed_count()
        assert count == 3
    
    @pytest.mark.asyncio
    async def test_get_topics(self, dedup_store):
        """Test getting list of topics"""
        timestamp = datetime.utcnow()
        
        topics = await dedup_store.get_topics()
        assert len(topics) == 0
        
        # Add events to different topics
        await dedup_store.mark_processed("app-logs", "evt-001", timestamp)
        await dedup_store.mark_processed("system-events", "evt-002", timestamp)
        await dedup_store.mark_processed("app-logs", "evt-003", timestamp)
        
        topics = await dedup_store.get_topics()
        assert len(topics) == 2
        assert "app-logs" in topics
        assert "system-events" in topics
    
    @pytest.mark.asyncio
    async def test_persistence_after_close_reopen(self, temp_db):
        """Test that dedup store persists after close and reopen"""
        topic = "test-topic"
        event_id = "evt-001"
        timestamp = datetime.utcnow()
        
        # Create and populate store
        store1 = DedupStore(db_path=temp_db)
        await store1.mark_processed(topic, event_id, timestamp)
        store1.close()
        
        # Reopen store
        store2 = DedupStore(db_path=temp_db)
        exists = await store2.exists(topic, event_id)
        assert exists  # Should still exist after reopening
        store2.close()


class TestIdempotentConsumer:
    """Test idempotent consumer"""
    
    def test_consumer_init(self, consumer):
        """Test consumer initialization"""
        assert consumer is not None
        assert consumer.get_processed_count() == 0
    
    @pytest.mark.asyncio
    async def test_process_unique_event(self, consumer, dedup_store):
        """Test processing a unique event"""
        await consumer.start()
        
        event = Event(
            topic="app-logs",
            event_id="evt-001",
            timestamp=datetime.utcnow(),
            source="service-a",
            payload={"level": "INFO"}
        )
        
        result = await consumer.process_event(event)
        assert result["status"] == "processed"
        
        await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_process_duplicate_event(self, consumer, dedup_store):
        """Test that duplicate events are skipped"""
        await consumer.start()
        
        event = Event(
            topic="app-logs",
            event_id="evt-001",
            timestamp=datetime.utcnow(),
            source="service-a",
            payload={"level": "INFO"}
        )
        
        # Process first time
        result1 = await consumer.process_event(event)
        assert result1["status"] == "processed"
        
        # Process same event again (duplicate)
        result2 = await consumer.process_event(event)
        assert result2["status"] == "skipped"
        assert result2["reason"] == "duplicate"
        
        await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_get_processed_events_by_topic(self, consumer, dedup_store):
        """Test getting processed events filtered by topic"""
        await consumer.start()
        
        # Process events
        event1 = Event(
            topic="app-logs",
            event_id="evt-001",
            timestamp=datetime.utcnow(),
            source="service-a",
            payload={}
        )
        
        event2 = Event(
            topic="system-events",
            event_id="evt-002",
            timestamp=datetime.utcnow(),
            source="service-b",
            payload={}
        )
        
        await consumer.process_event(event1)
        await consumer.process_event(event2)
        
        # Get by topic
        app_logs = consumer.get_processed_events(topic="app-logs")
        assert len(app_logs) == 1
        
        system_events = consumer.get_processed_events(topic="system-events")
        assert len(system_events) == 1
        
        await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_get_topics(self, consumer, dedup_store):
        """Test getting list of topics with events"""
        await consumer.start()
        
        event1 = Event(
            topic="app-logs",
            event_id="evt-001",
            timestamp=datetime.utcnow(),
            source="service-a",
            payload={}
        )
        
        event2 = Event(
            topic="system-events",
            event_id="evt-002",
            timestamp=datetime.utcnow(),
            source="service-b",
            payload={}
        )
        
        await consumer.process_event(event1)
        await consumer.process_event(event2)
        
        topics = consumer.get_topics()
        assert len(topics) == 2
        assert "app-logs" in topics
        assert "system-events" in topics
        
        await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_mark_and_check_processed(self, dedup_store):
        """Test marking event as processed and checking existence"""
        topic = "test-topic"
        event_id = "evt-001"
        timestamp = datetime.utcnow()
        
        # Initially should not exist
        exists = await dedup_store.exists(topic, event_id)
        assert exists is False
        
        # Mark as processed
        await dedup_store.mark_processed(topic, event_id, timestamp)
        
        # Now should exist
        exists = await dedup_store.exists(topic, event_id)
        assert exists is True
    
    @pytest.mark.asyncio
    async def test_duplicate_detection(self, dedup_store):
        """Test detection of duplicate events"""
        topic = "logs"
        event_id = "evt-001"
        timestamp = datetime.utcnow()
        
        # First insert
        await dedup_store.mark_processed(topic, event_id, timestamp)
        assert await dedup_store.exists(topic, event_id)
        
        # Second insert should not fail (due to UNIQUE constraint)
        await dedup_store.mark_processed(topic, event_id, timestamp)
        assert await dedup_store.exists(topic, event_id)
    
    @pytest.mark.asyncio
    async def test_different_topics_same_event_id(self, dedup_store):
        """Test that same event_id on different topics are treated separately"""
        event_id = "evt-001"
        timestamp = datetime.utcnow()
        
        # Insert on topic1
        await dedup_store.mark_processed("topic1", event_id, timestamp)
        assert await dedup_store.exists("topic1", event_id)
        
        # Same event_id on topic2 should not exist yet
        assert not await dedup_store.exists("topic2", event_id)
        
        # Insert on topic2
        await dedup_store.mark_processed("topic2", event_id, timestamp)
        assert await dedup_store.exists("topic2", event_id)
    
    @pytest.mark.asyncio
    async def test_get_processed_count(self, dedup_store):
        """Test getting count of processed events"""
        timestamp = datetime.utcnow()
        
        count = await dedup_store.get_processed_count()
        assert count == 0
        
        # Add some events
        for i in range(5):
            await dedup_store.mark_processed(f"topic", f"evt-{i:03d}", timestamp)
        
        count = await dedup_store.get_processed_count()
        assert count == 5
    
    @pytest.mark.asyncio
    async def test_get_topics(self, dedup_store):
        """Test getting list of topics"""
        timestamp = datetime.utcnow()
        
        topics = await dedup_store.get_topics()
        assert len(topics) == 0
        
        # Add events to different topics
        for topic in ["logs", "metrics", "traces"]:
            await dedup_store.mark_processed(topic, "evt-001", timestamp)
        
        topics = await dedup_store.get_topics()
        assert set(topics) == {"logs", "metrics", "traces"}
    
    @pytest.mark.asyncio
    async def test_persistence_after_close_reopen(self, temp_db):
        """Test that data persists after closing and reopening"""
        # Create and populate
        store1 = DedupStore(db_path=temp_db)
        timestamp = datetime.utcnow()
        await store1.mark_processed("topic1", "evt-001", timestamp)
        await store1.mark_processed("topic1", "evt-002", timestamp)
        store1.close()
        
        # Reopen and verify
        store2 = DedupStore(db_path=temp_db)
        assert await store2.exists("topic1", "evt-001")
        assert await store2.exists("topic1", "evt-002")
        assert await store2.get_processed_count() == 2
        store2.close()


class TestIdempotentConsumer:
    """Test idempotent consumer"""
    
    def create_event(self, topic: str, event_id: str, **kwargs) -> Event:
        """Helper to create Event"""
        return Event(
            topic=topic,
            event_id=event_id,
            source="test-source",
            timestamp=datetime.utcnow(),
            payload=kwargs.get("payload", {"test": "data"})
        )
    
    @pytest.mark.asyncio
    async def test_consumer_init(self, consumer):
        """Test consumer initialization"""
        assert consumer is not None
        assert consumer.get_processed_count() == 0
    
    @pytest.mark.asyncio
    async def test_process_unique_event(self, consumer):
        """Test processing a unique event"""
        await consumer.start()
        
        event = self.create_event("logs", "evt-001")
        result = await consumer.process_event(event)
        
        assert result["status"] == "processed"
        assert consumer.get_processed_count() == 1
        
        await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_process_duplicate_event(self, consumer):
        """Test that duplicate events are skipped"""
        await consumer.start()
        
        event = self.create_event("logs", "evt-001")
        
        # Process first time
        result1 = await consumer.process_event(event)
        assert result1["status"] == "processed"
        assert consumer.get_processed_count() == 1
        
        # Process second time (duplicate)
        result2 = await consumer.process_event(event)
        assert result2["status"] == "skipped"
        assert result2["reason"] == "duplicate"
        assert consumer.get_processed_count() == 1  # Count should not increase
        
        await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_get_processed_events_by_topic(self, consumer):
        """Test getting processed events filtered by topic"""
        await consumer.start()
        
        # Process events from different topics
        await consumer.process_event(self.create_event("logs", "evt-001"))
        await consumer.process_event(self.create_event("logs", "evt-002"))
        await consumer.process_event(self.create_event("metrics", "evt-003"))
        
        # Get events from "logs" topic
        events = consumer.get_processed_events(topic="logs")
        assert len(events) == 2
        
        # Get events from "metrics" topic
        events = consumer.get_processed_events(topic="metrics")
        assert len(events) == 1
        
        await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_get_topics(self, consumer):
        """Test getting list of topics with events"""
        await consumer.start()
        
        topics_initial = consumer.get_topics()
        assert len(topics_initial) == 0
        
        # Process events from different topics
        await consumer.process_event(self.create_event("logs", "evt-001"))
        await consumer.process_event(self.create_event("metrics", "evt-002"))
        await consumer.process_event(self.create_event("traces", "evt-003"))
        
        topics = consumer.get_topics()
        assert set(topics) == {"logs", "metrics", "traces"}
        
        await consumer.stop()
