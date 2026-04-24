"""
Unit Tests untuk Persistence dan Durability
"""

import pytest
import asyncio
import tempfile
import os
from datetime import datetime
from src.dedup_store import DedupStore
from src.consumer import IdempotentConsumer
from src.models import Event


@pytest.fixture
def temp_db():
    """Create temporary database"""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)


class TestPersistence:
    """Test persistence and durability"""
    
    def create_event(self, topic: str, event_id: str) -> Event:
        """Helper to create Event"""
        return Event(
            topic=topic,
            event_id=event_id,
            source="test-source",
            timestamp=datetime.utcnow(),
            payload={"test": "data"}
        )
    
    @pytest.mark.asyncio
    async def test_dedup_store_persistence(self, temp_db):
        """Test that dedup store data persists across instances"""
        timestamp = datetime.utcnow()
        
        # First instance: Add data
        store1 = DedupStore(db_path=temp_db)
        await store1.mark_processed("topic1", "evt-001", timestamp)
        await store1.mark_processed("topic1", "evt-002", timestamp)
        await store1.mark_processed("topic2", "evt-003", timestamp)
        count1 = await store1.get_processed_count()
        store1.close()
        
        assert count1 == 3
        
        # Second instance: Verify data still exists
        store2 = DedupStore(db_path=temp_db)
        
        # Check all events still exist
        assert await store2.exists("topic1", "evt-001")
        assert await store2.exists("topic1", "evt-002")
        assert await store2.exists("topic2", "evt-003")
        
        # Check count
        count2 = await store2.get_processed_count()
        assert count2 == 3
        
        # Check topics
        topics = await store2.get_topics()
        assert set(topics) == {"topic1", "topic2"}
        
        store2.close()
    
    @pytest.mark.asyncio
    async def test_consumer_persistence_across_restart(self, temp_db):
        """Test that consumer respects persistent dedup store"""
        timestamp = datetime.utcnow()
        
        # First consumer instance
        store1 = DedupStore(db_path=temp_db)
        consumer1 = IdempotentConsumer(dedup_store=store1)
        await consumer1.start()
        
        # Process some events
        event1 = self.create_event("logs", "evt-001")
        event2 = self.create_event("logs", "evt-002")
        
        result1 = await consumer1.process_event(event1)
        result2 = await consumer1.process_event(event2)
        
        assert result1["status"] == "processed"
        assert result2["status"] == "processed"
        
        await consumer1.stop()
        store1.close()
        
        # Second consumer instance (simulating restart)
        store2 = DedupStore(db_path=temp_db)
        consumer2 = IdempotentConsumer(dedup_store=store2)
        await consumer2.start()
        
        # Try to process same events again
        result3 = await consumer2.process_event(event1)
        result4 = await consumer2.process_event(event2)
        
        # Should be skipped (already processed by first instance)
        assert result3["status"] == "skipped"
        assert result4["status"] == "skipped"
        
        await consumer2.stop()
        store2.close()
    
    @pytest.mark.asyncio
    async def test_dedup_store_recovery_from_corruption(self, temp_db):
        """Test that dedup store can handle recovery"""
        timestamp = datetime.utcnow()
        
        # Add some data
        store1 = DedupStore(db_path=temp_db)
        await store1.mark_processed("topic", "evt-001", timestamp)
        store1.close()
        
        # Reopen should not fail
        store2 = DedupStore(db_path=temp_db)
        assert await store2.exists("topic", "evt-001")
        store2.close()
    
    @pytest.mark.asyncio
    async def test_large_batch_persistence(self, temp_db):
        """Test persistence with large number of events"""
        timestamp = datetime.utcnow()
        
        # Add many events
        store = DedupStore(db_path=temp_db)
        
        num_events = 1000
        for i in range(num_events):
            await store.mark_processed("topic", f"evt-{i:05d}", timestamp)
        
        count = await store.get_processed_count()
        assert count == num_events
        
        store.close()
        
        # Verify all are still there
        store2 = DedupStore(db_path=temp_db)
        count2 = await store2.get_processed_count()
        assert count2 == num_events
        store2.close()
    
    @pytest.mark.asyncio
    async def test_concurrent_writes_persistence(self, temp_db):
        """Test that concurrent writes are safely persisted"""
        timestamp = datetime.utcnow()
        store = DedupStore(db_path=temp_db)
        
        async def write_events(start_idx, count):
            for i in range(start_idx, start_idx + count):
                await store.mark_processed("topic", f"evt-{i:05d}", timestamp)
        
        # Run concurrent writes
        await asyncio.gather(
            write_events(0, 250),
            write_events(250, 250),
            write_events(500, 250),
            write_events(750, 250),
        )
        
        count = await store.get_processed_count()
        assert count == 1000
        
        store.close()
        
        # Verify persistence
        store2 = DedupStore(db_path=temp_db)
        count2 = await store2.get_processed_count()
        assert count2 == 1000
        store2.close()
