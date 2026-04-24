"""
Performance Tests untuk Scalability
"""

import pytest
import asyncio
import time
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


class TestPerformance:
    """Test performance and scalability"""
    
    def create_event(self, topic: str, event_id: str) -> Event:
        """Helper to create Event"""
        return Event(
            topic=topic,
            event_id=event_id,
            source="test-source",
            timestamp=datetime.utcnow(),
            payload={"test": f"event-{event_id}"}
        )
    
    @pytest.mark.asyncio
    async def test_process_large_batch(self, temp_db):
        """Test processing large batch of events"""
        store = DedupStore(db_path=temp_db)
        consumer = IdempotentConsumer(dedup_store=store)
        await consumer.start()
        
        num_events = 1000
        start_time = time.time()
        
        # Process batch of unique events
        for i in range(num_events):
            event = self.create_event(f"topic-{i % 10}", f"evt-{i:05d}")
            await consumer.process_event(event)
        
        elapsed = time.time() - start_time
        throughput = num_events / elapsed
        
        await consumer.stop()
        store.close()
        
        print(f"\nProcessed {num_events} unique events in {elapsed:.2f}s")
        print(f"Throughput: {throughput:.0f} events/sec")
        
        # Assert minimum performance (adjust based on system)
        assert elapsed < 30, f"Processing took too long: {elapsed:.2f}s"
        assert consumer.get_processed_count() == num_events
    
    @pytest.mark.asyncio
    async def test_dedup_detection_performance(self, temp_db):
        """Test performance of dedup detection with duplicates"""
        store = DedupStore(db_path=temp_db)
        consumer = IdempotentConsumer(dedup_store=store)
        await consumer.start()
        
        num_events = 5000
        num_unique = 1000
        duplicate_rate = 0.8  # 80% duplicates
        
        start_time = time.time()
        
        # Create mix of unique and duplicate events
        duplicates_detected = 0
        for i in range(num_events):
            unique_idx = i % num_unique
            event = self.create_event("logs", f"evt-{unique_idx:05d}")
            result = await consumer.process_event(event)
            if result["status"] == "skipped":
                duplicates_detected += 1
        
        elapsed = time.time() - start_time
        throughput = num_events / elapsed
        
        await consumer.stop()
        store.close()
        
        print(f"\nProcessed {num_events} events ({duplicates_detected} duplicates) in {elapsed:.2f}s")
        print(f"Throughput: {throughput:.0f} events/sec")
        print(f"Dedup accuracy: {(duplicates_detected / (num_events - num_unique) * 100):.1f}%")
        
        # Verify correctness
        assert consumer.get_processed_count() == num_unique
        assert duplicates_detected >= (num_events - num_unique) * 0.9
    
    @pytest.mark.asyncio
    async def test_concurrent_processing(self, temp_db):
        """Test concurrent event processing"""
        store = DedupStore(db_path=temp_db)
        consumer = IdempotentConsumer(dedup_store=store)
        await consumer.start()
        
        async def process_batch(start_idx, count, topic):
            for i in range(start_idx, start_idx + count):
                event = self.create_event(topic, f"evt-{i:05d}")
                await consumer.process_event(event)
        
        num_events_per_batch = 250
        
        start_time = time.time()
        
        # Process multiple topics concurrently
        await asyncio.gather(
            process_batch(0, num_events_per_batch, "topic-a"),
            process_batch(num_events_per_batch, num_events_per_batch, "topic-b"),
            process_batch(2 * num_events_per_batch, num_events_per_batch, "topic-c"),
            process_batch(3 * num_events_per_batch, num_events_per_batch, "topic-d"),
        )
        
        elapsed = time.time() - start_time
        total_events = 4 * num_events_per_batch
        throughput = total_events / elapsed
        
        await consumer.stop()
        store.close()
        
        print(f"\nConcurrent: Processed {total_events} events in {elapsed:.2f}s")
        print(f"Throughput: {throughput:.0f} events/sec")
        
        assert consumer.get_processed_count() == total_events
    
    @pytest.mark.asyncio
    async def test_memory_efficiency(self, temp_db):
        """Test memory usage with large dataset"""
        store = DedupStore(db_path=temp_db)
        
        num_events = 10000
        
        start_time = time.time()
        
        # Add many events to dedup store
        timestamp = datetime.utcnow()
        for i in range(num_events):
            await store.mark_processed("topic", f"evt-{i:06d}", timestamp)
        
        elapsed = time.time() - start_time
        
        store.close()
        
        print(f"\nAdded {num_events} events to store in {elapsed:.2f}s")
        
        # Verify all were added
        store2 = DedupStore(db_path=temp_db)
        count = await store2.get_processed_count()
        assert count == num_events
        store2.close()
    
    @pytest.mark.asyncio
    async def test_query_performance(self, temp_db):
        """Test query performance on large dataset"""
        store = DedupStore(db_path=temp_db)
        
        # Populate with events from multiple topics
        timestamp = datetime.utcnow()
        num_topics = 20
        events_per_topic = 500
        
        for topic_idx in range(num_topics):
            for event_idx in range(events_per_topic):
                await store.mark_processed(
                    f"topic-{topic_idx:03d}",
                    f"evt-{event_idx:05d}",
                    timestamp
                )
        
        # Test query performance
        start_time = time.time()
        entries = await store.get_all_entries(topic="topic-010")
        elapsed = time.time() - start_time
        
        print(f"\nQueried {len(entries)} entries in {elapsed*1000:.2f}ms")
        
        assert len(entries) == events_per_topic
        assert elapsed < 1.0, "Query took too long"
        
        store.close()
