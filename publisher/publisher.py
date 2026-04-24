"""
Publisher untuk Docker Compose demo
Mengirim event duplikat untuk testing idempotency
"""

import asyncio
import httpx
import logging
import os
from datetime import datetime
from uuid import uuid4
from typing import List

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def publish_events(aggregator_url: str, events: List[dict]):
    """
    Publish events ke aggregator
    
    Args:
        aggregator_url: URL aggregator endpoint
        events: List of events to publish
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{aggregator_url}/publish",
                json={"events": events},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.info(
                    f"Published {data['received']} events, "
                    f"Processed: {data['processed']}, "
                    f"Duplicates: {data['duplicates_detected']}"
                )
            else:
                logger.error(f"Error publishing events: {response.text}")
    except Exception as e:
        logger.error(f"Error publishing to aggregator: {str(e)}")


async def generate_events(count: int, topics: List[str]) -> List[dict]:
    """
    Generate test events
    
    Args:
        count: Number of events to generate
        topics: List of topics to use
        
    Returns:
        List of events
    """
    events = []
    for i in range(count):
        topic = topics[i % len(topics)]
        event = {
            "topic": topic,
            "event_id": f"evt-{i:06d}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": f"publisher-{os.getenv('HOSTNAME', 'local')}",
            "payload": {
                "sequence": i,
                "level": "INFO",
                "message": f"Test event {i}",
                "user_id": uuid4().hex[:8]
            }
        }
        events.append(event)
    
    return events


async def main():
    """Main publisher function"""
    
    aggregator_url = os.getenv("AGGREGATOR_URL", "http://aggregator:8080")
    num_events = int(os.getenv("NUM_EVENTS", "1000"))
    publish_interval = int(os.getenv("PUBLISH_INTERVAL", "1000"))  # milliseconds
    duplication_rate = float(os.getenv("DUPLICATION_RATE", "0.2"))  # 20%
    
    topics = ["app-logs", "system-events", "business-metrics"]
    
    logger.info(f"Publisher starting...")
    logger.info(f"Aggregator URL: {aggregator_url}")
    logger.info(f"Total events: {num_events}")
    logger.info(f"Publish interval: {publish_interval}ms")
    logger.info(f"Duplication rate: {duplication_rate*100}%")
    
    # Wait for aggregator to be ready
    await asyncio.sleep(5)
    
    # Generate initial events
    events = await generate_events(num_events, topics)
    
    # Publish events with duplicates
    event_index = 0
    while event_index < len(events):
        batch_size = min(100, len(events) - event_index)
        batch = events[event_index:event_index + batch_size]
        
        # Add some duplicates
        if event_index > 0 and asyncio.get_event_loop().time() % 10 < duplication_rate * 10:
            # Duplicate some earlier events
            dup_index = max(0, event_index - 50)
            duplicate = events[dup_index].copy()
            duplicate['event_id'] = events[dup_index]['event_id']  # Same ID = duplicate
            batch.append(duplicate)
            logger.info(f"Added duplicate to batch: {duplicate['event_id']}")
        
        # Publish batch
        await publish_events(aggregator_url, batch)
        
        event_index += batch_size
        
        # Wait before next batch
        await asyncio.sleep(publish_interval / 1000.0)
    
    logger.info("Publisher finished sending all events")
    
    # Final stats
    await asyncio.sleep(2)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{aggregator_url}/stats")
            if response.status_code == 200:
                stats = response.json()
                logger.info(f"Final stats: {stats}")
    except Exception as e:
        logger.error(f"Error getting final stats: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
