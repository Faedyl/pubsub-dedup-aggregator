"""
Unit Tests untuk API Endpoints
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime
from src.app import create_app


@pytest.fixture
def client():
    """Create test client"""
    app = create_app()
    return TestClient(app)


class TestHealthEndpoint:
    """Test /health endpoint"""
    
    def test_health_check(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data


class TestPublishEndpoint:
    """Test /publish endpoint"""
    
    def test_publish_single_event(self, client):
        """Test publishing single event"""
        payload = {
            "events": [
                {
                    "topic": "app-logs",
                    "event_id": "evt-001",
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "service-a",
                    "payload": {"level": "INFO", "message": "Test message"}
                }
            ]
        }
        
        response = client.post("/publish", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
    
    def test_publish_batch_events(self, client):
        """Test publishing batch of events"""
        now = datetime.utcnow().isoformat()
        payload = {
            "events": [
                {
                    "topic": "app-logs",
                    "event_id": f"evt-{i:03d}",
                    "timestamp": now,
                    "source": "service-a",
                    "payload": {"index": i}
                }
                for i in range(10)
            ]
        }
        
        response = client.post("/publish", json=payload)
        assert response.status_code == 200
    
    def test_publish_invalid_event_missing_topic(self, client):
        """Test that invalid event (missing topic) is rejected"""
        payload = {
            "events": [
                {
                    "event_id": "evt-001",
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "service-a",
                    "payload": {}
                }
            ]
        }
        
        response = client.post("/publish", json=payload)
        assert response.status_code == 422  # Pydantic validation error
    
    def test_publish_invalid_event_missing_event_id(self, client):
        """Test that invalid event (missing event_id) is rejected"""
        payload = {
            "events": [
                {
                    "topic": "app-logs",
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "service-a",
                    "payload": {}
                }
            ]
        }
        
        response = client.post("/publish", json=payload)
        assert response.status_code == 422  # Pydantic validation error
    
    def test_publish_empty_batch(self, client):
        """Test that empty batch is rejected"""
        payload = {"events": []}
        
        response = client.post("/publish", json=payload)
        assert response.status_code == 422  # Pydantic validation error


class TestEventsEndpoint:
    """Test /events endpoint"""
    
    def test_get_events(self, client):
        """Test getting list of events"""
        response = client.get("/events")
        assert response.status_code == 200
        data = response.json()
        assert "events" in data
        assert "count" in data
        assert isinstance(data["events"], list)
    
    def test_get_events_with_topic_filter(self, client):
        """Test getting events filtered by topic"""
        response = client.get("/events?topic=app-logs")
        assert response.status_code == 200
        data = response.json()
        assert "events" in data
    
    def test_get_events_with_limit(self, client):
        """Test getting events with limit parameter"""
        response = client.get("/events?limit=50")
        assert response.status_code == 200
        data = response.json()
        assert len(data["events"]) <= 50
    
    def test_get_events_invalid_limit_zero(self, client):
        """Test that limit=0 is rejected"""
        response = client.get("/events?limit=0")
        assert response.status_code == 422  # Validation error
    
    def test_get_events_invalid_limit_exceeds_max(self, client):
        """Test that limit > 1000 is rejected"""
        response = client.get("/events?limit=2000")
        assert response.status_code == 422


class TestStatsEndpoint:
    """Test /stats endpoint"""
    
    def test_get_stats(self, client):
        """Test getting statistics"""
        response = client.get("/stats")
        assert response.status_code == 200
        data = response.json()
        
        assert "received" in data
        assert "unique_processed" in data
        assert "duplicate_dropped" in data
        assert "topics" in data
        assert "uptime_seconds" in data
        
        # Verify types
        assert isinstance(data["received"], int)
        assert isinstance(data["unique_processed"], int)
        assert isinstance(data["duplicate_dropped"], int)
        assert isinstance(data["topics"], list)
        assert isinstance(data["uptime_seconds"], int)
    
    def test_stats_after_publish(self, client):
        """Test that stats are updated after publishing"""
        # Get initial stats
        response1 = client.get("/stats")
        stats1 = response1.json()
        initial_received = stats1["received"]
        
        # Publish an event
        payload = {
            "events": [
                {
                    "topic": "logs",
                    "event_id": "evt-stats-test",
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "service-a",
                    "payload": {}
                }
            ]
        }
        client.post("/publish", json=payload)
        
        # Get updated stats
        response2 = client.get("/stats")
        stats2 = response2.json()
        
        # Verify stats increased
        assert stats2["received"] > initial_received
