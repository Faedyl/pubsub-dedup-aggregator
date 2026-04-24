"""
Utility Functions
"""

import logging
from datetime import datetime
from typing import Any, Dict
import json

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO"):
    """
    Setup logging configuration
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def format_timestamp(dt: datetime = None) -> str:
    """
    Format datetime to ISO8601 string
    
    Args:
        dt: datetime object (default: now)
        
    Returns:
        ISO8601 formatted string
    """
    if dt is None:
        dt = datetime.utcnow()
    return dt.isoformat() + 'Z'


def safe_json_dumps(obj: Any, default_handler=None) -> str:
    """
    Safely serialize object to JSON
    
    Args:
        obj: Object to serialize
        default_handler: Custom handler for non-JSON types
        
    Returns:
        JSON string
    """
    def _default(o):
        if default_handler:
            return default_handler(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return str(o)
    
    return json.dumps(obj, default=_default, indent=2)


def validate_identifier(value: str, max_length: int = 255) -> bool:
    """
    Validate identifier format (topic, event_id, source)
    
    Args:
        value: String to validate
        max_length: Maximum length
        
    Returns:
        True if valid
    """
    if not value or not isinstance(value, str):
        return False
    
    if len(value) > max_length or len(value) < 1:
        return False
    
    # Allow alphanumeric, dash, underscore, dot
    allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.')
    return all(c in allowed_chars for c in value)


class EventMetrics:
    """Track event metrics"""
    
    def __init__(self):
        self.received = 0
        self.processed = 0
        self.duplicates = 0
        self.errors = 0
        self.start_time = datetime.utcnow()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        
        return {
            "received": self.received,
            "unique_processed": self.processed,
            "duplicate_dropped": self.duplicates,
            "errors": self.errors,
            "uptime_seconds": int(elapsed)
        }
    
    def on_received(self):
        """Mark event as received"""
        self.received += 1
    
    def on_processed(self):
        """Mark event as processed"""
        self.processed += 1
    
    def on_duplicate(self):
        """Mark as duplicate"""
        self.duplicates += 1
    
    def on_error(self):
        """Mark as error"""
        self.errors += 1


def calculate_throughput(event_count: int, duration_seconds: float) -> float:
    """
    Calculate throughput
    
    Args:
        event_count: Number of events
        duration_seconds: Duration in seconds
        
    Returns:
        Events per second
    """
    if duration_seconds <= 0:
        return 0
    return event_count / duration_seconds
