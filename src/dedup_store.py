"""
Deduplication Store - SQLite Implementation
"""

import sqlite3
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
import threading

logger = logging.getLogger(__name__)


class DedupStore:
    """
    Deduplication store dengan SQLite backend.
    Thread-safe untuk concurrent access.
    Persistent di disk untuk durability.
    """
    
    def __init__(self, db_path: str = "/app/data/dedup.db"):
        """
        Initialize dedup store
        
        Args:
            db_path: Path ke SQLite database file
        """
        self.db_path = db_path
        self._lock = threading.RLock()
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema"""
        try:
            # Ensure directory exists
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
                
                # Create dedup log table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS dedup_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        topic TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        processed_at TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(topic, event_id)
                    )
                """)
                
                # Create index untuk fast lookup
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_topic_event_id 
                    ON dedup_log(topic, event_id)
                """)
                
                conn.commit()
                logger.info(f"Dedup store initialized: {self.db_path}")
                
        except Exception as e:
            logger.error(f"Error initializing dedup store: {str(e)}")
            raise
    
    async def exists(self, topic: str, event_id: str) -> bool:
        """
        Check apakah event sudah diproses sebelumnya
        
        Args:
            topic: Event topic
            event_id: Event ID
            
        Returns:
            bool: True jika event sudah diproses, False jika belum
        """
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute(
                        "SELECT 1 FROM dedup_log WHERE topic = ? AND event_id = ? LIMIT 1",
                        (topic, event_id)
                    )
                    return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking dedup store: {str(e)}")
            raise
    
    async def mark_processed(self, topic: str, event_id: str, timestamp: datetime):
        """
        Mark event sebagai sudah diproses
        
        Args:
            topic: Event topic
            event_id: Event ID
            timestamp: Event timestamp
        """
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO dedup_log (topic, event_id, processed_at)
                        VALUES (?, ?, ?)
                        """,
                        (topic, event_id, timestamp.isoformat())
                    )
                    conn.commit()
                    logger.debug(f"Event marked processed: {topic}/{event_id}")
        except sqlite3.IntegrityError:
            # Event sudah ada, ignore
            logger.debug(f"Event already exists in dedup store: {topic}/{event_id}")
        except Exception as e:
            logger.error(f"Error marking event processed: {str(e)}")
            raise
    
    async def get_processed_count(self) -> int:
        """Get total unique events processed"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute("SELECT COUNT(*) FROM dedup_log")
                    result = cursor.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting processed count: {str(e)}")
            raise
    
    async def get_all_entries(self, topic: Optional[str] = None) -> list:
        """
        Get all dedup log entries
        
        Args:
            topic: Filter by topic (optional)
            
        Returns:
            list: List of dedup log entries
        """
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    
                    if topic:
                        cursor = conn.execute(
                            "SELECT * FROM dedup_log WHERE topic = ? ORDER BY processed_at DESC",
                            (topic,)
                        )
                    else:
                        cursor = conn.execute(
                            "SELECT * FROM dedup_log ORDER BY processed_at DESC"
                        )
                    
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting dedup entries: {str(e)}")
            raise
    
    async def get_topics(self) -> list:
        """Get list of unique topics"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute(
                        "SELECT DISTINCT topic FROM dedup_log ORDER BY topic"
                    )
                    return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting topics: {str(e)}")
            raise
    
    async def clear(self):
        """Clear all entries (untuk testing)"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("DELETE FROM dedup_log")
                    conn.commit()
                    logger.warning("Dedup store cleared")
        except Exception as e:
            logger.error(f"Error clearing dedup store: {str(e)}")
            raise
    
    def close(self):
        """Close database connection"""
        # SQLite handles connection pooling automatically
        logger.info("Dedup store closed")
