"""
UTS Sistem Terdistribusi - Pub-Sub Log Aggregator
Entry point aplikasi
"""

import asyncio
import logging
from src.app import create_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def main():
    """Main entry point"""
    logger.info("Starting Pub-Sub Log Aggregator...")
    
    app = create_app()
    
    # In production, use uvicorn server
    import uvicorn
    
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info"
    )
    
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
