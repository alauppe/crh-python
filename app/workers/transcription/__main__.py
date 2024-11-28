import asyncio
import sys
from ...config import ConfigManager
from ...auth import NSAuthHandler
from .transcription import run_transcription

async def main():
    if len(sys.argv) != 2:
        print("Usage: python -m app.workers.transcription <domain_id>")
        sys.exit(1)

    domain_id = sys.argv[1]
    config = ConfigManager()
    
    domain_config = config.get_domain_config(domain_id)
    if not domain_config:
        print(f"Error: Domain {domain_id} not found in configuration")
        sys.exit(1)

    auth_handler = NSAuthHandler(config)

    await run_transcription(domain_config, auth_handler)

if __name__ == "__main__":
    asyncio.run(main()) 