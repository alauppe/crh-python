import asyncio
import sys
from ...config import ConfigManager
from ...auth import NSAuthHandler
from .recording_handler import run_recording_handler

async def main():
    if len(sys.argv) != 2:
        print("Usage: python -m app.workers.recording_handler <domain_id>")
        sys.exit(1)

    domain_id = sys.argv[1]
    config = ConfigManager()
    auth_handler = NSAuthHandler(config)
    
    domain_config = config.get_domain_config(domain_id)
    if not domain_config:
        print(f"Error: Domain {domain_id} not found in configuration")
        sys.exit(1)

    await run_recording_handler(domain_config, auth_handler)

if __name__ == "__main__":
    asyncio.run(main()) 