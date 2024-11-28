import asyncio
import sys
from ...config import ConfigManager
from ...auth import NSAuthHandler
from .cdr_fetcher import run_cdr_fetcher

async def main():
    if len(sys.argv) < 4:
        print("Usage: python -m app.workers.cdr_fetcher <domain_id> <start_time> --auth-handler <auth_handler_id>")
        sys.exit(1)

    domain_id = sys.argv[1]
    start_time = int(sys.argv[2])
    
    config = ConfigManager()
    domain_config = config.get_domain_config(domain_id)
    if not domain_config:
        print(f"Error: Domain {domain_id} not found in configuration")
        sys.exit(1)

    # Get existing auth handler instance
    auth_handler = NSAuthHandler(config)  # Will return existing instance

    await run_cdr_fetcher(domain_config, auth_handler, start_time)

if __name__ == "__main__":
    asyncio.run(main()) 