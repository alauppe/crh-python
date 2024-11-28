import asyncio
import aiohttp
import json
from datetime import datetime
from app.config import ConfigManager
from app.auth import NSAuthHandler
from app.logging_config import get_logger

logger = get_logger("recording_test")

async def test_recording_query():
    # Initialize config and auth
    config = ConfigManager()
    auth_handler = NSAuthHandler(config)
    
    # Get a test domain config
    domain_config = config.get_domain_config("htel.23281.service")
    if not domain_config:
        logger.error("Test domain not found")
        return

    # Get auth token
    token = await auth_handler.get_token(domain_config.domain_id)
    if not token:
        logger.error("Failed to get token")
        return

    # Extract territory ID for API URL
    territory_id = domain_config.domain_id.split('.')[1]
    base_url = f"https://{territory_id}-hpbx.dashmanager.com/ns-api"

    # Use the call details we saw in the CDR fetcher logs
    test_call = {
        'orig_callid': '6d5b1a4bbf9af20168ba2d7dc859efaf',
        'term_callid': '20241121204835057617-ea8e649849f62246e767703cc248f669'
    }

    # Build recording query parameters
    params = {
        "object": "recording",
        "action": "read",
        "domain": domain_config.domain_id,
        "orig_callid": test_call['orig_callid'],
        "term_callid": test_call['term_callid']
    }

    logger.info(f"Testing recording query with parameters: {json.dumps(params, indent=2)}")

    # Make the API request
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{base_url}/",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            },
            data=params
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"API request failed: {error_text}")
                return

            data = await response.json()
            logger.info(f"API Response:\n{json.dumps(data, indent=2)}")

if __name__ == "__main__":
    asyncio.run(test_recording_query()) 