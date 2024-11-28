import asyncio
import os
from datetime import datetime, timedelta
import aiohttp
from typing import Dict, List, Optional
import urllib.parse

from ..config import ConfigManager, CustomerDomain
from ..database import CallDatabase
from ..auth import NSAuthHandler
from ..logging_config import get_logger

logger = get_logger("cdr_fetcher")

class CDRFetcher:
    def __init__(self, domain_config: CustomerDomain, auth_handler: NSAuthHandler):
        self.domain_config = domain_config
        self.auth_handler = auth_handler
        self.db = CallDatabase(domain_config.domain_id)
        
        # Extract territory ID from domain_id (format: domain.territory.service)
        self.territory_id = self.domain_config.domain_id.split('.')[1]
        # Construct correct NS API base URL
        self.base_url = f"https://{self.territory_id}-hpbx.dashmanager.com/ns-api"
        
    async def fetch_cdrs(self) -> None:
        """Fetch CDRs for the domain and store in database"""
        try:
            # Get authentication token
            token = await self.auth_handler.get_token(self.domain_config.domain_id)
            if not token:
                logger.error(f"Failed to get token for domain {self.domain_config.domain_id}")
                return

            # Calculate time range for CDR fetch
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=5)

            # Format times for NS API (YYYY-MM-DD HH:mm:ss format)
            start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
            end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

            # Build query parameters for NS API
            params = {
                "object": "cdr2",
                "action": "read",
                "domain": self.domain_config.domain_id,
                "datetime-start": start_str,
                "datetime-end": end_str,
                "limit": "9999"  # Maximum records
            }

            # URL encode the parameters
            encoded_params = urllib.parse.urlencode(params)

            # Fetch CDRs from NS API
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/",  # Note the trailing slash
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "application/json"
                    },
                    data=encoded_params
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(
                            f"Failed to fetch CDRs for domain {self.domain_config.domain_id}: "
                            f"Status {response.status}, Error: {error_text}"
                        )
                        return

                    data = await response.json()
                    
                    # Check if the response has the expected structure
                    if not isinstance(data, dict) or "result" not in data:
                        logger.error(
                            f"Unexpected response format for domain {self.domain_config.domain_id}: "
                            f"{data}"
                        )
                        return

                    await self._process_cdrs(data["result"])

        except Exception as e:
            logger.error(f"Error fetching CDRs for domain {self.domain_config.domain_id}: {str(e)}")
        finally:
            self.db.close()

    async def _process_cdrs(self, cdrs: List[Dict]) -> None:
        """Process and store CDRs in the database"""
        for cdr in cdrs:
            try:
                # Validate CDR data
                required_fields = ["orig_callid", "term_callid", "src_number", "dst_number", "call_start"]
                if not all(key in cdr for key in required_fields):
                    logger.warning(f"Invalid CDR data for domain {self.domain_config.domain_id}: {cdr}")
                    continue

                # Convert call_start to ISO format if needed
                try:
                    # NS API returns time in YYYY-MM-DD HH:mm:ss format
                    call_time = datetime.strptime(cdr["call_start"], "%Y-%m-%d %H:%M:%S")
                    call_time_iso = call_time.isoformat()
                except ValueError as e:
                    logger.error(f"Invalid date format in CDR: {cdr['call_start']}")
                    continue

                # Insert CDR into database
                with self.db.conn:
                    self.db.conn.execute("""
                        INSERT OR IGNORE INTO calls (
                            orig_callid, term_callid, source_number, dest_number, call_time
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        cdr["orig_callid"],
                        cdr["term_callid"],
                        cdr["src_number"],
                        cdr["dst_number"],
                        call_time_iso
                    ))

            except Exception as e:
                logger.error(
                    f"Error processing CDR for domain {self.domain_config.domain_id}: "
                    f"{str(e)}, CDR: {cdr}"
                )

async def run_cdr_fetcher(domain_config: CustomerDomain, auth_handler: NSAuthHandler) -> None:
    """Run the CDR fetcher worker"""
    worker_pid = os.getpid()
    logger.info(f"Starting CDR fetcher worker {worker_pid} for domain {domain_config.domain_id}")
    
    fetcher = CDRFetcher(domain_config, auth_handler)
    
    while True:
        try:
            await fetcher.fetch_cdrs()
            
            # Get polling interval from config, default to 5 minutes
            polling_interval = domain_config.cdr_fetcher.get("polling_interval", "5m")
            # Convert polling interval to seconds
            seconds = int(polling_interval[:-1]) * (60 if polling_interval.endswith('m') else 1)
            
            await asyncio.sleep(seconds)
            
        except Exception as e:
            logger.error(f"Error in CDR fetcher worker {worker_pid}: {str(e)}")
            await asyncio.sleep(60)  # Wait a minute before retrying after error