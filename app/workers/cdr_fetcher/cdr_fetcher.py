import asyncio
import os
from datetime import datetime, timedelta, timezone
import aiohttp
from typing import Dict, List, Optional
import urllib.parse
import json
from pathlib import Path
import sqlite3
import pytz

from ...config import ConfigManager, CustomerDomain
from ...database import CallDatabase
from ...auth import NSAuthHandler
from ...logging_config import get_logger

logger = get_logger("cdr_fetcher")

class CDRFetcher:
    def __init__(self, domain_config: CustomerDomain, auth_handler: NSAuthHandler, start_time: int):
        self.domain_config = domain_config
        self.auth_handler = auth_handler
        self.orchestrator_start_time = datetime.fromtimestamp(start_time, tz=timezone.utc)
        
        # Use domain-specific state directory
        self.state_dir = Path(f"state/{domain_config.domain_id}")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / "cdr_fetcher_state.json"
        
        # Extract territory ID from domain_id (format: domain.territory.service)
        self.territory_id = self.domain_config.domain_id.split('.')[1]
        # Construct correct NS API base URL
        self.base_url = f"https://{self.territory_id}-hpbx.dashmanager.com/ns-api"
        
        # Load or initialize state
        self.state = self._load_state()
        
        logger.info(
            f"Initialized CDR fetcher for domain {domain_config.domain_id}, "
            f"territory {self.territory_id}, "
            f"API URL: {self.base_url}"
        )
        
    def _init_db(self):
        """Initialize database tables"""
        with self.db:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS cdrs (
                    orig_callid TEXT PRIMARY KEY,
                    term_callid TEXT,
                    source_number TEXT,
                    dest_number TEXT,
                    call_time TEXT,
                    time_release INTEGER
                )
            """)
        
    def __del__(self):
        """Close database connection when object is destroyed"""
        if hasattr(self, 'db'):
            self.db.close()
        
    def _load_state(self) -> Dict:
        """Load or initialize fetcher state"""
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading state file: {e}")
        
        # Initialize with default state
        return {
            "last_successful_query": None,
            "last_successful_query_end": None
        }
    
    def _save_state(self):
        """Save current state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f)
        except Exception as e:
            logger.error(f"Error saving state file: {e}")

    def _get_query_timerange(self) -> tuple[datetime, datetime]:
        """Calculate the time range for the next query"""
        end_time = datetime.now(timezone.utc)
        
        # If we have a last record time, start from there minus min_lookback
        if self.state["last_successful_query_end"]:
            try:
                last_record_time = datetime.fromtimestamp(
                    int(self.state["last_successful_query_end"]), 
                    tz=timezone.utc
                )
                min_lookback = self._parse_time_interval(
                    self.domain_config.cdr_fetcher.get("min_lookback", "60m")
                )
                start_time = last_record_time - min_lookback
                
                # Don't go further back than orchestrator start minus initial_lookback
                initial_lookback = self._parse_time_interval(
                    self.domain_config.cdr_fetcher.get("initial_lookback", "48h")
                )
                earliest_allowed = self.orchestrator_start_time - initial_lookback
                
                if start_time < earliest_allowed:
                    logger.info(
                        f"Limiting query start time to {earliest_allowed.isoformat()} "
                        f"(orchestrator start minus initial_lookback)"
                    )
                    start_time = earliest_allowed
                
                logger.debug(
                    f"Last record was at {last_record_time.isoformat()}, "
                    f"querying from {start_time.isoformat()} with {min_lookback} lookback"
                )
            except ValueError:
                logger.error("Invalid last record time in state, using initial lookback")
                start_time = self._get_initial_start_time(end_time)
        else:
            # First run - use initial_lookback from orchestrator start time
            start_time = self.orchestrator_start_time - self._parse_time_interval(
                self.domain_config.cdr_fetcher.get("initial_lookback", "48h")
            )
            logger.debug(f"First run, using initial lookback to {start_time.isoformat()}")
        
        return start_time, end_time
    
    def _get_initial_start_time(self, end_time: datetime) -> datetime:
        """Calculate initial start time based on configuration"""
        initial_lookback = self._parse_time_interval(
            self.domain_config.cdr_fetcher.get("initial_lookback", "24h")
        )
        return end_time - initial_lookback
    
    def _parse_time_interval(self, interval: str) -> timedelta:
        """Parse time interval string (e.g., '48h', '60m') to timedelta"""
        try:
            value = int(interval[:-1])
            unit = interval[-1].lower()
            if unit == 'h':
                return timedelta(hours=value)
            elif unit == 'm':
                return timedelta(minutes=value)
            else:
                logger.error(f"Invalid time interval unit: {unit}, defaulting to 1 hour")
                return timedelta(hours=1)
        except (ValueError, IndexError):
            logger.error(f"Invalid time interval format: {interval}, defaulting to 1 hour")
            return timedelta(hours=1)
        
    async def _get_db_stats(self, date: datetime) -> int:
        """Get record count from database for a specific date"""
        db = CallDatabase(self.domain_config.domain_id, date)
        try:
            cursor = db.conn.execute("SELECT COUNT(*) FROM calls")
            return cursor.fetchone()[0]
        finally:
            db.close()

    async def _log_stats(self):
        """Log database statistics"""
        today = datetime.utcnow().date()
        today_count = await self._get_db_stats(datetime.utcnow())
        yesterday = (datetime.utcnow() - timedelta(days=1))
        yesterday_count = await self._get_db_stats(yesterday)
        
        logger.info(
            f"Domain: {self.domain_config.domain_id} | "
            f"Records: Today({today_count}), Yesterday({yesterday_count})"
        )

    async def fetch_cdrs(self) -> None:
        """Fetch CDRs for the domain and store in database"""
        try:
            logger.info(f"Fetching CDRs for domain {self.domain_config.domain_id}")
            token = await self.auth_handler.get_token(self.domain_config.domain_id)
            if not token:
                logger.error(f"Failed to get token for domain {self.domain_config.domain_id}")
                return

            # Get query timerange in UTC
            start_time, end_time = self._get_query_timerange()
            
            # Format times for NS API (which expects local time in YYYY-MM-DD HH:mm:ss format)
            # Convert UTC to America/New_York for NS API
            eastern = pytz.timezone('America/New_York')
            start_local = start_time.astimezone(eastern)
            end_local = end_time.astimezone(eastern)
            
            # Format for API
            start_str = start_local.strftime("%Y-%m-%d %H:%M:%S")
            end_str = end_local.strftime("%Y-%m-%d %H:%M:%S")
            
            logger.info(
                f"Domain {self.domain_config.domain_id} query range: "
                f"UTC: {start_time.isoformat()} to {end_time.isoformat()}, "
                f"Local: {start_str} to {end_str}"
            )

            # Build query parameters for NS API
            params = {
                "object": "cdr2",
                "action": "read",
                "domain": self.domain_config.domain_id,
                "datetime-start": start_str,
                "datetime-end": end_str,
                "limit": "9999"
            }

            logger.info(
                f"Domain {self.domain_config.domain_id} API Request:\n"
                f"URL: {self.base_url}/\n"
                f"Parameters: {json.dumps(params, indent=2)}"
            )

            # URL encode the parameters
            encoded_params = urllib.parse.urlencode(params)

            # Fetch CDRs from NS API
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/",
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

                    cdrs = await response.json()
                    
                    # Log only the first CDR for debugging
                    if isinstance(cdrs, list) and cdrs:
                        logger.debug(
                            f"Sample CDR (first of {len(cdrs)}):\n"
                            f"{json.dumps(cdrs[0], indent=2)}\n"
                            f"... ({len(cdrs)-1} more records)"
                        )
                    
                    # Check if the response is a list of CDRs
                    if not isinstance(cdrs, list):
                        logger.error(
                            f"Unexpected response format for domain {self.domain_config.domain_id}: "
                            f"{cdrs}"
                        )
                        return

                    # Log the number of CDRs received
                    logger.info(f"Received {len(cdrs)} CDRs for domain {self.domain_config.domain_id}")

                    # Process CDRs and update state if successful
                    await self._process_cdrs(cdrs)
                    self.state.update({
                        "last_successful_query": start_str,
                        "last_successful_query_end": end_str
                    })
                    self._save_state()

            # After successful processing, log stats
            await self._log_stats()

        except Exception as e:
            logger.error(f"Error fetching CDRs for domain {self.domain_config.domain_id}: {str(e)}")

    async def _process_cdrs(self, cdrs: List[Dict]) -> None:
        """Process and store CDRs in the database"""
        logger.info(f"Processing {len(cdrs)} CDRs")
        
        db = CallDatabase(self.domain_config.domain_id)
        latest_record_time = 0
        
        try:
            for cdr in cdrs:
                try:
                    cdr_data = cdr.get('CdrR', {})
                    if not cdr_data:
                        continue

                    # Track the latest record time we've seen
                    record_time = int(cdr_data['time_release'])
                    latest_record_time = max(latest_record_time, record_time)

                    # Store in database using CallDatabase
                    with db.conn:
                        db.conn.execute("""
                            INSERT OR IGNORE INTO calls (
                                cdrid, 
                                orig_callid, 
                                term_callid, 
                                source_number, 
                                dest_number, 
                                call_time, 
                                time_release, 
                                raw_cdr,
                                transcription_status
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            cdr_data['id'],
                            cdr_data['orig_callid'],
                            cdr_data['term_callid'],
                            cdr_data['orig_from_user'],
                            cdr_data['orig_to_user'],
                            datetime.fromtimestamp(int(cdr_data['time_start'])).isoformat(),
                            int(cdr_data['time_release']),
                            json.dumps(cdr),
                            'pending'
                        ))

                    logger.debug(
                        f"Stored CDR: {cdr_data['id']} "
                        f"({cdr_data['orig_from_user']} -> {cdr_data['orig_to_user']})"
                    )

                except Exception as e:
                    logger.error(
                        f"Error processing CDR for domain {self.domain_config.domain_id}: "
                        f"{str(e)}, CDR data: {cdr_data}"
                    )

            logger.info("Finished processing CDRs")
            
            # After processing all CDRs, update state with latest record time
            if latest_record_time > 0:
                self.state["last_successful_query_end"] = latest_record_time
                self._save_state()
                logger.debug(f"Updated last record time to {datetime.fromtimestamp(latest_record_time)}")

        finally:
            db.close()

async def run_cdr_fetcher(domain_config: CustomerDomain, auth_handler: NSAuthHandler, start_time: int) -> None:
    """Run the CDR fetcher worker"""
    worker_pid = os.getpid()
    logger.info(f"Starting CDR fetcher worker {worker_pid} for domain {domain_config.domain_id}")
    
    fetcher = CDRFetcher(domain_config, auth_handler, start_time)
    
    # Log initial stats
    await fetcher._log_stats()
    
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