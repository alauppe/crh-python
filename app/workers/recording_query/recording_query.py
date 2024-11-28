import asyncio
import os
from datetime import datetime, timedelta
import aiohttp
from typing import Dict, List, Optional
import json
from pathlib import Path

from ...config import ConfigManager, CustomerDomain
from ...database import CallDatabase
from ...auth import NSAuthHandler
from ...logging_config import get_logger

logger = get_logger("recording_query")

class RecordingQuery:
    def __init__(self, domain_config: CustomerDomain, auth_handler: NSAuthHandler):
        self.domain_config = domain_config
        self.auth_handler = auth_handler
        
        # Extract territory ID from domain_id
        self.territory_id = self.domain_config.domain_id.split('.')[1]
        self.base_url = f"https://{self.territory_id}-hpbx.dashmanager.com/ns-api"
        
        # Retry intervals in minutes
        self.retry_intervals = [3, 5, 7, 10]
        
        # Minimum delay after call end before checking recording (seconds)
        self.min_post_call_delay = 120

    async def query_recording(self, orig_callid: str, term_callid: str) -> Optional[Dict]:
        """Query recording details for a specific call"""
        try:
            token = await self.auth_handler.get_token(self.domain_config.domain_id)
            if not token:
                logger.error(f"Failed to get token for domain {self.domain_config.domain_id}")
                return None

            params = {
                "object": "recording",
                "action": "read",
                "domain": self.domain_config.domain_id,
                "orig_callid": orig_callid,
                "term_callid": term_callid
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/",
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
                        return None

                    recordings = await response.json()
                    if not recordings:
                        return None

                    # Find a valid recording (status converted/archived and duration > 0)
                    for recording in recordings:
                        if (recording['status'] in ['converted', 'archived'] and 
                            int(recording.get('duration', 0)) > 0):
                            return recording

                    return None

        except Exception as e:
            logger.error(f"Error querying recording: {str(e)}")
            return None

    def _get_next_retry_time(self, error_count: int) -> datetime:
        """Calculate next retry time based on error count"""
        if error_count <= len(self.retry_intervals):
            delay_minutes = self.retry_intervals[error_count - 1]
        else:
            delay_minutes = self.retry_intervals[-1]  # Use max delay for high error counts
        return datetime.utcnow() + timedelta(minutes=delay_minutes)

    async def process_pending_recordings(self) -> None:
        """Process all pending recordings for the domain"""
        try:
            db = CallDatabase(self.domain_config.domain_id)
            
            while True:
                # Debug log current status counts with more detail
                status_counts = db.conn.execute("""
                    SELECT 
                        recording_query_status,
                        COUNT(*) as count,
                        COUNT(CASE WHEN recording_query_worker_pid IS NOT NULL THEN 1 END) as locked,
                        COUNT(CASE WHEN json_extract(raw_cdr, '$.CdrR.duration') > '0' THEN 1 END) as with_duration,
                        COUNT(CASE WHEN datetime(time_release, 'unixepoch') <= datetime('now', '-120 seconds') THEN 1 END) as old_enough
                    FROM calls 
                    GROUP BY recording_query_status
                """).fetchall()
                
                logger.debug("Current recording_query_status counts:")
                for status, count, locked, with_duration, old_enough in status_counts:
                    logger.debug(
                        f"  {status}: {count} total, {locked} locked, "
                        f"{with_duration} with duration, {old_enough} old enough"
                    )

                # Get next record that's either pending or ready for retry
                logger.debug("Executing query to find pending recordings...")
                record = db.conn.execute("""
                    SELECT 
                        id,
                        orig_callid,
                        term_callid,
                        recording_query_status,
                        recording_query_worker_pid,
                        json_extract(raw_cdr, '$.CdrR.duration') as duration,
                        time_release,
                        datetime(time_release, 'unixepoch') as release_time,
                        error_count
                    FROM calls 
                    WHERE recording_query_status IN ('pending', 'retry') 
                    AND (
                        recording_query_status = 'pending' 
                        OR (
                            recording_query_status = 'retry' 
                            AND next_retry_time <= datetime('now')
                        )
                    )
                    AND recording_query_worker_pid IS NULL
                    AND datetime(time_release, 'unixepoch') <= datetime('now', '-120 seconds')
                    AND CAST(json_extract(raw_cdr, '$.CdrR.duration') AS INTEGER) > 0
                    LIMIT 1
                """).fetchone()

                if not record:
                    logger.debug("No pending recordings found")
                    break

                logger.debug(
                    f"Found record: ID={record['id']}, "
                    f"Status={record['recording_query_status']}, "
                    f"Duration={record['duration']}, "
                    f"Release={record['release_time']}"
                )

                try:
                    # Query recording details
                    recording = await self.query_recording(
                        record['orig_callid'], 
                        record['term_callid']
                    )

                    if recording:
                        # Found valid recording
                        logger.info(
                            f"Found recording for {record['id']}: "
                            f"URL={recording['url']}, "
                            f"Status={recording['status']}, "
                            f"Duration={recording.get('duration', 0)}"
                        )
                        
                        # Update database with recording details
                        with db.conn:
                            db.conn.execute("""
                                UPDATE calls
                                SET recording_query_status = 'has_recording',
                                    recording_query_worker_pid = NULL,
                                    recording_url = ?,
                                    recording_status = ?,
                                    recording_duration = ?,
                                    recording_metadata = ?,
                                    error_count = 0,
                                    next_retry_time = NULL,
                                    last_error = NULL
                                WHERE id = ?
                            """, (
                                recording['url'],
                                recording['status'],
                                int(recording.get('duration', 0)),
                                json.dumps(recording),
                                record['id']
                            ))
                    else:
                        # No recording found - schedule retry or mark as failed
                        error_count = record['error_count'] if record['error_count'] is not None else 0
                        error_count += 1
                        
                        if error_count < len(self.retry_intervals):
                            next_retry = self._get_next_retry_time(error_count)
                            with db.conn:
                                db.conn.execute("""
                                    UPDATE calls
                                    SET recording_query_status = 'retry',
                                        recording_query_worker_pid = NULL,
                                        error_count = ?,
                                        next_retry_time = ?,
                                        last_error = ?
                                    WHERE id = ?
                                """, (
                                    error_count,
                                    next_retry.isoformat(),
                                    "Recording not found or not ready",
                                    record['id']
                                ))
                            logger.info(
                                f"Scheduled retry #{error_count} for record {record['id']} "
                                f"at {next_retry.isoformat()}"
                            )
                        else:
                            # Max retries reached - mark as no recording
                            with db.conn:
                                db.conn.execute("""
                                    UPDATE calls
                                    SET recording_query_status = 'no_recording',
                                        recording_query_worker_pid = NULL
                                    WHERE id = ?
                                """, (record['id'],))
                            logger.info(f"No recording found for {record['id']} after max retries")

                except Exception as e:
                    logger.error(f"Error processing record {record['id']}: {str(e)}")
                    # Clear worker PID to allow retry
                    with db.conn:
                        db.conn.execute("""
                            UPDATE calls
                            SET recording_query_worker_pid = NULL
                            WHERE id = ?
                        """, (record['id'],))

                # Brief pause between records
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in process_pending_recordings: {str(e)}")
        finally:
            db.close()

async def run_recording_query(domain_config: CustomerDomain, auth_handler: NSAuthHandler) -> None:
    """Run the recording query worker"""
    worker_pid = os.getpid()
    logger.info(f"Starting recording query worker {worker_pid} for domain {domain_config.domain_id}")
    
    query = RecordingQuery(domain_config, auth_handler)
    
    while True:
        try:
            await query.process_pending_recordings()
            await asyncio.sleep(10)  # Short sleep between checks
            
        except Exception as e:
            logger.error(f"Error in recording query worker {worker_pid}: {str(e)}")
            await asyncio.sleep(60)  # Longer sleep after error