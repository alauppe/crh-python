import asyncio
import os
from datetime import datetime, timedelta
import aiohttp
from typing import Dict, Optional
import json
from pathlib import Path
import tempfile

from ...config import CustomerDomain
from ...database import CallDatabase
from ...logging_config import get_logger
from ...auth import NSAuthHandler

logger = get_logger("transcription")

class TranscriptionWorker:
    def __init__(self, domain_config: CustomerDomain, auth_handler: NSAuthHandler):
        self.domain_config = domain_config
        self.transcription_config = domain_config.call_transcription or {}
        
        # Get API key from domain config
        self.openai_api_key = domain_config.openai_api_key
        if not self.openai_api_key:
            raise ValueError("OpenAI API key not configured for domain")
            
        self.transcription_api_url = self.transcription_config.get(
            "api_url", 
            "http://v10.t5.io:8000/transcribe"
        )
        self.max_retries = 3
        self.retry_intervals = self.transcription_config.get(
            "retry_intervals", 
            [3, 5, 7, 10]
        )
        
        # Create temp directory for transcripts
        self.temp_dir = Path(tempfile.gettempdir()) / "transcription"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    async def transcribe_recording(self, recording_url: str) -> Optional[str]:
        """Send recording to transcription service"""
        try:
            payload = {
                "audio_url": recording_url,
                "openai_api_key": self.domain_config.openai_api_key
            }

            logger.info(f"Sending request to {self.transcription_api_url}")
            logger.debug(f"Request payload: {json.dumps({**payload, 'openai_api_key': '***'})}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.transcription_api_url,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json"
                    },
                    json=payload,
                    timeout=300  # 5 minute timeout for long recordings
                ) as response:
                    response_text = await response.text()
                    logger.info(f"Raw API Response: Status={response.status}, Body={response_text}")
                    
                    if response.status != 200:
                        logger.error(f"API Error: Status={response.status}, Response={response_text}")
                        return None

                    try:
                        data = json.loads(response_text)
                        if data.get("status") == "success" and "transcript" in data:  # Changed from text to transcript
                            transcript = data["transcript"]  # Get transcript field
                            logger.info(f"Successfully received transcript of length {len(transcript)}")
                            return transcript
                        else:
                            logger.error(f"Unexpected response format: {data}")
                            return None
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON response: {response_text}")
                        return None

        except Exception as e:
            logger.error(f"Error calling transcription service: {str(e)}", exc_info=True)
            return None

    def _generate_transcript_filename(self, record: Dict) -> str:
        """Generate filename for transcript file"""
        timestamp = datetime.fromtimestamp(int(record['time_release']))
        date_time = timestamp.strftime("%Y%m%d%H%M%S")
        return f"{date_time}_{record['dest_number']}_{record['source_number']}_{record['cdrid']}-transcript.txt"

    async def process_pending_transcriptions(self) -> None:
        """Process recordings that need transcription"""
        try:
            logger.debug("Starting process_pending_transcriptions")
            db = CallDatabase(self.domain_config.domain_id)
            
            while True:
                try:
                    # Get next recording to transcribe - modified query
                    with db.conn:
                        # Debug log the counts
                        counts = db.conn.execute("""
                            SELECT 
                                COUNT(*) as total,
                                SUM(CASE WHEN recording_query_status = 'has_recording' THEN 1 ELSE 0 END) as has_recording,
                                SUM(CASE WHEN recording_url IS NOT NULL THEN 1 ELSE 0 END) as has_url,
                                SUM(CASE WHEN transcription_status = 'pending' THEN 1 ELSE 0 END) as pending_trans
                            FROM calls
                        """).fetchone()
                        
                        logger.info(
                            f"Database status: Total={counts['total']}, "
                            f"HasRecording={counts['has_recording']}, "
                            f"HasURL={counts['has_url']}, "
                            f"PendingTranscription={counts['pending_trans']}"
                        )

                        # Log the SQL query for debugging
                        logger.debug("Executing query to find records for transcription...")
                        record = db.conn.execute("""
                            SELECT * FROM calls 
                            WHERE recording_query_status = 'has_recording'
                            AND recording_url IS NOT NULL
                            AND (
                                transcription_status = 'pending'
                                OR (
                                    transcription_status = 'retry'
                                    AND (
                                        transcription_next_retry_time IS NULL 
                                        OR datetime(transcription_next_retry_time) <= datetime('now')
                                    )
                                )
                            )
                            AND (transcription_worker_pid IS NULL OR transcription_worker_pid = ?)
                            AND (transcription_error_count IS NULL OR transcription_error_count < ?)
                            ORDER BY 
                                CASE transcription_status
                                    WHEN 'pending' THEN 0
                                    WHEN 'retry' THEN 1
                                    ELSE 2
                                END,
                                time_release ASC
                            LIMIT 1
                        """, (os.getpid(), self.max_retries)).fetchone()

                        if not record:
                            logger.debug("No records found for transcription")
                            break

                        logger.info(
                            f"Found record to transcribe: ID={record['id']}, "
                            f"Status={record['recording_query_status']}, "
                            f"TransStatus={record['transcription_status']}, "
                            f"URL={record['recording_url']}"
                        )

                        # Mark record as being processed with row locking
                        logger.debug(f"Attempting to lock record {record['id']} for processing...")
                        cursor = db.conn.execute("""
                            UPDATE calls 
                            SET transcription_worker_pid = ? 
                            WHERE id = ? 
                            AND transcription_worker_pid IS NULL
                            RETURNING *
                        """, (os.getpid(), record['id']))
                        
                        if not cursor.fetchone():
                            logger.warning(f"Failed to acquire lock for record {record['id']}")
                            continue

                        logger.info(f"Successfully locked record {record['id']} for processing")

                    try:
                        # Get transcript
                        logger.debug(f"Requesting transcription for URL: {record['recording_url']}")
                        transcript = await self.transcribe_recording(record['recording_url'])
                        
                        if transcript:
                            logger.info(f"Received transcript of length {len(transcript)} for record {record['id']}")
                            
                            # Generate filename and save transcript
                            filename = self._generate_transcript_filename(record)
                            temp_path = self.temp_dir / filename
                            
                            logger.debug(f"Saving transcript to temporary file: {temp_path}")
                            with open(temp_path, 'w') as f:
                                f.write(transcript)

                            # Upload to storage if configured
                            upload_success = True
                            if (self.domain_config.recording_handler and 
                                self.domain_config.recording_handler.get("storage") and 
                                any(s.get("enabled", True) for s in self.domain_config.recording_handler["storage"])):
                                
                                logger.debug("Uploading transcript to storage backends...")
                                from ..recording_handler.recording_handler import RecordingHandler
                                # Get auth handler singleton
                                auth_handler = NSAuthHandler()
                                handler = RecordingHandler(self.domain_config, auth_handler)
                                
                                try:
                                    upload_results = await handler._upload_to_all_backends(
                                        temp_path, 
                                        filename
                                    )
                                    upload_success = any(upload_results.values())  # Success if any backend worked
                                    logger.info(f"Storage upload results: {upload_results}")
                                except Exception as e:
                                    logger.error(f"Error uploading to storage: {str(e)}")
                                    upload_success = False
                                
                                # Clean up temp file
                                try:
                                    if temp_path.exists():
                                        temp_path.unlink()
                                        logger.debug(f"Cleaned up temporary file {temp_path}")
                                except Exception as e:
                                    logger.error(f"Error cleaning up temp file: {str(e)}")

                            # Update database with transcript and status
                            logger.debug(f"Updating database for record {record['id']}")
                            with db.conn:
                                db.conn.execute("""
                                    UPDATE calls
                                    SET transcription_status = 'completed',
                                        transcription_worker_pid = NULL,
                                        transcription_text = ?,
                                        transcription_uploaded = ?,
                                        transcription_error_count = 0,
                                        transcription_next_retry_time = NULL,
                                        transcription_last_error = NULL,
                                        summary_status = 'pending'
                                    WHERE id = ?
                                """, (transcript, upload_success, record['id']))

                            # Clear transcript from database if configured and uploaded
                            if (upload_success and 
                                self.transcription_config.get('clear_transcript_after_upload', False)):
                                logger.debug(f"Clearing transcript from database for record {record['id']}")
                                with db.conn:
                                    db.conn.execute("""
                                        UPDATE calls
                                        SET transcription_text = NULL
                                        WHERE id = ?
                                    """, (record['id'],))

                            logger.info(f"Successfully transcribed recording {record['id']}")

                        else:
                            # Handle failure
                            error_count = record['transcription_error_count'] + 1
                            logger.warning(f"Transcription failed for record {record['id']}, error count: {error_count}")
                            
                            if error_count < self.max_retries:
                                delay_minutes = self.retry_intervals[min(error_count - 1, len(self.retry_intervals) - 1)]
                                next_retry = datetime.utcnow() + timedelta(minutes=delay_minutes)
                                
                                with db.conn:
                                    db.conn.execute("""
                                        UPDATE calls
                                        SET transcription_status = 'retry',
                                            transcription_worker_pid = NULL,
                                            transcription_error_count = ?,
                                            transcription_next_retry_time = ?
                                        WHERE id = ?
                                    """, (error_count, next_retry.isoformat(), record['id']))
                                
                                logger.info(
                                    f"Scheduling transcription retry #{error_count} "
                                    f"for recording {record['id']} at {next_retry}"
                                )
                            else:
                                with db.conn:
                                    db.conn.execute("""
                                        UPDATE calls
                                        SET transcription_status = 'failed',
                                            transcription_worker_pid = NULL
                                        WHERE id = ?
                                    """, (record['id'],))
                                
                                logger.error(f"Max retries reached for transcription of recording {record['id']}")

                    except Exception as e:
                        error_msg = f"Error processing record {record['id']}: {str(e)}"
                        logger.error(error_msg, exc_info=True)  # Include stack trace
                        with db.conn:
                            db.conn.execute("""
                                UPDATE calls
                                SET transcription_status = 'error',
                                    transcription_worker_pid = NULL,
                                    transcription_last_error = ?
                                WHERE id = ?
                            """, (error_msg, record['id']))

                except Exception as e:
                    logger.error(f"Error in main processing loop: {str(e)}", exc_info=True)
                    await asyncio.sleep(5)  # Brief pause before retrying

        except Exception as e:
            logger.error(f"Critical error in process_pending_transcriptions: {str(e)}", exc_info=True)
        finally:
            logger.debug("Closing database connection")
            db.close()

async def run_transcription(domain_config: CustomerDomain, auth_handler: NSAuthHandler) -> None:
    """Run the transcription worker"""
    worker_pid = os.getpid()
    logger.info(f"Starting transcription worker {worker_pid} for domain {domain_config.domain_id}")
    
    try:
        # Pass auth handler to worker
        logger.debug("Initializing TranscriptionWorker...")
        worker = TranscriptionWorker(domain_config, auth_handler)  # Add auth_handler here
        logger.info("Successfully initialized TranscriptionWorker")
        
        while True:
            try:
                logger.debug("Starting process_pending_transcriptions cycle")
                await worker.process_pending_transcriptions()
                logger.debug("Completed process_pending_transcriptions cycle")
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in transcription worker {worker_pid}: {str(e)}", exc_info=True)
                await asyncio.sleep(60)
                
    except Exception as e:
        logger.error(f"Critical error initializing transcription worker: {str(e)}", exc_info=True)
        await asyncio.sleep(60)