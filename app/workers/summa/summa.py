import asyncio
import os
from datetime import datetime, timedelta
import aiohttp
from typing import Dict, Optional
import json
from pathlib import Path
import tempfile
import openai
from openai import OpenAI

from ...config import CustomerDomain
from ...database import CallDatabase
from ...logging_config import get_logger

logger = get_logger("summa")

class SummaWorker:
    def __init__(self, domain_config: CustomerDomain):
        self.domain_config = domain_config
        self.summa_config = domain_config.call_summary or {}
        
        # Configure OpenAI
        self.openai_api_key = domain_config.openai_api_key
        if not self.openai_api_key:
            raise ValueError("OpenAI API key not configured for domain")
            
        self.llm_prompt = self.summa_config.get(
            "llm_prompt", 
            "Provide a detailed summary of the call."
        )
        self.max_retries = 3
        self.retry_intervals = self.summa_config.get(
            "retry_intervals", 
            [3, 5, 7, 10]
        )
        
        # Create temp directory for summaries
        self.temp_dir = Path(tempfile.gettempdir()) / "summa"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    async def generate_summary(self, transcript: str) -> Optional[str]:
        """Generate summary using OpenAI's GPT-4"""
        try:
            # Create OpenAI client with API key
            client = OpenAI(api_key=self.openai_api_key)
            
            messages = [
                {"role": "system", "content": self.llm_prompt},
                {"role": "user", "content": transcript}
            ]
            
            logger.debug(f"Requesting summary with prompt: {self.llm_prompt}")
            response = client.chat.completions.create(
                model=self.summa_config.get("model", "gpt-4"),
                messages=messages,
                temperature=self.summa_config.get("temperature", 0.7),
                max_tokens=self.summa_config.get("max_tokens", 500)
            )
            
            if response.choices:
                summary = response.choices[0].message.content
                logger.debug(f"Received summary of length {len(summary)}")
                return summary
            
            logger.error("No summary generated in response")
            return None

        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            return None

    def _generate_summary_filename(self, record: Dict) -> str:
        """Generate filename for summary file"""
        timestamp = datetime.fromtimestamp(int(record['time_release']))
        date_time = timestamp.strftime("%Y%m%d%H%M%S")
        return f"{date_time}_{record['dest_number']}_{record['source_number']}_{record['cdrid']}-summary.txt"

    async def process_pending_summaries(self) -> None:
        """Process transcripts that need summarization"""
        try:
            logger.debug("Starting process_pending_summaries")
            db = CallDatabase(self.domain_config.domain_id)
            
            while True:
                try:
                    # Get next transcript to summarize
                    with db.conn:
                        record = db.conn.execute("""
                            SELECT * FROM calls 
                            WHERE transcription_status = 'completed'
                            AND transcription_text IS NOT NULL
                            AND (
                                summary_status = 'pending'
                                OR (
                                    summary_status = 'retry'
                                    AND summary_next_retry_time <= datetime('now')
                                )
                            )
                            AND summary_worker_pid IS NULL
                            AND summary_error_count < ?
                            LIMIT 1
                        """, (self.max_retries,)).fetchone()

                        if not record:
                            logger.debug("No records found for summarization")
                            break

                        logger.info(
                            f"Found record to summarize: ID={record['id']}, "
                            f"TransStatus={record['transcription_status']}"
                        )

                        # Mark record as being processed
                        cursor = db.conn.execute("""
                            UPDATE calls 
                            SET summary_worker_pid = ? 
                            WHERE id = ? 
                            AND summary_worker_pid IS NULL
                            RETURNING *
                        """, (os.getpid(), record['id']))
                        
                        if not cursor.fetchone():
                            logger.warning(f"Failed to acquire lock for record {record['id']}")
                            continue

                        logger.info(f"Successfully locked record {record['id']} for processing")

                    try:
                        # Generate summary
                        summary = await self.generate_summary(record['transcription_text'])
                        
                        if summary:
                            # Generate filename and save summary
                            filename = self._generate_summary_filename(record)
                            temp_path = self.temp_dir / filename
                            
                            logger.debug(f"Saving summary to temporary file: {temp_path}")
                            with open(temp_path, 'w') as f:
                                f.write(summary)

                            # Upload to storage if configured
                            upload_success = True
                            if self.domain_config.recording_handler:
                                logger.debug("Uploading summary to storage backends...")
                                from ..recording_handler.recording_handler import RecordingHandler
                                handler = RecordingHandler(self.domain_config, None)
                                upload_results = await handler._upload_to_all_backends(
                                    temp_path, 
                                    filename
                                )
                                upload_success = all(upload_results.values())
                                logger.info(f"Storage upload results: {upload_results}")

                            # Update database
                            with db.conn:
                                db.conn.execute("""
                                    UPDATE calls
                                    SET summary_status = 'completed',
                                        summary_worker_pid = NULL,
                                        summary_text = ?,
                                        summary_uploaded = ?,
                                        summary_error_count = 0,
                                        summary_next_retry_time = NULL,
                                        summary_last_error = NULL
                                    WHERE id = ?
                                """, (summary, upload_success, record['id']))

                            # Clear summary from database if configured and uploaded
                            if (upload_success and 
                                self.summa_config.get('clear_summary_after_upload', False)):
                                with db.conn:
                                    db.conn.execute("""
                                        UPDATE calls
                                        SET summary_text = NULL
                                        WHERE id = ?
                                    """, (record['id'],))

                            logger.info(f"Successfully summarized recording {record['id']}")

                        else:
                            # Handle failure
                            error_count = record['summary_error_count'] + 1
                            logger.warning(f"Summary generation failed for record {record['id']}, error count: {error_count}")
                            
                            if error_count < self.max_retries:
                                delay_minutes = self.retry_intervals[min(error_count - 1, len(self.retry_intervals) - 1)]
                                next_retry = datetime.utcnow() + timedelta(minutes=delay_minutes)
                                
                                with db.conn:
                                    db.conn.execute("""
                                        UPDATE calls
                                        SET summary_status = 'retry',
                                            summary_worker_pid = NULL,
                                            summary_error_count = ?,
                                            summary_next_retry_time = ?
                                        WHERE id = ?
                                    """, (error_count, next_retry.isoformat(), record['id']))
                                
                                logger.info(
                                    f"Scheduling summary retry #{error_count} "
                                    f"for recording {record['id']} at {next_retry}"
                                )
                            else:
                                with db.conn:
                                    db.conn.execute("""
                                        UPDATE calls
                                        SET summary_status = 'failed',
                                            summary_worker_pid = NULL
                                        WHERE id = ?
                                    """, (record['id'],))
                                
                                logger.error(f"Max retries reached for summary of recording {record['id']}")

                    except Exception as e:
                        error_msg = f"Error processing record {record['id']}: {str(e)}"
                        logger.error(error_msg, exc_info=True)
                        with db.conn:
                            db.conn.execute("""
                                UPDATE calls
                                SET summary_status = 'error',
                                    summary_worker_pid = NULL,
                                    summary_last_error = ?
                                WHERE id = ?
                            """, (error_msg, record['id']))

                except Exception as e:
                    logger.error(f"Error in main processing loop: {str(e)}", exc_info=True)
                    await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Critical error in process_pending_summaries: {str(e)}", exc_info=True)
        finally:
            logger.debug("Closing database connection")
            db.close()

async def run_summa(domain_config: CustomerDomain) -> None:
    """Run the summary worker"""
    worker_pid = os.getpid()
    logger.info(f"Starting summary worker {worker_pid} for domain {domain_config.domain_id}")
    
    try:
        logger.debug("Initializing SummaWorker...")
        worker = SummaWorker(domain_config)
        logger.info("Successfully initialized SummaWorker")
        
        while True:
            try:
                logger.debug("Starting process_pending_summaries cycle")
                await worker.process_pending_summaries()
                logger.debug("Completed process_pending_summaries cycle")
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in summary worker {worker_pid}: {str(e)}", exc_info=True)
                await asyncio.sleep(60)
                
    except Exception as e:
        logger.error(f"Critical error initializing summary worker: {str(e)}", exc_info=True)
        await asyncio.sleep(60) 