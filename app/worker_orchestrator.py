import asyncio
import os
import signal
from typing import Dict, List
import psutil
from datetime import datetime, timedelta, timezone

from .config import ConfigManager
from .auth import NSAuthHandler
from .logging_config import get_logger
from .workers.cdr_fetcher import run_cdr_fetcher
from .workers.recording_query import run_recording_query
from .workers.recording_handler import run_recording_handler
from .workers.transcription import run_transcription
from .workers.summa import run_summa
from .stats_monitor import StatsMonitor

logger = get_logger("worker_orchestrator")

class WorkerOrchestrator:
    def __init__(self, config_path: str = "config.json"):
        self.config = ConfigManager(config_path)
        self.start_time = datetime.now(timezone.utc)
        
        # Create single auth handler instance
        logger.info("Initializing auth handler")
        self.auth_handler = NSAuthHandler(self.config)
        
        self.stats_monitor = StatsMonitor(self.config)
        self.worker_processes: Dict[str, List[psutil.Process]] = {
            "cdr_fetcher": [],
            "recording_query": [],
            "recording_handler": [],
            "transcription": [],
            "summa": []
        }
        self.running = True
        
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Shutdown signal received, stopping workers...")
        self.running = False
        self._stop_all_workers()

    def _stop_all_workers(self):
        """Stop all worker processes"""
        for worker_type, processes in self.worker_processes.items():
            for process in processes:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except psutil.TimeoutExpired:
                    process.kill()
                except psutil.NoSuchProcess:
                    pass
            self.worker_processes[worker_type] = []

    async def _check_worker_health(self):
        """Check health of workers and restart if needed"""
        while self.running:
            try:
                for domain_id, domain_config in self.config.domain_map.items():
                    # Get concurrency settings directly from domain config
                    concurrency = domain_config.worker_concurrency

                    # Check CDR fetcher workers
                    existing_cdr_workers = sum(
                        1 for p in self.worker_processes["cdr_fetcher"]
                        if p.cmdline() and domain_id in p.cmdline()[-1]
                    )
                    
                    while existing_cdr_workers < concurrency["cdr_fetcher"]:
                        process = await asyncio.create_subprocess_exec(
                            "python", "-m", "app.workers.cdr_fetcher",
                            domain_id,
                            str(int(self.start_time.timestamp())),
                            "--auth-handler", str(id(self.auth_handler)),
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                        self.worker_processes["cdr_fetcher"].append(
                            psutil.Process(process.pid)
                        )
                        logger.debug(f"Started new CDR fetcher worker for domain {domain_id}")
                        existing_cdr_workers += 1

                    # Check recording query workers
                    recording_config = domain_config.recording_query or {}
                    recording_enabled = recording_config.get("enabled", True)
                    
                    if recording_enabled:
                        existing_recording_workers = sum(
                            1 for p in self.worker_processes["recording_query"]
                            if p.cmdline() and domain_id in p.cmdline()[-1]
                        )
                        
                        while existing_recording_workers < concurrency["recording_query"]:
                            process = await asyncio.create_subprocess_exec(
                                "python", "-m", "app.workers.recording_query",
                                domain_id,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            self.worker_processes["recording_query"].append(
                                psutil.Process(process.pid)
                            )
                            logger.info(f"Started new recording query worker for domain {domain_id}")
                            existing_recording_workers += 1

                    # Check recording handler workers
                    handler_config = domain_config.recording_handler or {}
                    handler_enabled = handler_config.get("enabled", True)
                    
                    if handler_enabled:
                        existing_handler_workers = sum(
                            1 for p in self.worker_processes["recording_handler"]
                            if p.cmdline() and domain_id in p.cmdline()[-1]
                        )
                        
                        while existing_handler_workers < concurrency["recording_handler"]:
                            process = await asyncio.create_subprocess_exec(
                                "python", "-m", "app.workers.recording_handler",
                                domain_id,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            self.worker_processes["recording_handler"].append(
                                psutil.Process(process.pid)
                            )
                            logger.info(f"Started new recording handler worker for domain {domain_id}")
                            existing_handler_workers += 1

                    # Check transcription workers
                    if domain_config.openai_api_key:
                        existing_transcription_workers = sum(
                            1 for p in self.worker_processes["transcription"]
                            if p.cmdline() and domain_id in p.cmdline()[-1]
                        )
                        
                        while existing_transcription_workers < concurrency["transcription"]:
                            process = await asyncio.create_subprocess_exec(
                                "python", "-m", "app.workers.transcription",
                                domain_id,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            self.worker_processes["transcription"].append(
                                psutil.Process(process.pid)
                            )
                            logger.info(f"Started new transcription worker for domain {domain_id}")
                            existing_transcription_workers += 1

                    # Check summa workers
                    if domain_config.openai_api_key and domain_config.call_summary.get("enabled", True):
                        existing_summa_workers = sum(
                            1 for p in self.worker_processes["summa"]
                            if p.cmdline() and domain_id in p.cmdline()[-1]
                        )
                        
                        while existing_summa_workers < concurrency.get("summa", 1):
                            process = await asyncio.create_subprocess_exec(
                                "python", "-m", "app.workers.summa",
                                domain_id,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            self.worker_processes["summa"].append(
                                psutil.Process(process.pid)
                            )
                            logger.info(f"Started new summa worker for domain {domain_id}")
                            existing_summa_workers += 1
            
            except Exception as e:
                logger.error(f"Error in worker health check: {str(e)}")
            
            await asyncio.sleep(30)  # Check every 30 seconds

    async def run(self):
        """Run the orchestrator"""
        try:
            # Start worker health check
            health_check_task = asyncio.create_task(self._check_worker_health())
            
            # Start stats monitor
            stats_task = asyncio.create_task(self.stats_monitor.display_stats())
            
            # Wait for shutdown signal
            while self.running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("Orchestrator shutdown requested")
        finally:
            self.running = False
            health_check_task.cancel()
            stats_task.cancel()
            self._stop_all_workers()

def main():
    """Main entry point for the worker orchestrator"""
    orchestrator = WorkerOrchestrator()
    asyncio.run(orchestrator.run())

if __name__ == "__main__":
    main() 