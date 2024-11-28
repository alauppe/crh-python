import asyncio
from datetime import datetime, timedelta
from typing import Dict
import sqlite3
from pathlib import Path
from .config import ConfigManager
from .logging_config import get_logger

logger = get_logger("stats_monitor")

class StatsMonitor:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.refresh_interval = 5  # seconds

    def _get_db_stats(self, domain_id: str, date: datetime) -> Dict:
        """Get statistics for a specific domain and date"""
        db_path = Path(f"data/{domain_id}/{date.strftime('%Y-%m-%d')}.db")
        if not db_path.exists():
            return {}
        
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            
            stats = {
                # CDR Stats
                "total_cdrs": conn.execute(
                    "SELECT COUNT(*) FROM calls"
                ).fetchone()[0],
                
                # Recording Query Stats
                "pending_recording_query": conn.execute(
                    "SELECT COUNT(*) FROM calls WHERE recording_query_status = 'pending'"
                ).fetchone()[0],
                "retry_recording_query": conn.execute(
                    "SELECT COUNT(*) FROM calls WHERE recording_query_status = 'retry'"
                ).fetchone()[0],
                "has_recording": conn.execute(
                    "SELECT COUNT(*) FROM calls WHERE recording_query_status = 'has_recording'"
                ).fetchone()[0],
                "no_recording": conn.execute(
                    "SELECT COUNT(*) FROM calls WHERE recording_query_status = 'no_recording'"
                ).fetchone()[0],
                
                # Recording Handler Stats
                "pending_upload": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE recording_query_status = 'has_recording'
                    AND (recording_handler_status IS NULL OR recording_handler_status = 'retry')
                """).fetchone()[0],
                "completed_upload": conn.execute(
                    "SELECT COUNT(*) FROM calls WHERE recording_handler_status = 'completed'"
                ).fetchone()[0],
                "failed_upload": conn.execute(
                    "SELECT COUNT(*) FROM calls WHERE recording_handler_status = 'failed'"
                ).fetchone()[0],
                
                # Storage Backend Stats
                "storage_results": conn.execute("""
                    SELECT storage_results FROM calls 
                    WHERE recording_handler_status = 'completed' 
                    ORDER BY id DESC LIMIT 1
                """).fetchone()
            }
            
            # Parse storage backend results if available
            if stats["storage_results"] and stats["storage_results"][0]:
                import json
                backend_stats = json.loads(stats["storage_results"][0])
                stats["storage_backends"] = backend_stats
            
            stats.update({
                # Transcription Stats
                "pending_transcription": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE recording_handler_status = 'completed'
                    AND transcription_status = 'pending'
                """).fetchone()[0],
                "completed_transcription": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE transcription_status = 'completed'
                """).fetchone()[0],
                "retry_transcription": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE transcription_status = 'retry'
                """).fetchone()[0],
                "failed_transcription": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE transcription_status = 'failed'
                """).fetchone()[0],
                # Summary Stats
                "pending_summary": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE transcription_status = 'completed'
                    AND summary_status = 'pending'
                """).fetchone()[0],
                "completed_summary": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE summary_status = 'completed'
                """).fetchone()[0],
                "retry_summary": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE summary_status = 'retry'
                """).fetchone()[0],
                "failed_summary": conn.execute("""
                    SELECT COUNT(*) FROM calls 
                    WHERE summary_status = 'failed'
                """).fetchone()[0],
            })
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats for {domain_id}: {str(e)}")
            return {}
        finally:
            conn.close()

    def _format_stats_table(self, stats_by_date: Dict[str, Dict]) -> str:
        """Format statistics in a table format"""
        if not stats_by_date:
            return "No data available"
            
        # Define columns and headers with separators
        columns = [
            ("Date", 12),
            ("Total", 8),
            ("PendQ", 6),
            ("RetryQ", 7),
            ("HasRec", 7),
            ("NoRec", 6),
            ("UpldQ", 6),
            ("Upld✓", 6),
            ("Upld✗", 6),
            ("Storage", 18),
            ("TranQ", 6),
            ("TranR", 6),
            ("Tran✓", 6),
            ("Tran✗", 6),
            ("SumQ", 5),
            ("Sum✓", 5),
        ]
        
        # Create header with separators
        header = "|".join(f"{name:^{width}}" for name, width in columns)
        separator = "-" * (sum(width for _, width in columns) + len(columns) - 1)
        
        # Create rows
        rows = []
        for date, stats in stats_by_date.items():
            storage_status = ""
            if "storage_backends" in stats:
                storage_status = " ".join(
                    f"{backend[:3]}:{'✓' if status else '✗'}"
                    for backend, status in stats["storage_backends"].items()
                )
            
            row = [
                (date, 12),
                (str(stats.get("total_cdrs", 0)), 8),
                (str(stats.get("pending_recording_query", 0)), 6),
                (str(stats.get("retry_recording_query", 0)), 7),
                (str(stats.get("has_recording", 0)), 7),
                (str(stats.get("no_recording", 0)), 6),
                (str(stats.get("pending_upload", 0)), 6),
                (str(stats.get("completed_upload", 0)), 6),
                (str(stats.get("failed_upload", 0)), 6),
                (storage_status, 18),
                (str(stats.get("pending_transcription", 0)), 6),
                (str(stats.get("retry_transcription", 0)), 6),
                (str(stats.get("completed_transcription", 0)), 6),
                (str(stats.get("failed_transcription", 0)), 6),
                (str(stats.get("pending_summary", 0)), 5),
                (str(stats.get("completed_summary", 0)), 5),
            ]
            rows.append("|".join(f"{val:^{width}}" for val, width in row))
        
        return "\n".join([header, separator] + rows)

    async def display_stats(self):
        """Continuously display statistics"""
        while True:
            # Clear screen
            print("\033[2J\033[H", end="")
            
            print(f"=== Queue Statistics === (Updated: {datetime.now().strftime('%H:%M:%S')})")
            
            # Get stats for last 3 days
            today = datetime.now()
            dates = [
                (today, "Today"),
                (today - timedelta(days=1), "Yesterday"),
                (today - timedelta(days=2), "2 Days Ago")
            ]
            
            for domain_id in self.config.domain_map:
                print(f"\nDomain: {domain_id}")
                
                # Collect stats for all dates
                stats_by_date = {}
                for date, label in dates:
                    stats = self._get_db_stats(domain_id, date)
                    if stats:  # Only include dates with data
                        stats_by_date[label] = stats
                
                # Display table
                print(self._format_stats_table(stats_by_date))
            
            await asyncio.sleep(self.refresh_interval) 