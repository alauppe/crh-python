import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional
import json

class CallDatabase:
    def __init__(self, domain_id: str, date: Optional[datetime] = None):
        """
        Initialize database connection for a specific domain and date
        
        Args:
            domain_id: Domain identifier
            date: Date for the database file (defaults to today)
        """
        self.domain_id = domain_id
        self.date = date or datetime.now()
        self.db_dir = Path(f"data/{domain_id}")
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.db_dir / f"{self.date.strftime('%Y-%m-%d')}.db"
        
        self.conn = self._init_database()
    
    def _init_database(self) -> sqlite3.Connection:
        """Initialize the database and create tables if they don't exist"""
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        
        # Enable WAL mode for better concurrent access
        conn.execute("PRAGMA journal_mode=WAL")
        
        # Create calls table with all worker fields
        conn.execute("""
            CREATE TABLE IF NOT EXISTS calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cdrid TEXT UNIQUE,
                orig_callid TEXT NOT NULL,
                term_callid TEXT NOT NULL,
                source_number TEXT NOT NULL,
                dest_number TEXT NOT NULL,
                call_time TEXT NOT NULL,
                duration INTEGER DEFAULT 0,
                time_release INTEGER,  -- Unix timestamp when call ended
                raw_cdr TEXT,  -- Store complete CDR JSON
                
                -- Recording fields
                recording_url TEXT,
                recording_status TEXT,
                recording_duration INTEGER,
                recording_metadata TEXT,
                
                -- Recording query worker fields
                recording_query_status TEXT DEFAULT 'pending',
                recording_query_worker_pid INTEGER,
                error_count INTEGER DEFAULT 0,
                last_error TEXT,
                next_retry_time TEXT,
                
                -- Recording handler worker fields
                recording_handler_status TEXT,
                recording_handler_worker_pid INTEGER,
                recording_s3_key TEXT,
                storage_results TEXT,  -- JSON string of upload results per backend
                metadata_uploaded BOOLEAN DEFAULT 0,  -- Track if JSON was uploaded
                
                -- Transcription worker fields
                transcription_status TEXT DEFAULT 'pending',
                transcription_worker_pid INTEGER,
                transcription_text TEXT,  -- Full transcript
                transcription_error_count INTEGER DEFAULT 0,
                transcription_last_error TEXT,
                transcription_next_retry_time TEXT,
                transcription_uploaded BOOLEAN DEFAULT 0,  -- Track if transcript was uploaded
                
                -- Summary worker fields
                summary_status TEXT DEFAULT 'pending',
                summary_worker_pid INTEGER,
                summary_text TEXT,  -- Call summary
                summary_error_count INTEGER DEFAULT 0,
                summary_last_error TEXT,
                summary_next_retry_time TEXT,
                summary_uploaded BOOLEAN DEFAULT 0,  -- Track if summary was uploaded
                
                -- Indexes for efficient querying
                UNIQUE(orig_callid, term_callid)
            )
        """)
        
        # Create indexes for common queries
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_recording_query_status 
            ON calls(recording_query_status, next_retry_time)
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_call_time 
            ON calls(call_time)
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_time_release 
            ON calls(time_release)
        """)
        
        return conn
    
    def acquire_record(self, worker_type: str, worker_pid: int) -> Optional[dict]:
        """
        Try to acquire a record for processing
        
        Args:
            worker_type: Type of worker ('cdr', 'recording_query', etc.)
            worker_pid: PID of the worker process
            
        Returns:
            Record dict if acquired, None if no records available
        """
        status_field = f"{worker_type}_status"
        pid_field = f"{worker_type}_worker_pid"
        
        with self.conn:
            # Find oldest unprocessed record
            cursor = self.conn.execute(f"""
                SELECT * FROM calls 
                WHERE {status_field} = 'pending'
                AND {pid_field} IS NULL
                ORDER BY call_time ASC
                LIMIT 1
            """)
            record = cursor.fetchone()
            
            if not record:
                return None
            
            # Try to acquire the record
            cursor = self.conn.execute(f"""
                UPDATE calls
                SET {pid_field} = ?
                WHERE id = ?
                AND {pid_field} IS NULL
                RETURNING *
            """, (worker_pid, record['id']))
            
            acquired = cursor.fetchone()
            
            if acquired:
                return dict(acquired)
            return None
    
    def update_status(self, record_id: int, worker_type: str, 
                     status: str, error: Optional[str] = None) -> None:
        """Update status and clear worker PID after processing"""
        status_field = f"{worker_type}_status"
        pid_field = f"{worker_type}_worker_pid"
        
        with self.conn:
            if error:
                self.conn.execute("""
                    UPDATE calls
                    SET error_count = error_count + 1,
                        last_error = ?,
                        {status_field} = ?,
                        {pid_field} = NULL
                    WHERE id = ?
                """, (error, status, record_id))
            else:
                self.conn.execute(f"""
                    UPDATE calls
                    SET {status_field} = ?,
                        {pid_field} = NULL
                    WHERE id = ?
                """, (status, record_id))
    
    def close(self):
        """Close the database connection"""
        self.conn.close() 