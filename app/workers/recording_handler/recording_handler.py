import asyncio
import os
from datetime import datetime, timedelta
import aiohttp
from typing import Dict, List, Optional
import json
from pathlib import Path
import boto3
import tempfile
from botocore.exceptions import ClientError
from abc import ABC, abstractmethod
from ftplib import FTP, FTP_TLS
import paramiko
import os.path

from ...config import ConfigManager, CustomerDomain
from ...database import CallDatabase
from ...auth import NSAuthHandler
from ...logging_config import get_logger

logger = get_logger("recording_handler")

class StorageBackend(ABC):
    @abstractmethod
    async def upload_file(self, local_path: Path, remote_key: str) -> bool:
        pass

    @abstractmethod
    async def verify_upload(self, remote_key: str) -> bool:
        pass

class S3Backend(StorageBackend):
    def __init__(self, config: Dict):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config["credentials"]["access_key"],
            aws_secret_access_key=config["credentials"]["secret_key"]
        )
        self.bucket_name = config["bucket"]

    async def upload_file(self, local_path: Path, remote_key: str) -> bool:
        try:
            self.s3_client.upload_file(str(local_path), self.bucket_name, remote_key)
            return True
        except Exception as e:
            logger.error(f"S3 upload error: {str(e)}")
            return False

    async def verify_upload(self, remote_key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=remote_key)
            return True
        except Exception as e:
            logger.error(f"S3 verification error: {str(e)}")
            return False

class B2Backend(StorageBackend):
    def __init__(self, config: Dict):
        import b2sdk.v2 as b2
        info = b2.InMemoryAccountInfo()
        self.b2_api = b2.B2Api(info)
        
        try:
            # Authorize with account ID and application key
            self.b2_api.authorize_account(
                "production",
                application_key_id=config["credentials"]["account_id"],
                application_key=config["credentials"]["application_key"]
            )
            
            # Get bucket by ID instead of name
            self.bucket = self.b2_api.get_bucket_by_id(config["bucket_id"])
            logger.info(f"Successfully initialized B2 backend for bucket {self.bucket.name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize B2 backend: {str(e)}")
            raise

    async def upload_file(self, local_path: Path, remote_key: str) -> bool:
        try:
            logger.debug(f"Uploading {local_path} to B2 as {remote_key}")
            uploaded_file = self.bucket.upload_local_file(
                local_file=str(local_path),
                file_name=remote_key,
                progress_listener=B2ProgressListener()
            )
            logger.info(f"Successfully uploaded {remote_key} to B2, file_id: {uploaded_file.id_}")
            return True
        except Exception as e:
            logger.error(f"B2 upload error: {str(e)}")
            return False

    async def verify_upload(self, remote_key: str) -> bool:
        try:
            file_version = self.bucket.get_file_info_by_name(remote_key)
            if file_version and file_version.size > 0:
                logger.info(f"Verified B2 upload for {remote_key}, size: {file_version.size}")
                return True
            logger.warning(f"B2 file {remote_key} exists but size is 0")
            return False
        except Exception as e:
            logger.error(f"B2 verification error: {str(e)}")
            return False

class B2ProgressListener:
    def set_total_bytes(self, total_byte_count):
        self.total_bytes = total_byte_count

    def bytes_completed(self, byte_count):
        progress = (byte_count / self.total_bytes) * 100 if self.total_bytes else 0
        logger.debug(f"B2 Upload Progress: {progress:.1f}%")

    def close(self):
        pass

class FTPBackend(StorageBackend):
    def __init__(self, config: Dict):
        self.config = config
        self.host = config["host"]
        self.port = config.get("port", 21)
        self.username = config["username"]
        self.password = config["password"]
        self.base_path = config.get("path", "/")

    def _connect(self) -> FTP:
        """Create FTP connection"""
        ftp = FTP()
        ftp.connect(self.host, self.port)
        ftp.login(self.username, self.password)
        return ftp

    async def upload_file(self, local_path: Path, remote_key: str) -> bool:
        try:
            ftp = self._connect()
            try:
                # Create directory structure if needed
                remote_dir = os.path.dirname(remote_key)
                if remote_dir:
                    self._make_dirs(ftp, os.path.join(self.base_path, remote_dir))

                # Upload file
                with open(local_path, 'rb') as f:
                    remote_path = os.path.join(self.base_path, remote_key)
                    logger.debug(f"Uploading to FTP: {remote_path}")
                    ftp.storbinary(f'STOR {remote_path}', f)
                return True
            finally:
                ftp.quit()
        except Exception as e:
            logger.error(f"FTP upload error: {str(e)}")
            return False

    async def verify_upload(self, remote_key: str) -> bool:
        try:
            ftp = self._connect()
            try:
                remote_path = os.path.join(self.base_path, remote_key)
                size = ftp.size(remote_path)
                return size > 0
            finally:
                ftp.quit()
        except Exception as e:
            logger.error(f"FTP verification error: {str(e)}")
            return False

    def _make_dirs(self, ftp: FTP, path: str):
        """Recursively create directory structure"""
        parts = path.split('/')
        for i in range(len(parts)):
            try:
                current = '/'.join(parts[:i+1])
                if current:
                    try:
                        ftp.mkd(current)
                    except:
                        pass  # Directory might already exist
            except Exception as e:
                logger.warning(f"Error creating directory {current}: {str(e)}")

class FTPSBackend(FTPBackend):
    def _connect(self) -> FTP_TLS:
        """Create FTPS connection with explicit TLS"""
        ftps = FTP_TLS()
        ftps.connect(self.host, self.port)
        ftps.login(self.username, self.password)
        ftps.prot_p()  # Set up secure data connection
        return ftps

class SFTPBackend(StorageBackend):
    def __init__(self, config: Dict):
        self.config = config
        self.host = config["host"]
        self.port = config.get("port", 22)
        self.username = config["username"]
        self.password = config.get("password")
        self.private_key_path = config.get("private_key_path")
        self.base_path = config.get("path", "/")

    def _connect(self) -> paramiko.SFTPClient:
        """Create SFTP connection"""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        if self.private_key_path:
            private_key = paramiko.RSAKey.from_private_key_file(self.private_key_path)
            ssh.connect(
                self.host, 
                self.port, 
                self.username, 
                pkey=private_key
            )
        else:
            ssh.connect(
                self.host, 
                self.port, 
                self.username, 
                self.password
            )
        
        return ssh.open_sftp()

    async def upload_file(self, local_path: Path, remote_key: str) -> bool:
        try:
            sftp = self._connect()
            try:
                # Create directory structure if needed
                remote_dir = os.path.dirname(remote_key)
                if remote_dir:
                    self._make_dirs(sftp, os.path.join(self.base_path, remote_dir))

                # Upload file
                remote_path = os.path.join(self.base_path, remote_key)
                logger.debug(f"Uploading to SFTP: {remote_path}")
                sftp.put(str(local_path), remote_path)
                return True
            finally:
                sftp.close()
        except Exception as e:
            logger.error(f"SFTP upload error: {str(e)}")
            return False

    async def verify_upload(self, remote_key: str) -> bool:
        try:
            sftp = self._connect()
            try:
                remote_path = os.path.join(self.base_path, remote_key)
                stats = sftp.stat(remote_path)
                return stats.st_size > 0
            finally:
                sftp.close()
        except Exception as e:
            logger.error(f"SFTP verification error: {str(e)}")
            return False

    def _make_dirs(self, sftp: paramiko.SFTPClient, path: str):
        """Recursively create directory structure"""
        parts = path.split('/')
        for i in range(len(parts)):
            try:
                current = '/'.join(parts[:i+1])
                if current:
                    try:
                        sftp.mkdir(current)
                    except:
                        pass  # Directory might already exist
            except Exception as e:
                logger.warning(f"Error creating directory {current}: {str(e)}")

class RecordingHandler:
    def __init__(self, domain_config: CustomerDomain, auth_handler: NSAuthHandler):
        self.domain_config = domain_config
        self.auth_handler = auth_handler
        
        # Initialize storage backends
        self.storage_backends: List[StorageBackend] = []
        for storage_config in domain_config.recording_handler["storage"]:
            if not storage_config.get("enabled", True):
                continue
                
            backend_type = storage_config["type"]
            if backend_type == "aws_s3":
                self.storage_backends.append(S3Backend(storage_config))
            elif backend_type == "backblaze_b2":
                self.storage_backends.append(B2Backend(storage_config))
            elif backend_type == "ftp":
                self.storage_backends.append(FTPBackend(storage_config))
            elif backend_type == "ftps":
                self.storage_backends.append(FTPSBackend(storage_config))
            elif backend_type == "sftp":
                self.storage_backends.append(SFTPBackend(storage_config))
        
        # Create temp directory for downloads
        self.temp_dir = Path(tempfile.gettempdir()) / "recording_handler"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def _generate_filenames(self, record: Dict) -> tuple[str, str]:
        """Generate filenames for recording and metadata"""
        timestamp = datetime.fromtimestamp(int(record['time_release']))
        date_time = timestamp.strftime("%Y%m%d%H%M%S")
        base_name = f"{date_time}_{record['dest_number']}_{record['source_number']}_{record['cdrid']}"
        return f"{base_name}.wav", f"{base_name}.json"

    async def _download_recording(self, url: str, dest_path: Path) -> bool:
        """Download recording from NS API"""
        try:
            # Get fresh token for download
            token = await self.auth_handler.get_token(self.domain_config.domain_id)
            if not token:
                logger.error("Failed to get token for recording download")
                return False

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"}
                ) as response:
                    if response.status != 200:
                        logger.error(f"Failed to download recording: {response.status}")
                        return False
                    
                    with open(dest_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)
                    
                    # Verify file was downloaded
                    if not dest_path.exists() or dest_path.stat().st_size == 0:
                        logger.error("Downloaded file is empty or missing")
                        return False
                    
                    logger.debug(f"Successfully downloaded recording to {dest_path}")
                    return True

        except Exception as e:
            logger.error(f"Error downloading recording: {str(e)}")
            return False

    async def _upload_to_all_backends(self, local_path: Path, remote_key: str) -> Dict[str, bool]:
        """Upload file to all configured storage backends"""
        results = {}
        for backend in self.storage_backends:
            success = await backend.upload_file(local_path, remote_key)
            if success:
                verified = await backend.verify_upload(remote_key)
                results[backend.__class__.__name__] = verified
            else:
                results[backend.__class__.__name__] = False
        return results

    async def _upload_metadata_to_s3(self, record: Dict, json_filename: str) -> bool:
        """Upload CDR metadata JSON to S3"""
        try:
            # Create temporary JSON file
            temp_json = self.temp_dir / json_filename
            try:
                # Write metadata to temp file
                with open(temp_json, 'w') as f:
                    json.dump(json.loads(record['raw_cdr']), f, indent=2)

                # Upload to all configured backends
                upload_results = await self._upload_to_all_backends(temp_json, json_filename)
                
                # Only consider success if all backends succeeded
                return all(upload_results.values())

            finally:
                # Clean up temp JSON file
                if temp_json.exists():
                    temp_json.unlink()

        except Exception as e:
            logger.error(f"Error uploading metadata to storage: {str(e)}")
            return False

    async def process_pending_recordings(self) -> None:
        """Process recordings that need to be uploaded"""
        try:
            db = CallDatabase(self.domain_config.domain_id)
            
            while True:
                # Get next recording to process with row locking
                with db.conn:
                    record = db.conn.execute("""
                        SELECT * FROM calls 
                        WHERE recording_query_status = 'has_recording'
                        AND (
                            recording_handler_status IS NULL 
                            OR recording_handler_status = 'retry'
                        )
                        AND recording_handler_worker_pid IS NULL
                        LIMIT 1
                    """).fetchone()

                    if not record:
                        break

                    # Mark record as being processed - with row locking
                    cursor = db.conn.execute("""
                        UPDATE calls 
                        SET recording_handler_worker_pid = ? 
                        WHERE id = ? 
                        AND recording_handler_worker_pid IS NULL
                        RETURNING *
                    """, (os.getpid(), record['id']))
                    
                    # If another worker got it first, skip
                    if not cursor.fetchone():
                        continue

                try:
                    temp_path = None
                    upload_verified = False
                    metadata_uploaded = False
                    
                    # Generate filenames
                    wav_filename, json_filename = self._generate_filenames(record)
                    temp_path = self.temp_dir / wav_filename

                    # Check if we have a previous temp file from a failed attempt
                    if temp_path.exists() and temp_path.stat().st_size > 0:
                        logger.info(f"Found existing temp file {temp_path}, attempting to use it")
                        download_needed = False
                    else:
                        download_needed = True

                    try:
                        # Step 1: Download recording (if needed)
                        if download_needed:
                            success = await self._download_recording(
                                record['recording_url'],
                                temp_path
                            )
                            if not success:
                                raise Exception("Failed to download recording")

                        # Step 2: Upload recording to all backends
                        upload_results = await self._upload_to_all_backends(temp_path, wav_filename)
                        
                        # Step 3: Verify all uploads succeeded
                        all_succeeded = all(upload_results.values())
                        
                        if all_succeeded:
                            # Step 4: Upload metadata JSON
                            metadata_success = await self._upload_metadata_to_s3(
                                record,
                                json_filename
                            )
                            
                            if metadata_success:
                                metadata_uploaded = True
                                upload_verified = True
                            else:
                                logger.error(f"Failed to upload metadata for {record['id']}")
                                raise Exception("Metadata upload failed")

                            # Update database with success status
                            with db.conn:
                                db.conn.execute("""
                                    UPDATE calls
                                    SET recording_handler_status = 'completed',
                                        recording_handler_worker_pid = NULL,
                                        recording_s3_key = ?,
                                        storage_results = ?,
                                        metadata_uploaded = ?,
                                        transcription_status = 'pending'
                                    WHERE id = ?
                                """, (
                                    wav_filename,
                                    json.dumps(upload_results),
                                    metadata_uploaded,
                                    record['id']
                                ))

                            # Optionally clear raw_cdr if configured
                            if metadata_uploaded and self.domain_config.recording_handler.get('clear_raw_cdr_after_upload', False):
                                with db.conn:
                                    db.conn.execute("""
                                        UPDATE calls
                                        SET raw_cdr = NULL
                                        WHERE id = ?
                                    """, (record['id'],))

                            logger.info(f"Successfully processed recording and metadata for {record['id']}")
                        else:
                            raise Exception(f"Upload failed for some backends: {upload_results}")

                    finally:
                        # Only remove temp file after successful upload verification
                        if temp_path and temp_path.exists():
                            if upload_verified:
                                try:
                                    temp_path.unlink()
                                    logger.debug(f"Cleaned up temporary file {temp_path}")
                                except Exception as e:
                                    logger.error(f"Error cleaning up temp file {temp_path}: {str(e)}")
                            else:
                                logger.info(
                                    f"Keeping temporary file {temp_path} for retry"
                                )

                except Exception as e:
                    error_msg = f"Error processing recording for call {record['id']}: {str(e)}"
                    logger.error(error_msg)
                    # Set status for retry
                    error_count = record.get('error_count', 0) + 1
                    retry_intervals = self.domain_config.recording_handler.get('retry_intervals', [3, 5, 7, 10])
                    
                    if error_count <= len(retry_intervals):
                        delay_minutes = retry_intervals[error_count - 1]
                        next_retry = datetime.utcnow() + timedelta(minutes=delay_minutes)
                        
                        with db.conn:
                            db.conn.execute("""
                                UPDATE calls
                                SET recording_handler_status = 'retry',
                                    recording_handler_worker_pid = NULL,
                                    error_count = ?,
                                    next_retry_time = ?,
                                    last_error = ?
                                WHERE id = ?
                            """, (
                                error_count,
                                next_retry.isoformat(),
                                error_msg,
                                record['id']
                            ))
                    else:
                        with db.conn:
                            db.conn.execute("""
                                UPDATE calls
                                SET recording_handler_status = 'failed',
                                    recording_handler_worker_pid = NULL,
                                    last_error = ?
                                WHERE id = ?
                            """, (error_msg, record['id']))

        except Exception as e:
            logger.error(f"Error in process_pending_recordings: {str(e)}")
        finally:
            db.close()

async def run_recording_handler(domain_config: CustomerDomain, auth_handler: NSAuthHandler) -> None:
    """Run the recording handler worker"""
    worker_pid = os.getpid()
    logger.info(f"Starting recording handler worker {worker_pid} for domain {domain_config.domain_id}")
    
    handler = RecordingHandler(domain_config, auth_handler)
    
    while True:
        try:
            await handler.process_pending_recordings()
            await asyncio.sleep(10)  # Short sleep between checks
            
        except Exception as e:
            logger.error(f"Error in recording handler worker {worker_pid}: {str(e)}")
            await asyncio.sleep(60)  # Longer sleep after error 