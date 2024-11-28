import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path
import sys

def setup_logging(service_name: str) -> logging.Logger:
    """
    Configure logging with rotation and multi-process support
    
    Args:
        service_name: Name of the service (e.g., 'cdr_fetcher', 'recording_handler')
    """
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.DEBUG)
    
    # Rotating file handler with daily rotation
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_dir / f"{service_name}.log",
        when='midnight',
        interval=1,
        backupCount=7,  # Keep logs for 7 days
        encoding='utf-8'
    )
    
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(processName)s:%(process)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    # Console handler with simpler format
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    
    return logger

def get_logger(service_name: str) -> logging.Logger:
    """Get or create a logger for the specified service"""
    logger = logging.getLogger(service_name)
    if not logger.handlers:
        return setup_logging(service_name)
    return logger 