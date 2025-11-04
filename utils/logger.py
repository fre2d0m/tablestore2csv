"""Logging utilities for TableStore exporter."""

import logging
import sys
from typing import Optional


class TqdmLoggingHandler(logging.Handler):
    """Custom logging handler that works with tqdm progress bars."""
    
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
    
    def emit(self, record):
        try:
            from tqdm import tqdm
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logger(name: str = 'tablestore_exporter', verbose: bool = False) -> logging.Logger:
    """
    Setup logger with appropriate formatting and level.
    
    Args:
        name: Logger name
        verbose: Enable debug logging
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    
    # Use TqdmLoggingHandler to avoid conflicts with progress bars
    handler = TqdmLoggingHandler()
    handler.setLevel(level)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get logger instance.
    
    Args:
        name: Logger name, defaults to 'tablestore_exporter'
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name or 'tablestore_exporter')

