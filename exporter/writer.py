"""CSV writer utilities."""

import csv
import os
from typing import List, Any
from threading import Lock
from utils.logger import get_logger

logger = get_logger(__name__)


class CSVWriter:
    """Thread-safe CSV writer."""
    
    def __init__(self, output_directory: str):
        """
        Initialize CSV writer.
        
        Args:
            output_directory: Base output directory
        """
        self.output_directory = output_directory
        self.file_locks = {}
        self.global_lock = Lock()
        
        # Create output directory if not exists
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
    
    def _get_file_lock(self, filepath: str) -> Lock:
        """
        Get lock for a specific file.
        
        Args:
            filepath: File path
            
        Returns:
            Lock object for the file
        """
        with self.global_lock:
            if filepath not in self.file_locks:
                self.file_locks[filepath] = Lock()
            return self.file_locks[filepath]
    
    def write_batch(
        self,
        filename: str,
        headers: List[str],
        rows: List[List[Any]],
        mode: str = 'w'
    ):
        """
        Write batch of rows to CSV file (thread-safe).
        
        Args:
            filename: Output filename
            headers: CSV headers
            rows: List of row data
            mode: Write mode ('w' for write, 'a' for append)
        """
        filepath = os.path.join(self.output_directory, filename)
        
        # Get lock for this file
        file_lock = self._get_file_lock(filepath)
        
        with file_lock:
            # Check if file exists
            file_exists = os.path.exists(filepath)
            
            # If appending and file doesn't exist, treat as write
            if mode == 'a' and not file_exists:
                mode = 'w'
            
            try:
                with open(filepath, mode, newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    
                    # Write headers if new file
                    if mode == 'w':
                        writer.writerow(headers)
                    
                    # Write rows
                    if rows:
                        writer.writerows(rows)
                
                logger.debug(f"Wrote {len(rows)} rows to {filename} (mode={mode})")
            
            except Exception as e:
                logger.error(f"Error writing to {filename}: {e}")
                raise
    
    def get_file_path(self, filename: str) -> str:
        """
        Get full file path.
        
        Args:
            filename: Filename
            
        Returns:
            Full file path
        """
        return os.path.join(self.output_directory, filename)
    
    def file_exists(self, filename: str) -> bool:
        """
        Check if file exists.
        
        Args:
            filename: Filename
            
        Returns:
            True if file exists
        """
        return os.path.exists(self.get_file_path(filename))

