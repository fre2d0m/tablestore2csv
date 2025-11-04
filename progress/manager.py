"""Progress manager for tracking and resuming exports."""

import json
import os
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Set, Optional
from threading import Lock
from dataclasses import dataclass, asdict
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Progress:
    """Represents export progress state."""
    config_hash: str
    completed_tasks: List[str]
    failed_tasks: Dict[str, str]  # task_id -> error_message
    total_tasks: int
    start_time: str
    last_update: str
    total_rows_exported: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Progress':
        """Create from dictionary."""
        return cls(**data)


class ProgressManager:
    """Manage export progress for resume functionality."""
    
    def __init__(self, progress_file: str = '.export_progress.json'):
        """
        Initialize progress manager.
        
        Args:
            progress_file: Path to progress file
        """
        self.progress_file = progress_file
        self.lock = Lock()
    
    def calculate_config_hash(self, config: Dict[str, Any]) -> str:
        """
        Calculate hash of configuration for validation.
        
        Args:
            config: Export configuration
            
        Returns:
            SHA256 hash string
        """
        # Create a stable string representation
        config_str = json.dumps(config, sort_keys=True)
        hash_obj = hashlib.sha256(config_str.encode('utf-8'))
        return f"sha256:{hash_obj.hexdigest()[:16]}"
    
    def load_progress(self) -> Optional[Progress]:
        """
        Load progress from file.
        
        Returns:
            Progress object if file exists, None otherwise
        """
        if not os.path.exists(self.progress_file):
            logger.info("No progress file found, starting fresh")
            return None
        
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            progress = Progress.from_dict(data)
            logger.info(f"Loaded progress: {len(progress.completed_tasks)}/{progress.total_tasks} tasks completed")
            return progress
        
        except Exception as e:
            logger.error(f"Error loading progress file: {e}")
            return None
    
    def save_progress(self, progress: Progress):
        """
        Save progress to file (thread-safe).
        
        Args:
            progress: Progress object to save
        """
        with self.lock:
            try:
                progress.last_update = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                with open(self.progress_file, 'w', encoding='utf-8') as f:
                    json.dump(progress.to_dict(), f, ensure_ascii=False, indent=2)
                
                logger.debug(f"Progress saved: {len(progress.completed_tasks)}/{progress.total_tasks}")
            
            except Exception as e:
                logger.error(f"Error saving progress: {e}")
    
    def create_new_progress(self, config: Dict[str, Any], total_tasks: int) -> Progress:
        """
        Create new progress state.
        
        Args:
            config: Export configuration
            total_tasks: Total number of tasks
            
        Returns:
            New Progress object
        """
        return Progress(
            config_hash=self.calculate_config_hash(config),
            completed_tasks=[],
            failed_tasks={},
            total_tasks=total_tasks,
            start_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            total_rows_exported=0
        )
    
    def validate_config_hash(self, progress: Progress, config: Dict[str, Any]) -> bool:
        """
        Validate that configuration hasn't changed.
        
        Args:
            progress: Loaded progress
            config: Current configuration
            
        Returns:
            True if config matches, False otherwise
        """
        current_hash = self.calculate_config_hash(config)
        matches = progress.config_hash == current_hash
        
        if not matches:
            logger.warning("Configuration has changed since last run")
            logger.warning(f"Previous hash: {progress.config_hash}")
            logger.warning(f"Current hash: {current_hash}")
        
        return matches
    
    def get_pending_tasks(self, all_task_ids: List[str], progress: Progress) -> List[str]:
        """
        Get list of pending tasks.
        
        Args:
            all_task_ids: List of all task IDs
            progress: Current progress
            
        Returns:
            List of pending task IDs
        """
        completed_set = set(progress.completed_tasks)
        pending = [tid for tid in all_task_ids if tid not in completed_set]
        
        logger.info(f"Pending tasks: {len(pending)}/{len(all_task_ids)}")
        return pending
    
    def mark_task_completed(self, progress: Progress, task_id: str, rows_exported: int = 0):
        """
        Mark a task as completed.
        
        Args:
            progress: Progress object
            task_id: Task ID to mark as completed
            rows_exported: Number of rows exported for this task
        """
        if task_id not in progress.completed_tasks:
            progress.completed_tasks.append(task_id)
        
        # Remove from failed tasks if it was there
        if task_id in progress.failed_tasks:
            del progress.failed_tasks[task_id]
        
        progress.total_rows_exported += rows_exported
    
    def mark_task_failed(self, progress: Progress, task_id: str, error_message: str):
        """
        Mark a task as failed.
        
        Args:
            progress: Progress object
            task_id: Task ID
            error_message: Error message
        """
        progress.failed_tasks[task_id] = error_message
        logger.error(f"Task {task_id} failed: {error_message}")
    
    def get_progress_summary(self, progress: Progress) -> Dict[str, Any]:
        """
        Get progress summary.
        
        Args:
            progress: Progress object
            
        Returns:
            Summary dictionary
        """
        completed_count = len(progress.completed_tasks)
        failed_count = len(progress.failed_tasks)
        pending_count = progress.total_tasks - completed_count - failed_count
        
        completion_rate = (completed_count / progress.total_tasks * 100) if progress.total_tasks > 0 else 0
        
        return {
            'total_tasks': progress.total_tasks,
            'completed': completed_count,
            'failed': failed_count,
            'pending': pending_count,
            'completion_rate': f"{completion_rate:.2f}%",
            'total_rows_exported': progress.total_rows_exported,
            'start_time': progress.start_time,
            'last_update': progress.last_update
        }
    
    def reset_progress(self):
        """Reset progress by deleting progress file."""
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)
            logger.info(f"Progress file deleted: {self.progress_file}")

