"""Task loader for loading export task definitions."""

import json
import glob
import os
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TaskDefinition:
    """Represents an export task definition."""
    task_id: str
    columns: List[str]
    filters: Dict[str, Any] = None
    
    def __post_init__(self):
        """Validate task definition."""
        if not self.task_id:
            raise ValueError("Task ID cannot be empty")
        # Allow empty columns - will be filtered out during loading
        if self.filters is None:
            self.filters = {}


class TaskLoader:
    """Load and parse export task definitions from various sources."""
    
    def load(self, tasks_config: Dict[str, Any]) -> Dict[str, TaskDefinition]:
        """
        Load tasks based on configuration.
        
        Args:
            tasks_config: Tasks configuration dict with 'source' and related fields
            
        Returns:
            Dictionary mapping task_id to TaskDefinition
        """
        source = tasks_config.get('source', 'file')
        
        if source == 'file':
            return self.load_from_file(tasks_config['path'])
        elif source == 'pattern':
            return self.load_from_glob(tasks_config['path'])
        elif source == 'inline':
            return self.load_inline(tasks_config['definitions'])
        else:
            raise ValueError(f"Unsupported task source: {source}")
    
    def load_from_file(self, path: str) -> Dict[str, TaskDefinition]:
        """
        Load tasks from a single JSON file.
        
        Args:
            path: Path to JSON file
            
        Returns:
            Dictionary of task definitions
        """
        logger.info(f"Loading tasks from file: {path}")
        
        if not os.path.exists(path):
            raise FileNotFoundError(f"Task file not found: {path}")
        
        # Check file size
        file_size = os.path.getsize(path)
        logger.info(f"Task file size: {file_size / 1024 / 1024:.2f} MB")
        
        if file_size > 10 * 1024 * 1024:  # > 10MB
            logger.warning("Large task file detected, this may take a while to load...")
        
        logger.info("Reading JSON file...")
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Loaded {len(data)} task definitions from file")
        logger.info("Parsing and validating tasks...")
        
        return self._parse_tasks(data)
    
    def load_from_glob(self, pattern: str) -> Dict[str, TaskDefinition]:
        """
        Load tasks from multiple files matching a glob pattern.
        
        Args:
            pattern: Glob pattern (e.g., 'tasks/*.json')
            
        Returns:
            Dictionary of task definitions (merged from all files)
        """
        logger.info(f"Loading tasks from pattern: {pattern}")
        
        files = glob.glob(pattern)
        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {pattern}")
        
        logger.info(f"Found {len(files)} task files")
        
        all_tasks = {}
        for file_path in sorted(files):
            logger.debug(f"Loading task file: {file_path}")
            file_tasks = self.load_from_file(file_path)
            
            # Check for duplicate task IDs
            duplicates = set(all_tasks.keys()) & set(file_tasks.keys())
            if duplicates:
                logger.warning(f"Duplicate task IDs found in {file_path}: {duplicates}")
            
            all_tasks.update(file_tasks)
        
        logger.info(f"Loaded {len(all_tasks)} total tasks from {len(files)} files")
        return all_tasks
    
    def load_inline(self, definitions: Dict[str, Any]) -> Dict[str, TaskDefinition]:
        """
        Load tasks from inline definitions in config.
        
        Args:
            definitions: Inline task definitions
            
        Returns:
            Dictionary of task definitions
        """
        logger.info(f"Loading {len(definitions)} inline tasks")
        return self._parse_tasks(definitions)
    
    def _parse_tasks(self, data: Dict[str, Any]) -> Dict[str, TaskDefinition]:
        """
        Parse task definitions from dictionary.
        
        Args:
            data: Task data dictionary
            
        Returns:
            Dictionary of TaskDefinition objects
        """
        tasks = {}
        skipped_count = 0
        total_count = len(data)
        
        # Show progress for large task sets
        show_progress = total_count > 1000
        progress_interval = max(100, total_count // 100)  # Show progress every 1% or 100 tasks
        
        for idx, (task_id, task_data) in enumerate(data.items(), 1):
            try:
                task_def = self._parse_single_task(task_id, task_data)
                
                # Skip tasks with no columns
                if not task_def.columns or len(task_def.columns) == 0:
                    if skipped_count < 10:  # Only log first 10
                        logger.debug(f"Skipping task {task_id}: no columns defined")
                    skipped_count += 1
                    continue
                
                tasks[task_id] = task_def
                
                # Show progress
                if show_progress and idx % progress_interval == 0:
                    logger.info(f"Parsing progress: {idx}/{total_count} ({idx*100//total_count}%)")
                
            except Exception as e:
                logger.error(f"Error parsing task {task_id}: {e}")
                raise
        
        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} tasks with no columns defined")
        
        logger.info(f"Successfully loaded {len(tasks)} valid tasks (skipped {skipped_count})")
        
        return tasks
    
    def _parse_single_task(self, task_id: str, task_data: Any) -> TaskDefinition:
        """
        Parse a single task definition.
        
        Supports multiple formats:
        1. List of columns: ["col1", "col2", "col3"]
        2. Dict with columns: {"columns": ["col1", "col2"], "filters": {...}}
        
        Args:
            task_id: Task identifier
            task_data: Task data (list or dict)
            
        Returns:
            TaskDefinition instance
        """
        # Compact format: just a list of columns
        if isinstance(task_data, list):
            return TaskDefinition(
                task_id=task_id,
                columns=task_data,
                filters={}
            )
        
        # Standard format: dict with columns and optional filters
        if isinstance(task_data, dict):
            columns = task_data.get('columns', [])
            filters = task_data.get('filters', {})
            
            return TaskDefinition(
                task_id=task_id,
                columns=columns,
                filters=filters
            )
        
        raise ValueError(f"Invalid task data format for {task_id}: {type(task_data)}")
    
    def validate_tasks(self, tasks: Dict[str, TaskDefinition]) -> Tuple[bool, List[str]]:
        """
        Validate task definitions.
        
        Args:
            tasks: Dictionary of task definitions
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        if not tasks:
            errors.append("No tasks defined")
            return False, errors
        
        for task_id, task_def in tasks.items():
            if not task_def.columns:
                errors.append(f"Task {task_id} has no columns defined")
            
            # Check for empty column names
            empty_cols = [c for c in task_def.columns if not c or not c.strip()]
            if empty_cols:
                errors.append(f"Task {task_id} has empty column names")
        
        return len(errors) == 0, errors

