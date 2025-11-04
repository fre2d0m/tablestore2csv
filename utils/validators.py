"""Configuration validation utilities."""

from typing import Any, Dict, List, Optional, Tuple
import os


def validate_config(config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate export configuration.
    
    Args:
        config: Export configuration dictionary
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Required fields
    required_fields = ['table', 'schema', 'filters', 'tasks', 'output']
    for field in required_fields:
        if field not in config:
            return False, f"Missing required field: {field}"
    
    # Validate schema
    schema = config['schema']
    required_schema_fields = ['partition_key', 'sort_key']
    for field in required_schema_fields:
        if field not in schema:
            return False, f"Missing required schema field: {field}"
    
    # Validate filters
    if not isinstance(config['filters'], dict):
        return False, "Filters must be a dictionary"
    
    # Validate tasks
    tasks = config['tasks']
    if 'source' not in tasks:
        return False, "Tasks must specify 'source'"
    
    if tasks['source'] in ['file', 'pattern']:
        if 'path' not in tasks:
            return False, f"Tasks with source '{tasks['source']}' must specify 'path'"
    elif tasks['source'] == 'inline':
        if 'definitions' not in tasks:
            return False, "Tasks with source 'inline' must specify 'definitions'"
    else:
        return False, f"Invalid tasks source: {tasks['source']}"
    
    # Validate output
    output = config['output']
    required_output_fields = ['format', 'directory', 'filename_pattern']
    for field in required_output_fields:
        if field not in output:
            return False, f"Missing required output field: {field}"
    
    if output['format'] != 'csv':
        return False, f"Unsupported output format: {output['format']}"
    
    return True, None


def validate_connection(connection: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate connection configuration.
    
    Args:
        connection: Connection configuration dictionary
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['endpoint', 'access_key_id', 'access_key_secret', 'instance_name']
    for field in required_fields:
        if field not in connection:
            return False, f"Missing required connection field: {field}"
        if not connection[field]:
            return False, f"Connection field '{field}' cannot be empty"
    
    return True, None


def validate_file_path(path: str, must_exist: bool = True) -> Tuple[bool, Optional[str]]:
    """
    Validate file path.
    
    Args:
        path: File path to validate
        must_exist: Whether file must exist
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not path:
        return False, "Path cannot be empty"
    
    if must_exist and not os.path.exists(path):
        return False, f"File does not exist: {path}"
    
    return True, None

