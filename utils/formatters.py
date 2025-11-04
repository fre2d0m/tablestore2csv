"""Data formatting utilities."""

from datetime import datetime, timezone
from typing import Union


def format_timestamp(timestamp_ms: Union[int, float], format_type: str = 'iso8601') -> str:
    """
    Format timestamp in milliseconds to various formats.
    
    Args:
        timestamp_ms: Timestamp in milliseconds
        format_type: Output format ('iso8601', 'datetime', 'date')
        
    Returns:
        Formatted timestamp string
    """
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    
    if format_type == 'iso8601':
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    elif format_type == 'datetime':
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    elif format_type == 'date':
        return dt.strftime('%Y-%m-%d')
    else:
        return str(timestamp_ms)


def parse_timestamp(timestamp_str: str) -> int:
    """
    Parse ISO 8601 timestamp string to milliseconds.
    
    Args:
        timestamp_str: ISO 8601 formatted timestamp
        
    Returns:
        Timestamp in milliseconds
    """
    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    return int(dt.timestamp() * 1000)


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string (e.g., '1.5 MB')
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string (e.g., '2h 15m 30s')
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return ' '.join(parts)

