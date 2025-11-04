"""Time range chunking for efficient querying."""

from datetime import datetime, timezone
from typing import List
from dataclasses import dataclass


@dataclass
class TimeChunk:
    """Represents a time chunk for querying."""
    start: int  # Milliseconds
    end: int    # Milliseconds
    year: int
    label: str  # Human-readable label


class TimeChunker:
    """Split time ranges into chunks for efficient querying."""
    
    @staticmethod
    def chunk_by_year(start_ms: int, end_ms: int) -> List[TimeChunk]:
        """
        Split time range into year chunks.
        
        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
            
        Returns:
            List of TimeChunk objects
        """
        chunks = []
        
        # Convert to datetime
        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
        
        # Start from the beginning of start year
        current_year = start_dt.year
        end_year = end_dt.year
        
        while current_year <= end_year:
            # Chunk boundaries
            year_start = datetime(current_year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            year_end = datetime(current_year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            
            # Adjust for actual range
            chunk_start = max(start_dt, year_start)
            chunk_end = min(end_dt, year_end)
            
            # Convert back to milliseconds
            chunk_start_ms = int(chunk_start.timestamp() * 1000)
            chunk_end_ms = int(chunk_end.timestamp() * 1000)
            
            chunks.append(TimeChunk(
                start=chunk_start_ms,
                end=chunk_end_ms,
                year=current_year,
                label=str(current_year)
            ))
            
            current_year += 1
        
        return chunks
    
    @staticmethod
    def chunk_by_month(start_ms: int, end_ms: int) -> List[TimeChunk]:
        """
        Split time range into month chunks.
        
        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
            
        Returns:
            List of TimeChunk objects
        """
        chunks = []
        
        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
        
        current_year = start_dt.year
        current_month = start_dt.month
        
        while True:
            # Month boundaries
            month_start = datetime(current_year, current_month, 1, 0, 0, 0, tzinfo=timezone.utc)
            
            # Next month
            next_month = current_month + 1
            next_year = current_year
            if next_month > 12:
                next_month = 1
                next_year += 1
            month_end = datetime(next_year, next_month, 1, 0, 0, 0, tzinfo=timezone.utc)
            
            # Adjust for actual range
            chunk_start = max(start_dt, month_start)
            chunk_end = min(end_dt, month_end)
            
            # Convert to milliseconds
            chunk_start_ms = int(chunk_start.timestamp() * 1000)
            chunk_end_ms = int(chunk_end.timestamp() * 1000)
            
            chunks.append(TimeChunk(
                start=chunk_start_ms,
                end=chunk_end_ms,
                year=current_year,
                label=f"{current_year}-{current_month:02d}"
            ))
            
            # Check if we've reached the end
            if month_end >= end_dt:
                break
            
            current_month = next_month
            current_year = next_year
        
        return chunks
    
    @staticmethod
    def chunk_by_day(start_ms: int, end_ms: int) -> List[TimeChunk]:
        """
        Split time range into day chunks.
        
        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
            
        Returns:
            List of TimeChunk objects
        """
        chunks = []
        
        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
        
        current_dt = datetime(start_dt.year, start_dt.month, start_dt.day, 0, 0, 0, tzinfo=timezone.utc)
        
        while current_dt < end_dt:
            # Day boundaries
            day_start = current_dt
            day_end = datetime(
                current_dt.year, current_dt.month, current_dt.day, 23, 59, 59, 999999,
                tzinfo=timezone.utc
            )
            
            # Adjust for actual range
            chunk_start = max(start_dt, day_start)
            chunk_end = min(end_dt, day_end)
            
            # Convert to milliseconds
            chunk_start_ms = int(chunk_start.timestamp() * 1000)
            chunk_end_ms = int(chunk_end.timestamp() * 1000)
            
            chunks.append(TimeChunk(
                start=chunk_start_ms,
                end=chunk_end_ms,
                year=current_dt.year,
                label=current_dt.strftime('%Y-%m-%d')
            ))
            
            # Next day
            from datetime import timedelta
            current_dt += timedelta(days=1)
        
        return chunks
    
    @classmethod
    def chunk(cls, start_ms: int, end_ms: int, chunk_by: str = 'year') -> List[TimeChunk]:
        """
        Split time range into chunks.
        
        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
            chunk_by: Chunk granularity ('year', 'month', 'day')
            
        Returns:
            List of TimeChunk objects
        """
        if chunk_by == 'year':
            return cls.chunk_by_year(start_ms, end_ms)
        elif chunk_by == 'month':
            return cls.chunk_by_month(start_ms, end_ms)
        elif chunk_by == 'day':
            return cls.chunk_by_day(start_ms, end_ms)
        else:
            raise ValueError(f"Unsupported chunk_by value: {chunk_by}")

