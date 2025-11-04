"""Filter engine for parsing and applying filters."""

from typing import Dict, Any, List, Optional, Tuple
from .operators import Filter, FilterOperator, TimeRangeFilter, parse_filter_value
from .time_chunker import TimeChunker, TimeChunk


class FilterEngine:
    """Engine for parsing and applying filters to TableStore queries."""
    
    def __init__(self, schema: Dict[str, Any]):
        """
        Initialize filter engine.
        
        Args:
            schema: Table schema configuration
        """
        self.schema = schema
        self.partition_key = schema['partition_key']
        self.sort_key = schema['sort_key']
        self.other_keys = schema.get('other_keys', [])
        
    def parse_filters(self, filters: Dict[str, Any]) -> Tuple[List[Filter], Optional[TimeRangeFilter]]:
        """
        Parse filter configuration.
        
        Args:
            filters: Filter configuration dictionary
            
        Returns:
            Tuple of (regular_filters, time_range_filter)
        """
        regular_filters = []
        time_range_filter = None
        
        for key, value in filters.items():
            if key == self.sort_key and isinstance(value, dict):
                # Check if this is a time range filter with chunk_by
                if 'gte' in value or 'lt' in value or 'chunk_by' in value:
                    time_range_filter = self._parse_time_range_filter(key, value)
                    continue
            
            # Parse as regular filter
            regular_filters.extend(self._parse_regular_filter(key, value))
        
        return regular_filters, time_range_filter
    
    def _parse_time_range_filter(self, key: str, value: Dict[str, Any]) -> TimeRangeFilter:
        """
        Parse time range filter with chunking support.
        
        Args:
            key: Filter key (typically sort_key)
            value: Filter value dict with gte, lt, chunk_by
            
        Returns:
            TimeRangeFilter instance
        """
        start = value.get('gte', 0)
        end = value.get('lt', float('inf'))
        chunk_by = value.get('chunk_by', 'year')
        
        return TimeRangeFilter(
            key=key,
            start=start,
            end=end,
            chunk_by=chunk_by
        )
    
    def _parse_regular_filter(self, key: str, value: Any) -> List[Filter]:
        """
        Parse regular filter (non-time-range).
        
        Args:
            key: Filter key
            value: Filter value (can be dict with multiple operators)
            
        Returns:
            List of Filter instances
        """
        filters = []
        
        # Direct value (shorthand for eq)
        if not isinstance(value, dict):
            filters.append(Filter(key, FilterOperator.EQ, value))
            return filters
        
        # Parse multiple operators in same filter
        for op_str, op_value in value.items():
            try:
                operator = FilterOperator(op_str)
                filters.append(Filter(key, operator, op_value))
            except ValueError:
                # Skip unknown operators
                continue
        
        return filters
    
    def get_time_chunks(self, time_filter: TimeRangeFilter) -> List[TimeChunk]:
        """
        Get time chunks from time range filter.
        
        Args:
            time_filter: TimeRangeFilter instance
            
        Returns:
            List of TimeChunk objects
        """
        return TimeChunker.chunk(
            time_filter.start,
            time_filter.end,
            time_filter.chunk_by
        )
    
    def separate_pk_and_attr_filters(
        self,
        filters: List[Filter]
    ) -> Tuple[List[Filter], List[Filter]]:
        """
        Separate filters into primary key filters and attribute filters.
        
        Args:
            filters: List of Filter instances
            
        Returns:
            Tuple of (pk_filters, attr_filters)
        """
        pk_keys = {self.partition_key, self.sort_key, *self.other_keys}
        
        pk_filters = []
        attr_filters = []
        
        for f in filters:
            if f.key in pk_keys:
                pk_filters.append(f)
            else:
                attr_filters.append(f)
        
        return pk_filters, attr_filters
    
    def apply_attribute_filters(self, row_data: Dict[str, Any], filters: List[Filter]) -> bool:
        """
        Apply attribute filters to a row.
        
        Args:
            row_data: Row data dictionary
            filters: List of attribute filters
            
        Returns:
            True if row passes all filters, False otherwise
        """
        for f in filters:
            value = row_data.get(f.key)
            if not f.evaluate(value):
                return False
        return True
    
    def build_pk_tuple_with_filters(
        self,
        partition_value: str,
        pk_filters: List[Filter],
        time_chunk: Optional[TimeChunk] = None,
        is_start: bool = True
    ) -> List[Tuple[str, Any]]:
        """
        Build primary key tuple for get_range() with filters applied.
        
        Args:
            partition_value: Value for partition key
            pk_filters: Primary key filters
            time_chunk: Time chunk for range (optional)
            is_start: Whether this is start key (True) or end key (False)
            
        Returns:
            List of (key, value) tuples for TableStore API
        """
        pk_tuple = []
        
        # Add partition key
        pk_tuple.append((self.partition_key, partition_value))
        
        # Add other primary keys with filters
        for key in self.other_keys:
            # Find filter for this key
            key_filter = next((f for f in pk_filters if f.key == key), None)
            if key_filter and key_filter.operator == FilterOperator.EQ:
                pk_tuple.append((key, key_filter.value))
        
        # Add sort key (time range)
        if time_chunk:
            time_value = time_chunk.start if is_start else time_chunk.end
            pk_tuple.append((self.sort_key, time_value))
        
        return pk_tuple

