"""Query builder for constructing TableStore API calls."""

from typing import Dict, Any, List, Tuple, Optional
from filters.time_chunker import TimeChunk
from filters.operators import Filter
from utils.logger import get_logger
from utils.formatters import format_timestamp

logger = get_logger(__name__)


class QueryBuilder:
    """Build TableStore query parameters."""
    
    def __init__(self, schema: Dict[str, Any], append_columns: List[str]):
        """
        Initialize query builder.
        
        Args:
            schema: Table schema configuration
            append_columns: Global append columns list
        """
        self.schema = schema
        self.partition_key = schema['partition_key']
        self.sort_key = schema['sort_key']
        self.other_keys = schema.get('other_keys', [])
        self.append_columns = append_columns
    
    def build_primary_key_range(
        self,
        partition_value: str,
        pk_filters: List[Filter],
        time_chunk: TimeChunk
    ) -> Tuple[List[Tuple[str, Any]], List[Tuple[str, Any]]]:
        """
        Build inclusive_start and exclusive_end primary keys for get_range().
        
        Args:
            partition_value: Value for partition key
            pk_filters: Primary key filters to apply
            time_chunk: Time chunk for range query
            
        Returns:
            Tuple of (inclusive_start, exclusive_end)
        """
        # Build start key
        inclusive_start = self._build_pk_tuple(
            partition_value,
            pk_filters,
            time_chunk.start
        )
        
        # Build end key
        exclusive_end = self._build_pk_tuple(
            partition_value,
            pk_filters,
            time_chunk.end
        )
        
        return inclusive_start, exclusive_end
    
    def _build_pk_tuple(
        self,
        partition_value: str,
        pk_filters: List[Filter],
        time_value: int
    ) -> List[Tuple[str, Any]]:
        """
        Build primary key tuple.
        
        Args:
            partition_value: Partition key value
            pk_filters: Primary key filters
            time_value: Sort key (time) value
            
        Returns:
            List of (key, value) tuples
        """
        pk_tuple = []
        
        # Add partition key
        pk_tuple.append((self.partition_key, partition_value))
        
        # Add other primary keys with filter values
        for key in self.other_keys:
            # Find EQ filter for this key
            key_filter = next((f for f in pk_filters if f.key == key and f.operator.value == 'eq'), None)
            if key_filter:
                pk_tuple.append((key, key_filter.value))
        
        # Add sort key (time)
        pk_tuple.append((self.sort_key, time_value))
        
        return pk_tuple
    
    def extract_row_data(
        self,
        row: Any,
        task_columns: List[str]
    ) -> List[Any]:
        """
        Extract row data with append columns and task columns.
        
        Args:
            row: TableStore row object
            task_columns: Column names from task definition
            
        Returns:
            List of values in order: [append_columns] + [task_columns]
        """
        # Parse primary keys
        primary_key_dict = {}
        for pk_item in row.primary_key:
            primary_key_dict[pk_item[0]] = pk_item[1]
        
        # Parse attribute columns
        columns_dict = {}
        for attr_item in row.attribute_columns:
            col_name = attr_item[0]
            col_value = attr_item[1]
            columns_dict[col_name] = col_value
        
        # Combine for lookup
        all_data = {**primary_key_dict, **columns_dict}
        
        # Build row data
        row_data = []
        
        # Add append columns (global, specified in export config)
        for col in self.append_columns:
            value = all_data.get(col, '-')
            # Format timestamps in ISO 8601
            if col == self.sort_key and isinstance(value, (int, float)):
                value = format_timestamp(value)
            row_data.append(value)
        
        # Add task-specific columns
        for col in task_columns:
            value = columns_dict.get(col, '-')
            row_data.append(value)
        
        return row_data
    
    def build_csv_headers(self, task_columns: List[str]) -> List[str]:
        """
        Build CSV headers.
        
        Args:
            task_columns: Column names from task definition
            
        Returns:
            List of header names: [append_columns] + [task_columns]
        """
        return self.append_columns + task_columns
    
    def validate_query_params(
        self,
        partition_value: str,
        time_chunk: TimeChunk
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate query parameters.
        
        Args:
            partition_value: Partition key value
            time_chunk: Time chunk
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not partition_value:
            return False, "Partition value cannot be empty"
        
        if time_chunk.start >= time_chunk.end:
            return False, f"Invalid time range: {time_chunk.start} >= {time_chunk.end}"
        
        return True, None

