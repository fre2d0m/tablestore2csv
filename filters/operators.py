"""Filter operator definitions and implementations."""

from enum import Enum
from typing import Any, Union, List, Tuple
from dataclasses import dataclass


class FilterOperator(Enum):
    """Supported filter operators."""
    EQ = 'eq'              # Equal
    NE = 'ne'              # Not equal
    GT = 'gt'              # Greater than
    GTE = 'gte'            # Greater than or equal
    LT = 'lt'              # Less than
    LTE = 'lte'            # Less than or equal
    IN = 'in'              # In list
    NOT_IN = 'not_in'      # Not in list
    BETWEEN = 'between'    # Between range (inclusive)


@dataclass
class Filter:
    """Represents a single filter condition."""
    key: str
    operator: FilterOperator
    value: Any
    
    def evaluate(self, actual_value: Any) -> bool:
        """
        Evaluate filter against actual value.
        
        Args:
            actual_value: The value to test
            
        Returns:
            True if filter passes, False otherwise
        """
        if actual_value is None:
            return False
        
        if self.operator == FilterOperator.EQ:
            return actual_value == self.value
        elif self.operator == FilterOperator.NE:
            return actual_value != self.value
        elif self.operator == FilterOperator.GT:
            return actual_value > self.value
        elif self.operator == FilterOperator.GTE:
            return actual_value >= self.value
        elif self.operator == FilterOperator.LT:
            return actual_value < self.value
        elif self.operator == FilterOperator.LTE:
            return actual_value <= self.value
        elif self.operator == FilterOperator.IN:
            return actual_value in self.value
        elif self.operator == FilterOperator.NOT_IN:
            return actual_value not in self.value
        elif self.operator == FilterOperator.BETWEEN:
            return self.value[0] <= actual_value <= self.value[1]
        
        return False


@dataclass
class TimeRangeFilter:
    """Represents a time range filter with chunking support."""
    key: str
    start: int  # Milliseconds
    end: int    # Milliseconds
    chunk_by: str = 'year'  # year, month, day
    
    def __post_init__(self):
        """Validate time range."""
        if self.start >= self.end:
            raise ValueError(f"Invalid time range: start ({self.start}) >= end ({self.end})")


def parse_filter_value(value: Any) -> Tuple[FilterOperator, Any]:
    """
    Parse filter value to determine operator and value.
    
    Args:
        value: Filter value (can be dict with operators or direct value)
        
    Returns:
        Tuple of (operator, value)
        
    Examples:
        >>> parse_filter_value(0)
        (FilterOperator.EQ, 0)
        >>> parse_filter_value({"gt": 0, "lt": 100})
        (FilterOperator.GT, 0)  # Returns first operator found
        >>> parse_filter_value({"in": [1, 2, 3]})
        (FilterOperator.IN, [1, 2, 3])
    """
    # Direct value - treat as equality
    if not isinstance(value, dict):
        return FilterOperator.EQ, value
    
    # Parse operator dict
    for op_str, op_value in value.items():
        try:
            operator = FilterOperator(op_str)
            return operator, op_value
        except ValueError:
            continue
    
    # If no valid operator found, treat as equality
    return FilterOperator.EQ, value

