"""TableStore export engine."""

# Lazy imports to avoid requiring tablestore at import time
__all__ = ['TableStoreExporter', 'QueryBuilder']

def __getattr__(name):
    """Lazy import of modules."""
    if name == 'TableStoreExporter':
        from .core import TableStoreExporter
        return TableStoreExporter
    elif name == 'QueryBuilder':
        from .query_builder import QueryBuilder
        return QueryBuilder
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

