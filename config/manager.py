"""Configuration manager for loading and validating configs."""

import json
import os
from typing import Dict, Any
from dataclasses import dataclass
from utils.logger import get_logger
from utils.validators import validate_config, validate_connection

logger = get_logger(__name__)


@dataclass
class ConnectionConfig:
    """TableStore connection configuration."""
    endpoint: str
    access_key_id: str
    access_key_secret: str
    instance_name: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConnectionConfig':
        """Create from dictionary."""
        return cls(
            endpoint=data['endpoint'],
            access_key_id=data['access_key_id'],
            access_key_secret=data['access_key_secret'],
            instance_name=data['instance_name']
        )


@dataclass
class ExportConfig:
    """Export configuration."""
    table: str
    schema: Dict[str, Any]
    filters: Dict[str, Any]
    append_columns: list
    tasks: Dict[str, Any]
    output: Dict[str, Any]
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExportConfig':
        """Create from dictionary."""
        return cls(
            table=data['table'],
            schema=data['schema'],
            filters=data['filters'],
            append_columns=data.get('append_columns', []),
            tasks=data['tasks'],
            output=data['output']
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'table': self.table,
            'schema': self.schema,
            'filters': self.filters,
            'append_columns': self.append_columns,
            'tasks': self.tasks,
            'output': self.output
        }


class ConfigManager:
    """Manage configuration loading and validation."""
    
    def __init__(self, config_path: str, connection_path: str = 'config/connection.json'):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to export configuration file
            connection_path: Path to connection configuration file
        """
        self.config_path = config_path
        self.connection_path = connection_path
        
        # Load configurations
        self.export_config = self.load_export_config()
        self.connection_config = self.load_connection_config()
        
        # Validate
        self.validate()
    
    def load_export_config(self) -> ExportConfig:
        """
        Load export configuration from file.
        
        Returns:
            ExportConfig instance
        """
        logger.info(f"Loading export config from: {self.config_path}")
        
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Export config file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return ExportConfig.from_dict(data)
    
    def load_connection_config(self) -> ConnectionConfig:
        """
        Load connection configuration from file.
        
        Returns:
            ConnectionConfig instance
        """
        logger.info(f"Loading connection config from: {self.connection_path}")
        
        if not os.path.exists(self.connection_path):
            raise FileNotFoundError(
                f"Connection config file not found: {self.connection_path}\n"
                f"Please create it based on config/connection.example.json"
            )
        
        with open(self.connection_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return ConnectionConfig.from_dict(data)
    
    def validate(self):
        """Validate all configurations."""
        # Validate export config
        is_valid, error = validate_config(self.export_config.to_dict())
        if not is_valid:
            raise ValueError(f"Invalid export configuration: {error}")
        
        # Validate connection config
        is_valid, error = validate_connection({
            'endpoint': self.connection_config.endpoint,
            'access_key_id': self.connection_config.access_key_id,
            'access_key_secret': self.connection_config.access_key_secret,
            'instance_name': self.connection_config.instance_name
        })
        if not is_valid:
            raise ValueError(f"Invalid connection configuration: {error}")
        
        logger.info("Configuration validation passed")
    
    def get_output_filename(self, partition_value: str, year: int) -> str:
        """
        Generate output filename based on pattern.
        
        Args:
            partition_value: Value of partition key
            year: Year for the data
            
        Returns:
            Filename string
        """
        pattern = self.export_config.output['filename_pattern']
        
        # Replace placeholders
        filename = pattern.format(
            partition_key=partition_value,
            table=self.export_config.table,
            year=year
        )
        
        return filename
    
    def get_output_directory(self) -> str:
        """
        Get output directory path.
        
        Returns:
            Output directory path
        """
        return self.export_config.output['directory']
    
    def get_table_name(self) -> str:
        """Get table name."""
        return self.export_config.table
    
    def get_schema(self) -> Dict[str, Any]:
        """Get table schema."""
        return self.export_config.schema
    
    def get_filters(self) -> Dict[str, Any]:
        """Get filters configuration."""
        return self.export_config.filters
    
    def get_append_columns(self) -> list:
        """Get global append columns."""
        return self.export_config.append_columns
    
    def get_tasks_config(self) -> Dict[str, Any]:
        """Get tasks configuration."""
        return self.export_config.tasks

