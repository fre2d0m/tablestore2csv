#!/usr/bin/env python3
"""
Validate export configuration without running export.
"""

import argparse
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.manager import ConfigManager
from tasks.loader import TaskLoader
from utils.logger import setup_logger, get_logger


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Validate export configuration')
    parser.add_argument('--config', required=True, help='Path to export config')
    parser.add_argument('--connection', default='config/connection.json', help='Path to connection config')
    parser.add_argument('--validate-tasks', action='store_true', help='Also validate task files')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    logger = setup_logger(verbose=args.verbose)
    logger.info("Validating configuration...")
    
    try:
        # Load config
        config_manager = ConfigManager(
            config_path=args.config,
            connection_path=args.connection
        )
        
        logger.info("✓ Configuration is valid")
        
        # Validate tasks if requested
        if args.validate_tasks:
            logger.info("Validating task files...")
            task_loader = TaskLoader()
            tasks = task_loader.load(config_manager.get_tasks_config())
            
            is_valid, errors = task_loader.validate_tasks(tasks)
            
            if is_valid:
                logger.info(f"✓ All {len(tasks)} tasks are valid")
            else:
                logger.error("✗ Task validation failed:")
                for error in errors:
                    logger.error(f"  - {error}")
                return 1
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Configuration Summary")
        logger.info("=" * 60)
        logger.info(f"Table: {config_manager.get_table_name()}")
        logger.info(f"Schema: {config_manager.get_schema()}")
        logger.info(f"Filters: {config_manager.get_filters()}")
        logger.info(f"Append columns: {config_manager.get_append_columns()}")
        logger.info(f"Output: {config_manager.get_output_directory()}")
        logger.info("=" * 60)
        logger.info("✓ All validation checks passed!")
        
        return 0
    
    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

