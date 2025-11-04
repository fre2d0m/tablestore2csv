#!/usr/bin/env python3
"""
TableStore Data Exporter - Main CLI Entry Point

A general-purpose exporter for Aliyun TableStore with declarative configuration.
"""

import argparse
import sys
from typing import Optional

from config.manager import ConfigManager
from exporter.core import TableStoreExporter
from utils.logger import setup_logger, get_logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Export data from Aliyun TableStore to CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic export
  python main.py --config=export_config.json
  
  # With custom threads and resume
  python main.py --config=export_config.json --threads=8 --resume
  
  # Override connection config
  python main.py --config=export_config.json --connection=prod.json
  
  # Dry run to validate configuration
  python main.py --config=export_config.json --dry-run
  
  # Verbose logging
  python main.py --config=export_config.json --verbose

For more information, see README.md
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--config',
        required=True,
        help='Path to export configuration file (required)'
    )
    
    # Optional arguments
    parser.add_argument(
        '--connection',
        default='config/connection.json',
        help='Path to connection configuration file (default: config/connection.json)'
    )
    
    parser.add_argument(
        '--threads',
        type=int,
        default=4,
        help='Number of concurrent worker threads (default: 4)'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from checkpoint if available'
    )
    
    parser.add_argument(
        '--output-dir',
        help='Override output directory from config'
    )
    
    parser.add_argument(
        '--progress-file',
        default='.export_progress.json',
        help='Path to progress checkpoint file (default: .export_progress.json)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration without executing export'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose (debug) logging'
    )
    
    parser.add_argument(
        '--no-progress-bar',
        action='store_true',
        help='Disable progress bars'
    )
    
    return parser.parse_args()


def validate_config(config_manager: ConfigManager) -> bool:
    """
    Validate configuration.
    
    Args:
        config_manager: Configuration manager
        
    Returns:
        True if valid, False otherwise
    """
    logger = get_logger()
    
    try:
        # Validation happens in ConfigManager.__init__
        logger.info("✓ Export configuration is valid")
        logger.info("✓ Connection configuration is valid")
        
        # Print config summary
        logger.info("=" * 80)
        logger.info("Configuration Summary")
        logger.info("=" * 80)
        logger.info(f"Table: {config_manager.get_table_name()}")
        logger.info(f"Partition key: {config_manager.get_schema()['partition_key']}")
        logger.info(f"Sort key: {config_manager.get_schema()['sort_key']}")
        logger.info(f"Append columns: {config_manager.get_append_columns()}")
        logger.info(f"Output directory: {config_manager.get_output_directory()}")
        logger.info("=" * 80)
        
        return True
    
    except Exception as e:
        logger.error(f"✗ Configuration validation failed: {e}")
        return False


def main() -> int:
    """Main entry point."""
    # Parse arguments
    args = parse_args()
    
    # Setup logging
    logger = setup_logger(verbose=args.verbose)
    
    logger.info("=" * 80)
    logger.info("TableStore Data Exporter")
    logger.info("=" * 80)
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from: {args.config}")
        config_manager = ConfigManager(
            config_path=args.config,
            connection_path=args.connection
        )
        
        # Override output directory if specified
        if args.output_dir:
            config_manager.export_config.output['directory'] = args.output_dir
            logger.info(f"Output directory overridden: {args.output_dir}")
        
        # Validate configuration
        if not validate_config(config_manager):
            return 1
        
        # Dry run mode
        if args.dry_run:
            logger.info("=" * 80)
            logger.info("Dry run completed successfully!")
            logger.info("Configuration is valid and ready for export.")
            logger.info("=" * 80)
            return 0
        
        # Create exporter
        logger.info("Initializing exporter...")
        exporter = TableStoreExporter(
            config_manager=config_manager,
            max_workers=args.threads
        )
        
        # Run export
        logger.info("Starting export...")
        logger.info("=" * 80)
        
        summary = exporter.export_all_tasks(resume=args.resume)
        
        # Print final summary
        logger.info("=" * 80)
        logger.info("Export completed successfully!")
        logger.info(f"Total tasks: {summary['total_tasks']}")
        logger.info(f"Completed: {summary['completed']}")
        logger.info(f"Failed: {summary['failed']}")
        logger.info(f"Total rows exported: {summary['total_rows_exported']:,}")
        logger.info(f"Duration: {summary['duration']:.2f}s")
        logger.info("=" * 80)
        
        # Return non-zero if any failures
        return 0 if summary['failed'] == 0 else 1
    
    except KeyboardInterrupt:
        logger.warning("\n\nExport interrupted by user!")
        logger.info("Progress has been saved. Use --resume to continue.")
        return 130  # Standard exit code for Ctrl+C
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

