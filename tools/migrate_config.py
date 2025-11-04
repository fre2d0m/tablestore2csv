#!/usr/bin/env python3
"""
Migrate old format configurations to new format.
"""

import argparse
import json
import sys


def migrate_mapping_to_tasks(mapping_file: str, output_file: str):
    """
    Convert old device_params.json format to new task format.
    
    Args:
        mapping_file: Old mapping file
        output_file: New task file
    """
    print(f"Loading old mapping from: {mapping_file}")
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        old_mapping = json.load(f)
    
    # Convert to new compact format (just arrays of columns)
    new_tasks = {}
    for device_id, columns in old_mapping.items():
        new_tasks[device_id] = columns
    
    # Write new format
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(new_tasks, f, ensure_ascii=False, indent=2)
    
    print(f"Converted {len(new_tasks)} tasks")
    print(f"Output written to: {output_file}")


def create_export_config(
    table_name: str,
    partition_key: str,
    sort_key: str,
    other_keys: list,
    filters: dict,
    append_columns: list,
    tasks_file: str,
    output_file: str
):
    """
    Create export config from parameters.
    
    Args:
        table_name: Table name
        partition_key: Partition key name
        sort_key: Sort key name
        other_keys: Other primary keys
        filters: Filter configuration
        append_columns: Global append columns
        tasks_file: Path to tasks file
        output_file: Output config file
    """
    config = {
        "table": table_name,
        "schema": {
            "partition_key": partition_key,
            "sort_key": sort_key,
            "other_keys": other_keys
        },
        "filters": filters,
        "append_columns": append_columns,
        "tasks": {
            "source": "file",
            "path": tasks_file
        },
        "output": {
            "format": "csv",
            "directory": "output_data",
            "filename_pattern": "{partition_key}_{table}_{year}.csv"
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    
    print(f"Export config created: {output_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Migrate old format to new format')
    
    subparsers = parser.add_subparsers(dest='command', help='Migration command')
    
    # Mapping migration
    mapping_parser = subparsers.add_parser('mapping', help='Convert old mapping file')
    mapping_parser.add_argument('--input', required=True, help='Old mapping file')
    mapping_parser.add_argument('--output', required=True, help='New task file')
    
    # Config creation
    config_parser = subparsers.add_parser('config', help='Create new export config')
    config_parser.add_argument('--table', required=True, help='Table name')
    config_parser.add_argument('--partition-key', required=True, help='Partition key name')
    config_parser.add_argument('--sort-key', required=True, help='Sort key name')
    config_parser.add_argument('--other-keys', default='', help='Comma-separated other keys')
    config_parser.add_argument('--filters', required=True, help='Filters JSON string')
    config_parser.add_argument('--append-columns', default='', help='Comma-separated append columns')
    config_parser.add_argument('--tasks-file', required=True, help='Path to tasks file')
    config_parser.add_argument('--output', required=True, help='Output config file')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'mapping':
            migrate_mapping_to_tasks(args.input, args.output)
        
        elif args.command == 'config':
            filters = json.loads(args.filters)
            other_keys = [k.strip() for k in args.other_keys.split(',') if k.strip()]
            append_columns = [c.strip() for c in args.append_columns.split(',') if c.strip()]
            
            create_export_config(
                args.table,
                args.partition_key,
                args.sort_key,
                other_keys,
                filters,
                append_columns,
                args.tasks_file,
                args.output
            )
        
        else:
            parser.print_help()
            return 1
        
        return 0
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

