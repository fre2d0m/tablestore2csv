#!/usr/bin/env python3
"""
Split large task files into smaller chunks.
"""

import argparse
import json
import os
import sys


def split_tasks(input_file: str, output_dir: str, chunk_size: int, prefix: str):
    """
    Split task file into smaller chunks.
    
    Args:
        input_file: Input JSON file
        output_dir: Output directory
        chunk_size: Number of tasks per chunk
        prefix: Output filename prefix
    """
    print(f"Loading tasks from: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        tasks = json.load(f)
    
    total_tasks = len(tasks)
    print(f"Total tasks: {total_tasks}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Split into chunks
    task_items = list(tasks.items())
    chunk_num = 1
    
    for i in range(0, len(task_items), chunk_size):
        chunk = dict(task_items[i:i + chunk_size])
        
        # Output filename
        output_file = os.path.join(output_dir, f"{prefix}{chunk_num}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(chunk, f, ensure_ascii=False, indent=2)
        
        print(f"Created: {output_file} ({len(chunk)} tasks)")
        chunk_num += 1
    
    print(f"\nSplit {total_tasks} tasks into {chunk_num - 1} files")
    print(f"Output directory: {output_dir}")
    print(f"\nUpdate your export config with:")
    print(f'  "tasks": {{"source": "pattern", "path": "{output_dir}/{prefix}*.json"}}')


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Split large task files')
    parser.add_argument('--input', required=True, help='Input task file')
    parser.add_argument('--output-dir', required=True, help='Output directory')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Tasks per file (default: 10000)')
    parser.add_argument('--prefix', default='tasks_batch_', help='Output filename prefix')
    
    args = parser.parse_args()
    
    try:
        split_tasks(args.input, args.output_dir, args.chunk_size, args.prefix)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

