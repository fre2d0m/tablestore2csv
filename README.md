# TableStore Data Exporter

A general-purpose, declarative data exporter for Aliyun TableStore with flexible configuration, multi-threading support, and automatic resume functionality.

## Features

- **Declarative Configuration**: SQL-like filter syntax with JSON configuration
- **Flexible Task Definition**: Support for inline, file-based, and glob pattern task loading
- **Time-based Chunking**: Automatically split large time ranges into year/month/day chunks
- **Multi-threading**: Concurrent export with configurable worker threads
- **Resume Support**: Automatic checkpoint and resume functionality
- **Year-based Output**: One CSV file per partition key per year
- **Global Append Columns**: Define columns to append once globally, not per task

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Create connection configuration
cp config/connection.example.json config/connection.json
# Edit config/connection.json with your credentials
```

## Quick Start

### 1. Create Connection Configuration

```json
{
  "endpoint": "https://your-instance.cn-hangzhou.ots.aliyuncs.com",
  "access_key_id": "your-access-key-id",
  "access_key_secret": "your-access-key-secret",
  "instance_name": "your-instance-name"
}
```

Save as `config/connection.json` (gitignored).

### 2. Create Export Configuration

```json
{
  "table": "sensor_data",
  
  "schema": {
    "partition_key": "id",
    "sort_key": "ct",
    "other_keys": ["dim"]
  },
  
  "filters": {
    "dim": 0,
    "ct": {
      "gte": 1738473882740,
      "lt": 1783734384870,
      "chunk_by": "year"
    }
  },
  
  "append_columns": ["ct"],
  
  "tasks": {
    "source": "file",
    "path": "tasks/device_mappings.json"
  },
  
  "output": {
    "format": "csv",
    "directory": "output_data",
    "filename_pattern": "{partition_key}_{table}_{year}.csv"
  }
}
```

Save as `export_config.json`.

### 3. Create Task Definitions

**Compact format** (recommended for large task files):

```json
{
  "2309193932839": ["p34:1", "p50:1", "p51:1"],
  "2309193932840": ["p32:1", "p54:1", "p56:1"],
  "2309193932841": ["p20:1", "p20:2", "p21:1"]
}
```

**Standard format** (with per-task filters):

```json
{
  "2309193932839": {
    "columns": ["p34:1", "p50:1", "p51:1"],
    "filters": {
      "p34:1": {"gt": 0, "lt": 100}
    }
  }
}
```

Save as `tasks/device_mappings.json`.

### 4. Run Export

```bash
# Basic export
python main.py --config=export_config.json

# With more threads
python main.py --config=export_config.json --threads=8

# With resume support
python main.py --config=export_config.json --resume --threads=8

# Dry run to validate
python main.py --config=export_config.json --dry-run
```

## Configuration Reference

### Export Configuration

#### Schema

- `partition_key`: Primary key used to partition data (one CSV per unique value)
- `sort_key`: Time-based sort key for range queries
- `other_keys`: Additional primary keys (e.g., `dim`, `status`)

#### Filters

Unified filter system supporting multiple operators:

```json
{
  "filters": {
    "dim": 0,                    // Simple equality
    "status": {"in": [1, 2, 3]}, // Set membership
    "ct": {
      "gte": 1700000000000,      // Time range
      "lt": 1800000000000,
      "chunk_by": "year"         // Split into yearly chunks
    }
  }
}
```

**Supported operators**:
- `eq` / direct value: Equality
- `ne`: Not equal
- `gt`, `gte`, `lt`, `lte`: Comparisons
- `in`, `not_in`: Set operations
- `between`: Range (inclusive)

#### Append Columns

Global columns appended to all tasks:

```json
{
  "append_columns": ["ct", "dim"]
}
```

These columns appear **at the end** of each CSV row.

#### Tasks

Three source types supported:

**File source** (single file):
```json
{
  "tasks": {
    "source": "file",
    "path": "tasks/device_mappings.json"
  }
}
```

**Pattern source** (glob patterns):
```json
{
  "tasks": {
    "source": "pattern",
    "path": "tasks/batch_*.json"
  }
}
```

**Inline source** (small task sets):
```json
{
  "tasks": {
    "source": "inline",
    "definitions": {
      "device_001": ["temp", "humidity"],
      "device_002": ["pressure", "voltage"]
    }
  }
}
```

#### Output

```json
{
  "output": {
    "format": "csv",
    "directory": "output_data",
    "filename_pattern": "{partition_key}_{table}_{year}.csv"
  }
}
```

**Filename patterns**:
- `{partition_key}`: Value of partition key (e.g., device ID)
- `{table}`: Table name
- `{year}`: Year of the data

Example output: `2309193932839_sensor_data_2023.csv`

## CLI Reference

```bash
python main.py [OPTIONS]

Required:
  --config PATH          Export configuration file

Optional:
  --connection PATH      Connection config (default: config/connection.json)
  --threads N            Worker threads (default: 4)
  --resume               Resume from checkpoint
  --output-dir PATH      Override output directory
  --progress-file PATH   Progress checkpoint file
  --dry-run              Validate without executing
  --verbose              Enable debug logging
  --no-progress-bar      Disable progress bars
```

## Utility Tools

### Validate Configuration

```bash
python tools/validate_config.py --config=export_config.json --validate-tasks
```

### Split Large Task Files

```bash
python tools/split_tasks.py \
  --input=huge_mappings.json \
  --output-dir=tasks/ \
  --chunk-size=10000 \
  --prefix=batch_
```

### Migrate Old Format

Convert old `device_params.json` to new format:

```bash
# Convert mapping file
python tools/migrate_config.py mapping \
  --input=device_params.json \
  --output=tasks/device_mappings.json

# Create export config
python tools/migrate_config.py config \
  --table=sensor_data \
  --partition-key=id \
  --sort-key=ct \
  --other-keys=dim \
  --filters='{"dim": 0, "ct": {"gte": 1700000000000, "lt": 1800000000000, "chunk_by": "year"}}' \
  --append-columns=ct \
  --tasks-file=tasks/device_mappings.json \
  --output=export_config.json
```

## Output Format

### CSV Structure

For task `2309193932839` with:
- `append_columns`: `["ct"]`
- `columns`: `["p34:1", "p50:1", "p51:1"]`

Output CSV:
```csv
ct,p34:1,p50:1,p51:1
2023-01-15T10:30:00Z,23.5,45.2,67.8
2023-01-15T10:31:00Z,24.1,46.3,68.2
...
```

### File Organization

```
output_data/
├── 2309193932839_sensor_data_2023.csv
├── 2309193932839_sensor_data_2024.csv
├── 2309193932839_sensor_data_2025.csv
├── 2309193932840_sensor_data_2023.csv
└── ...
```

## Progress and Resume

Progress is automatically saved to `.export_progress.json`:

```json
{
  "config_hash": "sha256:abc123...",
  "completed_tasks": ["2309193932839", "2309193932840"],
  "failed_tasks": {},
  "total_tasks": 1000,
  "start_time": "2025-11-04 10:00:00",
  "last_update": "2025-11-04 12:30:00",
  "total_rows_exported": 1250000
}
```

To resume after interruption:

```bash
python main.py --config=export_config.json --resume
```

## Examples

### Example 1: Simple Sensor Data Export

```bash
# Create minimal config
cat > export_config.json << 'EOF'
{
  "table": "sensor_data",
  "schema": {
    "partition_key": "id",
    "sort_key": "ct",
    "other_keys": ["dim"]
  },
  "filters": {
    "dim": 0,
    "ct": {"gte": 1700000000000, "lt": 1800000000000, "chunk_by": "year"}
  },
  "append_columns": ["ct"],
  "tasks": {
    "source": "file",
    "path": "tasks/devices.json"
  },
  "output": {
    "format": "csv",
    "directory": "output_data",
    "filename_pattern": "{partition_key}_{table}_{year}.csv"
  }
}
EOF

# Run
python main.py --config=export_config.json --threads=4
```

### Example 2: Large-Scale Export (10,000+ devices)

```bash
# 1. Split task file
python tools/split_tasks.py \
  --input=all_devices.json \
  --output-dir=tasks_split/ \
  --chunk-size=5000

# 2. Update config to use pattern
# "tasks": {"source": "pattern", "path": "tasks_split/tasks_batch_*.json"}

# 3. Run with high concurrency
python main.py --config=export_config.json --threads=16 --resume
```

### Example 3: Multi-Region Export

```json
{
  "filters": {
    "region": {"in": ["us-west", "eu-central"]},
    "status": 1,
    "ct": {"gte": 1700000000000, "lt": 1800000000000, "chunk_by": "month"}
  }
}
```

## Performance Tips

1. **Thread Count**: Start with 4-8 threads. Monitor network/CPU usage and adjust.
2. **Chunk Size**: Use `year` for multi-year exports, `month` for shorter ranges.
3. **Batch Size**: Default 5000 rows per batch write. Adjust in code if needed.
4. **Resume**: Always use `--resume` for large exports to handle interruptions.

## Troubleshooting

### Configuration Validation Failed

```bash
# Validate configuration
python tools/validate_config.py --config=export_config.json --validate-tasks
```

### Connection Errors

1. Check `config/connection.json` credentials
2. Verify endpoint URL format
3. Ensure network access to TableStore

### Progress File Conflicts

If configuration changes:

```bash
# Remove old progress file
rm .export_progress.json

# Start fresh
python main.py --config=export_config.json
```

### Empty Output Files

- Check filter conditions (may be too restrictive)
- Verify partition key values exist in table
- Use `--verbose` for detailed logging

## Architecture

```
tablestore2csv/
├── config/           # Configuration management
├── exporter/         # Core export engine
├── filters/          # Filter parsing and time chunking
├── tasks/            # Task loading
├── progress/         # Progress tracking
├── utils/            # Utilities
└── tools/            # Helper scripts
```

## License

This project is provided as-is for use with Aliyun TableStore.

## Support

For issues or questions, refer to the examples in `examples/` directory or check the plan documentation in the project root.
