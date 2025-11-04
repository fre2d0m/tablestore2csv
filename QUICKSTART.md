# Quick Start Guide

## 1. Install Dependencies

```bash
pip install -r requirements.txt
```

## 2. Create Connection Config

```bash
cp config/connection.example.json config/connection.json
```

Edit `config/connection.json` with your TableStore credentials:

```json
{
  "endpoint": "https://your-instance.cn-hangzhou.ots.aliyuncs.com",
  "access_key_id": "your-access-key-id",
  "access_key_secret": "your-access-key-secret",
  "instance_name": "your-instance-name"
}
```

## 3. Create Export Configuration

Create `my_export.json`:

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
    "path": "my_tasks.json"
  },
  
  "output": {
    "format": "csv",
    "directory": "output_data",
    "filename_pattern": "{partition_key}_{table}_{year}.csv"
  }
}
```

## 4. Create Task Definitions

Create `my_tasks.json`:

```json
{
  "2309193932839": ["p34:1", "p50:1", "p51:1"],
  "2309193932840": ["p32:1", "p54:1", "p56:1"]
}
```

## 5. Validate Configuration

```bash
python main.py --config=my_export.json --dry-run
```

## 6. Run Export

```bash
# Basic export (4 threads)
python main.py --config=my_export.json

# With more threads
python main.py --config=my_export.json --threads=8

# With resume support
python main.py --config=my_export.json --resume --threads=8
```

## 7. Check Output

```bash
ls -lh output_data/
```

Output files:
- `2309193932839_sensor_data_2023.csv`
- `2309193932839_sensor_data_2024.csv`
- `2309193932839_sensor_data_2025.csv`
- etc.

## Troubleshooting

### Validate your configuration
```bash
python tools/validate_config.py --config=my_export.json --validate-tasks
```

### Run validation tests
```bash
python test_validation.py
```

### Enable verbose logging
```bash
python main.py --config=my_export.json --verbose
```

## Next Steps

- Read [README.md](README.md) for detailed documentation
- Check [examples/](examples/) for more configuration examples
- Use tools in [tools/](tools/) for advanced scenarios

## Common Patterns

### Large Task Files

If you have 10,000+ tasks, split them:

```bash
python tools/split_tasks.py \
  --input=huge_tasks.json \
  --output-dir=tasks_split/ \
  --chunk-size=5000
```

Update config:
```json
{
  "tasks": {
    "source": "pattern",
    "path": "tasks_split/tasks_batch_*.json"
  }
}
```

### Time Range Adjustments

- **Year chunks**: Good for multi-year exports
  ```json
  "ct": {"gte": start, "lt": end, "chunk_by": "year"}
  ```

- **Month chunks**: Good for shorter ranges or very large datasets
  ```json
  "ct": {"gte": start, "lt": end, "chunk_by": "month"}
  ```

### Resume After Interruption

Simply rerun with `--resume`:
```bash
python main.py --config=my_export.json --resume
```

Progress is saved in `.export_progress.json` automatically.

