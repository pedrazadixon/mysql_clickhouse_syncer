# MySQL to ClickHouse Sync Tool

A robust Python-based data synchronization tool that continuously syncs data from MySQL to ClickHouse databases. Supports both incremental and full sync strategies with configurable mappings and type conversions.

## Features

- Incremental and full table synchronization
- Configurable column mappings and data type conversions
- Batch processing for efficient data transfer
- Automatic table creation in ClickHouse
- State persistence for reliable resumption
- Configurable sync intervals per table
- Partition support for ClickHouse tables
- Comprehensive logging

## Prerequisites

- Python 3.8+
- MySQL Server
- ClickHouse Server
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/pedrazadixon/mysql_clickhouse_syncer.git
cd mysql_clickhouse_syncer
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your settings:
```bash
cp config_sample.py config.py
```

## Configuration

Edit `config.py` to set up your database connections and table mappings:

### Database Configuration
```python
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'your_mysql_user',
    'password': 'your_mysql_password',
    'database': 'your_database',
}

CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'username': 'default',
    'password': 'your_clickhouse_password',
}
```

### Table Mappings
```python
TABLE_MAPPINGS = {
    'mysql_table_name': {
        'clickhouse_table': 'clickhouse_table_name',
        'columns': ['column1', 'column2', 'column3'],
        'id_column': 'id',
        'type_mapping': {
            'id': 'Int32',
            'created_at': 'DateTime',
        },
        'partition_by': 'toMonday(created_at)',  # Optional
        'sync_interval': 3600,  # in seconds
        'sync_type': 'incremental'  # or 'full'
    }
}
```

## Usage

Run the sync service:
```bash
python mysql_to_clickhouse_sync.py
```

The service will:
1. Create necessary tables in ClickHouse if they don't exist
2. Start monitoring configured tables
3. Sync data based on configured intervals
4. Maintain sync state for each table

## Sync Strategies

### Incremental Sync
- Tracks the last synced ID
- Only syncs new records
- Efficient for tables with sequential IDs
- Default strategy

### Full Sync
- Syncs entire table each time
- Useful for tables that need complete refreshes
- More resource-intensive
- Configure by setting `sync_type: 'full'`

## Monitoring

The tool provides detailed logging of sync operations:
- Sync progress
- Error messages
- Table creation events
- Batch processing status

Logs are written to stdout with timestamp and log level.

## Best Practices

1. Start with small batch sizes and adjust based on performance
2. Use appropriate sync intervals based on data update frequency
3. Consider ClickHouse partitioning for large tables
4. Monitor disk space and memory usage
5. Back up data before initial sync

## Limitations

- Requires a unique, auto-incrementing ID column for incremental sync
- Does not handle schema changes automatically
- Deletions in MySQL are not propagated in incremental sync mode

## Contributing

Contributions are welcome! Please feel free to submit pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.