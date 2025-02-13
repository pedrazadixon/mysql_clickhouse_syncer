# Database configurations
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

# Batch size for fetching data from MySQL
BATCH_SIZE = 1000

# Default type mapping from MySQL to ClickHouse
DEFAULT_TYPE_MAPPING = {
    'int': 'Nullable(Int32)',
    'bigint': 'Nullable(Int64)',
    'tinyint': 'Nullable(Int8)',
    'smallint': 'Nullable(Int16)',
    'mediumint': 'Nullable(Int32)',
    'varchar': 'Nullable(String)',
    'char': 'Nullable(String)',
    'text': 'Nullable(String)',
    'datetime': 'Nullable(DateTime)',
    'timestamp': 'Nullable(DateTime)',
    'date': 'Nullable(Date)',
    'decimal': 'Nullable(Decimal)',
    'float': 'Nullable(Float32)',
    'double': 'Nullable(Float64)',
    'json': 'Nullable(String)',
    'bool': 'Nullable(UInt8)',
    'boolean': 'Nullable(UInt8)'
}

# Table mapping configuration
# Format: 'mysql_table': {
#     'clickhouse_table': 'name_in_clickhouse',
#     'columns': ['col1', 'col2', 'col3'],
#     'id_column': 'column_to_track_sync'
# }
TABLE_MAPPINGS = {
    'example_table': {
        'clickhouse_table': 'example_table_ch',
        'columns': [
            'id',
            'name',
            'created_at'
        ],
        'id_column': 'id',
        'type_mapping': {
            'id': 'Int32',  # Primary key should not be nullable
            'created_at': 'DateTime',
        },
        'partition_by': 'toMonday(created_at)',  # Optional
        'sync_interval': 3600,  # Sync interval in seconds
        'sync_type': 'incremental'  # 'full' or 'incremental'
    }
}
