import clickhouse_connect
import mysql.connector
import time
import logging
import os
import json
from config import (
    MYSQL_CONFIG,
    CLICKHOUSE_CONFIG,
    TABLE_MAPPINGS,
    BATCH_SIZE,
    DEFAULT_TYPE_MAPPING
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def get_last_synced_id(table_name):
    filename = f'last_synced_id_{table_name}.txt'
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return int(f.read().strip())
    return 0


def save_last_synced_id(table_name, last_id):
    filename = f'last_synced_id_{table_name}.txt'
    with open(filename, 'w') as f:
        f.write(str(last_id))


def get_mysql_column_types(mysql_cursor, table_name, columns):
    mysql_cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s
    """, (table_name, MYSQL_CONFIG['database']))

    column_types = {}
    for col in mysql_cursor.fetchall():
        if col[0] in columns:
            column_types[col[0]] = col[1].lower()
            # Handle decimal precision
            if col[1].lower() == 'decimal':
                precision = col[2].lower().replace('decimal(', '').replace(')', '').split(',')
                column_types[col[0]] = f"decimal({precision[0]},{precision[1]})"
    return column_types


def map_to_clickhouse_type(mysql_type, column_name, table_config):
    # Check if there's a custom mapping for this column
    if 'type_mapping' in table_config and column_name in table_config['type_mapping']:
        return table_config['type_mapping'][column_name]

    # Handle decimal type separately
    if mysql_type.startswith('decimal'):
        return mysql_type.replace('decimal', 'Decimal')

    # Use default mapping
    base_type = mysql_type.split('(')[0]
    return DEFAULT_TYPE_MAPPING.get(base_type, 'String')


def create_clickhouse_table_if_not_exists(clickhouse_client, mysql_cursor, table_name, table_config):
    try:
        # Check if table exists
        try:
            clickhouse_client.command(f"DESCRIBE {table_config['clickhouse_table']}")
            return  # Table exists
        except:
            pass  # Table doesn't exist

        # Get MySQL column types
        mysql_types = get_mysql_column_types(mysql_cursor, table_name, table_config['columns'])

        # Prepare columns definition
        columns_def = []
        for col in table_config['columns']:
            ch_type = map_to_clickhouse_type(mysql_types[col], col, table_config)
            columns_def.append(f"`{col}` {ch_type}")

        # add _ver column DateTime
        columns_def.append("`__ver` DateTime")

        # Prepare ORDER BY clause
        order_by = table_config.get('order_by', [table_config['id_column']])
        order_by_clause = ', '.join(order_by)

        # Prepare PARTITION BY clause
        partition_by = table_config.get('partition_by')
        partition_clause = f"PARTITION BY {partition_by}" if partition_by else ""

        # Create table query
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_config['clickhouse_table']}
            (
                {','.join(columns_def)}
            )
            ENGINE = ReplacingMergeTree(__ver)
            {partition_clause}
            ORDER BY ({order_by_clause})
        """

        clickhouse_client.command(create_query)
        logging.info(f"Created table {table_config['clickhouse_table']} in ClickHouse")

    except Exception as e:
        logging.error(f"Error creating table {table_config['clickhouse_table']}: {str(e)}")
        raise


def get_max_id_from_mysql(mysql_cursor, table_name, id_column):
    mysql_cursor.execute(f"SELECT MAX({id_column}) FROM {table_name}")
    max_id = mysql_cursor.fetchone()[0]
    return max_id or 0


def get_max_id_from_clickhouse(clickhouse_client, table_config):
    try:
        result = clickhouse_client.query(
            f"SELECT MAX({table_config['id_column']}) as max_id FROM {table_config['clickhouse_table']}"
        )
        row = result.first_row
        return row[0] if row and row[0] is not None else 0
    except Exception as e:
        logging.error(f"Error getting max ID from ClickHouse: {str(e)}")
        return 0


def get_table_state(table_name, clickhouse_client=None, table_config=None):
    filename = f'state_{table_name}.json'

    try:
        # Try to read from state file first
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f)

        # If no state file exists but we have access to ClickHouse
        if clickhouse_client and table_config:
            try:
                max_id = get_max_id_from_clickhouse(clickhouse_client, table_config)
                state = {
                    'last_id': max_id,
                    'last_sync_time': 0  # Force immediate sync
                }
                # Save the state for future use
                with open(filename, 'w') as f:
                    json.dump(state, f)
                logging.info(f"Initialized state from ClickHouse for {table_name} with max_id: {max_id}")
                return state
            except Exception as e:
                logging.error(f"Error initializing state from ClickHouse: {str(e)}")
    except Exception as e:
        logging.error(f"Error handling state file for {table_name}: {str(e)}")

    # Default state if nothing else works
    return {'last_id': 0, 'last_sync_time': 0}


def save_table_state(table_name, last_id):
    filename = f'state_{table_name}.json'
    state = {
        'last_id': last_id,
        'last_sync_time': int(time.time())
    }

    try:
        with open(filename, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        logging.error(f"Error saving state file for {table_name}: {str(e)}")


def should_sync_table(table_name, table_config):
    state = get_table_state(table_name)
    current_time = int(time.time())
    sync_interval = table_config.get('sync_interval')
    return (current_time - state['last_sync_time']) >= sync_interval


def sync_table_full(mysql_cursor, clickhouse_client, table_name, table_config):
    try:
        create_clickhouse_table_if_not_exists(clickhouse_client, mysql_cursor, table_name, table_config)

        columns = table_config['columns']
        if '__ver' not in columns:
            columns.append('__ver')
        id_column = table_config['id_column']
        ch_table = table_config['clickhouse_table']

        # Get total count for logging
        mysql_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_records = mysql_cursor.fetchone()[0]

        logging.info(f"Starting full sync of {table_name}. Total records: {total_records}")

        columns_sql = ', '.join(columns)
        columns_sql = columns_sql.replace(', __ver', ', NOW() AS __ver')

        # Process in batches
        offset = 0
        while True:
            # Get batch of records
            mysql_cursor.execute(
                f"SELECT {columns_sql} FROM {table_name} "
                f"ORDER BY {id_column} LIMIT %s OFFSET %s",
                (BATCH_SIZE, offset)
            )
            records = mysql_cursor.fetchall()

            if not records:
                break

            # Insert new/updated records
            clickhouse_client.insert(
                ch_table,
                records,
                column_names=columns
            )

            offset += len(records)
            logging.info(f"Table {table_name}: Synced {offset}/{total_records} records")

        save_table_state(table_name, total_records)

        return True

    except Exception as e:
        logging.error(f"Error in full sync of table {table_name}: {str(e)}")
        return False


def update_existing_records(mysql_cursor, clickhouse_client, table_name, table_config, last_synced_id):
    try:
        if 'update_config' not in table_config:
            return

        update_config = table_config['update_config']
        timestamp_column = update_config['timestamp_column']
        update_interval = update_config['update_interval']
        columns = table_config['columns']
        if '__ver' not in columns:
            columns.append('__ver')
        id_column = table_config['id_column']
        ch_table = table_config['clickhouse_table']

        # Get records that were updated recently
        columns_sql = ', '.join(columns)
        columns_sql = columns_sql.replace(', __ver', ', NOW() AS __ver')
        query = f"""
            SELECT {columns_sql} 
            FROM {table_name}
            WHERE {timestamp_column} > DATE_SUB(NOW(), INTERVAL %s SECOND)
            AND {id_column} <= %s
        """

        mysql_cursor.execute(query, (update_interval, last_synced_id))
        records = mysql_cursor.fetchall()

        if not records:
            logging.info(f"No records to update in {table_name}")
            return

        # Process records in batches
        batch_size = BATCH_SIZE
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            # Insert updated records
            clickhouse_client.insert(
                ch_table,
                batch,
                column_names=columns
            )

            logging.info(f"Updated {len(batch)} existing records in {table_name}")

    except Exception as e:
        logging.error(f"Error updating existing records in {table_name}: {str(e)}")


def sync_table(mysql_cursor, clickhouse_client, table_name, table_config):
    sync_type = table_config.get('sync_type', 'incremental')

    # Full sync logic
    if sync_type == 'full':
        return sync_table_full(mysql_cursor, clickhouse_client, table_name, table_config)

    # Incremental sync logic
    try:
        # Create table if it doesn't exist
        create_clickhouse_table_if_not_exists(clickhouse_client, mysql_cursor, table_name, table_config)

        # Pass clickhouse_client and table_config to get_table_state
        state = get_table_state(table_name, clickhouse_client, table_config)
        last_synced_id = state['last_id']
        columns = table_config['columns']
        if '__ver' not in columns:
            columns.append('__ver')
        id_column = table_config['id_column']
        ch_table = table_config['clickhouse_table']

        # Get maximum ID from MySQL
        max_id = get_max_id_from_mysql(mysql_cursor, table_name, id_column)

        if max_id <= last_synced_id:
            logging.info(f"Table {table_name}: No new records to sync. Max ID: {max_id}")
            save_table_state(table_name, last_synced_id)  # Update timestamp even when no new records
            return False

        logging.info(f"Table {table_name}: Found new records. Max ID: {max_id}, Last synced: {last_synced_id}")

        # Construct the SQL query with specific columns
        columns_sql = ', '.join(columns)
        columns_sql = columns_sql.replace(', __ver', ', NOW() AS __ver')

        while last_synced_id < max_id:
            mysql_cursor.execute(
                f"SELECT {columns_sql} FROM {table_name} "
                f"WHERE {id_column} > %s "
                f"ORDER BY {id_column} ASC LIMIT %s",
                (last_synced_id, BATCH_SIZE)
            )
            records = mysql_cursor.fetchall()

            if not records:
                break

            # Insert data into ClickHouse
            clickhouse_client.insert(
                ch_table,
                records,
                column_names=columns
            )

            # Update last synced ID
            new_last_id = records[-1][columns.index(id_column)]
            save_table_state(table_name, new_last_id)
            last_synced_id = new_last_id

            logging.info(f"Table {table_name}: Synced {len(records)} records. Progress: {last_synced_id}/{max_id}")

        update_existing_records(mysql_cursor, clickhouse_client, table_name, table_config, last_synced_id)

        return True

    except Exception as e:
        logging.error(f"Error syncing table {table_name}: {str(e)}")
        return False


def sync_data():
    try:
        # Connect to databases
        logging.debug("Connecting to mysql...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        mysql_cursor = mysql_conn.cursor()
        logging.debug("Connecting to ClickHouse...")
        clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

        has_synced = False
        # Sync each configured table
        for table_name, table_config in TABLE_MAPPINGS.items():
            logging.debug(f"Checking table {table_name}")
            if should_sync_table(table_name, table_config):
                logging.info(f"Starting sync for table {table_name}")
                if sync_table(mysql_cursor, clickhouse_client, table_name, table_config):
                    has_synced = True
            else:
                logging.debug(f"Skipping {table_name}, not ready for sync yet")

        return has_synced

    except Exception as e:
        logging.error(f"Error during sync: {str(e)}")
        return False

    finally:
        if 'mysql_conn' in locals():
            mysql_conn.close()
        if 'clickhouse_client' in locals():
            clickhouse_client.close()


def main():
    logging.info("Starting sync service")
    while True:
        sync_data()
        logging.info("Waiting 60 seconds before next check...")
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    main()
