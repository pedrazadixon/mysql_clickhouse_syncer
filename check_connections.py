import clickhouse_connect
import mysql.connector
from config import (
    MYSQL_CONFIG,
    CLICKHOUSE_CONFIG,
)


def check_mysql_connection():
    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("Connected to MySQL")
        return mysql_conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def check_clickhouse_connection():
    try:
        clickhouse_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        print("Connected to ClickHouse")
        return clickhouse_client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        return None


if __name__ == "__main__":
    mysql_conn = check_mysql_connection()
    if mysql_conn is not None:
        mysql_conn.close()
    clickhouse_conn = check_clickhouse_connection()
    if clickhouse_conn is not None:
        clickhouse_conn.close()
