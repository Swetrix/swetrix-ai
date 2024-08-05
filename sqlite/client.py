import os
import sqlite3
from dotenv import load_dotenv

class SQLiteClient:
    def __init__(self):
        load_dotenv()
        self.db_path = os.getenv("SQLITE_DATABASE")

    def _get_connection(self):
        """Create a new database connection."""
        return sqlite3.connect(self.db_path)

    def execute_query(self, query: str):
        with self._get_connection() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            connection.commit()
        return result

    def insert_data(self, table: str, data: list, column_names: list):
        placeholders = ', '.join('?' for _ in column_names)
        columns = ', '.join(column_names)
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        with self._get_connection() as connection:
            cursor = connection.cursor()
            cursor.executemany(query, data)
            connection.commit()

    def drop_all_data_from_table(self, table_name: str):
        query = f"DELETE FROM {table_name}"
        self.execute_query(query)


sqlite_client = SQLiteClient()
