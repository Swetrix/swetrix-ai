import os
from dotenv import load_dotenv
from clickhouse_connect import get_client

class ClickHouseClient:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("CLICKHOUSE_HOST")
        self.port = os.getenv("CLICKHOUSE_PORT")
        self.user = os.getenv("CLICKHOUSE_USER")
        self.password = os.getenv("CLICKHOUSE_PASSWORD")
        self.database = os.getenv("CLICKHOUSE_DATABASE")

        self.client = get_client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def execute_query(self, query: str):
        return self.client.query(query)

    def insert_data(self, table: str, data: list, column_names: list):
        self.client.insert(
            table=table,
            data=data,
            column_names=column_names
        )

    def drop_all_data_from_table(self, table_name: str):
        """Drop all data from the table as we require to store only one record in the meantime"""
        query = f"TRUNCATE TABLE {table_name}"
        self.execute_query(query)


clickhouse_client = ClickHouseClient()