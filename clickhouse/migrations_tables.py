from client import ClickHouseClient


def create_tables():
    client = ClickHouseClient()

    training_tmp_query = """
    CREATE TABLE IF NOT EXISTS training_tmp (
        cat_features Array(String),
        cols Array(String),
        next_hrs Array(String),
        model String
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """

    predictions_query = """
    CREATE TABLE IF NOT EXISTS predictions (
        pid String,
        next_1_hour String,
        next_4_hour String,
        next_8_hour String,
        next_12_hour String,
        next_24_hour String,
        next_72_hour String,
        next_168_hour String
    ) ENGINE = MergeTree()
    ORDER BY pid
    """

    client.execute_query(training_tmp_query)
    client.execute_query(predictions_query)


client = ClickHouseClient()

if __name__ == "__main__":
    create_tables()
