from client import SQLiteClient


def create_tables():
    client = SQLiteClient()

    training_tmp_query = """
    CREATE TABLE IF NOT EXISTS training_tmp (
        cat_features TEXT,
        cols TEXT,
        next_hrs TEXT,
        model_path TEXT
    )
    """

    predictions_query = """
    CREATE TABLE IF NOT EXISTS predictions (
        pid TEXT PRIMARY KEY,
        next_1_hour TEXT,
        next_4_hour TEXT,
        next_8_hour TEXT,
        next_12_hour TEXT,
        next_24_hour TEXT,
        next_72_hour TEXT,
        next_168_hour TEXT
    )
    """

    client.execute_query(training_tmp_query)
    client.execute_query(predictions_query)


if __name__ == "__main__":
    create_tables()
