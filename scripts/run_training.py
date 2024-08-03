import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from clickhouse.client import clickhouse_client
from clickhouse.utils import serialize_model
from data.load_data import pre_process_data
from models.train_model import train_model
from logging_config import setup_logger
from datetime import datetime

logger = setup_logger("run_training")


def train():
    """Celery task which is called for a model training
    - Gets data from ``load_data`` module
    - Serialises model to ``base64`` encoding
    - Inserts data to the DB
    """
    logger.info(f"Start training the model {datetime.now()}")
    df, cat_features, cols, next_hrs = pre_process_data()
    model = train_model(df, cols, next_hrs)

    serialized_model = serialize_model(model)
    training_tmp_data = [
        (cat_features, cols.to_list(), next_hrs, serialized_model)
    ]

    # Drop previous(not relevant) data before the insertion of new training data
    clickhouse_client.drop_all_data_from_table("training_tmp")
    clickhouse_client.insert_data("training_tmp", training_tmp_data)

    logger.info(
        f"Training has been completed, removing previous records from the database and insert new {datetime.now()}"
    )
