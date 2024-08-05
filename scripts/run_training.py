import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlite.client import sqlite_client
from sqlite.utils import (
    serialize_model,
    save_model_to_file,
    remove_existing_models,
)
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

    model_directory = "trained_models/"
    remove_existing_models(model_directory)

    logger.info(f"Start training the model {datetime.now()}")
    df, cat_features, cols, next_hrs = pre_process_data()
    model = train_model(df, cols, next_hrs)

    serialized_model = serialize_model(model)

    model_name = f'model_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pkl'
    model_path = save_model_to_file(
        serialized_model.encode(), model_directory, model_name
    )
    logger.info(f"Model saved to {model_path}")

    training_tmp_data = [
        (
            json.dumps(cat_features),
            json.dumps(cols.tolist()),
            json.dumps(next_hrs),
            model_path,
        )
    ]

    # Drop previous(not relevant) data before the insertion of new training data
    sqlite_client.drop_all_data_from_table("training_tmp")

    sqlite_client.insert_data(
        table="training_tmp",
        data=training_tmp_data,
        column_names=["cat_features", "cols", "next_hrs", "model_path"],
    )

    logger.info(
        f"Training has been completed, removing previous records from the database and insert new {datetime.now()}"
    )
