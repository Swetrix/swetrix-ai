import os
import base64
import pickle
import json
from data.serialisation import (
    serialise_predictions,
    serialise_data_for_sqlite,
)
from sqlite.client import sqlite_client


"""
SQLite does not support the pickled objects yet, and it is a problem.
There is a solution to use `base64` encoding, store the model as a string and then decode it and use as a pickle object

Though it is a subject of discussion in the future. I personally prefer to store the model in S3 bucket, but this will require an
additional time for development which we do not have, as the priority is to test the model in production.
"""


def save_model_to_file(model, directory, model_name):
    file_path = os.path.join(directory, model_name)
    with open(file_path, "wb") as model_file:
        model_file.write(model)
    return file_path


def load_model_from_file(file_path):
    with open(file_path, "rb") as model_file:
        model = model_file.read()
    return model


def serialize_model(model):
    pickled_model = pickle.dumps(model)
    base64_model = base64.b64encode(pickled_model).decode("utf-8")
    return base64_model


def deserialize_model(base64_model):
    pickled_model = base64.b64decode(base64_model.encode("utf-8"))
    model = pickle.loads(pickled_model)
    return model


def fetch_model(model_path):
    """Get the serialized model from the database for predictions"""
    serialized_model = load_model_from_file(model_path)
    model = deserialize_model(serialized_model.decode())
    return model


def remove_existing_models(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path) and filename.endswith(".pkl"):
            os.remove(file_path)


def insert_predictions(predictions):
    """Insert serialised JSON data into the predictions table"""
    predictions_data = json.loads(predictions)
    processed_data = serialise_predictions(predictions_data)
    serialized_data = serialise_data_for_sqlite(processed_data)

    # Drop previous(not relevant) data before the insertion of new predictions
    sqlite_client.drop_all_data_from_table("predictions")
    sqlite_client.insert_data(
        table="predictions",
        data=serialized_data,
        column_names=[
            "pid",
            "next_1_hour",
            "next_4_hour",
            "next_8_hour",
            "next_12_hour",
            "next_24_hour",
            "next_72_hour",
            "next_168_hour",
        ],
    )
