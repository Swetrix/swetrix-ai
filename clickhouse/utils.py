import base64
import pickle
import json
from data.serialisation import (
    serialise_predictions,
    serialise_data_for_clickhouse,
)
from clickhouse.client import clickhouse_client


"""
Clickhouse does not support the pickled objects yet, and it is a problem.
There is a solution to use `base64` encoding, store the model as a string and then decode it and use as a pickle object

Though it is a subject of discussion in the future. I personally prefer to store the model in S3 bucket, but this will require an
additional time for development which we do not have, as the priority is to test the model in production.
"""


def serialize_model(model):
    pickled_model = pickle.dumps(model)
    base64_model = base64.b64encode(pickled_model).decode("utf-8")
    return base64_model


def deserialize_model(base64_model):
    pickled_model = base64.b64decode(base64_model.encode("utf-8"))
    model = pickle.loads(pickled_model)
    return model


def fetch_model():
    """Get the serialized model from the database for predictions"""
    result = clickhouse_client.execute_query("SELECT model FROM training_tmp")
    if result:
        serialized_model = result[0][0]
        model = deserialize_model(serialized_model)
        return model
    else:
        print("No model found")
        return None


def insert_predictions(predictions):
    """Insert serialised JSON data into the predictions table"""
    predictions_data = json.loads(predictions)
    processed_data = serialise_predictions(predictions_data)
    serialized_data = serialise_data_for_clickhouse(processed_data)

    # Drop previous(not relevant) data before the insertion of new predictions
    clickhouse_client.drop_all_data_from_table("predictions")
    clickhouse_client.insert_data("predictions", serialized_data)
