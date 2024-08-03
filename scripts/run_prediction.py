from clickhouse.utils import insert_predictions
from models.predict_model import predict_future_data

from logging_config import setup_logger
from datetime import datetime

logger = setup_logger("run_prediction")


def predict():
    """Celery task which is called for a model predictions
    - Predicts the future data
    - Inserts serialised predictions into the DB
    """
    logger.info(f"Started prediction of the data: {datetime.now()}")
    predictions = predict_future_data()
    insert_predictions(predictions)
    logger.info(
        f"Prediction has been completed, removing previous records from the database and insert new {datetime.now()}"
    )
