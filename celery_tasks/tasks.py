from celery_tasks.celery_config import celery_app
from scripts.run_training import train
from scripts.run_prediction import predict


@celery_app.task
def run_training_module():
    train()


@celery_app.task
def run_prediction_module():
    predict()
