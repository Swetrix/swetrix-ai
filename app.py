from fastapi import FastAPI, HTTPException
from sqlite.client import sqlite_client
import json
from enum import Enum
from celery_tasks.tasks import run_training_module, run_prediction_module

from sqlite.client import SQLiteClient

app = FastAPI()

sqlite_client = SQLiteClient()


class TimeFrameEnum(str, Enum):
    next_1_hour = "next_1_hour"
    next_4_hour = "next_4_hour"
    next_8_hour = "next_8_hour"
    next_12_hour = "next_12_hour"
    next_24_hour = "next_24_hour"
    next_72_hour = "next_72_hour"
    next_168_hour = "next_168_hour"


@app.get("/predict/")
def get_predictions(pid: str):
    """Get predictions for the specified `pid`"""

    project_exists_query = (
        f"SELECT pid FROM predictions WHERE pid = '{pid}' LIMIT 1"
    )
    project = sqlite_client.execute_query(project_exists_query)

    if not project:
        raise HTTPException(status_code=404, detail="Project does not exist.")

    predictions = {}
    for timeframe in TimeFrameEnum:
        prediction_query = (
            f"SELECT {timeframe.value} FROM predictions WHERE pid = '{pid}'"
        )
        result = sqlite_client.execute_query(prediction_query)

        if result:
            prediction_data = json.loads(result[0][0])
            if prediction_data:
                predictions[timeframe.value] = prediction_data

    if not predictions:
        raise HTTPException(
            status_code=404,
            detail="Data not found. Prediction is not available.",
        )

    return predictions


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


@app.post("/run_training/")
def trigger_training():
    """Trigger the training module via Celery"""
    run_training_module.delay()
    return {"message": "Training module triggered"}


@app.post("/run_prediction/")
def trigger_prediction():
    """Trigger the prediction module via Celery"""
    run_prediction_module.delay()
    return {"message": "Prediction module triggered"}
