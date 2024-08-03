from fastapi import FastAPI, HTTPException
from clickhouse.client import clickhouse_client
import json
from enum import Enum
from celery_tasks.tasks import run_training_module, run_prediction_module

app = FastAPI()


class TimeFrameEnum(str, Enum):
    next_1_hour = "next_1_hour"
    next_4_hour = "next_4_hour"
    next_8_hour = "next_8_hour"
    next_12_hour = "next_12_hour"
    next_24_hour = "next_24_hour"
    next_72_hour = "next_72_hour"
    next_168_hour = "next_168_hour"


@app.get("/predict/")
def get_predictions(pid: str, timeframe: TimeFrameEnum):
    """Get predictions for the specified `pid` and `timeframe`"""

    project_exists_query = (
        f"SELECT pid FROM predictions WHERE pid = '{pid}' LIMIT 1"
    )
    project = clickhouse_client.execute_query(project_exists_query)

    if not project:
        raise HTTPException(status_code=404, detail="Project does not exist.")

    prediction_query = (
        f"SELECT {timeframe} FROM predictions WHERE pid = '{pid}'"
    )
    result = clickhouse_client.execute_query(prediction_query)

    if not result or not result[0][0]:
        raise HTTPException(
            status_code=404,
            detail="Data not found. Prediction is not available.",
        )

    prediction_data = json.loads(result[0][0])

    if not prediction_data:
        raise HTTPException(
            status_code=404,
            detail="Data not found. Prediction is not available.",
        )

    return {timeframe: prediction_data}


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
