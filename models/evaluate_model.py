
from sklearn.metrics import r2_score, mean_absolute_error
from logging_config import setup_logger

logger = setup_logger('evaluate_model')

def evaluate_model(model, df, cols, next_hrs):
    """Evaluate R^2 and MAE metrics for the model"""
    y_pred = model.predict(df[cols])
    r2 = r2_score(df[next_hrs], y_pred)
    logger.info(f'R^2 score: {r2}')
    mae = mean_absolute_error(df[next_hrs], y_pred)
    logger.info(f'MAE: {mae}')