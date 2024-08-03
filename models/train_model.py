from sklearn.tree import DecisionTreeRegressor
from models.evaluate_model import evaluate_model


def train_model(df, cols, next_hrs):
    """
    Train the model, fit data into the model and evaluate model's efficiency
    """
    model = DecisionTreeRegressor()
    model.fit(df[cols], df[next_hrs])
    evaluate_model(model, df, cols, next_hrs)
    return model
