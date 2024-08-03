import pandas as pd
import numpy as np
from constants import columns, date_col, agg_cols
from clickhouse.utils import fetch_model
from clickhouse.client import clickhouse_client

"""
take cat_features
take cols
take next_hrs
take model

take recent data and convert it to pd dataframe
process data for predictions

predict
erase previous predictions
insert new predictions
"""


def get_projects_records() -> pd.DataFrame:
    # query the clickhouse database to get historical data for all projects and predict future traffic for them
    # temporary:
    """Read the csv file with encodings and add columns to it"""
    data = clickhouse_client.execute_query('SELECT * FROM analytics')
    df = pd.DataFrame(data, columns=columns)

    # Exclude specific columns
    columns_to_exclude = ["meta.key", "meta.value"]
    df = df.drop(columns=columns_to_exclude)
    
    return df


def get_variable_from_tmp(var_name: str):
    """Get the variable from training_tmp table"""
    result = clickhouse_client.execute_query(
        f"SELECT {var_name} FROM training_tmp"
    )
    # Clickhouse ORM returns a tuple of list, accessing the actual python object
    variable = result[0][0]  
    return variable


def preprocess_data(df, date_col):
    """
    Preprocesses the data by sorting, replacing null values, converting date column to datetime, and extracting date components.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    date_col (str): The name of the date column.

    Returns:
    pd.DataFrame: The preprocessed DataFrame.
    """
    # Sort the DataFrame by the date column
    df = df.sort_values(date_col)

    # Replace "\\N" with NaN
    df = df.replace({"\\N": np.nan})

    # Convert the date column to datetime
    df[date_col] = pd.to_datetime(df[date_col])

    # Extract date components
    df["year"] = df[date_col].dt.year
    df["month"] = df[date_col].dt.month
    df["day"] = df[date_col].dt.day
    df["day_of_week"] = df[date_col].dt.dayofweek
    df["hour"] = df[date_col].dt.hour

    return df


def filter_most_recent_hour(df, date_col):
    """
    Filters the DataFrame to keep only the rows corresponding to the most recent hour.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    date_col (str): The name of the date column.

    Returns:
    pd.DataFrame: The DataFrame filtered to the most recent hour.
    """
    # Determine the most recent values for year, month, day, and hour
    for col in ["year", "month", "day", "hour"]:
        df = df[df[col] == df.tail(1)[col].values[0]]

    # Drop the original date column
    df.drop([date_col], axis=1, inplace=True)

    return df


def encode_and_aggregate(df, cat_features, agg_cols):
    """
    Encodes categorical features, aggregates the data, and fills missing values.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    cat_features (list): List of categorical features to be one-hot encoded.
    agg_cols (list): List of columns to group by when aggregating data.

    Returns:
    pd.DataFrame: The aggregated DataFrame with encoded categorical features and missing values filled.
    """
    # Add a column for traffic
    df["traffic"] = 1

    # One-hot encode the categorical features
    df = pd.get_dummies(df, columns=cat_features, dtype="int")

    # Aggregate data by summing up the traffic
    df = df.groupby(agg_cols).sum().reset_index()

    # Fill NaN values with 0
    df = df.fillna(0)

    return df


def fill_missing_columns(df, all_cols):
    """
    Fills missing columns in the DataFrame with 0.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    all_cols (list): List of all possible columns.

    Returns:
    pd.DataFrame: The DataFrame with missing columns filled with 0.
    """
    missing_cols = [x for x in all_cols if x not in df.columns]
    df[missing_cols] = 0

    return df


def predict_future_data():
    """Pre-processing of data"""
    cat_features = get_variable_from_tmp("cat_features")
    cols = get_variable_from_tmp("cols")
    next_hrs = get_variable_from_tmp("next_hrs")
    model = fetch_model()

    df = get_projects_records()
    df = preprocess_data(df, date_col)
    df = filter_most_recent_hour(df, date_col)
    df = encode_and_aggregate(df, cat_features, agg_cols)
    df = fill_missing_columns(df, cols)

    y = pd.DataFrame(model.predict(df[cols]), columns=next_hrs)
    y["pid"] = df["pid"]
    y = y.set_index("pid").reset_index()

    return y.to_json(orient="records")
