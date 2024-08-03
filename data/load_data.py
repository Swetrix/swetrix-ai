import pandas as pd
import numpy as np
from constants import columns, date_col, agg_cols
from logging_config import setup_logger
from clickhouse.client import clickhouse_client

logger = setup_logger("load_data")

import warnings

warnings.filterwarnings("ignore")


def read_data_csv(file_path="analytics-obfuscated-faked.csv"):
    """Read the csv file with encodings and add columns to it"""
    data = clickhouse_client.execute_query('SELECT * FROM analytics')
    df = pd.DataFrame(data, columns=columns)

    # Exclude specific columns
    columns_to_exclude = ["meta.key", "meta.value"]
    df = df.drop(columns=columns_to_exclude)
    
    return df


def sort_df_by_date_col(date_col, df):
    """Sort the dataframe by a date column ``created``"""
    return df.sort_values(date_col)


def convert_df_to_datetime(df):
    """Convert dataframe ``created`` to the datetime object"""
    df[date_col] = pd.to_datetime(df[date_col])
    return df


def filter_df_by_specific_date(df, time_delta_years=1):
    """Filters data for learning in within df"""
    start_date = df[date_col].max() - pd.DateOffset(years=time_delta_years)
    df = df[df[date_col] >= start_date]
    return df


# To save memory and time, train with top 15 most frequent pid
def filter_df_with_most_frequent_pid(df, project_amount=15):
    """Group dataframe by pid, and filter df to leave only those specific pids"""
    x = df.pid.value_counts().head(project_amount).reset_index()
    pid = x["pid"].unique()
    df = df[df.pid.isin(pid)]
    return df


def replace_null_values(df):
    """Replace occurrences of "\\N" in the DataFrame with NaN."""
    return df.replace({"\\N": np.nan})


def categorize_features(df, id_column="pid", threshold=300):
    """
    Categorizes features based on their uniqueness count. Features with more
    unique values than the threshold are dropped, while others are kept as
    categorical features.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    id_column (str): The column to exclude from categorization (default is "pid").
    threshold (int): The maximum number of unique values a column can have to be
                     considered a categorical feature (default is 100).

    Returns:
    pd.DataFrame: The DataFrame with high-uniqueness columns dropped.
    list: A list of columns that are kept as categorical features.
    """
    cat_features = []
    dropped_cols = []
    # Exclude id_column and the last column
    columns_to_process = df.drop(id_column, axis=1).columns[:-1]

    for col in columns_to_process:
        n = df[col].nunique()
        # Additionaly exclude campaing columns too hard to predict right now and resource consuming
        if col not in ['dv', 'br', 'os', 'lc', 'cc', 'unique']:
            df.drop(col, axis=1, inplace=True)
            continue

        if n > threshold:
            df.drop(col, axis=1, inplace=True)
            dropped_cols.append(col)
        else:
            cat_features.append(col)

    logger.info(f"Dropped columns: {dropped_cols}")
    logger.info(f"Kept columns (Categorical Features): {cat_features}")

    return df, cat_features


def extract_date_components(df, date_col):
    """
    Extracts year, month, day, day of the week, and hour from a date column in the DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    date_col (str): The name of the date column to extract components from.

    Returns:
    pd.DataFrame: The DataFrame with new columns for year, month, day, day of the week, and hour.
    """
    df["year"] = df[date_col].dt.year
    df["month"] = df[date_col].dt.month
    df["day"] = df[date_col].dt.day
    df["day_of_week"] = df[date_col].dt.dayofweek
    df["hour"] = df[date_col].dt.hour
    return df


def add_traffic_table(df):
    """Add traffic table to the df"""
    df["traffic"] = 1
    return df


def convert_cat_features_to_dummies(df, cat_features):
    """Convert categorical variable into dummy/indicator variables."""
    df = pd.get_dummies(df, columns=cat_features, dtype="int")
    return df


def process_pid_data(df, pid, date_col, agg_cols):
    """
    Processes data for a single pid, aggregating traffic and generating a DataFrame with hourly date ranges.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    pid (str): The unique identifier for the data subset.
    date_col (str): The name of the date column.
    agg_cols (list): Columns to group by when aggregating traffic.

    Returns:
    pd.DataFrame: The processed DataFrame for the given pid.
    """
    # Filter data for the given pid
    df_pid = df[df.pid == pid]

    # Determine the start and end timestamps
    start_timestamp = df_pid["created"].min()
    end_timestamp = df_pid["created"].max()

    # Drop the original date column
    df_pid.drop([date_col], axis=1, inplace=True)

    # Aggregate traffic data
    traffic = df_pid.groupby(agg_cols).sum().reset_index()

    # Create a date range with an hourly frequency
    date_range = pd.date_range(
        start=start_timestamp, end=end_timestamp, freq="H"
    )

    # Create a DataFrame from the date range
    x = pd.DataFrame(date_range, columns=[date_col])

    # Extract date components
    x = extract_date_components(x, date_col)

    # Merge the aggregated traffic data with the date range DataFrame
    y = pd.merge(x, traffic, how="left")

    # Add the pid column
    y["pid"] = pid

    return y


def combine_all_pids(df, date_col, agg_cols):
    """
    Combines processed data for all unique pids into a single DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    date_col (str): The name of the date column.
    agg_cols (list): Columns to group by when aggregating traffic.

    Returns:
    pd.DataFrame: The combined DataFrame for all pids.
    """
    combined_df = pd.DataFrame()

    # Process each unique pid and combine the results
    for pid in df.pid.unique():
        y = process_pid_data(df, pid, date_col, agg_cols)
        combined_df = pd.concat([combined_df, y], axis=0)

    # Fill in empty values in the combined df
    combined_df = combined_df.fillna(0)
    return combined_df


def set_target_columns(df):
    """Return all columns except of year, month, day, psid, ssid"""
    target_columns = df.columns[7:]
    return target_columns


def remove_date_col(df):
    """Drop the date columun"""
    df.drop([date_col], axis=1, inplace=True)
    return df


def get_cols_withohut_pid(df):
    """Return the columns EXCEPT OF PID"""
    cols = df.drop("pid", axis=1).columns
    return df, cols


def create_target_traffic_by_target_columns(df, target_columns):
    """Extract the traffic for next hours"""
    next_hrs = []
    for hr in [
        1,
        4,
        8,
        12,
        24,
        72,
        168,
    ]:  # TODO dynamical value, as well dynamical for the database
        for (
            target
        ) in target_columns:  # TODO save target_columns into the database
            df[f"{target}_next_{str(hr)}_hr"] = df.groupby("pid")[
                target
            ].shift(-1 * hr)
            next_hrs = next_hrs + [f"{target}_next_{str(hr)}_hr"]
    return next_hrs


def pre_process_data():
    # Pre-processing
    df = read_data_csv()
    df = sort_df_by_date_col(date_col, df)
    df = convert_df_to_datetime(df)
    df = filter_df_by_specific_date(df, time_delta_years=1)
    df = filter_df_with_most_frequent_pid(df)
    df = replace_null_values(df)
    df, cat_features = categorize_features(df)
    df = extract_date_components(df, date_col)
    df = add_traffic_table(df)
    df = convert_cat_features_to_dummies(df, cat_features)
    df = combine_all_pids(df, date_col, agg_cols)

    # Setting data fro predictions
    target_columns = set_target_columns(df)
    df = remove_date_col(df)
    df, cols = get_cols_withohut_pid(df)
    next_hrs = create_target_traffic_by_target_columns(df, target_columns)

    # Clear N/A
    df = df.dropna()
    return df, cat_features, cols, next_hrs
