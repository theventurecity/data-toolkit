import pandas as pd
from datetime import datetime, timedelta


def adjust_dates(df, date_column, base_date=None):
    """
    Adjusts all dates in the dataframe by adding the number of days between
    the maximum date in the specified column and today.

    Parameters:
    - df (pd.DataFrame): The DataFrame containing the dates.
    - date_column (str): The name of the column containing date values.
    - base_date (datetime, optional): The date to compare against the max date. Defaults to today.

    Returns:
    - pd.DataFrame: A DataFrame with adjusted dates.
    """
    if base_date is None:
        base_date = datetime.today()
    else:
        base_date = pd.to_datetime(base_date)

    # Ensure the date column is in datetime format
    if date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column])
    else:
        return pd.DataFrame()

    # Find the max date in the specified column
    max_date = df[date_column].max()

    # Calculate the difference in days from today
    delta_days = (base_date - max_date).days

    # Add the delta to all date columns in the DataFrame
    for col in df.select_dtypes(include=['datetime64']):
        df[col] = df[col] + timedelta(days=delta_days)

    return df



def adjust_transaction_dates(filename, date_col_name, base_date=None):
    relpath_filename = f'../data/{filename}'
    t = pd.read_csv(relpath_filename)
    t_adjusted = adjust_dates(t, date_col_name, base_date)
    return t_adjusted


def write_adjusted_dates_to_file(input_filename, 
                                 output_filename, 
                                 date_col_name,
                                 base_date=None):
    relpath_output_filename = f'../data/{output_filename}'
    output_df = adjust_transaction_dates(input_filename, 
                                         date_col_name,
                                         base_date)
    output_df.to_csv(relpath_output_filename, index=False)



