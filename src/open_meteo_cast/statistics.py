import pandas as pd

def calculate_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the 10th percentile, median (50th percentile), and 90th percentile
    for each row of a DataFrame. The DataFrame is expected to have a DatetimeIndex.

    Args:
        df: The input DataFrame with a DatetimeIndex and subsequent
            columns containing numerical data.

    Returns:
        A new DataFrame with the original index and 'p10', 'median', and 'p90' columns.
    """
    if df.empty:
        return pd.DataFrame(columns=['p10', 'median', 'p90'])

    # The data columns are the entire dataframe
    data_columns = df

    # Calculate statistics for each row
    p10 = data_columns.apply(lambda row: row.quantile(0.10), axis=1)
    median = data_columns.apply(lambda row: row.median(), axis=1)
    p90 = data_columns.apply(lambda row: row.quantile(0.90), axis=1)

    # Create a new DataFrame with the results
    statistics_df = pd.DataFrame({
        'p10': p10,
        'median': median,
        'p90': p90
    }, index=df.index) # Keep the original index

    return statistics_df