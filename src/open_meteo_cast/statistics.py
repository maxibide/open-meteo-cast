import pandas as pd

def calculate_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the 10th percentile, median (50th percentile), and 90th percentile
    for each row of a DataFrame, excluding the first column (assumed to be 'date').

    Args:
        df: The input DataFrame with the first column as 'date' and subsequent
            columns containing numerical data.

    Returns:
        A new DataFrame with 'date', 'p10', 'median', and 'p90' columns.
    """
    if df.empty:
        return pd.DataFrame(columns=['date', 'p10', 'median', 'p90'])

    # Ensure the first column is treated as the date column
    date_column = df.iloc[:, 0]
    data_columns = df.iloc[:, 1:]

    # Calculate statistics for each row
    p10 = data_columns.apply(lambda row: row.quantile(0.10), axis=1)
    median = data_columns.apply(lambda row: row.median(), axis=1)
    p90 = data_columns.apply(lambda row: row.quantile(0.90), axis=1)

    # Create a new DataFrame with the results
    statistics_df = pd.DataFrame({
        'date': date_column,
        'p10': p10,
        'median': median,
        'p90': p90
    })

    return statistics_df