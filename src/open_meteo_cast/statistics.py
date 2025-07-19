import pandas as pd
import numpy as np

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

def calculate_precipitation_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the probability of precipitation (>0) and the conditional average
    of precipitation for each row of a DataFrame.

    Args:
        df: The input DataFrame with a DatetimeIndex and subsequent
            columns containing numerical data.

    Returns:
        A new DataFrame with the original index and 'probability' and 'conditional_average' columns.
    """
    if df.empty:
        return pd.DataFrame(columns=['probability', 'conditional_average'])

    # Calculate the probability of precipitation > 0
    probability = (df > 0).mean(axis=1)  # The mean of booleans (True=1, False=0) equals the proportion of members forecasting precipitation.

    # Calculate the conditional average of precipitation (where > 0)
    conditional_average = df[df > 0].mean(axis=1).fillna(0)

    # Create a new DataFrame with the results
    statistics_df = pd.DataFrame({
        'probability': probability,
        'conditional_average': conditional_average
    }, index=df.index) # Keep the original index

    return statistics_df

def calculate_octa_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the probability for each cloud cover octa (0-8) for each row.

    Args:
        df: The input DataFrame with a DatetimeIndex and subsequent
            columns containing cloud cover data in octas (0-8).

    Returns:
        A new DataFrame with the original index and columns for the
        probability of each octa ('octa_0_prob', 'octa_1_prob', etc.).
    """
    if df.empty:
        return pd.DataFrame()

    # Calculate the probability for each octa value (0-8)
    probabilities = {}
    for octa in range(9):
        probabilities[f'octa_{octa}_prob'] = (df == octa).mean(axis=1)

    # Create a new DataFrame with the results
    statistics_df = pd.DataFrame(probabilities, index=df.index)

    return statistics_df

def calculate_wind_direction_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the probability of wind direction falling into one of 8 octants.

    Args:
        df: The input DataFrame with a DatetimeIndex and subsequent
            columns containing wind direction data in degrees (0-360).

    Returns:
        A new DataFrame with the original index and columns for the
        probability of each octant ('N_prob', 'NE_prob', etc.).
    """
    if df.empty:
        return pd.DataFrame()

    # Map degrees to octants: 0:N, 1:NE, 2:E, 3:SE, 4:S, 5:SW, 6:W, 7:NW
    octants_numeric = np.floor(((df + 22.5) % 360) / 45).astype(int)
    
    octant_labels = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    
    probabilities = {}
    for i, label in enumerate(octant_labels):
        probabilities[f'{label}_prob'] = (octants_numeric == i).mean(axis=1)
        
    statistics_df = pd.DataFrame(probabilities, index=df.index)
    
    return statistics_df

def calculate_weather_code_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the probability for each weather code (0-99) for each row.

    Args:
        df: The input DataFrame with a DatetimeIndex and subsequent
            columns containing weather code data as integers (0-99).

    Returns:
        A new DataFrame with the original index and columns for the
        probability of each weather code ('wc_prob_00', 'wc_prob_01', etc.).
    """
    if df.empty:
        return pd.DataFrame()

    # Calculate the probability for each integer weather code value (0-99)
    probabilities = {}
    for code in range(100):
        column_name = f"wc_prob_{code:02d}"
        probabilities[column_name] = (df == code).mean(axis=1)

    # Create a new DataFrame with the results
    statistics_df = pd.DataFrame(probabilities, index=df.index)

    return statistics_df
