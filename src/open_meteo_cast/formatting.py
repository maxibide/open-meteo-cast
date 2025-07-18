import pandas as pd
import numpy as np

def format_statistics_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies specific rounding and formatting rules to a statistics DataFrame.

    Args:
        df: The DataFrame to format.

    Returns:
        The formatted DataFrame.
    """
    formatted_df = df.copy()
    for col in formatted_df.columns:
        if pd.api.types.is_numeric_dtype(formatted_df[col]):
            if col.startswith('cloud_cover'):
                if 'prob' in col:
                    formatted_df[col] = formatted_df[col].round(2)
                else:
                    formatted_df[col] = formatted_df[col].round(0).astype('Int64')
            elif 'prob' in col:
                formatted_df[col] = formatted_df[col].round(2)
            elif col.startswith('precipitation') and col.endswith('_probability'):
                # Round up to the nearest 0.05 for probability
                formatted_df[col] = np.ceil(formatted_df[col] * 20) / 20
            else:
                formatted_df[col] = formatted_df[col].round(1)
    return formatted_df
