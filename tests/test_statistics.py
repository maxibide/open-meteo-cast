import pandas as pd
import pytest
from src.open_meteo_cast.statistics import calculate_percentiles

def test_calculate_percentiles_basic():
    data = {
        'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
        'member1': [10, 20],
        'member2': [12, 22],
        'member3': [11, 21],
        'member4': [13, 23],
        'member5': [14, 24]
    }
    df = pd.DataFrame(data)
    
    stats_df = calculate_percentiles(df)
    
    assert 'date' in stats_df.columns
    assert 'p10' in stats_df.columns
    assert 'median' in stats_df.columns
    assert 'p90' in stats_df.columns
    
    assert len(stats_df) == 2
    assert stats_df['date'].iloc[0] == pd.to_datetime('2023-01-01')
    assert stats_df['date'].iloc[1] == pd.to_datetime('2023-01-02')

    # For row 0: [10, 12, 11, 13, 14]
    # Sorted: [10, 11, 12, 13, 14]
    # p10: 10 + 0.1*(14-10) = 10.4 (linear interpolation)
    # median: 12
    # p90: 14 - 0.1*(14-10) = 13.6 (linear interpolation)
    assert stats_df['p10'].iloc[0] == pytest.approx(10.4)
    assert stats_df['median'].iloc[0] == pytest.approx(12.0)
    assert stats_df['p90'].iloc[0] == pytest.approx(13.6)

    # For row 1: [20, 22, 21, 23, 24]
    # Sorted: [20, 21, 22, 23, 24]
    # p10: 20 + 0.1*(24-20) = 20.4
    # median: 22
    # p90: 24 - 0.1*(24-20) = 23.6
    assert stats_df['p10'].iloc[1] == pytest.approx(20.4)
    assert stats_df['median'].iloc[1] == pytest.approx(22.0)
    assert stats_df['p90'].iloc[1] == pytest.approx(23.6)

def test_calculate_percentiles_empty_df():
    df = pd.DataFrame(columns=['date', 'member1', 'member2'])
    stats_df = calculate_percentiles(df)
    assert stats_df.empty
    assert list(stats_df.columns) == ['date', 'p10', 'median', 'p90']

def test_calculate_percentiles_single_data_column():
    data = {
        'date': pd.to_datetime(['2023-01-01']),
        'member1': [10]
    }
    df = pd.DataFrame(data)
    stats_df = calculate_percentiles(df)
    assert stats_df['p10'].iloc[0] == pytest.approx(10.0)
    assert stats_df['median'].iloc[0] == pytest.approx(10.0)
    assert stats_df['p90'].iloc[0] == pytest.approx(10.0)

def test_calculate_percentiles_non_numeric_data_columns():
    data = {
        'date': pd.to_datetime(['2023-01-01']),
        'member1': [10],
        'member2': ['a']
    }
    df = pd.DataFrame(data)
    with pytest.raises(TypeError):
        calculate_percentiles(df)
