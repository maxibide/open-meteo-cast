import pandas as pd
import pytest
from src.open_meteo_cast.statistics import calculate_percentiles, calculate_precipitation_statistics

def test_calculate_percentiles_basic():
    data = {
        'member1': [10, 20],
        'member2': [12, 22],
        'member3': [11, 21],
        'member4': [13, 23],
        'member5': [14, 24]
    }
    index = pd.to_datetime(['2023-01-01', '2023-01-02'])
    df = pd.DataFrame(data, index=index)
    
    stats_df = calculate_percentiles(df)
    
    assert isinstance(stats_df.index, pd.DatetimeIndex)
    assert 'p10' in stats_df.columns
    assert 'median' in stats_df.columns
    assert 'p90' in stats_df.columns
    
    assert len(stats_df) == 2
    assert stats_df.index[0] == pd.to_datetime('2023-01-01')
    assert stats_df.index[1] == pd.to_datetime('2023-01-02')

    # For row 0: [10, 12, 11, 13, 14]
    # Sorted: [10, 11, 12, 13, 14]
    # p10: 10.4
    # median: 12
    # p90: 13.6
    assert stats_df['p10'].iloc[0] == pytest.approx(10.4)
    assert stats_df['median'].iloc[0] == pytest.approx(12.0)
    assert stats_df['p90'].iloc[0] == pytest.approx(13.6)

    # For row 1: [20, 22, 21, 23, 24]
    # Sorted: [20, 21, 22, 23, 24]
    # p10: 20.4
    # median: 22
    # p90: 23.6
    assert stats_df['p10'].iloc[1] == pytest.approx(20.4)
    assert stats_df['median'].iloc[1] == pytest.approx(22.0)
    assert stats_df['p90'].iloc[1] == pytest.approx(23.6)

def test_calculate_precipitation_statistics_nan_to_zero():
    data = {
        'member1': [0, 0, 10],
        'member2': [0, -1, 12],
        'member3': [0, 0, 11]
    }
    index = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
    df = pd.DataFrame(data, index=index)

    stats_df = calculate_precipitation_statistics(df)

    # For row 0: [0, 0, 0] -> probability = 0, conditional_average = NaN (should be 0)
    assert stats_df['probability'].iloc[0] == pytest.approx(0.0)
    assert stats_df['conditional_average'].iloc[0] == pytest.approx(0.0)

    # For row 1: [0, -1, 0] -> probability = 0, conditional_average = NaN (should be 0)
    assert stats_df['probability'].iloc[1] == pytest.approx(0.0)
    assert stats_df['conditional_average'].iloc[1] == pytest.approx(0.0)

    # For row 2: [10, 12, 11] -> probability = 1, conditional_average = 11
    assert stats_df['probability'].iloc[2] == pytest.approx(1.0)
    assert stats_df['conditional_average'].iloc[2] == pytest.approx(11.0)

def test_calculate_percentiles_empty_df():
    df = pd.DataFrame(columns=['member1', 'member2'])
    stats_df = calculate_percentiles(df)
    assert stats_df.empty
    assert list(stats_df.columns) == ['p10', 'median', 'p90']

def test_calculate_percentiles_single_data_column():
    data = {
        'member1': [10]
    }
    index = pd.to_datetime(['2023-01-01'])
    df = pd.DataFrame(data, index=index)
    stats_df = calculate_percentiles(df)
    assert stats_df['p10'].iloc[0] == pytest.approx(10.0)
    assert stats_df['median'].iloc[0] == pytest.approx(10.0)
    assert stats_df['p90'].iloc[0] == pytest.approx(10.0)

def test_calculate_percentiles_non_numeric_data_columns():
    data = {
        'member1': [10],
        'member2': ['a']
    }
    index = pd.to_datetime(['2023-01-01'])
    df = pd.DataFrame(data, index=index)
    with pytest.raises(TypeError):
        calculate_percentiles(df)