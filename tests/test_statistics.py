import pandas as pd
import pytest
import numpy as np
from src.open_meteo_cast.statistics import calculate_percentiles, calculate_precipitation_statistics, calculate_octa_probabilities, calculate_wind_direction_probabilities

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

    # For row 0: [10, 11, 12, 13, 14]
    # p10: 10.4
    # median: 12
    # p90: 13.6
    assert stats_df['p10'].iloc[0] == pytest.approx(10.4)
    assert stats_df['median'].iloc[0] == pytest.approx(12.0)
    assert stats_df['p90'].iloc[0] == pytest.approx(13.6)

    # For row 1: [20, 21, 22, 23, 24]
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

def test_calculate_octa_probabilities_basic():
    data = {
        'member1': [0, 1, 2, 3, 4, 5, 6, 7, 8],
        'member2': [0, 0, 1, 2, 3, 4, 5, 6, 7],
        'member3': [0, 0, 0, 1, 2, 3, 4, 5, 6],
        'member4': [0, 0, 0, 0, 1, 2, 3, 4, 5],
        'member5': [0, 0, 0, 0, 0, 1, 2, 3, 4]
    }
    index = pd.to_datetime([f'2023-01-01 0{i}:00:00' for i in range(9)])
    df = pd.DataFrame(data, index=index)

    stats_df = calculate_octa_probabilities(df)

    assert isinstance(stats_df.index, pd.DatetimeIndex)
    assert 'octa_0_prob' in stats_df.columns
    assert 'octa_8_prob' in stats_df.columns
    assert len(stats_df) == 9

    # Example for the first row: [0, 0, 0, 0, 0]
    # Probability of octa 0 should be 1.0
    assert stats_df['octa_0_prob'].iloc[0] == pytest.approx(1.0)
    for i in range(1, 9):
        assert stats_df[f'octa_{i}_prob'].iloc[0] == pytest.approx(0.0)

    # Example for the last row: [8, 7, 6, 5, 4]
    # Probability of octa 8 should be 1/5 = 0.2
    assert stats_df['octa_8_prob'].iloc[8] == pytest.approx(0.2)
    assert stats_df['octa_7_prob'].iloc[8] == pytest.approx(0.2)
    assert stats_df['octa_6_prob'].iloc[8] == pytest.approx(0.2)
    assert stats_df['octa_5_prob'].iloc[8] == pytest.approx(0.2)
    assert stats_df['octa_4_prob'].iloc[8] == pytest.approx(0.2)
    for i in range(4):
        assert stats_df[f'octa_{i}_prob'].iloc[8] == pytest.approx(0.0)

def test_calculate_octa_probabilities_empty_df():
    df = pd.DataFrame(columns=[f'member{i}' for i in range(5)])
    stats_df = calculate_octa_probabilities(df)
    assert stats_df.empty

def test_calculate_octa_probabilities_single_data_column():
    data = {
        'member1': [3]
    }
    index = pd.to_datetime(['2023-01-01'])
    df = pd.DataFrame(data, index=index)
    stats_df = calculate_octa_probabilities(df)
    assert stats_df['octa_3_prob'].iloc[0] == pytest.approx(1.0)
    for i in range(9):
        if i != 3:
            assert stats_df[f'octa_{i}_prob'].iloc[0] == pytest.approx(0.0)

def test_calculate_wind_direction_probabilities_basic():
    data = {
        'member1': [0, 45, 90, 135, 180, 225, 270, 315],
        'member2': [360, 44, 89, 134, 179, 224, 269, 314],
        'member3': [22.4, 67.4, 112.4, 157.4, 202.4, 247.4, 292.4, 337.4],
        'member4': [22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.5, 337.5],
        'member5': [337.5, 22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.5]
    }
    index = pd.to_datetime([f'2023-01-01 0{i}:00:00' for i in range(8)])
    df = pd.DataFrame(data, index=index)

    stats_df = calculate_wind_direction_probabilities(df)

    assert isinstance(stats_df.index, pd.DatetimeIndex)
    assert 'N_prob' in stats_df.columns
    assert 'NW_prob' in stats_df.columns
    assert len(stats_df) == 8

    # Test probabilities for each octant
    assert stats_df['N_prob'].iloc[0] == pytest.approx(0.8)  # 4/5 members show North
    assert stats_df['NE_prob'].iloc[1] == pytest.approx(0.8) # 4/5 members show North-East
    assert stats_df['E_prob'].iloc[2] == pytest.approx(0.8)
    assert stats_df['SE_prob'].iloc[3] == pytest.approx(0.8)
    assert stats_df['S_prob'].iloc[4] == pytest.approx(0.8)
    assert stats_df['SW_prob'].iloc[5] == pytest.approx(0.8)
    assert stats_df['W_prob'].iloc[6] == pytest.approx(0.8)
    assert stats_df['NW_prob'].iloc[7] == pytest.approx(0.8)

def test_calculate_wind_direction_probabilities_empty_df():
    df = pd.DataFrame(columns=[f'member{i}' for i in range(5)])
    stats_df = calculate_wind_direction_probabilities(df)
    assert stats_df.empty

def test_calculate_wind_direction_probabilities_single_data_column():
    data = {
        'member1': [190] # South
    }
    index = pd.to_datetime(['2023-01-01'])
    df = pd.DataFrame(data, index=index)
    stats_df = calculate_wind_direction_probabilities(df)
    assert stats_df['S_prob'].iloc[0] == pytest.approx(1.0)
    for octant in ['N', 'NE', 'E', 'SE', 'SW', 'W', 'NW']:
        assert stats_df[f'{octant}_prob'].iloc[0] == pytest.approx(0.0)