import numpy as np
import pandas as pd
import matplotlib.dates as mdates

def add_temperature_features(df_synop_raw):
    """
    This function takes a DataFrame of synoptic data and adds features to it, such as average temperature for today and lagged average temperatures for previous days.

    Parameters:
    synop_data (pd.DataFrame): A DataFrame containing synoptic data with at least 'date' and 'temperature' columns.

    Returns:
    pd.DataFrame: The original DataFrame with additional feature columns.
    """
    
    df_synop_raw['date'] = pd.to_datetime(df_synop_raw['date'], format='%Y-%m-%d')
    df_synop_raw['measurement_hour_dt'] = df_synop_raw['date'] + pd.to_timedelta(df_synop_raw['measurement_hour'], unit='h')
    df_synop_raw['measurement_hour'] = df_synop_raw['measurement_hour_dt'].dt.time
    df_synop_data = df_synop_raw[['date', 'measurement_hour', 'measurement_hour_dt', 'temperature', 'wind_speed', 'wind_direction', 'relative_humidity', 'rainfall_sum', 'pressure']].copy()

    avg_temp_today = (
        df_synop_data
        .groupby('date')['temperature']
        .mean()
        .rename('avg_temp_today')
        )

    min_temp_today = (
        df_synop_data
        .groupby('date')['temperature']
        .min()
        .rename('min_temp_today')
        )

    max_temp_today = (
        df_synop_data
        .groupby('date')['temperature']
        .max()
        .rename('max_temp_today')
        )

    # Add the per-day averages as columns on the existing dataframe
    df_synop_data['avg_temp_today'] = (
        df_synop_data['date'].map(avg_temp_today)
    )

    df_synop_data['min_temp_today'] = (
        df_synop_data['date'].map(min_temp_today)
    )

    df_synop_data['max_temp_today'] = (
        df_synop_data['date'].map(max_temp_today)
    )

    temp_lag_days = [1, 3, 7, 14]
    for day in temp_lag_days:
        df_synop_data[f'date_lag_{day}_day'] = df_synop_data['date'] - pd.Timedelta(days=day)
        df_synop_data[f'avg_temp_lag_{day}_day'] = (
                df_synop_data[f'date_lag_{day}_day'].map(avg_temp_today)
            )
        df_synop_data.drop(columns=[f'date_lag_{day}_day'], inplace=True)

    return df_synop_data


def add_rain_features(df_synop_data):
    """
    This function takes a DataFrame of synoptic data and adds features related to rainfall, such as lagged rainfall sums for previous days and hours.
    Parameters:
    synop_data (pd.DataFrame): A DataFrame containing synoptic data with at least 'date', 'measurement_hour_dt', and 'rainfall_sum' columns.
    Returns:
    pd.DataFrame: The original DataFrame with additional rainfall feature columns.
    """ 

    rain_lag_days = [1, 3, 7]
    for day in rain_lag_days:
        df_synop_data[f'date_lag_{day}_day'] = df_synop_data['date'] - pd.Timedelta(days=day)
        df_synop_data[f'rainfall_sum_lag_{day}_day'] = (
                df_synop_data[f'date_lag_{day}_day'].map(df_synop_data.groupby('date')['rainfall_sum'].sum())
            )
        df_synop_data.drop(columns=[f'date_lag_{day}_day'], inplace=True)

    rain_hour_lag_hours = [1, 3, 6]
    for hour in rain_hour_lag_hours:
        df_synop_data[f'measurement_hour_dt_lag_{hour}_hour'] = df_synop_data['measurement_hour_dt'] - pd.Timedelta(hours=hour)
        df_synop_data[f'rainfall_sum_lag_{hour}_hour'] = (
                df_synop_data[f'measurement_hour_dt_lag_{hour}_hour'].map(df_synop_data.groupby('measurement_hour_dt')['rainfall_sum'].sum())
            )
        df_synop_data.drop(columns=[f'measurement_hour_dt_lag_{hour}_hour'], inplace=True)

    return df_synop_data


def add_wind_features(df_synop_data):
    """
    This function takes a DataFrame of synoptic data and adds features related to wind, such as wind vector components (u and v) based on wind speed and direction.
    Parameters:
    synop_data (pd.DataFrame): A DataFrame containing synoptic data with at least 'wind_speed' and 'wind_direction' columns.
    Returns:
    pd.DataFrame: The original DataFrame with additional wind feature columns.
    """

    theta = np.radians(df_synop_data['wind_direction'])
    df_synop_data['wind_u'] = - df_synop_data['wind_speed'] * np.sin(theta)
    df_synop_data['wind_v'] = - df_synop_data['wind_speed'] * np.cos(theta)
    df_synop_data.drop(columns=['wind_direction'], inplace=True)

    df_synop_data.rename(columns={"wind_speed": "wind_speed_now"}, inplace=True)

    return df_synop_data


def sort_synop_values(df_synop_data):
    """
    This function takes a DataFrame of synoptic data and sorts it by measurement hour in descending order, keeping only the relevant columns.
    Parameters:
    synop_data (pd.DataFrame): A DataFrame containing synoptic data with at least 'measurement_hour_dt' and other relevant columns.
    Returns:
    pd.DataFrame: The sorted DataFrame with selected columns.
    """
    df_synop_data_sorted = df_synop_data.sort_values(
        by='measurement_hour_dt', ascending=False)[[
            'date', 
            'measurement_hour', 
            'measurement_hour_dt', 
            'temperature', 
            'avg_temp_today', 
            'min_temp_today', 
            'max_temp_today', 
            'avg_temp_lag_1_day', 
            'avg_temp_lag_3_day', 
            'avg_temp_lag_7_day', 
            'avg_temp_lag_14_day', 
            'wind_speed_now', 
            'wind_u', 
            'wind_v', 
            'rainfall_sum', 
            'rainfall_sum_lag_1_day', 
            'rainfall_sum_lag_3_day', 
            'rainfall_sum_lag_7_day', 
            'rainfall_sum_lag_1_hour', 
            'rainfall_sum_lag_3_hour', 
            'rainfall_sum_lag_6_hour', 
            'relative_humidity', 
            'pressure'
            ]]
    
    return df_synop_data_sorted

def delete_missing_values(df):
    return df.dropna()

def make_features_pipeline(df_synop_raw, df_pollen_raw = None):
    weather_features = add_temperature_features(df_synop_raw)
    weather_features = add_rain_features(weather_features)
    weather_features = add_wind_features(weather_features)
    weather_features = delete_missing_values(weather_features)
    weather_features = sort_synop_values(weather_features)

    return weather_features


if __name__ == "__main__":
    from db_config import get_database_render_url
    from sqlalchemy import create_engine
    engine_render = create_engine(get_database_render_url(), pool_pre_ping=True)
    df_synop_raw_test = pd.read_sql(
        """SELECT * FROM public.synop_data
        ORDER BY date DESC, measurement_hour ASC """,
        engine_render
    )
    df_synop_features = make_features_pipeline(df_synop_raw_test)
    print(df_synop_features.head(48)) 