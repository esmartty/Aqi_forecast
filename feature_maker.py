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
    Adds lagged rolling rainfall features based on previous hours/days.

    Parameters:
    df_synop_data (pd.DataFrame): DataFrame containing:
        - measurement_hour_dt
        - rainfall_sum

    Returns:
    pd.DataFrame: DataFrame with rainfall rolling features.
    """

    rain_lag_days = [1, 3, 7]
    rain_lag_hours = [2, 3, 4, 5, 6]

    df = df_synop_data.copy()

    df = df.sort_values("measurement_hour_dt")

     # Days pollen series
    rain_days = (
                    df.groupby("date")["rainfall_sum"]
                        .sum()
                )

    # Rolling by days
    for lag in rain_lag_days:
        out = f"rainfall_sum_last_{lag}_day(s)"

        rolling_rainfall_sum = (
            rain_days
            .rolling(window=lag, min_periods=1)
            .sum()
        )

        df[out] = df["date"].map(rolling_rainfall_sum)

    # Hourly rainfall series
    rain_horly = (
        df.set_index("measurement_hour_dt")["rainfall_sum"]
        #.shift(1)   # exclude current hour
    )

    # Rolling by hours
    for hour in rain_lag_hours:
        col = f"rainfall_sum_last_{hour}_hour()"

        df[col] = (
            rain_horly
            .rolling(f"{hour}h", min_periods=1)
            .sum()
            .reindex(df["measurement_hour_dt"])
            .values
        )

    return df


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

def add_gdd_features(df_synop_data):
    """
    This function takes a DataFrame of synoptic data and adds features related to Growing Degree Days (GDD) based on temperature.
    GDD=max(avg_temp−base,0)
    Parameters:
    synop_data (pd.DataFrame): A DataFrame containing synoptic data with at least 'temperature' and 'date' columns.
    Returns:
    pd.DataFrame: The original DataFrame with additional GDD feature columns.
    """
    temp_base_list = [0, 5, 10]
    gdd_lag_days = [2, 7, 14, 30]

    df = df_synop_data.copy()
    df = df.sort_values("date")

    # Calculate daily GDD for each temperature base
    for temp_base in temp_base_list:
        gdd_col = f"GDD_temp_base_{temp_base}"

        df[gdd_col] = (
            df["avg_temp_today"] - temp_base
        ).clip(lower=0)
        #df[gdd_col] = df['avg_temp_today'].apply(lambda x: max(x - temp_base, 0))

        daily_gdd  = (
                    df.groupby("date")[gdd_col]
                      .first()
                )
        
    # Calculate rolling cumulative GDD
        for lag in gdd_lag_days:
            out = f"cum_GDD_temp_base_{temp_base}_since_{lag}_day(s)_ago"

            rolling_gdd = (
                daily_gdd
                .rolling(window=lag, min_periods=1)
                .sum()
            )

            df[out] = df["date"].map(rolling_gdd)

    return df


def sort_values(df):
    return df.sort_values(
        by='measurement_hour_dt', ascending=False, ignore_index=True)


def add_pollen_features(df_pollen_raw):
    """
    This function takes a DataFrame of pollen data and adds features to it, such as avg_pollen_today, min_pollen_today, max_pollen_today, daily_pollen_sum.
    Adds lagged rolling grass_pollen features based on previous days/hours.

    Parameters:
    pollen_data (pd.DataFrame): A DataFrame containing pollen data with at least 'date', 'ftime and 'grass_pollen' columns.

    Returns:
    pd.DataFrame: The original DataFrame with additional feature columns.
    """
    
    df_pollen_raw['fdate'] = pd.to_datetime(df_pollen_raw['fdate'], format='%Y-%m-%d')
    df_pollen_raw['measurement_hour_dt'] = df_pollen_raw['fdate'] + pd.to_timedelta(df_pollen_raw['ftime'].astype(str))
    df_pollen_raw['ftime'] = pd.to_datetime(df_pollen_raw['ftime'], format='%H:%M:%S').dt.time

    df_pollen_data = df_pollen_raw[['fdate', 'ftime', 'measurement_hour_dt', 'grass_pollen']].copy()


    avg_pollen_by_camps_ids = (
        df_pollen_data
        .groupby(['fdate', 'ftime'], as_index=False)[['measurement_hour_dt', 'grass_pollen']]
        .mean()
    )

    #print("avg_pollen_by_camps_ids", avg_pollen_by_camps_ids.tail(48))

    avg_pollen_today = (
        avg_pollen_by_camps_ids
        .groupby('fdate')['grass_pollen']
        .mean()
        .rename('avg_pollen_today')
    )

    min_pollen_today = (
        avg_pollen_by_camps_ids
        .groupby('fdate')['grass_pollen']
        .min()
        .rename('min_pollen_today')
    )

    max_pollen_today = (
        avg_pollen_by_camps_ids
        .groupby('fdate')['grass_pollen']
        .max()
        .rename('max_pollen_today')
    )

    daily_pollen_sum = (
        avg_pollen_by_camps_ids
        .groupby('fdate')['grass_pollen']
        .sum()
        .rename('daily_pollen_sum')
    )

    # Add the per-day averages as columns on the existing dataframe
    avg_pollen_by_camps_ids['avg_pollen_today'] = (
        avg_pollen_by_camps_ids['fdate'].map(avg_pollen_today)
    )

    avg_pollen_by_camps_ids['min_pollen_today'] = (
        avg_pollen_by_camps_ids['fdate'].map(min_pollen_today)
    )

    avg_pollen_by_camps_ids['max_pollen_today'] = (
        avg_pollen_by_camps_ids['fdate'].map(max_pollen_today)
    )

    avg_pollen_by_camps_ids['daily_pollen_sum'] = (
        avg_pollen_by_camps_ids['fdate'].map(daily_pollen_sum) 
    )

    pollen_lag_days = [2, 3, 7]
    pollen_lag_hours = [1, 3, 6, 12]
    
    df = avg_pollen_by_camps_ids.copy()

    df = df.sort_values("measurement_hour_dt")

    # Days pollen series
    pollen_days = (
                    df.groupby("fdate")["grass_pollen"]
                      .sum()
                )

    # Rolling by days
    for lag in pollen_lag_days:
        out = f"pollen_sum_last_{lag}_day(s)"

        rolling_pollen_sum = (
            pollen_days
            .rolling(window=lag, min_periods=1)
            .sum()
        )

        df[out] = df["fdate"].map(rolling_pollen_sum)

    # Hourly pollen series
    pollen_hourly = (
        df.set_index("measurement_hour_dt")["grass_pollen"]
        #.shift(1)   # exclude current hour
    )
    
    # Rolling by hours
    for hour in pollen_lag_hours:
        col = f"pollen_sum_last_{hour}_hour(s)"

        df[col] = (
            pollen_hourly
            .rolling(f"{hour}h", min_periods=1)
            .sum()
            .reindex(df["measurement_hour_dt"])
            .values
        )

    return df

def delete_missing_values(df):
    return df.dropna()

def make_features_pipeline(df_synop_raw = None, df_pollen_raw = None):
    weather_features = add_temperature_features(df_synop_raw)
    weather_features = add_rain_features(weather_features)
    weather_features = add_wind_features(weather_features)
    weather_features = add_gdd_features(weather_features)
    weather_features = delete_missing_values(weather_features)
    weather_features = sort_values(weather_features)
    # weather_features = add_pollen_features(df_pollen_raw)
    # weather_features = sort_values(weather_features)

    return weather_features


if __name__ == "__main__":
    from db_config import get_database_url, get_database_render_url
    from sqlalchemy import create_engine

    engine_render = create_engine(get_database_render_url(), pool_pre_ping=True)
    engine_local = create_engine(get_database_url(), pool_pre_ping=True)

    df_pollen_raw = pd.read_sql(
    "SELECT * FROM public.pollen_by_date_time",
    engine_local
)
    df_synop_raw_test = pd.read_sql(
        """SELECT * FROM public.synop_data
        ORDER BY date DESC, measurement_hour ASC """,
        engine_render
    )

    df_synop_features = make_features_pipeline(df_synop_raw_test, df_pollen_raw)

    with pd.option_context('display.max_columns', 100):
        print(df_synop_features[12:60]) 
