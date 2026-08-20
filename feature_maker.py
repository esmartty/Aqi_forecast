import numpy as np
import pandas as pd
import matplotlib.dates as mdates
from pathlib import Path


def add_temperature_features(df_synop_raw):
    """
    Add temperature features based on data available at prediction time.

    Features:
    - current available weather values
    - today's average/min/max temperature until now
    - previous N days average/min/max temperature

    Assumptions:
    - one row = one hourly measurement
    - weather data availability delay is fixed
    """

    WEATHER_DELAY_HOURS = 1
    TEMP_LAG_DAYS = [1, 3, 7, 14]

    df = df_synop_raw.copy()

    df["date"] = pd.to_datetime(df["date"])

    df["measurement_hour_dt"] = (
        df["date"]
        + pd.to_timedelta(df["measurement_hour"], unit="h")
    )

    df["measurement_hour"] = (
        df["measurement_hour_dt"].dt.time
    )

    df = df[
        [
            "date",
            "measurement_hour",
            "measurement_hour_dt",
            "temperature",
            "wind_speed",
            "wind_direction",
            "relative_humidity",
            "rainfall_sum",
            "pressure"
        ]
    ].copy()


    df = df.sort_values("measurement_hour_dt")


    # --------------------------------------------------
    # Weather values available at prediction time
    # --------------------------------------------------

    weather_cols = df.columns.difference(['date', 'measurement_hour', 'measurement_hour_dt'])

    for col in weather_cols:
        df[f"{col}_available"] = (
            df[col]
            .shift(WEATHER_DELAY_HOURS)
        )


    # --------------------------------------------------
    # Today's temperature statistics available until now
    # --------------------------------------------------

    temp_available = (
        df["temperature_available"]
    )

    df["avg_temp_today_available_until_now"] = (
        temp_available
        .groupby(df["date"])
        .expanding()
        .mean()
        .reset_index(level=0, drop=True)
    )

    df["min_temp_today_available_until_now"] = (
        temp_available
        .groupby(df["date"])
        .expanding()
        .min()
        .reset_index(level=0, drop=True)
    )

    df["max_temp_today_available_until_now"] = (
        temp_available
        .groupby(df["date"])
        .expanding()
        .max()
        .reset_index(level=0, drop=True)
    )

    # -------------------------------------------------
    # Count available weather hours current day
    # -------------------------------------------------

    df = df.set_index("measurement_hour_dt")

    df["hours_available_today"] = (
    df["temperature_available"]
    .notna()
    .groupby(df["date"])
    .cumsum()
)

    df = df.reset_index()

    # --------------------------------------------------
    # Historical daily temperature statistics
    # Completed days only
    # --------------------------------------------------

    daily_temp = (
        df.groupby("date")
        .agg(
            avg_temp=("temperature", "mean"),
            min_temp=("temperature", "min"),
            max_temp=("temperature", "max"),
            hours_available=("temperature", "count")
            )
    )

    for lag in TEMP_LAG_DAYS:

        lag_date = df["date"] - pd.Timedelta(days=lag)

        df[f"avg_temp_{lag}d_ago"] = lag_date.map(daily_temp["avg_temp"])

        df[f"min_temp_{lag}d_ago"] = lag_date.map(daily_temp["min_temp"])

        df[f"max_temp_{lag}d_ago"] = lag_date.map(daily_temp["max_temp"])
        
        df[f"hours_temp_{lag}d_ago"] = lag_date.map(daily_temp["hours_available"])

    for col in weather_cols:
        df.drop(columns=[col], inplace=True)

    return df


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

    rain_days = (
                    df.groupby("date")["rainfall_sum_available"]
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
        df.set_index("measurement_hour_dt")["rainfall_sum_available"]
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

    theta = np.radians(df_synop_data['wind_direction_available'])
    df_synop_data['wind_u'] = - df_synop_data['wind_speed_available'] * np.sin(theta)
    df_synop_data['wind_v'] = - df_synop_data['wind_speed_available'] * np.cos(theta)
    df_synop_data.drop(columns=['wind_direction_available'], inplace=True)

    return df_synop_data


def add_gdd_features(df_synop_data):
    """
    Add hourly Growing Degree Days (GDD) features.

    GDD is calculated using only temperature available
    at the prediction time.

    Assumptions:
    - One row = one hourly observation.
    - Prediction is made at the current timestamp.
    - temperature_available contains the latest temperature  that was available at prediction time.
    - GDD contribution is calculated for each available hour.

    Required columns:
    - date
    - measurement_hour_dt
    - temperature_available

    Returns:
    DataFrame with hourly GDD features.
    """

    temp_base_list = [0, 5, 10]
    gdd_lag_days = [2, 7, 14, 30]

    df = df_synop_data.copy()

    df["measurement_hour_dt"] = pd.to_datetime(
        df["measurement_hour_dt"]
    )

    df = df.sort_values("measurement_hour_dt")

    df = df.set_index("measurement_hour_dt")


    # --------------------------------------------------
    # GDD contribution for each available hour
    # --------------------------------------------------
    for temp_base in temp_base_list:

        gdd_col = f"GDD_temp_base_{temp_base}"

        # GDD contribution of current hour
        df[gdd_col] = (
            df["temperature_available"] - temp_base
        ).clip(lower=0)

        # --------------------------------------------------
        # GDD today until current timestamp
        # --------------------------------------------------

        out_today = (
            f"GDD_temp_base_{temp_base}_today_until_now"
        )

        df[out_today] = (
            df[gdd_col]
            .groupby(df["date"])
            .cumsum()
        )

        # --------------------------------------------------
        # GDD accumulated over recent time windows
        # --------------------------------------------------
        for lag in gdd_lag_days:

            out = (
                f"cum_GDD_temp_base_{temp_base}_"
                f"last_{lag}_day(s)_until_available"
            )

            df[out] = (
                df[gdd_col]
                .rolling(
                    window=f"{lag}D",
                    min_periods=1
                )
                .sum()
            )


    df = df.reset_index()

    return df

def sort_values(df):
    return df.sort_values(
        by='measurement_hour_dt', ascending=True, ignore_index=True)


def round_numeric_columns(df, decimal_places=2):
    """
    Rounds all numeric columns in the DataFrame to the specified number of decimal places.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    decimal_places (int): The number of decimal places to round to.

    Returns:
    pd.DataFrame: The DataFrame with rounded numeric columns.
    """
    numeric_cols = df.select_dtypes(['int', 'float']).columns
    df[numeric_cols] = df[numeric_cols].round(decimal_places)
    return df


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

    df_grass_pollen_data = df_pollen_raw[['fdate', 'ftime', 'measurement_hour_dt', 'grass_pollen']].copy()


    # --------------------------------------------------
    # Average pollen across monitoring stations
    # for each hour
    # --------------------------------------------------
    df = (
        df_grass_pollen_data
        .groupby(['fdate', 'ftime'], as_index=False)[['measurement_hour_dt', 'grass_pollen']]
        .mean()
    )

    df = df.sort_values("measurement_hour_dt")

    # --------------------------------------------------
    # Pollen available at prediction time
    # --------------------------------------------------

    df["grass_pollen_available"] = (
        df["grass_pollen"].shift(1)
    )

    # --------------------------------------------------
    # Current-day pollen statistics
    # until current timestamp
    # --------------------------------------------------

    df["avg_pollen_today_until_now"] = (
        df.groupby("fdate")["grass_pollen_available"]
        .expanding()
        .mean()
        .reset_index(level=0, drop=True)
        .values
    )

    df["min_pollen_today_until_now"] = (
        df.groupby("fdate")["grass_pollen_available"]
        .expanding()
        .min()
        .reset_index(level=0, drop=True)
        .values
    )

    df["max_pollen_today_until_now"] = (
        df.groupby("fdate")["grass_pollen_available"]
        .expanding()
        .max()
        .reset_index(level=0, drop=True)
        .values
    )

    df["pollen_sum_today_until_now"] = (
        df.groupby("fdate")["grass_pollen_available"]
        .cumsum()
    )

    # --------------------------------------------------
    # Historical daily pollen
    # --------------------------------------------------

    pollen_days = (
        df.groupby("fdate")["grass_pollen"]
        .sum()
    )
    pollen_lag_days = [2, 3, 7]

    # Rolling by days
    for lag in pollen_lag_days:
        out = f"pollen_sum_last_{lag}_day(s)"

        rolling_pollen_sum = (
            pollen_days
            .rolling(window=lag, min_periods=1)
            .sum()
        )

        df[out] = df["fdate"].map(rolling_pollen_sum)

    df.drop(columns=['grass_pollen'], inplace=True)

    # --------------------------------------------------
    # Historical hourly pollen
    # --------------------------------------------------
    pollen_hourly = (
        df.set_index("measurement_hour_dt")["grass_pollen_available"]
        #.shift(1)   # exclude current hour
    )

    pollen_lag_hours = [1, 3, 6, 12]
    
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


def merge_weather_and_pollen_features(df_weather, df_pollen):
    """
    Merges weather and pollen features on the date and measurement hour.

    Parameters:
    df_weather (pd.DataFrame): DataFrame containing weather features.
    df_pollen (pd.DataFrame): DataFrame containing pollen features.

    Returns:
    pd.DataFrame: Merged DataFrame with both weather and pollen features.
    """
    merged_df = pd.merge(
        df_weather,
        df_pollen,
        on=['measurement_hour_dt'],
        how='inner'
    )

    merged_df.drop(columns=['fdate', 'ftime'], inplace=True)

    return merged_df


def add_seasonal_features(merged_features):
    """
    Adds seasonal features to the DataFrame based on the date.

    Parameters:
    merged_features (pd.DataFrame): DataFrame containing a 'date' column.

    Returns:
    pd.DataFrame: DataFrame with additional seasonal feature columns.
    """
    merged_features['month'] = merged_features['date'].dt.month
    merged_features['day_of_year'] = merged_features['date'].dt.dayofyear
    merged_features['hour_of_day'] = merged_features['measurement_hour_dt'].dt.hour

    #adding a sine and cosine transformation for seasonality
    merged_features['sin_day_of_year'] = np.sin(2 * np.pi * merged_features['day_of_year'] / 365.25)
    merged_features['cos_day_of_year'] = np.cos(2 * np.pi * merged_features['day_of_year'] / 365.25)
    merged_features['sin_hour_of_day'] = np.sin(2 * np.pi * merged_features['hour_of_day'] / 24)
    merged_features['cos_hour_of_day'] = np.cos(2 * np.pi * merged_features['hour_of_day'] / 24)

    merged_features.drop(columns=['day_of_year', 'hour_of_day'], inplace=True)

    return merged_features


def make_features_pipeline(df_synop_raw = None, df_pollen_raw = None):
    weather_features = add_temperature_features(df_synop_raw)
    weather_features = add_rain_features(weather_features)
    weather_features = add_wind_features(weather_features)
    weather_features = add_gdd_features(weather_features)
    weather_features = delete_missing_values(weather_features)
    weather_features = sort_values(weather_features)

    pollen_features = add_pollen_features(df_pollen_raw)
    pollen_features = delete_missing_values(pollen_features)
    pollen_features = sort_values(pollen_features)

    merged_features = merge_weather_and_pollen_features(weather_features, pollen_features)
    merged_features = add_seasonal_features(merged_features)
    merged_features = round_numeric_columns(merged_features)

    return merged_features


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

    df_with_all_features = make_features_pipeline(df_synop_raw_test, df_pollen_raw)

    with pd.option_context('display.max_columns', 100):
        print("merged_features columns", df_with_all_features.columns)

    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"df_with_all_features_{timestamp}.csv"
    df_with_all_features.to_csv(output_path, index=False)
    print(f"Saved df_with_all_features to {output_path}")
 
