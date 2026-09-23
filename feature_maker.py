import numpy as np
import pandas as pd
import matplotlib.dates as mdates
from pathlib import Path


def add_temperature_features(
    df_synop_raw,
    weather_delay_hours=1,
    temp_lag_days=(1, 3, 7, 14),
):
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
    weather_by_time = df.set_index("measurement_hour_dt")
    available_time = (
        df["measurement_hour_dt"]
        - pd.to_timedelta(weather_delay_hours, unit="h")
    )

    for col in weather_cols:
        df[f"{col}_available"] = (
            available_time.map(weather_by_time[col])
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

    for lag in temp_lag_days:

        lag_date = df["date"] - pd.Timedelta(days=lag)

        df[f"avg_temp_{lag}d_ago"] = lag_date.map(daily_temp["avg_temp"])

        df[f"min_temp_{lag}d_ago"] = lag_date.map(daily_temp["min_temp"])

        df[f"max_temp_{lag}d_ago"] = lag_date.map(daily_temp["max_temp"])
        
        df[f"hours_temp_{lag}d_ago"] = lag_date.map(daily_temp["hours_available"])

    for col in weather_cols:
        df.drop(columns=[col], inplace=True)

    temperature_series = df.set_index("measurement_hour_dt")["temperature_available"].sort_index()
    for window in (12, 24):
        temp_mean = temperature_series.rolling(f"{window}h", min_periods=1).mean()
        df[f"temperature_mean_last_{window}h"] = (
            temp_mean.reindex(df["measurement_hour_dt"]).to_numpy()
        )

    return df


def add_rain_features(
    df_synop_data,
    rain_lag_days=(1, 3, 7),
    rain_lag_hours=(2, 3, 4, 5, 6),
):
    """
    Adds lagged rolling rainfall features based on previous hours/days.

    Parameters:
    df_synop_data (pd.DataFrame): DataFrame containing:
        - measurement_hour_dt
        - rainfall_sum

    Returns:
    pd.DataFrame: DataFrame with rainfall rolling features.
    """

    df = df_synop_data.copy()

    df = df.sort_values("measurement_hour_dt")

    rain_days = (
                    df.groupby("date")["rainfall_sum_available"]
                        .sum()
                )
    rain_days = rain_days.reindex(
        pd.date_range(rain_days.index.min(), rain_days.index.max(), freq="D")
    )

    # Rolling by days
    for lag in rain_lag_days:
        out = f"rainfall_sum_last_{lag}_day(s)"

        rolling_rainfall_sum = (
            rain_days
            .shift(1)
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

    rainfall_series = df.set_index("measurement_hour_dt")["rainfall_sum_available"].sort_index()
    for window in (6, 12):
        rain_mean = rainfall_series.rolling(f"{window}h", min_periods=1).mean()
        df[f"rainfall_mean_last_{window}h"] = (
            rain_mean.reindex(df["measurement_hour_dt"]).to_numpy()
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


def add_gdd_features(
    df_synop_data,
    temp_base_list=(0, 5, 10),
    gdd_lag_days=(2, 7, 14, 30),
):
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


def add_pollen_trend_features(df_pollen):
    df = df_pollen.copy().sort_values("measurement_hour_dt").reset_index(drop=True)
    pollen_col = "grass_pollen_available"
    pollen_series = df.set_index("measurement_hour_dt")[pollen_col].sort_index()

    for lag in (1, 3, 6):
        df[f"pollen_delta_{lag}h"] = df[pollen_col] - df[pollen_col].shift(lag)

    df["pollen_acceleration_3h"] = df["pollen_delta_3h"] - df["pollen_delta_6h"]

    for window in (6, 12, 24):
        rolling_mean = pollen_series.rolling(f"{window}h", min_periods=1).mean()
        rolling_std = pollen_series.rolling(f"{window}h", min_periods=1).std().fillna(0)
        rolling_max = pollen_series.rolling(f"{window}h", min_periods=1).max()
        rolling_min = pollen_series.rolling(f"{window}h", min_periods=1).min()
        df[f"pollen_mean_last_{window}h"] = rolling_mean.reindex(df["measurement_hour_dt"]).to_numpy()
        df[f"pollen_std_last_{window}h"] = rolling_std.reindex(df["measurement_hour_dt"]).to_numpy()
        if window == 24:
            df["pollen_max_last_24h"] = rolling_max.reindex(df["measurement_hour_dt"]).to_numpy()
            df["pollen_min_last_24h"] = rolling_min.reindex(df["measurement_hour_dt"]).to_numpy()

    for window in (12, 24):
        delta = pollen_series.diff().fillna(0)
        slope = delta.rolling(f"{window}h", min_periods=1).mean()
        df[f"pollen_slope_last_{window}h"] = slope.reindex(df["measurement_hour_dt"]).to_numpy()

    current_to_24h_ratio = pollen_series / pollen_series.rolling("24h", min_periods=1).mean()
    df["pollen_ratio_current_to_24h_avg"] = current_to_24h_ratio.reindex(df["measurement_hour_dt"]).to_numpy()
    return df


def add_pollen_features(
    df_pollen_raw,
    pollen_lag_days=(2, 3, 7),
    pollen_lag_hours=(1, 3, 6, 12),
    target_parameter="grass_pollen",
):
    """
    This function takes a DataFrame of pollen data and adds features to it, such as avg_pollen_today, min_pollen_today, max_pollen_today, daily_pollen_sum.
    Adds lagged rolling grass_pollen features based on previous days/hours.

    Parameters:
    pollen_data (pd.DataFrame): A DataFrame containing pollen data with at least 'date', 'ftime and target_pollen columns.

    Returns:
    pd.DataFrame: The original DataFrame with additional feature columns.
    """
    df_pollen_raw = df_pollen_raw.copy()
    df_pollen_raw['fdate'] = pd.to_datetime(df_pollen_raw['fdate'], format='%Y-%m-%d')
    df_pollen_raw['measurement_hour_dt'] = df_pollen_raw['fdate'] + pd.to_timedelta(df_pollen_raw['ftime'].astype(str))
    df_pollen_raw['ftime'] = pd.to_datetime(df_pollen_raw['ftime'], format='%H:%M:%S').dt.time

    df_pollen_data = df_pollen_raw[
        ['fdate', 'ftime', 'measurement_hour_dt', target_parameter]
    ].copy()


    # --------------------------------------------------
    # Average pollen across monitoring stations
    # for each hour
    # --------------------------------------------------
    df = (
        df_pollen_data
        .groupby(
            ['fdate', 'ftime'],
            as_index=False,
        )[['measurement_hour_dt', target_parameter]]
        .mean()
    )

    df = df.sort_values("measurement_hour_dt")

    # --------------------------------------------------
    # Pollen available at prediction time
    # --------------------------------------------------

    available_target = f"{target_parameter}_available"
    pollen_by_time = df.set_index("measurement_hour_dt")
    available_time = df["measurement_hour_dt"] - pd.Timedelta(hours=1)
    df[available_target] = (
        available_time.map(pollen_by_time[target_parameter])
    )

    # --------------------------------------------------
    # Current-day pollen statistics
    # until current timestamp
    # --------------------------------------------------

    df["avg_pollen_today_until_now"] = (
        df.groupby("fdate")[available_target]
        .expanding()
        .mean()
        .reset_index(level=0, drop=True)
        .values
    )

    df["min_pollen_today_until_now"] = (
        df.groupby("fdate")[available_target]
        .expanding()
        .min()
        .reset_index(level=0, drop=True)
        .values
    )

    df["max_pollen_today_until_now"] = (
        df.groupby("fdate")[available_target]
        .expanding()
        .max()
        .reset_index(level=0, drop=True)
        .values
    )

    df["pollen_sum_today_until_now"] = (
        df.groupby("fdate")[available_target]
        .cumsum()
    )

    # --------------------------------------------------
    # Historical daily pollen
    # --------------------------------------------------

    pollen_days = (
        df.groupby("fdate")[target_parameter]
        .sum()
    )
    pollen_days = pollen_days.reindex(
        pd.date_range(pollen_days.index.min(), pollen_days.index.max(), freq="D")
    )
    # Rolling by days
    for lag in pollen_lag_days:
        out = f"pollen_sum_last_{lag}_day(s)"

        rolling_pollen_sum = (
            pollen_days
            .shift(1)
            .rolling(window=lag, min_periods=1)
            .sum()
        )

        df[out] = df["fdate"].map(rolling_pollen_sum)

    #df.drop(columns=['grass_pollen'], inplace=True)

    # --------------------------------------------------
    # Historical hourly pollen
    # --------------------------------------------------
    pollen_hourly = (
        df.set_index("measurement_hour_dt")[available_target]
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

    df = add_pollen_trend_features(df)
    return df

def add_weather_interaction_features(df):
    df = df.copy()

    if "relative_humidity_available" in df.columns:
        df["humidity_x_pollen_level"] = (
            df["relative_humidity_available"] * df["grass_pollen_available"]
        )
        df["rainfall_x_pollen_trend"] = (
            df["rainfall_sum_available"] * df.get("pollen_delta_1h", 0.0)
        )

    if "temperature_available" in df.columns and "cum_GDD_temp_base_5_last_30_day(s)_until_available" in df.columns:
        df["temperature_x_gdd_x_pollen"] = (
            df["temperature_available"]
            * df["cum_GDD_temp_base_5_last_30_day(s)_until_available"]
            * df["grass_pollen_available"]
        )

    if {"wind_u", "wind_v", "relative_humidity_available", "grass_pollen_available"}.issubset(df.columns):
        df["wind_u_humidity_pollen"] = (
            df["wind_u"] * df["relative_humidity_available"] * df["grass_pollen_available"]
        )
        df["wind_v_humidity_pollen"] = (
            df["wind_v"] * df["relative_humidity_available"] * df["grass_pollen_available"]
        )

    if {"rainfall_sum_last_7_day(s)", "relative_humidity_available", "cos_day_of_year"}.issubset(df.columns):
        df["rain_humidity_dayofyear_interaction"] = (
            df["rainfall_sum_last_7_day(s)"]
            * df["relative_humidity_available"]
            * df["cos_day_of_year"]
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


class FeatureMaker:
    """Create features using a shared, configurable feature definition."""

    def __init__(
        self,
        weather_delay_hours=1,
        temp_lag_days=(1, 3, 7, 14),
        rain_lag_days=(1, 3, 7),
        rain_lag_hours=(2, 3, 4, 5, 6, 12, 24),
        temp_base_list=(0, 5, 10),
        gdd_lag_days=(2, 7, 14, 30),
        pollen_lag_days=(2, 3, 7),
        pollen_lag_hours=(1, 3, 6, 12),
        pollen_target="grass_pollen",
    ):
        self.weather_delay_hours = weather_delay_hours
        self.temp_lag_days = tuple(temp_lag_days)
        self.rain_lag_days = tuple(rain_lag_days)
        self.rain_lag_hours = tuple(rain_lag_hours)
        self.temp_base_list = tuple(temp_base_list)
        self.gdd_lag_days = tuple(gdd_lag_days)
        self.pollen_lag_days = tuple(pollen_lag_days)
        self.pollen_lag_hours = tuple(pollen_lag_hours)
        self.pollen_target = pollen_target

    def make_features_pipeline(self, df_synop_raw=None, df_pollen_raw=None):
        weather_features = add_temperature_features(
            df_synop_raw,
            weather_delay_hours=self.weather_delay_hours,
            temp_lag_days=self.temp_lag_days,
        )
        weather_features = add_rain_features(
            weather_features,
            rain_lag_days=self.rain_lag_days,
            rain_lag_hours=self.rain_lag_hours,
        )
        weather_features = add_wind_features(weather_features)
        weather_features = add_gdd_features(
            weather_features,
            temp_base_list=self.temp_base_list,
            gdd_lag_days=self.gdd_lag_days,
        )
        weather_features = delete_missing_values(weather_features)
        weather_features = sort_values(weather_features)

        pollen_features = add_pollen_features(
            df_pollen_raw,
            pollen_lag_days=self.pollen_lag_days,
            pollen_lag_hours=self.pollen_lag_hours,
            target_parameter=self.pollen_target,
        )
        pollen_features = delete_missing_values(pollen_features)
        pollen_features = sort_values(pollen_features)

        merged_features = merge_weather_and_pollen_features(
            weather_features,
            pollen_features,
        )
        merged_features = add_seasonal_features(merged_features)
        merged_features = add_weather_interaction_features(merged_features)
        if "relative_humidity_available" in merged_features.columns:
            humidity_series = merged_features.set_index("measurement_hour_dt")["relative_humidity_available"].sort_index()
            for window in (12,):
                humidity_trend = humidity_series.diff().fillna(0).rolling(f"{window}h", min_periods=1).mean()
                merged_features[f"humidity_trend_last_{window}h"] = (
                    humidity_trend.reindex(merged_features["measurement_hour_dt"]).to_numpy()
                )

        if "rainfall_sum_available" in merged_features.columns:
            rainfall_series = merged_features.set_index("measurement_hour_dt")["rainfall_sum_available"].sort_index()
            for window in (6, 12):
                rain_window_mean = rainfall_series.rolling(f"{window}h", min_periods=1).sum()
                merged_features[f"rainfall_previous_{window}h"] = (
                    rain_window_mean.reindex(merged_features["measurement_hour_dt"]).to_numpy()
                )

        return round_numeric_columns(merged_features)


def make_features_pipeline(df_synop_raw = None, df_pollen_raw = None):
    return FeatureMaker().make_features_pipeline(df_synop_raw, df_pollen_raw)


if __name__ == "__main__":
    from db_config import get_database_url, get_database_render_url
    from sqlalchemy import create_engine

    engine_render = create_engine(get_database_render_url(), pool_pre_ping=True)
    engine_local = create_engine(get_database_url(), pool_pre_ping=True)

    df_pollen_raw = pd.read_sql(
    "SELECT * FROM public.pollen_by_date_time",
    engine_local
)
    df_synop_raw = pd.read_sql(
        """SELECT * FROM public.synop_data
        ORDER BY date DESC, measurement_hour ASC """,
        engine_render
    )

    df_with_all_features = make_features_pipeline(df_synop_raw, df_pollen_raw)

    with pd.option_context('display.max_columns', 100):
        print("merged_features columns", df_with_all_features.columns)

    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"df_with_all_features_{timestamp}.csv"
    df_with_all_features.to_csv(output_path, index=False)
    print(f"Saved df_with_all_features to {output_path}")
 
