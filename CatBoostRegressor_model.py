import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine
from sklearn.model_selection import TimeSeriesSplit
from catboost import CatBoostRegressor

from feature_maker import FeatureMaker
from db_config import get_database_url, get_database_render_url


TARGET = "grass_pollen"
HORIZONS = range(1, 13)
TRAIN_END = pd.Timestamp("2026-07-15 23:00:00")
TEST_START = pd.Timestamp("2026-07-16 00:00:00")

def main():
    engine_render = create_engine(get_database_render_url(), pool_pre_ping=True)
    engine_local = create_engine(get_database_url(), pool_pre_ping=True)

    df_pollen_raw = pd.read_sql(
        "SELECT * FROM public.pollen_by_date_time", engine_local
    )
    df_synop_raw = pd.read_sql(
        """SELECT * FROM public.synop_data
        ORDER BY date DESC, measurement_hour ASC """, engine_render
    )

    maker = FeatureMaker(pollen_target=TARGET)
    df = maker.make_features_pipeline(df_synop_raw, df_pollen_raw)
    df = df.sort_values("measurement_hour_dt").reset_index(drop=True)

    if df["measurement_hour_dt"].duplicated().any():
        raise ValueError("Feature data must contain unique prediction timestamps")

# ============================================================
# FEATURES
# ============================================================

    FEATURES = [
        col for col in df.columns
        if col not in {
            TARGET, "date", "measurement_hour", "measurement_hour_dt",
        }
    ]

# ============================================================
# TARGETS
# ============================================================
    models = {}
    test_data = {}
    prepared_data = {}

    for h in HORIZONS:
        target_col = f"target_{h}h"
        future_time = df["measurement_hour_dt"] + pd.Timedelta(hours=h)
        target_by_time = df.set_index("measurement_hour_dt")[TARGET]
        df[target_col] = future_time.map(target_by_time).to_numpy()

# ============================================================
# TRAIN / FINAL TEST
# ============================================================

        valid_train = (
            (df["measurement_hour_dt"] <= TRAIN_END)
            & (future_time <= TRAIN_END)
            & df[target_col].notna()
        )
        valid_test = (
            (df["measurement_hour_dt"] >= TEST_START)
            & df[target_col].notna()
        )

        X_train = df.loc[valid_train, FEATURES]

        y_train = df.loc[valid_train, target_col]

        X_test_h = df.loc[valid_test, FEATURES]

        y_test_h = df.loc[valid_test, target_col]

        prepared_data[h] = {
            "X_train": X_train,
            "y_train": y_train,
            "X_test": X_test_h,
            "y_test": y_test_h,
            "train_times": df.loc[
                valid_train,
                "measurement_hour_dt",
            ].reset_index(drop=True),
            "test_times": df.loc[
                valid_test,
                "measurement_hour_dt",
            ].reset_index(drop=True),
        }

    #The search evaluates these shared configurations: depth: [4, 6, 8], learning_rate: [0.03, 0.05].
    #For each configuration, it calculates aggregate time-series CV MAE across all 12 horizons and 4 folds. 
    #The best global depth and learning_rate are then used for every horizon.

    param_grid = [
        {"depth": 4, "learning_rate": 0.03},
        {"depth": 4, "learning_rate": 0.05},
        {"depth": 6, "learning_rate": 0.03},
        {"depth": 6, "learning_rate": 0.05},
        {"depth": 8, "learning_rate": 0.03},
        {"depth": 8, "learning_rate": 0.05},
    ]
    global_search_results = []

    for params in param_grid:
        all_fold_scores = []

        for h in HORIZONS:
            X_train = prepared_data[h]["X_train"]
            y_train = prepared_data[h]["y_train"]
            train_times = prepared_data[h]["train_times"]
            tscv = TimeSeriesSplit(n_splits=4, gap=h)

            for train_idx, val_idx in tscv.split(X_train):
                validation_start = train_times.iloc[val_idx].min()
                train_idx = train_idx[
                    (train_times.iloc[train_idx] + pd.Timedelta(hours=h)).to_numpy()
                    < validation_start
                ]
                if len(train_idx) == 0:
                    raise ValueError(f"CV training fold is empty for horizon {h}")

                model = CatBoostRegressor(
                    iterations=500,
                    depth=params["depth"],
                    learning_rate=params["learning_rate"],
                    loss_function="MAE",
                    verbose=False,
                    random_seed=42,
                )
                model.fit(
                    X_train.iloc[train_idx],
                    y_train.iloc[train_idx],
                    eval_set=(X_train.iloc[val_idx], y_train.iloc[val_idx]),
                    use_best_model=True,
                )
                predictions = model.predict(X_train.iloc[val_idx])
                all_fold_scores.append(
                    np.mean(
                        np.abs(y_train.iloc[val_idx].to_numpy() - predictions)
                    )
                )

        global_search_results.append({
            **params,
            "cv_mae": np.mean(all_fold_scores),
        })

    best_params = min(global_search_results, key=lambda result: result["cv_mae"])
    print(
        "Global parameters: "
        f"depth={best_params['depth']}, "
        f"learning_rate={best_params['learning_rate']}, "
        f"CV MAE={best_params['cv_mae']:.3f}"
    )

    cv_results = []

    for h in HORIZONS:
        X_train = prepared_data[h]["X_train"]
        y_train = prepared_data[h]["y_train"]
        train_times = prepared_data[h]["train_times"]
        tscv = TimeSeriesSplit(n_splits=4, gap=h)
        fold_scores = []
        best_iterations = []

        for train_idx, val_idx in tscv.split(X_train):
            validation_start = train_times.iloc[val_idx].min()
            train_idx = train_idx[
                (train_times.iloc[train_idx] + pd.Timedelta(hours=h)).to_numpy()
                < validation_start
            ]
            if len(train_idx) == 0:
                raise ValueError(f"CV training fold is empty for horizon {h}")

            model = CatBoostRegressor(
                iterations=500,
                depth=best_params["depth"],
                learning_rate=best_params["learning_rate"],
                loss_function="MAE",
                verbose=False,
                random_seed=42,
            )
            model.fit(
                X_train.iloc[train_idx],
                y_train.iloc[train_idx],
                eval_set=(X_train.iloc[val_idx], y_train.iloc[val_idx]),
                use_best_model=True,
            )
            predictions = model.predict(X_train.iloc[val_idx])
            fold_scores.append(
                np.mean(
                    np.abs(y_train.iloc[val_idx].to_numpy() - predictions)
                )
            )
            best_iterations.append(model.get_best_iteration())

        final_model = CatBoostRegressor(
            iterations=int(np.median(best_iterations)) + 1,
            depth=best_params["depth"],
            learning_rate=best_params["learning_rate"],
            loss_function="MAE",
            verbose=False,
            random_seed=42,
        )
        final_model.fit(X_train, y_train)
        models[h] = final_model
        test_data[h] = {
            "X_test": prepared_data[h]["X_test"],
            "y_test": prepared_data[h]["y_test"],
        }

        cv_mae = np.mean(fold_scores)
        cv_results.append({
            "horizon": h,
            "cv_mae": cv_mae,
            "best_iteration_mean": np.mean(best_iterations),
            "best_iteration_median": np.median(best_iterations),
        })
        print(
            f"Horizon +{h}h | CV MAE = {cv_mae:.3f} | "
            f"best iteration = {np.median(best_iterations):.0f}"
        )


# ============================================================
# Final Test
# ============================================================

    final_results = []
    final_predictions = {}
    visualization_rows = []

    for h in HORIZONS:

        model = models[h]
        X_test_h = test_data[h]["X_test"]
        y_test_h = test_data[h]["y_test"]


        predictions = model.predict(X_test_h)
        final_predictions[h] = predictions

        train_times = prepared_data[h]["train_times"]
        test_times = prepared_data[h]["test_times"]
        y_train_h = prepared_data[h]["y_train"]
        visualization_rows.extend([
            {
                "horizon": h,
                "split": "train",
                "prediction_origin_time": origin_time,
                "target_time": origin_time + pd.Timedelta(hours=h),
                "observed": observed,
                "predicted": np.nan,
            }
            for origin_time, observed in zip(
                train_times,
                y_train_h.to_numpy(),
            )
        ])
        visualization_rows.extend([
            {
                "horizon": h,
                "split": "test",
                "prediction_origin_time": origin_time,
                "target_time": origin_time + pd.Timedelta(hours=h),
                "observed": observed,
                "predicted": prediction,
            }
            for origin_time, observed, prediction in zip(
                test_times,
                y_test_h.to_numpy(),
                predictions,
            )
        ])

        errors = y_test_h.to_numpy() - predictions

        mae = np.mean(np.abs(errors))
        rmse = np.sqrt(np.mean(errors ** 2))

        final_results.append({"horizon": h, "MAE": mae, "RMSE": rmse, "n_test": len(y_test_h)})


# ============================================================
# Results Summary
# ============================================================

    print("\nCV results:")
    print(pd.DataFrame(cv_results))
    print("\nFINAL TEST results:")
    print(pd.DataFrame(final_results))

    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    prediction_path = output_dir / (
        f"final_model_predictions_{timestamp}.csv"
    )
    pd.DataFrame(visualization_rows).to_csv(prediction_path, index=False)
    print(f"Saved prediction data for visualization to {prediction_path}")


if __name__ == "__main__":
    main()