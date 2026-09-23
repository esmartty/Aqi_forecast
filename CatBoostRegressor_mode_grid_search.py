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
HORIZON_FAMILIES = {
    "short": list(range(1, 5)),
    "long": list(range(5, 13)),
}

TRAIN_END = pd.Timestamp("2026-06-30 23:00:00")
TEST_START = pd.Timestamp("2026-07-01 00:00:00")

PARAM_GRID = [
    {"depth": depth, "learning_rate": learning_rate}
    for depth in [4, 6, 8]
    for learning_rate in [0.03, 0.05]
]
CV_SPLITS = 4
ITERATIONS = 500


def create_model(params, iterations=ITERATIONS):
    return CatBoostRegressor(
        iterations=int(iterations),
        depth=int(params["depth"]),
        learning_rate=float(params["learning_rate"]),
        loss_function="MAE",
        verbose=False,
        random_seed=42,
    )


def evaluate_horizon_cv_mae(X_train, y_train, train_times, horizon, params):
    tscv = TimeSeriesSplit(n_splits=CV_SPLITS, gap=horizon)
    scores = []
    best_iterations = []

    for train_idx, val_idx in tscv.split(X_train):
        validation_start = train_times.iloc[val_idx].min()
        train_idx = train_idx[
            (train_times.iloc[train_idx] + pd.Timedelta(hours=horizon)).to_numpy()
            < validation_start
        ]
        if len(train_idx) == 0:
            raise ValueError(f"CV training fold is empty for horizon {horizon}")

        model = create_model(params)
        model.fit(
            X_train.iloc[train_idx],
            y_train.iloc[train_idx],
            eval_set=(X_train.iloc[val_idx], y_train.iloc[val_idx]),
            use_best_model=True,
        )
        predictions = model.predict(X_train.iloc[val_idx])
        scores.append(
            np.mean(np.abs(y_train.iloc[val_idx].to_numpy() - predictions))
        )
        best_iterations.append(model.get_best_iteration())

    return float(np.mean(scores)), scores, best_iterations


def evaluate_grid(X_train, y_train, train_times, horizon):
    results = []
    for params in PARAM_GRID:
        cv_mae, fold_scores, best_iterations = evaluate_horizon_cv_mae(
            X_train, y_train, train_times, horizon, params
        )
        results.append({
            **params,
            "cv_mae": cv_mae,
            "fold_scores": fold_scores,
            "best_iterations": best_iterations,
        })
    return results


def select_best_grid_result(results):
    return min(results, key=lambda result: result["cv_mae"])


def summarize_feature_importance(model, feature_names, top_n=10):
    importances = model.get_feature_importance()
    feature_df = pd.DataFrame({
        "feature": feature_names,
        "importance": importances,
    }).sort_values("importance", ascending=False)
    return feature_df.head(top_n).reset_index(drop=True)


def get_horizon_family(horizon):
    return "short" if horizon <= 4 else "long"


def build_tuning_benefit_summary(global_horizon_scores, horizon_best_params):
    rows = []
    for horizon, params in sorted(horizon_best_params.items()):
        global_cv_mae = float(global_horizon_scores[horizon])
        horizon_cv_mae = float(params["cv_mae"])
        rows.append({
            "horizon": horizon,
            "global_cv_mae": global_cv_mae,
            "horizon_cv_mae": horizon_cv_mae,
            "mae_delta": horizon_cv_mae - global_cv_mae,
            "benefits_from_separate_tuning": horizon_cv_mae < global_cv_mae,
            "global_depth": params["global_depth"],
            "global_learning_rate": params["global_learning_rate"],
            "horizon_depth": params["depth"],
            "horizon_learning_rate": params["learning_rate"],
        })
    return pd.DataFrame(rows)


def main():
    engine_render = create_engine(get_database_render_url(), pool_pre_ping=True)
    engine_local = create_engine(get_database_url(), pool_pre_ping=True)

    df_pollen_raw = pd.read_sql(
        "SELECT * FROM public.pollen_by_date_time", engine_local
    )
    df_synop_raw = pd.read_sql(
        """SELECT * FROM public.synop_data
        ORDER BY date DESC, measurement_hour ASC """,
        engine_render,
    )

    maker = FeatureMaker(pollen_target=TARGET)
    df = maker.make_features_pipeline(df_synop_raw, df_pollen_raw)
    df = df.sort_values("measurement_hour_dt").reset_index(drop=True)

    if df["measurement_hour_dt"].duplicated().any():
        raise ValueError("Feature data must contain unique prediction timestamps")

    features = [
        col for col in df.columns
        if col not in {TARGET, "date", "measurement_hour", "measurement_hour_dt"}
    ]

    target_by_time = df.set_index("measurement_hour_dt")[TARGET]
    target_columns = {}
    for horizon in HORIZONS:
        future_time = df["measurement_hour_dt"] + pd.Timedelta(hours=horizon)
        target_columns[f"target_{horizon}h"] = future_time.map(target_by_time).to_numpy()
    df = pd.concat([df, pd.DataFrame(target_columns, index=df.index)], axis=1)

    prepared_data = {}
    for horizon in HORIZONS:
        target_col = f"target_{horizon}h"
        future_time = df["measurement_hour_dt"] + pd.Timedelta(hours=horizon)
        valid_train = (
            (df["measurement_hour_dt"] <= TRAIN_END)
            & (future_time <= TRAIN_END)
            & df[target_col].notna()
        )
        valid_test = (
            (df["measurement_hour_dt"] >= TEST_START)
            & df[target_col].notna()
        )
        prepared_data[horizon] = {
            "X_train": df.loc[valid_train, features],
            "y_train": df.loc[valid_train, target_col],
            "X_test": df.loc[valid_test, features],
            "y_test": df.loc[valid_test, target_col],
            "train_times": df.loc[
                valid_train, "measurement_hour_dt"
            ].reset_index(drop=True),
            "test_times": df.loc[
                valid_test, "measurement_hour_dt"
            ].reset_index(drop=True),
        }

    global_grid_results = []
    for params in PARAM_GRID:
        horizon_scores = {}
        for horizon in HORIZONS:
            prepared = prepared_data[horizon]
            horizon_scores[horizon], _, _ = evaluate_horizon_cv_mae(
                prepared["X_train"],
                prepared["y_train"],
                prepared["train_times"],
                horizon,
                params,
            )
        global_grid_results.append({
            **params,
            "cv_mae": float(np.mean(list(horizon_scores.values()))),
            "horizon_scores": horizon_scores,
        })

    global_best = min(global_grid_results, key=lambda result: result["cv_mae"])
    global_horizon_scores = global_best["horizon_scores"]
    print(
        "Global parameters: "
        f"depth={global_best['depth']}, "
        f"learning_rate={global_best['learning_rate']:.4f}, "
        f"CV MAE={global_best['cv_mae']:.3f}"
    )

    models = {}
    cv_results = []
    horizon_best_params = {}
    comparison_rows = []
    feature_importance_rows = []
    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

    comparison_rows.append({
        "scope": "global",
        "horizon": None,
        "depth": global_best["depth"],
        "learning_rate": global_best["learning_rate"],
        "cv_mae": global_best["cv_mae"],
    })

    for horizon in HORIZONS:
        prepared = prepared_data[horizon]
        candidates = evaluate_grid(
            prepared["X_train"],
            prepared["y_train"],
            prepared["train_times"],
            horizon,
        )
        horizon_best = select_best_grid_result(candidates)
        horizon_best_params[horizon] = {
            **horizon_best,
            "global_depth": global_best["depth"],
            "global_learning_rate": global_best["learning_rate"],
        }

        selected_params = horizon_best
        if global_horizon_scores[horizon] <= horizon_best["cv_mae"]:
            selected_params = global_best

        cv_mae, fold_scores, best_iterations = evaluate_horizon_cv_mae(
            prepared["X_train"],
            prepared["y_train"],
            prepared["train_times"],
            horizon,
            selected_params,
        )
        final_iterations = int(np.median(best_iterations)) + 1
        final_model = create_model(selected_params, iterations=final_iterations)
        final_model.fit(prepared["X_train"], prepared["y_train"])
        models[horizon] = final_model

        comparison_rows.append({
            "scope": "horizon",
            "horizon": horizon,
            "depth": horizon_best["depth"],
            "learning_rate": horizon_best["learning_rate"],
            "cv_mae": horizon_best["cv_mae"],
        })
        cv_results.append({
            "horizon": horizon,
            "horizon_family": get_horizon_family(horizon),
            "cv_mae": cv_mae,
            "best_iteration_mean": np.mean(best_iterations),
            "best_iteration_median": np.median(best_iterations),
            "chosen_depth": selected_params["depth"],
            "chosen_learning_rate": selected_params["learning_rate"],
            "chosen_iterations": final_iterations,
        })
        print(
            "Horizon +{h}h | family={family} | selected depth={depth}, "
            "learning_rate={lr:.4f}, CV MAE = {mae:.3f} | best iteration = {iteration:.0f}"
            .format(
                h=horizon,
                family=get_horizon_family(horizon),
                depth=selected_params["depth"],
                lr=selected_params["learning_rate"],
                mae=cv_mae,
                iteration=np.median(best_iterations),
            )
        )

        top_features = summarize_feature_importance(
            final_model, features, top_n=10
        )
        top_features.insert(0, "horizon", horizon)
        feature_importance_rows.append(top_features)

    for family_name, horizons in HORIZON_FAMILIES.items():
        family_mae = [
            row["cv_mae"] for row in cv_results
            if row["horizon_family"] == family_name
        ]
        print(
            f"Family {family_name} horizons {horizons}: "
            f"mean CV MAE = {np.mean(family_mae):.3f}"
        )

    benefit_summary = build_tuning_benefit_summary(
        global_horizon_scores, horizon_best_params
    )
    benefit_summary["mae_delta"] = benefit_summary["mae_delta"].round(4)
    print("\nGlobal vs per-horizon tuning summary:")
    print(benefit_summary)

    comparison_path = output_dir / f"param_search_comparison_grid_{timestamp}.csv"
    pd.DataFrame(comparison_rows).to_csv(comparison_path, index=False)
    benefit_path = output_dir / f"tuning_benefit_summary_grid_{timestamp}.csv"
    benefit_summary.to_csv(benefit_path, index=False)
    importance_path = output_dir / f"feature_importance_by_horizon_grid_{timestamp}.csv"
    pd.concat(feature_importance_rows, ignore_index=True).to_csv(
        importance_path, index=False
    )

    final_results = []
    visualization_rows = []
    for horizon in HORIZONS:
        prepared = prepared_data[horizon]
        predictions = models[horizon].predict(prepared["X_test"])
        errors = prepared["y_test"].to_numpy() - predictions
        final_results.append({
            "horizon": horizon,
            "MAE": np.mean(np.abs(errors)),
            "RMSE": np.sqrt(np.mean(errors ** 2)),
            "n_test": len(prepared["y_test"]),
        })
        visualization_rows.extend([
            {
                "horizon": horizon,
                "split": "train",
                "prediction_origin_time": origin_time,
                "target_time": origin_time + pd.Timedelta(hours=horizon),
                "observed": observed,
                "predicted": np.nan,
            }
            for origin_time, observed in zip(
                prepared["train_times"], prepared["y_train"].to_numpy()
            )
        ])
        visualization_rows.extend([
            {
                "horizon": horizon,
                "split": "test",
                "prediction_origin_time": origin_time,
                "target_time": origin_time + pd.Timedelta(hours=horizon),
                "observed": observed,
                "predicted": prediction,
            }
            for origin_time, observed, prediction in zip(
                prepared["test_times"],
                prepared["y_test"].to_numpy(),
                predictions,
            )
        ])

    print("\nCV results:")
    print(pd.DataFrame(cv_results))
    print("\nFINAL TEST results:")
    print(pd.DataFrame(final_results))

    prediction_path = output_dir / f"final_model_predictions_grid_{timestamp}.csv"
    pd.DataFrame(visualization_rows).to_csv(prediction_path, index=False)
    print(f"Saved prediction data for visualization to {prediction_path}")


if __name__ == "__main__":
    main()
