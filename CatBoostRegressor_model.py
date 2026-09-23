import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine
from sklearn.model_selection import TimeSeriesSplit
from catboost import CatBoostRegressor
import optuna

from feature_maker import FeatureMaker
from db_config import get_database_url, get_database_render_url


TARGET = "grass_pollen"
HORIZONS = range(1, 13)
HORIZON_FAMILIES = {
    "short": list(range(1, 5)),
    "long": list(range(5, 13)),
}
# TRAIN_END = pd.Timestamp("2026-07-15 23:00:00")
# TEST_START = pd.Timestamp("2026-07-16 00:00:00")

TRAIN_END = pd.Timestamp("2026-06-30 23:00:00")
TEST_START = pd.Timestamp("2026-07-01 00:00:00")

OPTUNA_TRIALS = 20
CV_SPLITS = 4


def create_model(params):
    return CatBoostRegressor(
        iterations=int(params["iterations"]),
        depth=int(params["depth"]),
        learning_rate=float(params["learning_rate"]),
        l2_leaf_reg=float(params["l2_leaf_reg"]),
        random_strength=float(params["random_strength"]),
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
        scores.append(np.mean(np.abs(y_train.iloc[val_idx].to_numpy() - predictions)))
        best_iterations.append(model.get_best_iteration())

    return float(np.mean(scores)), scores, best_iterations


def suggest_params(trial):
    return {
        "depth": trial.suggest_int("depth", 4, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 50.0, log=True),
        "random_strength": trial.suggest_float("random_strength", 0.0, 3.0),
        "iterations": trial.suggest_int("iterations", 500, 1500, step=100),
    }


def optimise_horizon(X_train, y_train, train_times, horizon, n_trials=OPTUNA_TRIALS):
    def objective(trial):
        params = suggest_params(trial)
        cv_mae, _, _ = evaluate_horizon_cv_mae(
            X_train, y_train, train_times, horizon, params
        )
        return cv_mae

    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=42),
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    return {**study.best_params, "cv_mae": study.best_value}


def optimise_global(prepared_data, n_trials=OPTUNA_TRIALS):
    def objective(trial):
        params = suggest_params(trial)
        scores = []
        for horizon in HORIZONS:
            prepared = prepared_data[horizon]
            cv_mae, _, _ = evaluate_horizon_cv_mae(
                prepared["X_train"],
                prepared["y_train"],
                prepared["train_times"],
                horizon,
                params,
            )
            scores.append(cv_mae)
        return float(np.mean(scores))

    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=42),
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    return {**study.best_params, "cv_mae": study.best_value}


def summarise_param_search(global_results, horizon_results):
    summary = {
        "global_best": None,
        "horizon_best_params": {},
    }

    if global_results:
        summary["global_best"] = min(global_results, key=lambda result: result["cv_mae"])

    if horizon_results:
        for horizon, results in horizon_results.items():
            if isinstance(results, dict):
                best = results
            else:
                best = min(results, key=lambda result: result["cv_mae"])
            summary["horizon_best_params"][horizon] = best

    return summary


def build_tuning_benefit_summary(
    global_best,
    horizon_best_params,
    global_horizon_scores,
):
    if global_best is None:
        return pd.DataFrame(columns=[
            "horizon",
            "global_cv_mae",
            "horizon_cv_mae",
            "mae_delta",
            "benefits_from_separate_tuning",
            "global_depth",
            "global_learning_rate",
            "global_l2_leaf_reg",
            "global_random_strength",
            "global_iterations",
            "horizon_depth",
            "horizon_learning_rate",
            "horizon_l2_leaf_reg",
            "horizon_random_strength",
            "horizon_iterations",
        ])

    rows = []
    for horizon, params in sorted(horizon_best_params.items()):
        global_cv_mae = float(global_horizon_scores[horizon])
        horizon_cv_mae = float(params["cv_mae"])
        mae_delta = horizon_cv_mae - global_cv_mae
        rows.append({
            "horizon": horizon,
            "global_cv_mae": global_cv_mae,
            "horizon_cv_mae": horizon_cv_mae,
            "mae_delta": mae_delta,
            "benefits_from_separate_tuning": bool(horizon_cv_mae < global_cv_mae),
            "global_depth": global_best["depth"],
            "global_learning_rate": global_best["learning_rate"],
            "global_l2_leaf_reg": global_best["l2_leaf_reg"],
            "global_random_strength": global_best["random_strength"],
            "global_iterations": global_best["iterations"],
            "horizon_depth": params["depth"],
            "horizon_learning_rate": params["learning_rate"],
            "horizon_l2_leaf_reg": params["l2_leaf_reg"],
            "horizon_random_strength": params["random_strength"],
            "horizon_iterations": params["iterations"],
        })

    return pd.DataFrame(rows)


def summarize_feature_importance(model, feature_names, top_n=10):
    importances = model.get_feature_importance()
    feature_df = pd.DataFrame({
        "feature": feature_names,
        "importance": importances,
    }).sort_values("importance", ascending=False)
    return feature_df.head(top_n).reset_index(drop=True)


def get_horizon_family(horizon):
    return "short" if horizon <= 4 else "long"


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
    target_columns = {}
    target_by_time = df.set_index("measurement_hour_dt")[TARGET]

    for h in HORIZONS:
        target_col = f"target_{h}h"
        future_time = df["measurement_hour_dt"] + pd.Timedelta(hours=h)
        target_columns[target_col] = future_time.map(target_by_time).to_numpy()

    df = pd.concat([df, pd.DataFrame(target_columns, index=df.index)], axis=1)

    for h in HORIZONS:
        target_col = f"target_{h}h"
        future_time = df["measurement_hour_dt"] + pd.Timedelta(hours=h)

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

    best_params = optimise_global(prepared_data)
    global_search_results = [best_params]
    global_horizon_scores = {}
    for h in HORIZONS:
        prepared = prepared_data[h]
        global_horizon_scores[h], _, _ = evaluate_horizon_cv_mae(
            prepared["X_train"],
            prepared["y_train"],
            prepared["train_times"],
            h,
            best_params,
        )
    print(
        "Global parameters: "
        f"depth={best_params['depth']}, "
        f"learning_rate={best_params['learning_rate']:.4f}, "
        f"l2_leaf_reg={best_params['l2_leaf_reg']:.3f}, "
        f"random_strength={best_params['random_strength']:.3f}, "
        f"iterations={best_params['iterations']}, "
        f"CV MAE={best_params['cv_mae']:.3f}"
    )

    horizon_search_results = {}
    horizon_best_params = {}
    cv_results = []
    family_cv_results = {name: [] for name in HORIZON_FAMILIES}

    for h in HORIZONS:
        X_train = prepared_data[h]["X_train"]
        y_train = prepared_data[h]["y_train"]
        train_times = prepared_data[h]["train_times"]
        horizon_best_params[h] = optimise_horizon(
            X_train, y_train, train_times, h
        )
        horizon_search_results[h] = [horizon_best_params[h]]
        selected_params = horizon_best_params[h]
        if global_horizon_scores[h] <= selected_params["cv_mae"]:
            selected_params = best_params.copy()

        cv_mae, fold_scores, best_iterations = evaluate_horizon_cv_mae(
            X_train, y_train, train_times, h, selected_params
        )

        final_params = selected_params.copy()
        final_params["iterations"] = int(np.median(best_iterations)) + 1
        final_model = create_model(final_params)
        final_model.fit(X_train, y_train)
        models[h] = final_model
        test_data[h] = {
            "X_test": prepared_data[h]["X_test"],
            "y_test": prepared_data[h]["y_test"],
        }

        cv_results.append({
            "horizon": h,
            "horizon_family": get_horizon_family(h),
            "cv_mae": cv_mae,
            "best_iteration_mean": np.mean(best_iterations),
            "best_iteration_median": np.median(best_iterations),
            "chosen_depth": selected_params["depth"],
            "chosen_learning_rate": selected_params["learning_rate"],
            "chosen_l2_leaf_reg": selected_params["l2_leaf_reg"],
            "chosen_random_strength": selected_params["random_strength"],
            "chosen_iterations": final_params["iterations"],
        })
        family_cv_results[get_horizon_family(h)].append(cv_mae)
        print(
            "Horizon +{h}h | family={family} | selected depth={depth}, learning_rate={lr:.4f}, "
            "CV MAE = {cv_mae:.3f} | best iteration = {best_iter:.0f}"
            .format(
                h=h,
                family=get_horizon_family(h),
                depth=selected_params["depth"],
                lr=selected_params["learning_rate"],
                cv_mae=cv_mae,
                best_iter=np.median(best_iterations),
            )
        )

    for family_name, horizons in HORIZON_FAMILIES.items():
        family_mae = [
            row["cv_mae"] for row in cv_results if row["horizon_family"] == family_name
        ]
        if family_mae:
            print(
                f"Family {family_name} horizons {horizons}: "
                f"mean CV MAE = {np.mean(family_mae):.3f}"
            )

    comparison_summary = summarise_param_search(global_search_results, horizon_search_results)
    comparison_rows = [
        {
            "scope": "global",
            "horizon": None,
            "depth": comparison_summary["global_best"]["depth"],
            "learning_rate": comparison_summary["global_best"]["learning_rate"],
            "l2_leaf_reg": comparison_summary["global_best"]["l2_leaf_reg"],
            "random_strength": comparison_summary["global_best"]["random_strength"],
            "iterations": comparison_summary["global_best"]["iterations"],
            "cv_mae": comparison_summary["global_best"]["cv_mae"],
        }
    ]
    comparison_rows.extend(
        {
            "scope": "horizon",
            "horizon": horizon,
            "depth": params["depth"],
            "learning_rate": params["learning_rate"],
            "l2_leaf_reg": params["l2_leaf_reg"],
            "random_strength": params["random_strength"],
            "iterations": params["iterations"],
            "cv_mae": params["cv_mae"],
        }
        for horizon, params in comparison_summary["horizon_best_params"].items()
    )

    tuning_benefit_summary = build_tuning_benefit_summary(
        comparison_summary["global_best"],
        comparison_summary["horizon_best_params"],
        global_horizon_scores,
    )
    tuning_benefit_summary["mae_delta"] = tuning_benefit_summary["mae_delta"].round(4)
    tuning_benefit_summary = tuning_benefit_summary.sort_values("horizon").reset_index(drop=True)

    print("\nGlobal vs per-horizon tuning summary:")
    print(tuning_benefit_summary)
    print("\nHorizons benefiting from separate tuning:")
    beneficial = tuning_benefit_summary[
        tuning_benefit_summary["benefits_from_separate_tuning"]
    ]
    if beneficial.empty:
        print("No horizon improves over the global configuration.")
    else:
        print(beneficial[["horizon", "mae_delta", "global_cv_mae", "horizon_cv_mae"]])

    feature_importance_rows = []
    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    comparison_path = output_dir / f"param_search_comparison_{timestamp}.csv"
    pd.DataFrame(comparison_rows).to_csv(comparison_path, index=False)
    print(f"Saved parameter search comparison to {comparison_path}")

    benefit_path = output_dir / f"tuning_benefit_summary_{timestamp}.csv"
    tuning_benefit_summary.to_csv(benefit_path, index=False)
    print(f"Saved tuning benefit summary to {benefit_path}")

    for h in HORIZONS:
        model = models[h]
        top_features = summarize_feature_importance(model, FEATURES, top_n=10)
        top_features.insert(0, "horizon", h)
        feature_importance_rows.append(top_features)
        print(f"\nTop features for horizon +{h}h:")
        print(top_features.to_string(index=False))

    feature_importance_df = pd.concat(feature_importance_rows, ignore_index=True)
    importance_path = output_dir / f"feature_importance_by_horizon_{timestamp}.csv"
    feature_importance_df.to_csv(importance_path, index=False)
    print(f"Saved feature importance summary to {importance_path}")


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