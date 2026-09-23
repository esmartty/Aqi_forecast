import pandas as pd

from CatBoostRegressor_model import (
    build_tuning_benefit_summary,
    summarise_param_search,
)


def test_summarise_param_search_keeps_global_and_horizon_results():
    global_results = [
        {"depth": 4, "learning_rate": 0.03, "cv_mae": 12.0},
        {"depth": 4, "learning_rate": 0.05, "cv_mae": 11.5},
        {"depth": 6, "learning_rate": 0.03, "cv_mae": 12.7},
    ]
    horizon_results = {
        1: {"depth": 4, "learning_rate": 0.05, "cv_mae": 8.2},
        2: {"depth": 6, "learning_rate": 0.03, "cv_mae": 9.9},
    }

    summary = summarise_param_search(global_results, horizon_results)

    assert summary["global_best"]["depth"] == 4
    assert summary["global_best"]["learning_rate"] == 0.05
    assert summary["horizon_best_params"][1]["depth"] == 4
    assert summary["horizon_best_params"][2]["depth"] == 6
    assert summary["horizon_best_params"][1]["cv_mae"] == 8.2


def test_build_tuning_benefit_summary_marks_when_horizon_tuning_is_better():
    global_best = {"depth": 4, "learning_rate": 0.03, "cv_mae": 12.0}
    horizon_best = {
        1: {"depth": 4, "learning_rate": 0.05, "cv_mae": 10.0},
        2: {"depth": 6, "learning_rate": 0.03, "cv_mae": 13.0},
    }

    summary = build_tuning_benefit_summary(global_best, horizon_best)

    assert summary.loc[0, "horizon"] == 1
    assert summary.loc[0, "mae_delta"] == -2.0
    assert summary.loc[0, "benefits_from_separate_tuning"] == True
    assert summary.loc[1, "benefits_from_separate_tuning"] == False
