import argparse
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


HORIZONS = range(1, 13)


def find_latest_prediction_file(output_dir):
    files = sorted(output_dir.glob("final_model_predictions_*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No prediction CSV found in {output_dir}. Run CatBoostRegressor_model.py first."
        )
    return files[-1]


def create_visualization(prediction_path, output_path=None):
    prediction_path = Path(prediction_path)
    data = pd.read_csv(
        prediction_path,
        parse_dates=["prediction_origin_time", "target_time"],
    )

    if output_path is None:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_path = prediction_path.parent / (
            f"final_model_predictions_vs_test_{timestamp}.png"
        )
    else:
        output_path = Path(output_path)

    fig, axes = plt.subplots(3, 4, figsize=(20, 12), sharey=True)
    axes = axes.ravel()

    for axis, horizon in zip(axes, HORIZONS):
        horizon_data = data[data["horizon"] == horizon]
        train_data = horizon_data[horizon_data["split"] == "train"]
        test_data = horizon_data[horizon_data["split"] == "test"]

        axis.plot(
            train_data["target_time"],
            train_data["observed"],
            label="Train observed",
            color="0.65",
            linewidth=0.9,
        )
        axis.plot(
            test_data["target_time"],
            test_data["observed"],
            label="Test observed",
            color="black",
            linewidth=1.2,
        )
        axis.plot(
            test_data["target_time"],
            test_data["predicted"],
            label="Test predicted",
            color="tab:blue",
            linewidth=1.0,
        )
        axis.set_title(f"Horizon +{horizon}h (target time)")
        axis.xaxis.set_major_locator(mdates.AutoDateLocator())
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M"))
        axis.grid(alpha=0.25)

    axes[0].set_ylabel("grass_pollen")
    axes[4].set_ylabel("grass_pollen")
    axes[8].set_ylabel("grass_pollen")
    axes[0].legend()
    fig.autofmt_xdate()
    fig.suptitle("Final model predictions vs observed test values")
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved visualization to {output_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("prediction_file", nargs="?", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    output_dir = Path(__file__).resolve().parent / "output"
    prediction_path = args.prediction_file or find_latest_prediction_file(output_dir)
    create_visualization(prediction_path, args.output)


if __name__ == "__main__":
    main()
