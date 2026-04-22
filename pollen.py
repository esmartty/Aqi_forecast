import cdsapi
import zipfile
import os
import xarray as xr
import pandas as pd

#https://ads.atmosphere.copernicus.eu/datasets/cams-europe-air-quality-forecasts?tab=download

def get_camps_request(date):
    return {
        "variable": [
            "alder_pollen",
            "birch_pollen",
            "grass_pollen",
            "mugwort_pollen",
            "olive_pollen",
            "ragweed_pollen"
        ],
        "model": ["ensemble"],
        "level": ["0"],
        "date": [f"{date}/{date}"],
        "type": ["forecast"],
        "time": ["00:00"],
        #"leadtime_hour": ["0", "4", "8", "12", "16", "20", "24"],
        "leadtime_hour": ["0","24","48","72","96"],
        "data_format": "netcdf_zip",
        "area": [50.2, 19.7, 49.9, 20.2]
    }

def download_cams_pollen(date, output_zip="data.zip"):
    request = get_camps_request(date)
    client = cdsapi.Client()

    dataset = "cams-europe-air-quality-forecasts"

    client.retrieve(dataset, request).download(output_zip)

    return output_zip


def unzip_file(zip_path, extract_to="pollen_data"):
    os.makedirs(extract_to, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
        nc_file = zip_ref.namelist()[0]

    return os.path.join(extract_to, nc_file)


def nc_to_dataframe(nc_path):
    ds = xr.open_dataset(nc_path)

    print(ds)

    df = ds.to_dataframe().reset_index()

    return df

def get_analysis_time(date):
    return date


def clean_dataframe(df, analysis_time):
    df = df.dropna()

    df["analysis_time"] = pd.to_datetime(analysis_time).date()
    df["forecast_time"] = (
    pd.to_datetime(analysis_time) +
    pd.to_timedelta(df["time"], unit="h")
    )

    pollen_cols = [
        "apg_conc", #alder_pollen
        "bpg_conc", #birch_pollen
        "gpg_conc", #grass_pollen
        "mpg_conc", #mugwort_pollen
        "opg_conc", #olive_pollen
        "rwpg_conc" #ragweed_pollen
    ]

    df["pollen_total"] = df[pollen_cols].sum(axis=1)

    return df


def run_pipeline(date):
    zip_path = download_cams_pollen(date)
    nc_path = unzip_file(zip_path)
    df = nc_to_dataframe(nc_path)
    df = clean_dataframe(df, date)

    # Delete files after processing
    os.remove(zip_path)
    os.remove(nc_path)
    os.rmdir("pollen_data")  # Remove the directory

    return df


if __name__ == "__main__":
    df = run_pipeline("2026-04-16")
    pd.set_option('display.max_rows', None)
    print(df.tail())
    print(df["forecast_time"].dtype)
    print(print(df[["forecast_time", "time"]].tail(100)))