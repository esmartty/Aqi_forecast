from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from io import BytesIO
from types import SimpleNamespace

import pytest
import requests

import loader_open_meteo_weather_forecast_data as loader
import loader_open_meteo_weather_forecast_data_service as loader_service
from open_meteo_s3 import inspect_model_run


class FakeConnection:
    def execute(self, statement, params):
        self.params = params
        return self

    def scalar_one(self):
        return self.params["run_id"]


class FakeEngine:
    def __init__(self):
        self.connection = FakeConnection()

    def begin(self):
        return nullcontext(self.connection)


@pytest.mark.parametrize(
    ("load_type", "api_available_at", "expected_api_available_at"),
    [
        (
            "latest",
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        ),
        (
            "historical",
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            None,
        ),
    ],
)
def test_insert_model_update_data_maps_s3_metadata(
    monkeypatch,
    load_type,
    api_available_at,
    expected_api_available_at,
):
    s3_metadata = {
        "exists": True,
        "last_modified_utc": "2026-01-03T00:00:00+00:00",
        "json": {"created_at": "2026-01-02T23:59:00Z"},
    }
    monkeypatch.setattr(loader, "s3_client", lambda: object())
    monkeypatch.setattr(
        loader,
        "inspect_model_run",
        lambda s3, model, run_time: s3_metadata,
    )
    engine = FakeEngine()

    loader.insert_model_update_data(
        engine,
        {
            "model": "ecmwf_ifs",
            "run_time_utc": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "api_available_at_utc": api_available_at,
            "modification_time_utc": None,
        },
        load_type=load_type,
    )

    params = engine.connection.params
    assert params["load_type"] == load_type
    assert params["api_available_at_utc"] == expected_api_available_at
    assert params["s3_last_modified"] == s3_metadata["last_modified_utc"]
    assert params["meta_json_created_at"] == s3_metadata["json"]["created_at"]


def test_inspect_model_run_reads_metadata_at_utc_key():
    class FakeS3:
        def head_object(self, **kwargs):
            self.key = kwargs["Key"]
            return {
                "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "ContentLength": 24,
            }

        def get_object(self, **kwargs):
            return {"Body": BytesIO(b'{"created_at":"2026-01-01T22:59:00Z"}')}

    s3 = FakeS3()
    run_time = datetime(
        2026,
        1,
        2,
        1,
        tzinfo=timezone(timedelta(hours=2)),
    )

    result = inspect_model_run(s3, "ecmwf_ifs", run_time)

    assert s3.key == "data_run/ecmwf_ifs/2026/01/01/2300Z/meta.json"
    assert result["last_modified_utc"] == "2026-01-01T00:00:00+00:00"
    assert result["json"]["created_at"] == "2026-01-01T22:59:00Z"


def test_historical_loader_fetches_all_ecmwf_cycles(monkeypatch):
    api_run_times = []
    model_updates = []
    forecast_inserts = []
    monkeypatch.setattr(
        loader.open_meteo_weather_forecast,
        "get_forecast_data",
        lambda run_time: api_run_times.append(run_time) or [],
    )
    monkeypatch.setattr(
        loader,
        "insert_model_update_data",
        lambda engine, model_update_data, load_type: (
            model_updates.append((model_update_data, load_type)) or "run-id"
        ),
    )
    monkeypatch.setattr(
        loader,
        "insert_weather_forecast_data",
        lambda engine, data, run_id: forecast_inserts.append((data, run_id)),
    )

    loader.insert_weather_data_helper(
        object(),
        start_date="2026-01-02",
        end_date="2026-01-02",
    )

    assert api_run_times == [
        "2026-01-02T00:00",
        "2026-01-02T06:00",
        "2026-01-02T12:00",
        "2026-01-02T18:00",
    ]
    assert [update[0]["run_time_utc"].hour for update in model_updates] == [
        0,
        6,
        12,
        18,
    ]
    assert all(load_type == "historical" for _, load_type in model_updates)
    assert forecast_inserts == [([], "run-id")] * 4


def test_historical_loader_skips_future_cycles_and_warns_for_unavailable_run(
    monkeypatch,
    caplog,
):
    now_utc = datetime(2026, 11, 25, 17, 54, tzinfo=timezone.utc)
    api_run_times = []
    model_updates = []
    forecast_inserts = []

    monkeypatch.setattr(loader, "_current_utc_time", lambda: now_utc)

    def get_forecast_data(run_time):
        api_run_times.append(run_time)
        if run_time == "2026-11-25T12:00":
            raise requests.exceptions.HTTPError(
                "400 Client Error",
                response=SimpleNamespace(status_code=400),
            )
        return []

    monkeypatch.setattr(
        loader.open_meteo_weather_forecast,
        "get_forecast_data",
        get_forecast_data,
    )
    monkeypatch.setattr(
        loader,
        "insert_model_update_data",
        lambda engine, model_update_data, load_type: (
            model_updates.append(model_update_data) or "run-id"
        ),
    )
    monkeypatch.setattr(
        loader,
        "insert_weather_forecast_data",
        lambda engine, data, run_id: forecast_inserts.append((data, run_id)),
    )

    loader.insert_weather_data_helper(
        object(),
        start_date="2026-11-25",
        end_date="2026-11-25",
    )

    assert api_run_times == [
        "2026-11-25T00:00",
        "2026-11-25T06:00",
        "2026-11-25T12:00",
    ]
    assert [update["run_time_utc"].hour for update in model_updates] == [0, 6]
    assert forecast_inserts == [([], "run-id")] * 2
    assert any(
        record.levelname == "WARNING"
        and "2026-11-25T12:00 is not available yet" in record.message
        and record.exc_info is None
        for record in caplog.records
    )


def test_latest_loader_passes_run_id_to_forecast_insert(monkeypatch):
    forecast_inserts = []
    monkeypatch.setattr(
        loader.open_meteo_weather_forecast,
        "fetch_last_forecast_weather_model_update",
        lambda: {"run_time": "2026-01-02T00:00"},
    )
    monkeypatch.setattr(
        loader,
        "insert_model_update_data",
        lambda engine, model_update_data, load_type: "latest-run-id",
    )
    monkeypatch.setattr(
        loader.open_meteo_weather_forecast,
        "get_forecast_data",
        lambda run_time: [{"run_time": run_time}],
    )
    monkeypatch.setattr(
        loader,
        "insert_weather_forecast_data",
        lambda engine, data, run_id: forecast_inserts.append((data, run_id)),
    )

    loader.insert_weather_data_helper(object())

    assert forecast_inserts == [
        ([{"run_time": "2026-01-02T00:00"}], "latest-run-id")
    ]


def test_latest_model_update_logs_datetime_values_as_json(monkeypatch, capsys):
    run_info = {
        "run_time": "2026-01-02T00:00",
        "run_time_utc": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "metadata": {"updated_at": datetime(2026, 1, 2, tzinfo=timezone.utc)},
    }
    monkeypatch.setattr(
        loader.open_meteo_weather_forecast,
        "get_latest_run_metadata",
        lambda: run_info,
    )
    monkeypatch.setattr(
        loader.open_meteo_weather_forecast,
        "check_run_availability",
        lambda received_run_info: None,
    )

    result = (
        loader.open_meteo_weather_forecast
        .fetch_last_forecast_weather_model_update()
    )

    assert result is run_info
    assert '"run_time_utc": "2026-01-02 00:00:00+00:00"' in capsys.readouterr().out


def test_production_environment_loads_latest_run(monkeypatch):
    calls = []
    engine = object()
    monkeypatch.setattr(
        loader,
        "insert_weather_data_helper",
        lambda received_engine: calls.append(received_engine),
    )

    loader.run_weather_data_load(engine, "production")

    assert calls == [engine]


def test_local_environment_loads_from_last_historical_run(monkeypatch):
    class FakeEngine:
        def connect(self):
            return nullcontext(object())

    calls = []
    engine = FakeEngine()
    last_run = datetime(2026, 1, 2, 18, tzinfo=timezone.utc)
    monkeypatch.setattr(
        loader_service,
        "fetch_last_historical_open_meteo_weather_forecast_data_date",
        lambda connection: last_run,
    )
    monkeypatch.setattr(
        loader,
        "insert_weather_data_helper",
        lambda *args: calls.append(args),
    )

    loader.run_weather_data_load(engine, "local")

    assert calls == [
        (
            engine,
            "2026-01-02",
            datetime.now().date().strftime("%Y-%m-%d"),
        )
    ]


def test_local_environment_accepts_transaction_connection(monkeypatch):
    class FakeConnection:
        def execute(self, statement, params):
            return self

    connection = FakeConnection()
    calls = []
    last_run = datetime(2026, 1, 2, 18, tzinfo=timezone.utc)
    monkeypatch.setattr(
        loader_service,
        "fetch_last_historical_open_meteo_weather_forecast_data_date",
        lambda received_connection: (
            last_run if received_connection is connection else None
        ),
    )
    monkeypatch.setattr(
        loader,
        "insert_weather_data_helper",
        lambda *args: calls.append(args),
    )

    loader.run_weather_data_load(connection, "local")

    assert calls == [
        (
            connection,
            "2026-01-02",
            datetime.now().date().strftime("%Y-%m-%d"),
        )
    ]


def test_local_environment_requires_a_historical_run(monkeypatch):
    class FakeEngine:
        def connect(self):
            return nullcontext(object())

    monkeypatch.setattr(
        loader_service,
        "fetch_last_historical_open_meteo_weather_forecast_data_date",
        lambda connection: None,
    )

    with pytest.raises(RuntimeError, match="No historical model runs found"):
        loader.run_weather_data_load(FakeEngine(), "local")


def test_local_environment_accepts_manual_historical_date_range(monkeypatch):
    calls = []
    engine = object()
    monkeypatch.setattr(
        loader,
        "insert_weather_data_helper",
        lambda *args: calls.append(args),
    )

    loader.run_weather_data_load(
        engine,
        "local",
        start_date="2026-01-01",
        end_date="2026-01-03",
    )

    assert calls == [(engine, "2026-01-01", "2026-01-03")]


def test_manual_historical_load_requires_both_dates():
    with pytest.raises(ValueError, match="Both start_date and end_date"):
        loader.run_weather_data_load(
            object(),
            "local",
            start_date="2026-01-01",
        )


def test_historical_loader_rejects_reversed_date_range():
    with pytest.raises(ValueError, match="start_date must be on or before end_date"):
        loader.insert_weather_data_helper(
            object(),
            start_date="2026-01-03",
            end_date="2026-01-01",
        )


def test_local_cursor_uses_last_historical_model_run():
    class FakeResult:
        def mappings(self):
            return self

        def first(self):
            return {"last_historical_date": datetime(2026, 1, 2, 18)}

    class FakeConnection:
        def execute(self, statement, params):
            self.statement = str(statement)
            self.params = params
            return FakeResult()

    connection = FakeConnection()

    result = (
        loader_service
        .fetch_last_historical_open_meteo_weather_forecast_data_date(connection)
    )

    assert result == datetime(2026, 1, 2, 18)
    assert "MAX(run_time_utc)" in connection.statement
    assert "public.weather_model_runs" in connection.statement
    assert "load_type = 'historical'" in connection.statement
    assert connection.params == {"model": "ecmwf_ifs"}


def test_insert_weather_forecast_data_batches_timestamped_rows(monkeypatch):
    class FakeConnection:
        def execute(self, statement, rows):
            self.statement = str(statement)
            self.rows = rows

    class FakeEngine:
        def __init__(self):
            self.connection = FakeConnection()

        def begin(self):
            return nullcontext(self.connection)

    monkeypatch.setattr(
        loader,
        "query_coord_to_id",
        lambda connection: {(50.0, 20.0): 7},
    )
    hourly = {
        "time": ["2026-01-02T00:00", "2026-01-02T01:00"],
        **{
            variable: [index, index + 1]
            for index, variable in enumerate(
                loader.open_meteo_weather_forecast.HOURLY_VARIABLES
            )
        },
    }
    engine = FakeEngine()

    inserted_count = loader.insert_weather_forecast_data(
        engine,
        [{"latitude": 50.0, "longitude": 20.0, "hourly": hourly}],
        run_id="run-id",
    )

    assert inserted_count == 2
    assert "ON CONFLICT (weather_location_id, forecast_time, run_id)" in (
        engine.connection.statement
    )
    assert engine.connection.rows[0]["weather_location_id"] == 7
    assert engine.connection.rows[0]["run_id"] == "run-id"
    assert engine.connection.rows[0]["forecast_time"] == datetime(
        2026,
        1,
        2,
        tzinfo=timezone.utc,
    )
    assert engine.connection.rows[0]["temperature_2m"] == 0
    assert engine.connection.rows[0]["temperature_2m_unit"] == "°C"
    assert engine.connection.rows[0]["convective_inhibition_unit"] == "J/kg"
