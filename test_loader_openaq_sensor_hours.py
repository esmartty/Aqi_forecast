import os
import datetime

# Ensure OpenAQ token is available before importing openaq_data.
os.environ.setdefault("OPENAQ_TOKEN", "testtoken")

from openaq_exceptions import SensorHoursIngestionError
import openaq_data
import loader_openaq_sensor_hours as loader


def test_openaq_data_uses_shared_exception_class():
    assert openaq_data.SensorHoursIngestionError is SensorHoursIngestionError
    assert loader.SensorHoursIngestionError is SensorHoursIngestionError


class DummyConnection:
    def __init__(self):
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_timeout_408_splits_into_smaller_windows(monkeypatch):
    start = datetime.datetime(2021, 5, 3, 18, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    end = datetime.datetime(2026, 5, 29, 1, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    calls = []

    def fake_json_sensor_hours(sensor_id, datetime_from=None, datetime_to=None, limit=100, page=1, timeout=None):
        calls.append((datetime_from, datetime_to, page))
        if datetime_from == start and datetime_to == end:
            raise SensorHoursIngestionError(
                sensor_id=sensor_id,
                datetime_from=datetime_from,
                datetime_to=datetime_to,
                status=408,
                reason="window_too_heavy",
            )
        return {"results": []}

    monkeypatch.setattr(openaq_data, "json_sensor_hours", fake_json_sensor_hours)

    loader._ingest_sensor_hours_range(DummyConnection(), 35870, start, end, sql="sql", limit=100)

    assert calls[0] == (start, end, 1)
    assert len(calls) == 3

    midpoint = start + (end - start) / 2
    assert calls[1] == (start, midpoint, 1)
    assert calls[2] == (midpoint, end, 1)
