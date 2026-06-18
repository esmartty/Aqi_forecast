class SensorHoursIngestionError(RuntimeError):
    """Raised when ingestion of OpenAQ sensor hours fails permanently."""

    def __init__(self, sensor_id, datetime_from=None, datetime_to=None, reason=None, status=None):
        self.sensor_id = sensor_id
        self.datetime_from = datetime_from
        self.datetime_to = datetime_to
        self.reason = reason
        self.status = status
        super().__init__(
            f"sensor={sensor_id}, "
            f"range={datetime_from}→{datetime_to}, "
            f"status={status}, reason={reason}"
        )
