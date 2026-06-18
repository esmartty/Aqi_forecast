import json
from datetime import datetime

def parse_numeric(value, cast_type=float):
    if value in (None, "-", ""):
        return None
    try:
        return cast_type(value)
    except (TypeError, ValueError):
        return None
    
def parse_utc(dt: str):
    if not dt:
        return None
    return datetime.fromisoformat(dt.replace("Z", "+00:00"))


def _normalize_datetime_param(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value

def _json_value(value):
    return json.dumps(value) if isinstance(value, (dict, list)) else value


if __name__ == "__main__":
    print(datetime)
    print(datetime.fromisoformat("2019-01-14T09:00:00Z"))