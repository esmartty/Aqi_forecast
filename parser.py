def parse_numeric(value, cast_type=float):
    if value in (None, "-", ""):
        return None
    try:
        return cast_type(value)
    except (TypeError, ValueError):
        return None