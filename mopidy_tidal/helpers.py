import datetime


def to_timestamp(dt):
    if not dt:
        return 0
    if isinstance(dt, str):
        dt = datetime.datetime.fromisoformat(dt)
    if isinstance(dt, datetime.datetime):
        dt = dt.timestamp()
    return int(dt)


def is_available(tidal_item) -> bool:
    """Whether a TIDAL item can be played back.

    Items that don't expose the flag are considered available.
    """
    return bool(getattr(tidal_item, "available", True))
