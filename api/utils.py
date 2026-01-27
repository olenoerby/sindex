from datetime import datetime
from email.utils import parsedate_to_datetime


def parse_retry_after_seconds(header_value: str):
    """Parse a Retry-After header value and return seconds (int) or None.

    Accepts either an integer number of seconds, or an HTTP date string.
    Returns None when the header is missing or cannot be parsed.
    """
    if not header_value:
        return None
    v = header_value.strip()
    # integer seconds
    try:
        return int(v)
    except Exception:
        pass

    # try HTTP-date
    try:
        dt = parsedate_to_datetime(v)
        # ensure timezone-aware -> convert to UTC
        if dt.tzinfo is not None:
            now = datetime.utcnow().astimezone(dt.tzinfo)
        else:
            now = datetime.utcnow()
        delta = (dt - now).total_seconds()
        return int(delta) if delta > 0 else 0
    except Exception:
        return None
