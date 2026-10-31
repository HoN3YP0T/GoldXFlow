"""UTC time helpers. Intervals are half-open [start, end) in epoch milliseconds."""
from __future__ import annotations
from datetime import datetime, timezone, timedelta, date

DAY_MS = 86_400_000

def utcnow() -> datetime: return datetime.now(timezone.utc)
def to_ms(dt: datetime) -> int: return int(dt.timestamp() * 1000)
def from_ms(ms: int) -> datetime: return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
def iso(ms: int | None) -> str | None:
    return None if ms is None else from_ms(ms).isoformat(timespec="milliseconds").replace("+00:00", "Z")
def parse_iso(s: str) -> datetime:
    d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)

def minus_months(dt: datetime, months: int) -> datetime:
    """Calendar-month subtraction, clamping the day to the target month's length."""
    y, m = dt.year, dt.month - months
    while m <= 0: m += 12; y -= 1
    import calendar
    d = min(dt.day, calendar.monthrange(y, m)[1])
    return dt.replace(year=y, month=m, day=d)

def day_start_ms(d: date) -> int: return to_ms(datetime(d.year, d.month, d.day, tzinfo=timezone.utc))
def month_bounds_ms(y: int, m: int) -> tuple[int, int]:
    s = datetime(y, m, 1, tzinfo=timezone.utc)
    e = datetime(y + (m == 12), 1 if m == 12 else m + 1, 1, tzinfo=timezone.utc)
    return to_ms(s), to_ms(e)
def days_between(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d; d += timedelta(days=1)
