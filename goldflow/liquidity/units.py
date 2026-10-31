"""Exact decimal-string -> integer-unit conversion (Phase 8).

Binance sends prices and quantities as decimal strings. Book state is kept in integer ticks and lots so that every
sum, comparison and digest is exact. A value that is not an exact multiple of the instrument's tick/step is a data
error (the caller invalidates the book); it is never rounded.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


class UnitError(ValueError):
    """A price or quantity is malformed, negative or not an exact multiple of the unit."""


class UnitScale:
    """Converts decimal strings to integer multiples of `unit` (e.g. tick "0.01" or step "0.001")."""

    def __init__(self, unit: str):
        d = Decimal(unit)
        if d <= 0:
            raise ValueError(f"unit must be > 0, got {unit!r}")
        exp = d.as_tuple().exponent
        self.decimals = max(0, -exp) if isinstance(exp, int) else 0
        self.unit = unit
        self._unit_scaled = int(d.scaleb(self.decimals))  # e.g. 0.05 at 2 decimals -> 5

    def to_units(self, s: str) -> int:
        """Exact conversion; raises UnitError if `s` is not a non-negative exact multiple of the unit."""
        if not isinstance(s, str) or not s:
            raise UnitError(f"expected a decimal string, got {s!r}")
        if s[0] == "-":
            raise UnitError(f"negative value {s!r}")
        if "e" in s or "E" in s:
            try:
                d = Decimal(s)
            except InvalidOperation as e:
                raise UnitError(f"malformed number {s!r}") from e
            q = d.scaleb(self.decimals)
            if q != q.to_integral_value():
                raise UnitError(f"{s!r} is not a multiple of {self.unit}")
            scaled = int(q)
        else:
            whole, _, frac = s.partition(".")
            if not whole.isdigit() or (frac and not frac.isdigit()):
                raise UnitError(f"malformed number {s!r}")
            extra = frac[self.decimals :]
            if extra.strip("0"):
                raise UnitError(f"{s!r} is not a multiple of {self.unit}")
            scaled = int(whole) * 10**self.decimals + int((frac[: self.decimals]).ljust(self.decimals, "0") or "0")
        units, rem = divmod(scaled, self._unit_scaled)
        if rem:
            raise UnitError(f"{s!r} is not a multiple of {self.unit}")
        return units

    def from_float(self, x: float) -> int:
        """Exact conversion of a float that was produced from a decimal string (Parquet DepthEvent columns).
        Raises UnitError if the float is not within 1e-6 units of an integer multiple."""
        if x < 0 or x != x:
            raise UnitError(f"invalid value {x!r}")
        u = x * 10**self.decimals / self._unit_scaled
        r = round(u)
        if abs(u - r) > 1e-6:
            raise UnitError(f"{x!r} is not a multiple of {self.unit}")
        return int(r)

    def to_str(self, units: int) -> str:
        """Canonical decimal string for an integer number of units."""
        scaled = units * self._unit_scaled
        if self.decimals == 0:
            return str(scaled)
        sign = "-" if scaled < 0 else ""
        a = abs(scaled)
        return f"{sign}{a // 10**self.decimals}.{a % 10**self.decimals:0{self.decimals}d}"

    def to_float(self, units: int) -> float:
        return float(self.to_str(units))
