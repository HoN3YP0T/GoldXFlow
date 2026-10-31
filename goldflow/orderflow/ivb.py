"""Phase 10: IVB. **Disabled until its definition is supplied and signed off (decision D1).**

"IVB" has no single standard definition: it could mean Initial Value Balance, Initial Volume Balance or a proprietary
indicator. Nothing here guesses. The module exists so that:

* the config can carry an ``ivb`` block, which ``Config.load`` validates: ``enabled: true`` is refused while there is no
  signed-off definition, and also while no implementation exists for it (both conditions are explicit);
* code that asks for IVB gets ``IVBUnavailable``, never a substitute such as the Initial Balance or another metric.

To enable it, supply: the exact meaning, the formula, its inputs, the time window and the interpretation, ideally with
a worked example or a chart. It will then be implemented, tested against that example, and versioned.
"""

from __future__ import annotations

from typing import Any

STATUS = "DISABLED: definition required (decision D1). IVB is never replaced by IB or any other metric."
REQUIRED_FIELDS = ("meaning", "formula", "inputs", "window", "interpretation", "worked_example", "signed_off_by")


class IVBUnavailable(RuntimeError):
    pass


def validate(raw: dict[str, Any] | None) -> None:
    """Refuse an enabled IVB until a complete, signed-off definition exists **and** an implementation for it exists."""
    blk = raw or {}
    if not blk.get("enabled"):
        return
    d = blk.get("definition") or {}
    missing = [k for k in REQUIRED_FIELDS if not d.get(k)]
    if missing:
        raise ValueError(f"ivb.enabled is true but the definition is incomplete (missing {missing}); {STATUS}")
    raise ValueError("ivb.enabled is true and a definition was supplied, but it has not been implemented and tested yet; "
                     "keep ivb.enabled false until the implementation for this definition ships")  # fmt: skip


def ivb(*_: Any, **__: Any) -> None:
    raise IVBUnavailable(STATUS)
