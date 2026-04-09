"""Backend-agnostic diff computation for the compare tables dialog.

Used by the Tkinter and Textual UI backends to highlight rows that changed
between the staging and base tables.  The function is pure (no UI
dependencies) and fully unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DiffResult:
    """Per-row state information produced by :func:`compute_row_diffs`.

    Attributes:
        stg_row_states: ``"match"`` / ``"changed"`` / ``"only_stg"`` for each
            row in the staging table (same length and order as the input).
        base_row_states: Same, for each row in the base table.
        stg_changed_cols: For each staging row, the set of column names that
            differ from the matching base row (empty for ``"match"`` and
            ``"only_stg"`` rows).
        base_changed_cols: Same, for base rows.
        summary: Human-readable summary string, e.g.
            ``"12 matching | 3 differing | 0 only in staging | 0 only in base"``.
    """

    stg_row_states: list[str] = field(default_factory=list)
    base_row_states: list[str] = field(default_factory=list)
    stg_changed_cols: list[set[str]] = field(default_factory=list)
    base_changed_cols: list[set[str]] = field(default_factory=list)
    summary: str = ""


def _pk_tuple(row: list | tuple, pk_indices: list[int]) -> tuple:
    """Extract a PK value tuple from *row* using *pk_indices*.

    ``None`` values are preserved (not normalised) so distinct rows with
    ``NULL`` PKs are not collapsed.  In practice PK columns are ``NOT NULL``
    so this is mostly defensive.
    """
    return tuple(row[i] for i in pk_indices)


def compute_row_diffs(
    stg_headers: list[str],
    stg_rows: list,
    base_headers: list[str],
    base_rows: list,
    pk_cols: list[str],
    exclude_cols: list[str] | None = None,
) -> DiffResult:
    """Compare staging and base rows and return per-row diff information.

    Rows are matched by primary key (``pk_cols``), not by positional order.
    Columns are matched by header name, not position — headers may differ in
    order or membership between the two tables.  Only columns present in
    **both** headers are considered for diffing, minus ``pk_cols`` (which are
    used for matching, not comparison) and ``exclude_cols`` (which would not
    be updated by the upsert anyway).

    Values are compared with native Python equality (``v1 == v2``).  Do *not*
    convert to strings first: that hides type-equivalent differences and
    surfaces formatting-only differences.

    Args:
        stg_headers: Column headers for staging rows.
        stg_rows: Staging row data (each row is a list/tuple of cell values).
        base_headers: Column headers for base rows.
        base_rows: Base row data.
        pk_cols: Primary key column names used to match rows across tables.
        exclude_cols: Optional list of column names to skip when diffing.

    Returns:
        A :class:`DiffResult` with per-row states, per-row sets of changed
        column names, and a summary string.
    """
    exclude = set(exclude_cols or [])
    pk_set = set(pk_cols)

    # Build column name -> index maps for both tables.
    stg_header_idx = {h: i for i, h in enumerate(stg_headers)}
    base_header_idx = {h: i for i, h in enumerate(base_headers)}

    # Columns eligible for comparison: intersection minus PK and excluded.
    shared_cols = [h for h in stg_headers if h in base_header_idx and h not in pk_set and h not in exclude]

    # Build PK index lists (in the declared pk_cols order so tuples align).
    # If a PK column is missing from either header, we cannot match — return
    # an all-only result.
    try:
        stg_pk_idx = [stg_header_idx[c] for c in pk_cols]
        base_pk_idx = [base_header_idx[c] for c in pk_cols]
    except KeyError:
        # Missing PK column — classify every row as only-in-its-table.
        return DiffResult(
            stg_row_states=["only_stg"] * len(stg_rows),
            base_row_states=["only_base"] * len(base_rows),
            stg_changed_cols=[set() for _ in stg_rows],
            base_changed_cols=[set() for _ in base_rows],
            summary=(f"0 matching | 0 differing | {len(stg_rows)} only in staging | {len(base_rows)} only in base"),
        )

    # Build PK tuple -> row index maps for O(1) lookup.
    stg_pk_map: dict[tuple, int] = {}
    for i, row in enumerate(stg_rows):
        stg_pk_map[_pk_tuple(row, stg_pk_idx)] = i

    base_pk_map: dict[tuple, int] = {}
    for i, row in enumerate(base_rows):
        base_pk_map[_pk_tuple(row, base_pk_idx)] = i

    stg_keys = set(stg_pk_map)
    base_keys = set(base_pk_map)
    common_keys = stg_keys & base_keys
    only_stg_keys = stg_keys - base_keys
    only_base_keys = base_keys - stg_keys

    # Initialise per-row state lists.
    stg_row_states: list[str] = [""] * len(stg_rows)
    base_row_states: list[str] = [""] * len(base_rows)
    stg_changed_cols: list[set[str]] = [set() for _ in stg_rows]
    base_changed_cols: list[set[str]] = [set() for _ in base_rows]

    # Classify only-in-one rows.
    for k in only_stg_keys:
        stg_row_states[stg_pk_map[k]] = "only_stg"
    for k in only_base_keys:
        base_row_states[base_pk_map[k]] = "only_base"

    # Compare common rows cell-by-cell.
    matching_count = 0
    differing_count = 0
    for k in common_keys:
        si = stg_pk_map[k]
        bi = base_pk_map[k]
        stg_row = stg_rows[si]
        base_row = base_rows[bi]
        changed: set[str] = set()
        for col in shared_cols:
            sv = stg_row[stg_header_idx[col]]
            bv = base_row[base_header_idx[col]]
            if not _values_equal(sv, bv):
                changed.add(col)
        if changed:
            stg_row_states[si] = "changed"
            base_row_states[bi] = "changed"
            stg_changed_cols[si] = changed
            base_changed_cols[bi] = set(changed)  # copy
            differing_count += 1
        else:
            stg_row_states[si] = "match"
            base_row_states[bi] = "match"
            matching_count += 1

    summary = (
        f"{matching_count} matching | "
        f"{differing_count} differing | "
        f"{len(only_stg_keys)} only in staging | "
        f"{len(only_base_keys)} only in base"
    )

    return DiffResult(
        stg_row_states=stg_row_states,
        base_row_states=base_row_states,
        stg_changed_cols=stg_changed_cols,
        base_changed_cols=base_changed_cols,
        summary=summary,
    )


def _values_equal(a: Any, b: Any) -> bool:
    """Native equality with ``None == None`` kept distinct from ``None == ""``."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return bool(a == b)
    except (TypeError, ValueError):
        # Unorderable / exotic types that raise on ``==``. Compare repr
        # as a last resort so the diff is still meaningful.
        return repr(a) == repr(b)
