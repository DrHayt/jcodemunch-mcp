"""find_unused_paths — symbols reachable on paper but never executed (Phase 3).

Distinct from ``find_dead_code`` (static-only graph reachability) — this
tool only flags code that has *runtime evidence of absence*: zero hits
in ``runtime_calls`` over the configured window. A symbol with no
runtime hits but zero static callers is dead by both definitions; a
symbol with no runtime hits but plenty of static callers is the
interesting "looks reachable, never runs" finding only this tool can
surface.

Excludes test files and entry-point heuristics by default — unused tests
aren't "dead" and main/__init__/wsgi/etc. are entry points the runtime
trace probably doesn't capture.

Returns:
  ``{
      'repo': 'owner/name',
      'since_days': D,
      'cutoff_iso': cutoff date for "recent enough" runtime evidence,
      'results': [
          {
              'symbol_id', 'name', 'kind', 'file', 'line',
              'last_seen': '' if never observed,
              'reason': 'no_runtime_evidence' | 'stale_only',
          },
          ...
      ],
      'total_unused': N,
      '_meta': {timing_ms, total_symbols_scanned, excluded_test_files,
                excluded_entry_points, runtime_data_present, ...}
  }``

When no traces have been ingested at all, ``results`` is empty and
``_meta.runtime_data_present`` is False — every symbol would be
trivially "unused" otherwise, which would mislead the agent.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from ._utils import resolve_repo
from .find_dead_code import _is_test_file, _is_entry_point_filename
from ..storage import IndexStore


def find_unused_paths(
    repo: str,
    since_days: int = 90,
    *,
    include_tests: bool = False,
    include_entry_points: bool = False,
    max_results: int = 200,
    storage_path: Optional[str] = None,
) -> dict:
    """Return symbols with zero (or stale) runtime hits within the window.

    Args:
        repo: Repository identifier.
        since_days: Look-back window. ``>=1``. Symbols last observed
            before ``now - since_days`` days surface as ``stale_only``.
            Symbols never observed surface as ``no_runtime_evidence``.
        include_tests: Include symbols in test files. Default False.
        include_entry_points: Include symbols in entry-point filenames
            (``main.py``, ``__main__.py``, ``wsgi.py``, ``app.py``,
            ``manage.py``, etc.). Default False.
        max_results: Cap on returned rows.
        storage_path: Custom storage path.

    Returns:
        See module docstring.
    """
    start = time.perf_counter()
    since_days = max(1, since_days)
    max_results = max(1, min(max_results, 1000))
    try:
        owner, name = resolve_repo(repo, storage_path)
    except ValueError as e:
        return {"error": str(e)}

    store = IndexStore(base_path=storage_path)
    db_path = store._sqlite._db_path(owner, name)  # type: ignore[attr-defined]
    if not db_path.exists():
        return {"error": f"Repository not indexed: {owner}/{name}"}

    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        # If runtime_calls is empty, every symbol would trivially qualify —
        # so refuse to dump the entire symbol set and return runtime_data_present=False.
        runtime_present = (
            conn.execute("SELECT 1 FROM runtime_calls LIMIT 1").fetchone() is not None
        )
        if not runtime_present:
            return {
                "repo": f"{owner}/{name}",
                "since_days": since_days,
                "cutoff_iso": cutoff,
                "results": [],
                "total_unused": 0,
                "_meta": {
                    "timing_ms": round((time.perf_counter() - start) * 1000, 1),
                    "runtime_data_present": False,
                    "tip": (
                        "No traces ingested yet. find_unused_paths only fires once at "
                        "least one runtime signal exists; otherwise every symbol would "
                        "be 'unused' and the result would be useless. "
                        "Run `import_runtime_signal` first."
                    ),
                },
            }

        # Symbols never observed — left join + IS NULL is the canonical pattern.
        # We also surface "observed once but not within the window".
        # Note: SQLite's HAVING does not reliably match a COALESCE'd empty
        # string against a literal '', so we keep the raw NULL from MAX()
        # and use IS NULL to catch never-observed symbols. The stale
        # case (observed before cutoff) is the second OR branch.
        rows = conn.execute(
            """
            SELECT
                s.id    AS symbol_id,
                s.name  AS name,
                s.kind  AS kind,
                s.file  AS file,
                s.line  AS line,
                MAX(rc.last_seen) AS last_seen_raw
            FROM symbols s
            LEFT JOIN runtime_calls rc ON rc.symbol_id = s.id
            GROUP BY s.id
            HAVING last_seen_raw IS NULL OR last_seen_raw < ?
            ORDER BY (last_seen_raw IS NOT NULL), last_seen_raw ASC, s.file ASC, s.line ASC
            """,
            (cutoff,),
        ).fetchall()

        excluded_tests = 0
        excluded_entry_points = 0
        results: list[dict] = []
        for r in rows:
            file_path = r["file"] or ""
            if not include_tests and _is_test_file(file_path):
                excluded_tests += 1
                continue
            if not include_entry_points and _is_entry_point_filename(file_path):
                excluded_entry_points += 1
                continue
            last_seen_raw = r["last_seen_raw"]
            reason = "no_runtime_evidence" if last_seen_raw is None else "stale_only"
            results.append({
                "symbol_id": r["symbol_id"],
                "name": r["name"],
                "kind": r["kind"],
                "file": file_path,
                "line": r["line"],
                "last_seen": last_seen_raw or "",
                "reason": reason,
            })
            if len(results) >= max_results:
                break

        # Total scanned for the _meta breakdown
        total_scanned = conn.execute("SELECT COUNT(*) AS n FROM symbols").fetchone()["n"]
    finally:
        conn.close()

    elapsed = (time.perf_counter() - start) * 1000
    return {
        "repo": f"{owner}/{name}",
        "since_days": since_days,
        "cutoff_iso": cutoff,
        "results": results,
        "total_unused": len(results),
        "_meta": {
            "timing_ms": round(elapsed, 1),
            "total_symbols_scanned": total_scanned,
            "excluded_test_files": excluded_tests,
            "excluded_entry_points": excluded_entry_points,
            "runtime_data_present": True,
            "truncated": len(results) >= max_results,
            "tip": (
                "no_runtime_evidence = never observed in any ingested trace. "
                "stale_only = observed before --since-days cutoff. "
                "Pair with find_dead_code for the static-graph view: symbols here "
                "AND in find_dead_code are dead by both definitions; symbols here "
                "BUT NOT in find_dead_code are reachable on paper but never run — "
                "the most interesting category."
            ),
        },
    }
