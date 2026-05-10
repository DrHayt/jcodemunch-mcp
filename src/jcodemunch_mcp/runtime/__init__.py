"""Runtime trace ingestion infrastructure (Phase 0 scaffold).

Phase 0 ships the schema, the redaction chokepoint, and the symbol resolver.
No ingest tools yet — those land in Phase 1.

Public surface:
- redact_trace_record(record, source) — single redaction chokepoint
- resolve_to_symbol_id(repo, file_path, line_no, function_name) — best-effort resolver
- VALID_SOURCES — frozenset of accepted source labels
"""

from .redact import redact_trace_record
from .resolve import resolve_to_symbol_id

VALID_SOURCES = frozenset({"otel", "sql_log", "stack_log", "apm"})

__all__ = ["redact_trace_record", "resolve_to_symbol_id", "VALID_SOURCES"]
