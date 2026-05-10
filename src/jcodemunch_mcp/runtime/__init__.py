"""Runtime trace ingestion infrastructure.

Phase 0 shipped the schema, the redaction chokepoint, and the symbol
resolver. Phase 1 adds the OTel JSON file-import path.

Public surface:
- redact_trace_record(record, source) — single redaction chokepoint
- resolve_to_symbol_id(conn, file_path, line_no, function_name) — best-effort resolver
- ingest_otel_file(...) — orchestrate parse → redact → resolve → upsert
- parse_otel_file(path) — pure OTel JSON / JSONL iterator (no DB writes)
- VALID_SOURCES — frozenset of accepted source labels
"""

from .confidence import (
    RuntimeConfidenceProbe,
    attach_runtime_confidence,
    attach_runtime_confidence_by_file,
)
from .ingest import ingest_otel_file
from .otel import OtelSpan, parse_otel_file
from .redact import redact_trace_record
from .resolve import resolve_to_symbol_id

VALID_SOURCES = frozenset({"otel", "sql_log", "stack_log", "apm"})

__all__ = [
    "redact_trace_record",
    "resolve_to_symbol_id",
    "parse_otel_file",
    "ingest_otel_file",
    "OtelSpan",
    "RuntimeConfidenceProbe",
    "attach_runtime_confidence",
    "attach_runtime_confidence_by_file",
    "VALID_SOURCES",
]
