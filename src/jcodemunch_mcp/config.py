"""Centralized JSONC config for jcodemunch-mcp."""

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Global config storage
_GLOBAL_CONFIG: dict[str, Any] = {}
_PROJECT_CONFIGS: dict[str, dict[str, Any]] = {}  # repo -> merged config

DEFAULTS = {
    "use_ai_summaries": True,
    "max_folder_files": 2000,
    "max_index_files": 10000,
    "staleness_days": 7,
    "max_results": 500,
    "extra_ignore_patterns": [],
    "extra_extensions": {},
    "context_providers": True,
    "meta_fields": None,  # None = all fields
    "languages": None,  # None = all languages
    "disabled_tools": [],
    "descriptions": {},
    "transport": "stdio",
    "host": "127.0.0.1",
    "port": 8901,
    "rate_limit": 0,
    "watch": False,
    "watch_debounce_ms": 2000,
    "freshness_mode": "relaxed",
    "claude_poll_interval": 5.0,
    "log_level": "WARNING",
    "log_file": None,
    "redact_source_root": False,
    "stats_file_interval": 3,
    "share_savings": True,
    "summarizer_concurrency": 4,
    "allow_remote_summarizer": False,
}

CONFIG_TYPES = {
    "use_ai_summaries": bool,
    "max_folder_files": int,
    "max_index_files": int,
    "staleness_days": int,
    "max_results": int,
    "extra_ignore_patterns": list,
    "extra_extensions": dict,
    "context_providers": bool,
    "meta_fields": (list, type(None)),
    "languages": (list, type(None)),
    "disabled_tools": list,
    "descriptions": dict,
    "transport": str,
    "host": str,
    "port": int,
    "rate_limit": int,
    "watch": bool,
    "watch_debounce_ms": int,
    "freshness_mode": str,
    "claude_poll_interval": float,
    "log_level": str,
    "log_file": (str, type(None)),
    "redact_source_root": bool,
    "stats_file_interval": int,
    "share_savings": bool,
    "summarizer_concurrency": int,
    "allow_remote_summarizer": bool,
}


def _strip_jsonc(text: str) -> str:
    """Strip // and /* */ comments from JSONC, respecting quoted strings."""
    result, i, n = [], 0, len(text)
    in_str = False
    while i < n:
        ch = text[i]
        if in_str:
            result.append(ch)
            if ch == '\\' and i + 1 < n:
                result.append(text[i + 1])
                i += 2
                continue
            if ch == '"':
                in_str = False
            i += 1
        elif ch == '"':
            in_str = True
            result.append(ch)
            i += 1
        elif ch == '/' and i + 1 < n and text[i + 1] == '/':
            # Line comment — skip to end of line
            end = text.find('\n', i)
            i = n if end == -1 else end
        elif ch == '/' and i + 1 < n and text[i + 1] == '*':
            # Block comment — skip to */
            end = text.find('*/', i + 2)
            i = n if end == -1 else end + 2
        else:
            result.append(ch)
            i += 1
    return ''.join(result)
