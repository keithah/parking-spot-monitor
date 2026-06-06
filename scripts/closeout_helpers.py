from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence


def smoke_env(
    *,
    rtsp_placeholder: str,
    matrix_token_placeholder: str,
    base: Mapping[str, str] | None = None,
    pythonpath_prefix: str | None = None,
) -> dict[str, str]:
    env = dict(os.environ if base is None else base)
    env["RTSP_URL"] = rtsp_placeholder
    env["MATRIX_ACCESS_TOKEN"] = matrix_token_placeholder
    if pythonpath_prefix is not None:
        existing_pythonpath = env.get("PYTHONPATH")
        env["PYTHONPATH"] = pythonpath_prefix if not existing_pythonpath else f"{pythonpath_prefix}{os.pathsep}{existing_pythonpath}"
    return env


def redact_text(text: str, patterns: Sequence[re.Pattern[str]]) -> str:
    redacted = text
    for pattern in patterns:
        if pattern.groups >= 3:
            redacted = pattern.sub(lambda match: f"{match.group(1)}{match.group(2)}<redacted>", redacted)
        else:
            redacted = pattern.sub("<redacted>", redacted)
    return redacted


def bounded_text(text: str, *, limit: int) -> str:
    if len(text) <= limit:
        return text
    marker = f"... <{len(text) - limit} chars omitted> ...\n"
    tail_limit = max(0, limit - len(marker))
    return f"{marker}{text[-tail_limit:]}"


def decode_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def safe_output(
    stdout: str | bytes | None,
    stderr: str | bytes | None,
    *,
    patterns: Sequence[re.Pattern[str]],
    limit: int,
) -> str:
    rendered = ""
    out = decode_output(stdout)
    err = decode_output(stderr)
    if out:
        rendered += f"stdout:\n{out}"
    if err:
        rendered += f"\nstderr:\n{err}"
    return bounded_text(redact_text(rendered.strip(), patterns), limit=limit)


def assert_no_forbidden_markers(rendered: str, forbidden_markers: Sequence[str]) -> None:
    for marker in forbidden_markers:
        if marker in rendered:
            raise RuntimeError(f"redaction failure for marker: {marker}")
