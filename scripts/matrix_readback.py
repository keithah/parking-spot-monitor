from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def fetch_matrix_room_messages(*, homeserver: str, room_id: str, access_token: str, timeout_seconds: float, limit: int) -> dict[str, Any]:
    parsed = urllib.parse.urlparse(homeserver)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("matrix_readback_invalid_homeserver")
    room_segment = urllib.parse.quote(room_id, safe="")
    query = urllib.parse.urlencode({"dir": "b", "limit": max(1, int(limit))})
    base = parsed._replace(path=parsed.path.rstrip("/"), params="", query="", fragment="").geturl()
    url = f"{base}/_matrix/client/v3/rooms/{room_segment}/messages?{query}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - operator-provided homeserver
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"matrix_readback_http_{exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("matrix_readback_unavailable") from exc
    except json.JSONDecodeError as exc:
        raise ValueError("matrix_readback_malformed_json") from exc
