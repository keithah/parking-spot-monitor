from __future__ import annotations

from pathlib import Path
import re
import tempfile

import yaml


ROOT = Path(__file__).resolve().parents[1]


def read_tracked(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")

def read_yaml(path: str) -> object:
    return yaml.safe_load(read_tracked(path))

def assert_contains_all(text: str, required: list[str]) -> None:
    missing = [token for token in required if token not in text]
    assert not missing, f"missing documented operator tokens: {missing}"

def read_readme_section(heading: str) -> str:
    readme = read_tracked("README.md")
    pattern = re.compile(rf"^## {re.escape(heading)}\s*$", re.MULTILINE)
    match = pattern.search(readme)
    assert match is not None, f"README.md missing section heading: ## {heading}"
    next_heading = re.search(r"^## ", readme[match.end() :], re.MULTILINE)
    section_end = match.end() + next_heading.start() if next_heading else len(readme)
    return readme[match.start() : section_end]

def assert_section_case(section: str, case_name: str, required: list[str]) -> None:
    missing = [token for token in required if token not in section]
    assert not missing, f"README.md troubleshooting case '{case_name}' missing tokens: {missing}"

def read_matrix_command_contract() -> str:
    from parking_spot_monitor.matrix_command_catalog import format_command_help_reply
    from parking_spot_monitor.matrix_commands import MatrixCommandParseError, MatrixCommandResponse, MatrixCommandService, MatrixTextEvent, parse_matrix_command

    def parse_error(body: str) -> str:
        try:
            parse_matrix_command(body)
        except MatrixCommandParseError as exc:
            return str(exc)
        raise AssertionError(f"expected Matrix command parse failure for {body!r}")

    class Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def send_text(self, **kwargs: object) -> str:
            self.calls.append({"kind": "text", **kwargs})
            return "$text"

        def upload_image(self, **kwargs: object) -> str:
            self.calls.append({"kind": "upload", **kwargs})
            return "matrix-content-uri"

        def send_image(self, **kwargs: object) -> str:
            self.calls.append({"kind": "image", **kwargs})
            return "$image"

    client = Client()
    service = MatrixCommandService(client=client, archive=object(), room_id="!room:example", authorized_senders=["@op:example"])
    with tempfile.TemporaryDirectory() as tmp:
        image_path = Path(tmp) / "latest.jpg"
        image_path.write_bytes(b"jpeg")
        service._send_command_response(
            MatrixTextEvent(event_id="$event", sender="@op:example", room_id="!room:example", body="!parking latest"),
            MatrixCommandResponse(text="Latest", image_path=image_path, image_info={"mimetype": "image/jpeg", "size": 4, "w": 1, "h": 1}),
        )
    image_call = next(call for call in client.calls if call["kind"] == "image")

    false_alert = parse_matrix_command("!parking false-alert left_spot open")
    missed_alert = parse_matrix_command("!parking missed-alert left_spot occupied at 2026-05-18T19:00:00Z")
    return "\n".join(
        [
            format_command_help_reply("{command_prefix}"),
            parse_error("!parking correct"),
            parse_error("!parking learn left_spot open"),
            parse_error("!parking who extra"),
            parse_error("!parking why"),
            parse_error("!parking explain"),
            parse_error("!parking analytics bogus"),
            parse_error("!parking at"),
            parse_error("!parking confidence extra"),
            parse_error("!parking lab run"),
            parse_error("!parking lab run bogus"),
            parse_error("!parking lab status latest extra"),
            parse_error("!parking lab status bogus"),
            parse_error("!parking why bad/spot"),
            parse_error("!parking recent extra"),
            f"false-alert action={false_alert.action}",
            f"false-alert spot_id={false_alert.spot_id}",
            f"false-alert actual_state={false_alert.actual_state}",
            f"missed-alert action={missed_alert.action}",
            f"missed-alert spot_id={missed_alert.spot_id}",
            f"missed-alert actual_state={missed_alert.actual_state}",
            f"missed-alert subject_id={missed_alert.subject_id}",
            str(image_call["body"]),
            str(image_call["txn_id"]),
        ]
    )
