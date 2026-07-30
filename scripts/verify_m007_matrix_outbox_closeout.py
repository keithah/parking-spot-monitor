#!/usr/bin/env python3
"""Finite M007 Matrix outbox closeout smoke runner.

The runner is intentionally secret-free and finite. It validates focused host
regressions, Docker packaging/config rendering, then uses the built image for a
two-container restart-like smoke against the same mounted /data directory:
first a Matrix upload failure leaves a retrying outbox plus health visibility;
second a fresh container drains the same outbox and proves already-delivered
phases are skipped.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.closeout_helpers import (
    assert_no_forbidden_markers,
    bounded_text,
    redact_text,
    safe_output,
    smoke_env,
)
from scripts.test_suite_paths import MATRIX_OUTBOX_TEST_MODULES

TIMEOUT_SECONDS = 180
DOCKER_TIMEOUT_SECONDS = 300
OUTPUT_LIMIT = 4_000
DOCKER_IMAGE_TAG = "parking-spot-monitor:m007-outbox-smoke"
PLACEHOLDER_RTSP_URL = "placeholder-rtsp-url-for-m007-closeout"
PLACEHOLDER_MATRIX_TOKEN = "placeholder-matrix-token-for-m007-closeout"
M007_ENV_PASSTHROUGH_KEYS = (
    "DOCKER_HOST",
    "DOCKER_CONTEXT",
    "DOCKER_TLS_VERIFY",
    "DOCKER_CERT_PATH",
    "DOCKER_CONFIG",
    "XDG_RUNTIME_DIR",
    "BUILDKIT_HOST",
    "DOCKER_BUILDKIT",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)

M007_CLOSEOUT_START = "M007_CLOSEOUT_START"
M007_CLOSEOUT_PASS = "M007_CLOSEOUT_PASS"
M007_CLOSEOUT_FAIL = "M007_CLOSEOUT_FAIL"
M007_CLOSEOUT_RESULT = "M007_CLOSEOUT_RESULT"
M007_OUTBOX_FAILURE_OK = "M007_OUTBOX_FAILURE_OK"
M007_OUTBOX_RECOVERY_OK = "M007_OUTBOX_RECOVERY_OK"
M007_OUTBOX_HEALTH_OK = "M007_OUTBOX_HEALTH_OK"
M007_OUTBOX_DEAD_LETTER_OK = "M007_OUTBOX_DEAD_LETTER_OK"
M007_OUTBOX_QUARANTINE_OK = "M007_OUTBOX_QUARANTINE_OK"
M007_OUTBOX_RETENTION_OK = "M007_OUTBOX_RETENTION_OK"

SENSITIVE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"rtsp://[^\s'\"]+", re.IGNORECASE),
    re.compile(r"(?i)bearer\s+[^\s'\"]+"),
    re.compile(r"(?i)(matrix[_-]?(?:access[_-]?)?token|access_token|authorization)([=:]\s*)([^\s'\"]+)"),
    re.compile(re.escape(PLACEHOLDER_RTSP_URL)),
    re.compile(re.escape(PLACEHOLDER_MATRIX_TOKEN)),
    re.compile(r"Traceback \(most recent call last\)", re.IGNORECASE),
    re.compile(r"BEGIN RAW IMAGE BYTES|END RAW IMAGE BYTES", re.IGNORECASE),
)

FORBIDDEN_OUTPUT_MARKERS = (
    PLACEHOLDER_RTSP_URL,
    PLACEHOLDER_MATRIX_TOKEN,
    "Traceback (most recent call last)",
    "BEGIN RAW IMAGE BYTES",
    "END RAW IMAGE BYTES",
)


@dataclass(frozen=True)
class SmokeCommand:
    label: str
    argv: tuple[str, ...]
    timeout_seconds: int = TIMEOUT_SECONDS


def _failure_smoke_snippet() -> str:
    return r'''
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery
from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.__main__ import _matrix_outbox_health_payload
from parking_spot_monitor.health import HealthStatus, write_health_status
from parking_spot_monitor.matrix import MatrixError

DATA = Path('/data')
LEAK_MARKER = 'should-' + 'not-leak'
ROOM_ID = '!parking-room:example.org'
EVENT_ID = 'occupancy-open-event:left_spot:2026-05-18T20:01:02Z'

class FakeMatrixClient:
    def __init__(self):
        self.calls = []

    def send_text(self, *, room_id, txn_id, body):
        self.calls.append({'kind': 'text', 'room_id': room_id, 'txn_id': txn_id, 'body': body})
        return '$text:example.org'

    def upload_image(self, *, filename, data, content_type):
        self.calls.append({'kind': 'upload', 'filename': filename, 'content_type': content_type, 'bytes': len(data)})
        raise MatrixError(f'upload timeout Authorization: Bearer {LEAK_MARKER}', error_type='timeout')

    def send_image(self, *, room_id, txn_id, body, content_uri, info):
        self.calls.append({'kind': 'image', 'room_id': room_id, 'txn_id': txn_id})
        return '$image:example.org'

DATA.mkdir(parents=True, exist_ok=True)
source = DATA / 'latest.jpg'
Image.new('RGB', (8, 6), color=(25, 50, 75)).save(source, format='JPEG')
client = FakeMatrixClient()
delivery = MatrixOutboxDelivery(
    client=client,
    room_id=ROOM_ID,
    data_dir=DATA,
    snapshots_dir=DATA / 'snapshots',
    outbox=LocalOutbox(DATA / 'matrix-outbox.json'),
)
result = delivery.send_open_spot_alert({
    'event_type': 'occupancy-open-event',
    'spot_id': 'left_spot',
    'previous_status': 'occupied',
    'new_status': 'empty',
    'observed_at': datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc),
    'snapshot_path': str(source),
})
assert result.retrying_count == 1, result
assert [call['kind'] for call in client.calls] == ['text', 'upload'], client.calls
records = LocalOutbox(DATA / 'matrix-outbox.json').list_records()
assert len(records) == 1, records
record = records[0]
assert record.state == 'retrying', record
assert record.retry_reason == 'matrix_upload_timeout', record.retry_reason
assert record.phase_states == {'text': 'delivered', 'upload': 'pending', 'image': 'pending'}, record.phase_states
assert record.phase_results['text'] == {'matrix_event_id': '$text:example.org'}, record.phase_results
health_payload = _matrix_outbox_health_payload(DATA / 'matrix-outbox.json')
write_health_status(
    DATA / 'health.json',
    HealthStatus(status='degraded', updated_at='2026-05-18T20:02:00Z', iteration=1, matrix_outbox=health_payload),
)
health = json.loads((DATA / 'health.json').read_text(encoding='utf-8'))
matrix_outbox = health['matrix_outbox']
assert matrix_outbox['available'] is True, matrix_outbox
assert matrix_outbox['counts_by_state'] == {'retrying': 1}, matrix_outbox
assert matrix_outbox['retry_reason_counts'] == {'matrix_upload_timeout': 1}, matrix_outbox
assert {phase['phase']: phase['state'] for phase in matrix_outbox['items'][0]['phases']} == {
    'image': 'pending',
    'text': 'delivered',
    'upload': 'pending',
}, matrix_outbox
rendered = json.dumps(health).lower()
assert LEAK_MARKER not in rendered
assert 'authorization' not in rendered
print('M007_OUTBOX_FAILURE_OK state=retrying phases=text:delivered,upload:pending,image:pending')
print('M007_OUTBOX_HEALTH_OK counts_by_state=retrying:1 retry_reason=matrix_upload_timeout')
'''.strip()


def _recovery_smoke_snippet() -> str:
    return r'''
from __future__ import annotations

import json
from pathlib import Path

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery
from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.__main__ import _matrix_outbox_health_payload
from parking_spot_monitor.health import HealthStatus, write_health_status

DATA = Path('/data')
ROOM_ID = '!parking-room:example.org'
EVENT_ID = 'occupancy-open-event:left_spot:2026-05-18T20:01:02Z'

class FakeMatrixClient:
    def __init__(self):
        self.calls = []

    def send_text(self, *, room_id, txn_id, body):
        self.calls.append({'kind': 'text', 'room_id': room_id, 'txn_id': txn_id, 'body': body})
        return '$text:example.org'

    def upload_image(self, *, filename, data, content_type):
        self.calls.append({'kind': 'upload', 'filename': filename, 'content_type': content_type, 'bytes': len(data)})
        return 'mxc://example.org/recovered-open'

    def send_image(self, *, room_id, txn_id, body, content_uri, info):
        self.calls.append({'kind': 'image', 'room_id': room_id, 'txn_id': txn_id, 'content_uri': content_uri, 'info': dict(info)})
        return '$image:example.org'

client = FakeMatrixClient()
delivery = MatrixOutboxDelivery(
    client=client,
    room_id=ROOM_ID,
    data_dir=DATA,
    snapshots_dir=DATA / 'snapshots',
    outbox=LocalOutbox(DATA / 'matrix-outbox.json'),
)
result = delivery.drain_outbox()
assert result.attempted_count == 1, result
assert result.delivered_count == 1, result
assert result.retrying_count == 0, result
assert [call['kind'] for call in client.calls] == ['upload', 'image'], client.calls
assert client.calls[1]['txn_id'] == f'{EVENT_ID}:image', client.calls
assert client.calls[1]['content_uri'] == 'mxc://example.org/recovered-open', client.calls
records = LocalOutbox(DATA / 'matrix-outbox.json').list_records()
assert len(records) == 1, records
record = records[0]
assert record.state == 'delivered', record
assert record.phase_states == {'text': 'delivered', 'upload': 'delivered', 'image': 'delivered'}, record.phase_states
health_payload = _matrix_outbox_health_payload(DATA / 'matrix-outbox.json')
write_health_status(
    DATA / 'health.json',
    HealthStatus(status='ok', updated_at='2026-05-18T20:03:00Z', iteration=2, matrix_outbox=health_payload),
)
health = json.loads((DATA / 'health.json').read_text(encoding='utf-8'))
assert health['matrix_outbox']['counts_by_state'] == {'delivered': 1}, health
print('M007_OUTBOX_RECOVERY_OK state=delivered skipped=text called=upload,image')
'''.strip()


def _quarantine_smoke_snippet() -> str:
    return r'''
from __future__ import annotations

import json
from pathlib import Path

from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.__main__ import _matrix_outbox_health_payload
from parking_spot_monitor.health import HealthStatus, write_health_status

DATA = Path('/data')
DATA.mkdir(parents=True, exist_ok=True)
leak_marker = 'quarantine-' + 'secret'
raw_payload = '{"schema_version": 1, "items": [ BEGIN RAW IMAGE BYTES Authorization: Bearer ' + leak_marker
(DATA / 'matrix-outbox.json').write_text(raw_payload, encoding='utf-8')

summary = _matrix_outbox_health_payload(DATA / 'matrix-outbox.json')
assert summary is not None, summary
assert summary['available'] is True, summary
assert summary['counts_by_state'] == {}, summary
assert summary['recovery']['quarantined_count'] == 1, summary
assert summary['recovery']['reason_counts'] == {'invalid_json': 1}, summary
assert summary['recovery']['events'][0]['reason'] == 'invalid_json', summary
quarantine_path = Path(summary['recovery']['events'][0]['quarantine_path'])
assert quarantine_path.is_file(), summary
assert quarantine_path.parent.name == '.matrix-outbox-quarantine', summary
write_health_status(
    DATA / 'health.json',
    HealthStatus(status='degraded', updated_at='2026-05-18T20:04:00Z', iteration=3, matrix_outbox=summary),
)
health = json.loads((DATA / 'health.json').read_text(encoding='utf-8'))
rendered = json.dumps(health).lower()
assert leak_marker not in rendered
assert 'authorization' not in rendered
assert 'begin raw image bytes' not in rendered
assert 'traceback' not in rendered
assert LocalOutbox(DATA / 'matrix-outbox.json').list_records() == []
print('M007_OUTBOX_QUARANTINE_OK reason=invalid_json quarantined_count=1')
'''.strip()


def _dead_letter_smoke_snippet() -> str:
    return r'''
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery
from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.__main__ import _matrix_outbox_health_payload
from parking_spot_monitor.health import HealthStatus, write_health_status
from parking_spot_monitor.matrix import MatrixError

DATA = Path('/data')
ROOM_ID = '!parking-room:example.org'
DATA.mkdir(parents=True, exist_ok=True)
source = DATA / 'dead-letter-latest.jpg'
Image.new('RGB', (8, 6), color=(90, 20, 20)).save(source, format='JPEG')

class PermanentFailureClient:
    def __init__(self):
        self.calls = []

    def send_text(self, *, room_id, txn_id, body):
        self.calls.append({'kind': 'text', 'txn_id': txn_id})
        return '$dead-letter-text:example.org'

    def upload_image(self, *, filename, data, content_type):
        self.calls.append({'kind': 'upload', 'filename': filename, 'bytes': len(data)})
        raise MatrixError('Matrix upload rejected', error_type='http_status', status_code=403)

    def send_image(self, *, room_id, txn_id, body, content_uri, info):
        self.calls.append({'kind': 'image', 'txn_id': txn_id})
        return '$unexpected:example.org'

client = PermanentFailureClient()
outbox = LocalOutbox(DATA / 'matrix-outbox.json')
delivery = MatrixOutboxDelivery(
    client=client,
    room_id=ROOM_ID,
    data_dir=DATA,
    snapshots_dir=DATA / 'snapshots',
    outbox=outbox,
)
result = delivery.send_open_spot_alert({
    'event_type': 'occupancy-open-event',
    'spot_id': 'dead_letter_spot',
    'previous_status': 'occupied',
    'new_status': 'empty',
    'observed_at': datetime(2026, 5, 18, 20, 5, 0, tzinfo=timezone.utc),
    'snapshot_path': str(source),
})
assert result.attempted_count == 1, result
assert result.delivered_count == 0, result
assert result.retrying_count == 0, result
assert [call['kind'] for call in client.calls] == ['text', 'upload'], client.calls
records = LocalOutbox(DATA / 'matrix-outbox.json').list_records()
assert len(records) == 1, records
record = records[0]
assert record.state == 'dead_lettered', record
assert record.dead_letter_reason == 'matrix_upload_http_403', record.dead_letter_reason
assert record.phase_states == {'text': 'delivered', 'upload': 'failed', 'image': 'pending'}, record.phase_states

later_client = PermanentFailureClient()
later_delivery = MatrixOutboxDelivery(
    client=later_client,
    room_id=ROOM_ID,
    data_dir=DATA,
    snapshots_dir=DATA / 'snapshots',
    outbox=LocalOutbox(DATA / 'matrix-outbox.json'),
)
later = later_delivery.drain_outbox()
assert later.attempted_count == 0, later
assert later_client.calls == [], later_client.calls
health_payload = _matrix_outbox_health_payload(DATA / 'matrix-outbox.json')
write_health_status(
    DATA / 'health.json',
    HealthStatus(status='degraded', updated_at='2026-05-18T20:06:00Z', iteration=4, matrix_outbox=health_payload),
)
health = json.loads((DATA / 'health.json').read_text(encoding='utf-8'))
matrix_outbox = health['matrix_outbox']
assert matrix_outbox['counts_by_state'] == {'dead_lettered': 1}, matrix_outbox
assert matrix_outbox['dead_letter_reason_counts'] == {'matrix_upload_http_403': 1}, matrix_outbox
assert matrix_outbox['items'][0]['dead_letter_reason'] == 'matrix_upload_http_403', matrix_outbox
assert {phase['phase']: phase['state'] for phase in matrix_outbox['items'][0]['phases']} == {
    'image': 'pending',
    'text': 'delivered',
    'upload': 'failed',
}, matrix_outbox
rendered = json.dumps(health).lower()
assert 'matrix upload rejected' not in rendered
assert 'authorization' not in rendered
print('M007_OUTBOX_DEAD_LETTER_OK state=dead_lettered reason=matrix_upload_http_403 later_attempted=0')
'''.strip()


def _retention_smoke_snippet() -> str:
    return r'''
from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery
from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.matrix import MatrixError

DATA = Path('/data')
ROOM_ID = '!parking-room:example.org'
DATA.mkdir(parents=True, exist_ok=True)
(DATA / 'matrix-outbox.json').unlink(missing_ok=True)
shutil.rmtree(DATA / 'snapshots', ignore_errors=True)
source = DATA / 'latest.jpg'
Image.new('RGB', (12, 10), color=(5, 100, 200)).save(source, format='JPEG')

class FirstPassClient:
    def __init__(self):
        self.calls = []

    def send_text(self, *, room_id, txn_id, body):
        self.calls.append({'kind': 'text', 'txn_id': txn_id})
        return '$retention-text:example.org'

    def upload_image(self, *, filename, data, content_type):
        self.calls.append({'kind': 'upload', 'filename': filename, 'bytes': len(data)})
        raise MatrixError('temporary upload timeout', error_type='timeout')

    def send_image(self, *, room_id, txn_id, body, content_uri, info):
        self.calls.append({'kind': 'image', 'txn_id': txn_id})
        return '$unexpected:example.org'

first = FirstPassClient()
delivery = MatrixOutboxDelivery(
    client=first,
    room_id=ROOM_ID,
    data_dir=DATA,
    snapshots_dir=DATA / 'snapshots',
    outbox=LocalOutbox(DATA / 'matrix-outbox.json'),
    snapshot_retention_count=1,
)
first_result = delivery.send_open_spot_alert({
    'event_type': 'occupancy-open-event',
    'spot_id': 'retention_spot',
    'previous_status': 'occupied',
    'new_status': 'empty',
    'observed_at': datetime(2026, 5, 18, 20, 7, 0, tzinfo=timezone.utc),
    'snapshot_path': str(source),
})
assert first_result.retrying_count == 1, first_result
record = LocalOutbox(DATA / 'matrix-outbox.json').list_records()[0]
retained_path = Path(record.intent.metadata['retained_snapshot_path'])
assert retained_path.is_file(), record
original_retained_bytes = retained_path.read_bytes()
stale = DATA / 'snapshots' / 'occupancy-open-event-stale-2026-05-18t20-00-00z.jpg'
Image.new('RGB', (12, 10), color=(250, 10, 10)).save(stale, format='JPEG')
os.utime(stale, (1, 1))
Image.new('RGB', (12, 10), color=(10, 250, 10)).save(source, format='JPEG')
assert source.read_bytes() != original_retained_bytes

class RecoveryClient:
    def __init__(self):
        self.upload_bytes = None
        self.calls = []

    def send_text(self, *, room_id, txn_id, body):
        self.calls.append({'kind': 'text', 'txn_id': txn_id})
        return '$retention-text-again:example.org'

    def upload_image(self, *, filename, data, content_type):
        self.calls.append({'kind': 'upload', 'filename': filename, 'bytes': len(data)})
        self.upload_bytes = data
        return 'mxc://example.org/retained-original'

    def send_image(self, *, room_id, txn_id, body, content_uri, info):
        self.calls.append({'kind': 'image', 'txn_id': txn_id, 'content_uri': content_uri})
        return '$retention-image:example.org'

recovery = RecoveryClient()
recovery_delivery = MatrixOutboxDelivery(
    client=recovery,
    room_id=ROOM_ID,
    data_dir=DATA,
    snapshots_dir=DATA / 'snapshots',
    outbox=LocalOutbox(DATA / 'matrix-outbox.json'),
    snapshot_retention_count=1,
)
result = recovery_delivery.drain_outbox()
assert result.attempted_count == 1, result
assert result.delivered_count == 1, result
assert [call['kind'] for call in recovery.calls] == ['upload', 'image'], recovery.calls
assert recovery.upload_bytes == original_retained_bytes
assert retained_path.is_file(), retained_path
assert not stale.exists(), stale
records = LocalOutbox(DATA / 'matrix-outbox.json').list_records()
assert records[0].state == 'delivered', records
assert records[0].phase_results['upload']['content_uri'] == 'mxc://example.org/retained-original', records[0].phase_results
print('M007_OUTBOX_RETENTION_OK retained_original_upload=true stale_pruned=true state=delivered')
'''.strip()


def _build_commands(temp_data_dir: Path) -> tuple[SmokeCommand, ...]:
    config_path = ROOT / "config.yaml.example"
    volume_arg = f"{temp_data_dir}:/data"
    return (
        SmokeCommand(
            label="pytest-matrix-outbox-health",
            argv=(
                sys.executable,
                "-m",
                "pytest",
                *MATRIX_OUTBOX_TEST_MODULES,
                "tests/test_health.py",
                "tests/test_startup_services_and_outbox.py::test_runtime_open_alert_failure_persists_retryable_matrix_outbox",
                "tests/test_startup_services_and_outbox.py::test_runtime_worker_restarts_existing_matrix_outbox_without_new_occupancy_event",
                "-q",
            ),
        ),
        SmokeCommand(
            label="validate-config-entrypoint",
            argv=(
                sys.executable,
                "-m",
                "parking_spot_monitor",
                "--config",
                "config.yaml.example",
                "--validate-config",
            ),
            timeout_seconds=30,
        ),
        SmokeCommand(
            label="docker-build",
            argv=("docker", "build", "-t", DOCKER_IMAGE_TAG, "."),
            timeout_seconds=DOCKER_TIMEOUT_SECONDS,
        ),
        SmokeCommand(
            label="docker-compose-config",
            argv=("docker", "compose", "config", "--quiet"),
            timeout_seconds=30,
        ),
        SmokeCommand(
            label="docker-matrix-outbox-failure",
            argv=(
                "docker",
                "run",
                "--rm",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-e",
                "RTSP_URL",
                "-e",
                "MATRIX_ACCESS_TOKEN",
                "-v",
                f"{config_path}:/config/config.yaml:ro",
                "-v",
                volume_arg,
                DOCKER_IMAGE_TAG,
                "python",
                "-c",
                _failure_smoke_snippet(),
            ),
            timeout_seconds=60,
        ),
        SmokeCommand(
            label="docker-matrix-outbox-recovery",
            argv=(
                "docker",
                "run",
                "--rm",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-e",
                "RTSP_URL",
                "-e",
                "MATRIX_ACCESS_TOKEN",
                "-v",
                f"{config_path}:/config/config.yaml:ro",
                "-v",
                volume_arg,
                DOCKER_IMAGE_TAG,
                "python",
                "-c",
                _recovery_smoke_snippet(),
            ),
            timeout_seconds=60,
        ),
        SmokeCommand(
            label="docker-matrix-outbox-quarantine",
            argv=(
                "docker",
                "run",
                "--rm",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-e",
                "RTSP_URL",
                "-e",
                "MATRIX_ACCESS_TOKEN",
                "-v",
                f"{config_path}:/config/config.yaml:ro",
                "-v",
                volume_arg,
                DOCKER_IMAGE_TAG,
                "python",
                "-c",
                _quarantine_smoke_snippet(),
            ),
            timeout_seconds=60,
        ),
        SmokeCommand(
            label="docker-matrix-outbox-dead-letter",
            argv=(
                "docker",
                "run",
                "--rm",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-e",
                "RTSP_URL",
                "-e",
                "MATRIX_ACCESS_TOKEN",
                "-v",
                f"{config_path}:/config/config.yaml:ro",
                "-v",
                volume_arg,
                DOCKER_IMAGE_TAG,
                "python",
                "-c",
                _dead_letter_smoke_snippet(),
            ),
            timeout_seconds=60,
        ),
        SmokeCommand(
            label="docker-matrix-outbox-retention",
            argv=(
                "docker",
                "run",
                "--rm",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-e",
                "RTSP_URL",
                "-e",
                "MATRIX_ACCESS_TOKEN",
                "-v",
                f"{config_path}:/config/config.yaml:ro",
                "-v",
                volume_arg,
                DOCKER_IMAGE_TAG,
                "python",
                "-c",
                _retention_smoke_snippet(),
            ),
            timeout_seconds=60,
        ),
    )


def _run_command(command: SmokeCommand, *, env: Mapping[str, str]) -> int:
    print(f"{M007_CLOSEOUT_START} {command.label}", flush=True)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            list(command.argv),
            cwd=ROOT,
            env=dict(env),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=command.timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        rendered = safe_output(
            exc.stdout,
            exc.stderr,
            patterns=SENSITIVE_PATTERNS,
            limit=OUTPUT_LIMIT,
        )
        assert_no_forbidden_markers(rendered, FORBIDDEN_OUTPUT_MARKERS)
        print(f"{M007_CLOSEOUT_FAIL} {command.label} timeout_seconds={command.timeout_seconds} duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 124
    except FileNotFoundError as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        rendered = safe_output(
            "",
            str(exc),
            patterns=SENSITIVE_PATTERNS,
            limit=OUTPUT_LIMIT,
        )
        assert_no_forbidden_markers(rendered, FORBIDDEN_OUTPUT_MARKERS)
        print(f"{M007_CLOSEOUT_FAIL} {command.label} exit_code=127 duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 127

    elapsed_ms = int((time.monotonic() - started) * 1000)
    rendered = safe_output(
        completed.stdout,
        completed.stderr,
        patterns=SENSITIVE_PATTERNS,
        limit=OUTPUT_LIMIT,
    )
    assert_no_forbidden_markers(rendered, FORBIDDEN_OUTPUT_MARKERS)
    if completed.returncode == 0:
        print(f"{M007_CLOSEOUT_PASS} {command.label} duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 0

    print(f"{M007_CLOSEOUT_FAIL} {command.label} exit_code={completed.returncode} duration_ms={elapsed_ms}", flush=True)
    if rendered:
        print(rendered, flush=True)
    return completed.returncode


def main(argv: Sequence[str] | None = None) -> int:
    if argv:
        print("usage: verify_m007_matrix_outbox_closeout.py", file=sys.stderr)
        return 2

    env = smoke_env(
        rtsp_placeholder=PLACEHOLDER_RTSP_URL,
        matrix_token_placeholder=PLACEHOLDER_MATRIX_TOKEN,
        base=None,
        pythonpath_prefix=str(ROOT / "src"),
        passthrough_keys=M007_ENV_PASSTHROUGH_KEYS,
    )
    with tempfile.TemporaryDirectory(prefix="m007-outbox-smoke-data-") as temp_dir:
        for command in _build_commands(Path(temp_dir)):
            exit_code = _run_command(command, env=env)
            if exit_code != 0:
                print(f"{M007_CLOSEOUT_RESULT} failed label={command.label} exit_code={exit_code}", flush=True)
                return exit_code
    print(f"{M007_CLOSEOUT_RESULT} passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
