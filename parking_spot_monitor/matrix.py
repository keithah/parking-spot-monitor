from __future__ import annotations

from parking_spot_monitor.matrix_alerts import (
    LIFECYCLE_EVENT_TYPES,
    MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
    MONITOR_STARTED_EVENT_TYPE,
    OCCUPIED_SPOT_EVENT_TYPE,
    OPEN_SPOT_EVENT_TYPE,
    OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
    format_lifecycle_notice,
    format_live_proof_image_body,
    format_live_proof_text,
    format_occupied_spot_alert,
    format_open_spot_alert,
    format_owner_vehicle_quiet_window_alert,
    format_quiet_window_notice,
    live_proof_event_id,
    monitor_lifecycle_event,
    monitor_lifecycle_event_id,
    occupied_spot_event_id,
    open_spot_event_id,
    owner_vehicle_quiet_window_event_id,
)
from parking_spot_monitor.matrix_client import CLIENT_API_PREFIX, MEDIA_API_PREFIX, MatrixClient
from parking_spot_monitor.matrix_cockpit import (
    MatrixOperatorCockpitContext,
    build_incident_review_response,
    build_latest_snapshot_response,
    format_detection_lab_run_reply,
    format_detection_lab_status_reply,
    format_operator_analytics_reply,
    format_operator_confidence_reply,
    format_operator_config_reply,
    format_operator_recent_reply,
    format_operator_status_reply,
    format_operator_why_reply,
)
from parking_spot_monitor.matrix_commands import (
    MatrixCommandService,
    parse_matrix_command,
)
from parking_spot_monitor.matrix_delivery import MatrixDelivery
from parking_spot_monitor.matrix_models import (
    MatrixCommand,
    MatrixCommandParseError,
    MatrixCommandPollResult,
    MatrixCommandResponse,
    MatrixSyncResult,
    MatrixTextEvent,
)
from parking_spot_monitor.matrix_snapshots import (
    JPEG_MIMETYPE,
    MAX_MATRIX_UPLOAD_IMAGE_BYTES,
    MATRIX_UPLOAD_INITIAL_MAX_DIMENSION,
    MATRIX_UPLOAD_JPEG_QUALITIES,
    MATRIX_UPLOAD_MIN_DIMENSION,
    MatrixSnapshot,
    SnapshotRetentionResult,
    prepare_event_snapshot,
    prune_event_snapshots,
)
from parking_spot_monitor.matrix_support import (
    MatrixError,
)
