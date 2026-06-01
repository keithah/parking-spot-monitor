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
from parking_spot_monitor.matrix_client import CLIENT_API_PREFIX, MEDIA_API_PREFIX, MatrixClient, _parse_sync_response, _room_message_path
from parking_spot_monitor.matrix_cockpit import (
    MatrixOperatorCockpitContext,
    _active_spot_assignments_with_runtime_status,
    _format_active_spot_assignments_reply,
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
    _coerce_command_response,
    _confidence_text,
    _format_command_help_reply,
    _format_profile_summary_reply,
    _validate_actual_state,
    _validate_command_image_info,
    _validate_label,
    _validate_lab_job_id,
    _validate_lab_kind,
    _validate_profile_id,
    _validate_spot_id,
    _validate_subject_id,
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
    _matrix_snapshot_upload,
)
from parking_spot_monitor.matrix_support import (
    MatrixError,
    _http_status_error,
    _require_non_empty,
    _require_response_key,
    _sanitize_diagnostics,
)
