from __future__ import annotations

# Source-backed operator/docs contract kept in this compatibility module because
# docs tests intentionally read parking_spot_monitor/matrix.py as the public
# Matrix command surface, even though implementations now live in split modules.
OPERATOR_COMMAND_SOURCE_CONTRACT = """
{command_prefix} status — show runtime health and spot status
{command_prefix} config — show safe monitor configuration
{command_prefix} latest — show latest runtime summary and raw full-frame image evidence
{command_prefix} why <spot_id> — explain recent parking decisions for one spot from bounded local memory
{command_prefix} explain <spot_id> — alias for why with the same bounded local-memory explanation
{command_prefix} recent — show recent decision, alert, suppression, command, and lab records from bounded local memory
{command_prefix} at <time> <spot_id> — review the nearest retained timeline frame and local decision memory for an incident
{command_prefix} confidence — show artifact-derived spot stability, weak evidence, timeline health, and Matrix delivery status
{command_prefix} correct <spot_id> <open|occupied> — record the actual spot state for a wrong alert
{command_prefix} learn <spot_id> <open|occupied> at <time> — record a retained-timeline calibration label for review
{command_prefix} who — list active parking sessions by spot and attach a fresh current snapshot when configured
{command_prefix} false-alert <spot_id> <open|occupied> — explicit alias for correcting a false alert
{command_prefix} missed-alert <spot_id> <open|occupied> at <time> — explicit alias for recording missed timeline evidence
{command_prefix} analytics [today|7d|30d|all] — show spot-level historical occupancy metrics from local vehicle-history sessions
{command_prefix} lab run replay — start a bounded local replay lab job using fixed inputs
{command_prefix} lab run tuning — start a bounded local tuning lab job using fixed inputs
{command_prefix} lab status [job_id|latest] — show the latest or selected redacted lab job status
usage: !parking status
usage: !parking latest
usage: !parking why <spot_id>
usage: !parking explain <spot_id>
usage: !parking recent
usage: !parking at <time> <spot_id>
usage: !parking confidence
usage: !parking correct <spot_id> <open|occupied>
usage: !parking learn <spot_id> <open|occupied> at <time>
usage: !parking who
usage: !parking analytics [today|7d|30d|all]
usage: !parking lab run <replay|tuning>
usage: !parking lab status [job_id|latest]
invalid spot id
invalid lab job kind
invalid lab job id
if parts[1] in {"correct", "false-alert"}
if parts[1] in {"learn", "missed-alert"}
Raw full-frame {image_path.name} evidence
command:{event.event_id}:image
matrix-send-failed
"""

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
