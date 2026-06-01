from __future__ import annotations

# Compatibility exports for older tests/importers. New runtime code should import
# from the focused runtime_* modules directly.
from parking_spot_monitor.runtime_commands import _poll_matrix_commands_once
from parking_spot_monitor.runtime_decision_memory import (
    _append_decision_memory,
    _append_detection_memory_records,
    _append_lab_outcome_memory,
    _append_runtime_state_memory_records,
    _rejected_summary,
    _rejection_reason_counts,
)
from parking_spot_monitor.runtime_detection import (
    _accepted_by_spot,
    _candidate_summaries,
    _candidate_summary,
    _configured_spot_polygons,
    _detect_accepts_inference_image_size,
    _detect_spot_crop_vehicles_for_frame,
    _detect_vehicles_for_frame,
    _frame_scale,
    _frame_size_dict,
    _image_size,
    _process_detection_for_capture,
    _scaled_min_bbox_area,
    _spot_polygon,
    _stringify_rejection_counts,
)
from parking_spot_monitor.runtime_overlay import (
    _is_expected_debug_overlay_error,
    _write_debug_overlay,
    _write_overlay_for_capture,
)
from parking_spot_monitor.runtime_presence import (
    _best_rejected_detection,
    _log_missed_occupied_spot_diagnostics,
    _presence_by_spot,
    _rejection_suppresses_open,
)
from parking_spot_monitor.runtime_state_update import FrameUpdateResult, _update_runtime_state_for_frame
from parking_spot_monitor.runtime_vehicle_events import (
    VehicleHistoryEventResult,
    _dataclass_like_payload,
    _estimate_for_alert,
    _occupancy_history_event_id,
    _occupied_alert_payload,
    _owner_vehicle_profile_confidence_is_high_enough,
    _owner_vehicle_quiet_window_alerts,
    _profile_label_for_alert,
    _record_vehicle_history_events,
)
from parking_spot_monitor.state import save_runtime_state
