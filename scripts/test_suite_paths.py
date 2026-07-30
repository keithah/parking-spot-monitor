"""Canonical focused test-module groups used by closeout smoke runners."""

CONFIG_TEST_MODULES = (
    "tests/test_config_matrix.py",
    "tests/test_config_model_and_storage.py",
    "tests/test_config_paths_and_streams.py",
    "tests/test_config_runtime_validation.py",
)

MATRIX_TEST_MODULES = (
    "tests/test_matrix_client_and_delivery.py",
    "tests/test_matrix_command_service_analytics.py",
    "tests/test_matrix_command_service_core.py",
    "tests/test_matrix_command_service_media.py",
    "tests/test_matrix_messages_and_parsing.py",
    "tests/test_matrix_snapshot_retention.py",
    "tests/test_matrix_snapshot_upload.py",
)

MATRIX_COCKPIT_TEST_MODULES = (
    "tests/test_matrix_operator_cockpit_analytics.py",
    "tests/test_matrix_operator_cockpit_incident_and_lab.py",
    "tests/test_matrix_operator_cockpit_latest_and_confidence.py",
    "tests/test_matrix_operator_cockpit_status.py",
    "tests/test_matrix_operator_cockpit_who_resize.py",
)

MATRIX_OUTBOX_TEST_MODULES = (
    "tests/test_matrix_outbox_retention_and_failures.py",
    "tests/test_matrix_outbox_retry_and_derivatives.py",
    "tests/test_matrix_outbox_worker_lifecycle.py",
)

OPERATOR_FEEDBACK_TEST_MODULES = (
    "tests/test_operator_feedback_alert_corrections.py",
    "tests/test_operator_feedback_learning_models.py",
    "tests/test_operator_feedback_learning_replay.py",
    "tests/test_operator_feedback_models_and_store.py",
)

OUTBOX_PERSISTENCE_TEST_MODULES = (
    "tests/test_outbox_persistence_concurrency.py",
    "tests/test_outbox_persistence_phases.py",
    "tests/test_outbox_persistence_retention.py",
    "tests/test_outbox_persistence_scheduling.py",
)

STARTUP_TEST_MODULES = (
    "tests/test_startup_cadence_and_shutdown.py",
    "tests/test_startup_capture_and_pacing.py",
    "tests/test_startup_decision_memory.py",
    "tests/test_startup_detection_processing.py",
    "tests/test_startup_failures_and_state.py",
    "tests/test_startup_live_proof_and_bootstrap.py",
    "tests/test_startup_matrix_dispatch.py",
    "tests/test_startup_model_and_logging.py",
    "tests/test_startup_recovery_and_cli.py",
    "tests/test_startup_runtime_alerts.py",
    "tests/test_startup_runtime_commands_and_health.py",
    "tests/test_startup_services_and_outbox.py",
)

VEHICLE_HISTORY_TEST_MODULES = (
    "tests/test_vehicle_history_corrections.py",
    "tests/test_vehicle_history_jpeg_publication.py",
    "tests/test_vehicle_history_maintenance.py",
    "tests/test_vehicle_history_owned_cleanup.py",
    "tests/test_vehicle_history_profiles.py",
    "tests/test_vehicle_history_recovery_and_images.py",
    "tests/test_vehicle_history_sessions_and_health.py",
)
