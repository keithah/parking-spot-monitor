from __future__ import annotations

from tests.support._vehicle_history import *  # noqa: F403


@pytest.mark.parametrize(
    ("bbox", "message"),
    [
        ((1, 1, 1, 3), "empty"),
        ((math.nan, 1, 3, 4), "finite"),
        ((20, 20, 25, 25), "empty"),
    ],
)
def test_attach_occupied_images_rejects_invalid_bbox_without_mutating_session_json(
    tmp_path: Path, bbox: tuple[float, float, float, float], message: str
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event(spot_id="bad bbox"))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    before = active_path.read_text()
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))

    with pytest.raises(ArchiveWriteError, match=message):
        archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=bbox)

    assert active_path.read_text() == before
    assert not list((tmp_path / "vehicle-history" / "images").rglob("*.jpg"))


def test_attach_occupied_images_rejects_missing_and_non_jpeg_sources_without_mutating_session_json(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    missing_active = archive.start_session(occupied_event(spot_id="missing source"))
    missing_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{missing_active.session_id}.json"
    missing_before = missing_path.read_text()

    with pytest.raises(ArchiveWriteError, match="missing or unreadable"):
        archive.attach_occupied_images(session_id=missing_active.session_id, source_frame_path=tmp_path / "missing.jpg", bbox=(0, 0, 2, 2))

    assert missing_path.read_text() == missing_before

    non_jpeg_active = archive.start_session(occupied_event(spot_id="non jpeg"))
    non_jpeg_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{non_jpeg_active.session_id}.json"
    non_jpeg_before = non_jpeg_path.read_text()
    png = tmp_path / "source.png"
    Image.new("RGB", (4, 4), (1, 2, 3)).save(png, format="PNG")

    with pytest.raises(ArchiveWriteError, match="must be a JPEG"):
        archive.attach_occupied_images(session_id=non_jpeg_active.session_id, source_frame_path=png, bbox=(0, 0, 2, 2))

    assert non_jpeg_path.read_text() == non_jpeg_before
    assert not list((tmp_path / "vehicle-history" / "images").rglob("*.jpg"))


def test_image_atomic_replace_failure_cleans_temp_files_and_keeps_active_json_unmodified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="replace fail"))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    before = active_path.read_text()
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))
    real_replace = os.replace

    def failing_replace(
        src: str | bytes | os.PathLike[str],
        dst: str | bytes | os.PathLike[str],
        *args: object,
        **kwargs: object,
    ) -> None:
        if kwargs.get("dst_dir_fd") is not None and Path(dst).suffix == ".jpg":
            raise PermissionError("cannot write rtsp://camera access_token=supersecret Traceback raw_image_bytes")
        real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, "replace", failing_replace)

    with pytest.raises(ArchiveWriteError):
        archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=(0, 0, 2, 2))

    assert active_path.read_text() == before
    images_dir = tmp_path / "vehicle-history" / "images"
    assert not list(images_dir.rglob("*.jpg")) if images_dir.exists() else True
    assert not list(images_dir.rglob("*.tmp")) if images_dir.exists() else True
    records = logger_records(stream)
    failure = [record for record in records if record["event"] == "vehicle-session-images-failed"][-1]
    assert failure["phase"] == "image-capture"
    assert failure["path_name"] == f"{active.session_id}.json"
    assert failure["session_id"] == active.session_id
    assert failure["error_type"] == "VehicleHistoryImageError"
    snapshot = archive.health_snapshot()
    last_error = snapshot["last_vehicle_history_error"]
    assert last_error is not None
    assert last_error["phase"] == "image-capture"
    assert last_error["path_name"] == f"{active.session_id}.json"
    assert last_error["session_id"] == active.session_id
    assert last_error["error_type"] == "VehicleHistoryImageError"
    assert "cannot write" in last_error["error_message"]
    assert "supersecret" not in last_error["error_message"]
    assert "Traceback" not in last_error["error_message"]
    assert "raw_image_bytes" not in last_error["error_message"]
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "Traceback" not in rendered
    assert "raw_image_bytes" not in rendered


def test_match_or_create_profile_creates_profile_updates_session_and_close_preserves_assignment(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="profile new"))
    source = write_test_jpeg(tmp_path / "profile-source.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=(0, 0, 96, 48))

    assignment = archive.match_or_create_profile(session_id=active.session_id)

    assert assignment.status == "new_profile"
    assert assignment.profile_id is not None
    assert assignment.profile_id.startswith("prof_")
    assert assignment.profile_confidence == pytest.approx(1.0)
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    raw_active = json.loads(active_path.read_text())
    assert raw_active["profile_id"] == assignment.profile_id
    assert raw_active["profile_confidence"] == pytest.approx(1.0)
    profile_path = tmp_path / "vehicle-history" / "profiles" / "active" / f"{assignment.profile_id}.json"
    raw_profile = json.loads(profile_path.read_text())
    assert raw_profile["schema_version"] == 1
    assert raw_profile["profile_id"] == assignment.profile_id
    assert raw_profile["label"] is None
    assert raw_profile["status"] == "active"
    assert raw_profile["sample_count"] == 1
    assert raw_profile["sample_session_ids"] == [active.session_id]
    assert raw_profile["exemplar_crop_path"] == f"{active.session_id}.jpg"
    assert "NaN" not in json.dumps(raw_profile)

    closed = archive.close_session(open_event(spot_id="profile new"))

    assert closed is not None
    assert closed.profile_id == assignment.profile_id
    assert closed.profile_confidence == pytest.approx(1.0)
    records = logger_records(stream)
    assert any(record["event"] == "vehicle-session-profile-created" for record in records)


def test_match_or_create_profile_matches_existing_profile_and_is_idempotent(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="first", observed_at="2026-05-18T13:00:00Z"))
    first_source = write_test_jpeg(tmp_path / "first.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=first.session_id, source_frame_path=first_source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=first.session_id)
    archive.close_session(open_event(spot_id="first", observed_at="2026-05-18T13:02:00Z"))

    second = archive.start_session(occupied_event(spot_id="second", observed_at="2026-05-18T13:03:00Z"))
    second_source = write_test_jpeg(tmp_path / "second.jpg", size=(96, 48), color=(122, 42, 42))
    archive.attach_occupied_images(session_id=second.session_id, source_frame_path=second_source, bbox=(0, 0, 96, 48))

    matched = archive.match_or_create_profile(session_id=second.session_id)
    profile_path = tmp_path / "vehicle-history" / "profiles" / "active" / f"{created.profile_id}.json"
    after_match = json.loads(profile_path.read_text())
    second_assignment = archive.match_or_create_profile(session_id=second.session_id)
    after_noop = json.loads(profile_path.read_text())

    assert matched.status == "matched"
    assert matched.profile_id == created.profile_id
    assert matched.profile_confidence is not None and matched.profile_confidence > 0.9
    assert after_match["sample_count"] == 2
    assert second.session_id in after_match["sample_session_ids"]
    assert second_assignment.profile_id == created.profile_id
    assert after_noop == after_match
    assert len(list((tmp_path / "vehicle-history" / "profiles" / "active").glob("*.json"))) == 1
    records = logger_records(stream)
    assert any(record["event"] == "vehicle-session-profile-matched" for record in records)
    assert any(record["event"] == "vehicle-session-profile-noop" for record in records)


def test_assign_owner_profile_to_active_spot_updates_session_and_profile_sample(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="right_spot", observed_at="2026-05-18T13:00:00Z"))
    first_source = write_test_jpeg(tmp_path / "owner-first.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=first.session_id, source_frame_path=first_source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=first.session_id)
    assert created.profile_id is not None
    archive.close_session(open_event(spot_id="right_spot", observed_at="2026-05-18T13:02:00Z"))
    (tmp_path / "vehicle-history" / "owner-vehicles.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_vehicles": [
                    {
                        "profile_id": created.profile_id,
                        "label": "Keith's black Tesla",
                        "description": "black Tesla, tinted windows, roof rack",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    current = archive.start_session(occupied_event(spot_id="right_spot", observed_at="2026-05-18T13:03:00Z"))
    current_source = write_test_jpeg(tmp_path / "owner-current.jpg", size=(120, 60), color=(121, 41, 41))
    archive.attach_occupied_images(session_id=current.session_id, source_frame_path=current_source, bbox=(0, 0, 120, 60))

    assignment = archive.assign_owner_profile_to_active_spot("right_spot")

    assert assignment.status == "owner_assigned"
    assert assignment.session_id == current.session_id
    assert assignment.profile_id == created.profile_id
    assert assignment.profile_confidence == pytest.approx(1.0)
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{current.session_id}.json"
    active_payload = json.loads(active_path.read_text(encoding="utf-8"))
    assert active_payload["profile_id"] == created.profile_id
    assert active_payload["profile_confidence"] == pytest.approx(1.0)
    profile_path = tmp_path / "vehicle-history" / "profiles" / "active" / f"{created.profile_id}.json"
    profile_payload = json.loads(profile_path.read_text(encoding="utf-8"))
    assert profile_payload["sample_count"] == 2
    assert profile_payload["sample_session_ids"] == [first.session_id, current.session_id]
    records = logger_records(stream)
    assert any(
        record["event"] == "vehicle-session-owner-profile-assigned"
        and record["spot_id"] == "right_spot"
        and record["session_id"] == current.session_id
        and record["profile_id"] == created.profile_id
        for record in records
    )


def test_active_spot_assignments_summarizes_owner_and_unknown_active_sessions(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    owner_session = archive.start_session(occupied_event(spot_id="right_spot", observed_at="2026-05-18T13:00:00Z"))
    owner_source = write_test_jpeg(tmp_path / "owner-current.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=owner_session.session_id, source_frame_path=owner_source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=owner_session.session_id)
    assert created.profile_id is not None
    (tmp_path / "vehicle-history" / "owner-vehicles.json").write_text(
        json.dumps({"schema_version": 1, "owner_vehicles": [{"profile_id": created.profile_id, "label": "Keith's black Tesla"}]}),
        encoding="utf-8",
    )
    unknown = archive.start_session(occupied_event(spot_id="left_spot", observed_at="2026-05-18T13:05:00Z"))
    unknown_source = write_test_jpeg(tmp_path / "unknown.jpg", size=(96, 48), color=(90, 90, 90))
    archive.attach_occupied_images(session_id=unknown.session_id, source_frame_path=unknown_source, bbox=(0, 0, 96, 48))

    assignments = archive.active_spot_assignments()

    assert assignments == [
        {
            "spot_id": "left_spot",
            "session_id": unknown.session_id,
            "profile_id": None,
            "profile_label": None,
            "profile_confidence": None,
            "is_owner": False,
            "owner_label": None,
            "profile_sample_count": None,
            "started_at": unknown.started_at,
        },
        {
            "spot_id": "right_spot",
            "session_id": owner_session.session_id,
            "profile_id": created.profile_id,
            "profile_label": "Keith's black Tesla",
            "profile_confidence": 1.0,
            "is_owner": True,
            "owner_label": "Keith's black Tesla",
            "profile_sample_count": 1,
            "started_at": owner_session.started_at,
        },
    ]


def test_match_or_create_profile_does_not_update_owner_profile_for_low_confidence_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="owner", observed_at="2026-05-18T13:00:00Z"))
    first_source = write_test_jpeg(tmp_path / "owner.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=first.session_id, source_frame_path=first_source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=first.session_id)
    assert created.profile_id is not None
    archive.close_session(open_event(spot_id="owner", observed_at="2026-05-18T13:02:00Z"))
    owner_registry_path = tmp_path / "vehicle-history" / "owner-vehicles.json"
    owner_registry_path.write_text(
        json.dumps({"schema_version": 1, "owner_vehicles": [{"profile_id": created.profile_id, "label": "Keith's black Tesla"}]}),
        encoding="utf-8",
    )

    candidate = archive.start_session(occupied_event(spot_id="left", observed_at="2026-05-18T13:03:00Z"))
    candidate_source = write_test_jpeg(tmp_path / "candidate.jpg", size=(96, 48), color=(122, 42, 42))
    archive.attach_occupied_images(session_id=candidate.session_id, source_frame_path=candidate_source, bbox=(0, 0, 96, 48))
    profile_path = tmp_path / "vehicle-history" / "profiles" / "active" / f"{created.profile_id}.json"
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{candidate.session_id}.json"
    before_profile = profile_path.read_text(encoding="utf-8")
    before_session = active_path.read_text(encoding="utf-8")

    def low_confidence_owner_match(_descriptor: object, _profiles: object) -> MatchResult:
        return MatchResult(
            status=MatchStatus.MATCHED,
            profile_id=created.profile_id,
            confidence=0.90,
            distance=0.10,
            reason="forced-low-confidence-owner-match",
        )

    monkeypatch.setattr("parking_spot_monitor.vehicle_history_profiles._match_vehicle_profile", low_confidence_owner_match)

    assignment = archive.match_or_create_profile(session_id=candidate.session_id)

    assert assignment.status == "unknown"
    assert assignment.profile_id is None
    assert assignment.profile_confidence is None
    assert assignment.reason == "owner-profile-confidence-too-low"
    assert profile_path.read_text(encoding="utf-8") == before_profile
    assert active_path.read_text(encoding="utf-8") == before_session
    records = logger_records(stream)
    assert any(
        record["event"] == "vehicle-session-profile-owner-match-skipped"
        and record["reason"] == "owner-profile-confidence-too-low"
        and record["profile_confidence"] == 0.90
        for record in records
    )


def test_ambiguous_profile_match_leaves_session_and_profiles_unchanged(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    base = archive.start_session(occupied_event(spot_id="base", observed_at="2026-05-18T13:00:00Z"))
    source = write_test_jpeg(tmp_path / "base.jpg", size=(96, 48), color=(90, 90, 90))
    archive.attach_occupied_images(session_id=base.session_id, source_frame_path=source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=base.session_id)
    assert created.profile_id is not None
    active_profiles = tmp_path / "vehicle-history" / "profiles" / "active"
    first_profile_path = active_profiles / f"{created.profile_id}.json"
    second_profile = json.loads(first_profile_path.read_text())
    second_profile["profile_id"] = "prof_duplicate_candidate"
    (active_profiles / "prof_duplicate_candidate.json").write_text(json.dumps(second_profile, allow_nan=False))
    before_first = first_profile_path.read_text()
    before_second = (active_profiles / "prof_duplicate_candidate.json").read_text()

    ambiguous = archive.start_session(occupied_event(spot_id="ambiguous", observed_at="2026-05-18T13:05:00Z"))
    ambiguous_source = write_test_jpeg(tmp_path / "ambiguous.jpg", size=(96, 48), color=(90, 90, 90))
    archive.attach_occupied_images(session_id=ambiguous.session_id, source_frame_path=ambiguous_source, bbox=(0, 0, 96, 48))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{ambiguous.session_id}.json"
    before_session = active_path.read_text()

    assignment = archive.match_or_create_profile(session_id=ambiguous.session_id)

    assert assignment.status == "ambiguous"
    assert assignment.profile_id is None
    assert assignment.profile_confidence is None
    assert active_path.read_text() == before_session
    assert first_profile_path.read_text() == before_first
    assert (active_profiles / "prof_duplicate_candidate.json").read_text() == before_second


def test_malformed_profile_json_is_quarantined_without_blocking_valid_profile_match(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="valid profile", observed_at="2026-05-18T13:00:00Z"))
    source = write_test_jpeg(tmp_path / "valid-profile.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=first.session_id, source_frame_path=source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=first.session_id)
    assert created.profile_id is not None
    archive.close_session(open_event(spot_id="valid profile", observed_at="2026-05-18T13:02:00Z"))
    bad_path = tmp_path / "vehicle-history" / "profiles" / "active" / "broken.json"
    bad_path.write_text("{not-json rtsp://camera.local access_token=supersecret raw_image_bytes")

    second = archive.start_session(occupied_event(spot_id="uses valid", observed_at="2026-05-18T13:03:00Z"))
    second_source = write_test_jpeg(tmp_path / "uses-valid.jpg", size=(96, 48), color=(122, 42, 42))
    archive.attach_occupied_images(session_id=second.session_id, source_frame_path=second_source, bbox=(0, 0, 96, 48))
    matched = archive.match_or_create_profile(session_id=second.session_id)
    snapshot = archive.health_snapshot()

    assert matched.profile_id == created.profile_id
    assert not bad_path.exists()
    assert len(list((tmp_path / "vehicle-history" / "profiles" / "quarantine").glob("broken.json.corrupt-*"))) == 1
    assert snapshot["profile_quarantine_count"] == 1
    assert snapshot["profile_count"] == 1
    assert snapshot["profile_sample_count"] == 2
    assert snapshot["last_vehicle_history_error"] is not None
    assert snapshot["last_vehicle_history_error"]["phase"] == "profile-load"
    records = logger_records(stream)
    assert any(record["event"] == "vehicle-profile-quarantined" for record in records)
    assert any(record["event"] == "vehicle-session-profile-failed" for record in records)
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered
    assert "raw_image_bytes" not in rendered


def test_profile_assignment_requires_occupied_crop_and_descriptor_failures_do_not_mutate_session(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    missing_crop = archive.start_session(occupied_event(spot_id="missing crop"))

    with pytest.raises(ArchiveWriteError, match="occupied_crop_path"):
        archive.match_or_create_profile(session_id=missing_crop.session_id)

    bad_crop = archive.start_session(occupied_event(spot_id="bad crop", observed_at="2026-05-18T13:05:00Z"))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{bad_crop.session_id}.json"
    raw = json.loads(active_path.read_text())
    raw["occupied_crop_path"] = str(tmp_path / "not-a-jpeg.txt")
    active_path.write_text(json.dumps(raw))
    (tmp_path / "not-a-jpeg.txt").write_text("not image bytes access_token=supersecret")
    before = active_path.read_text()

    with pytest.raises(ArchiveWriteError, match="file is unreadable"):
        archive.match_or_create_profile(session_id=bad_crop.session_id)

    assert active_path.read_text() == before
    snapshot = archive.health_snapshot()
    assert snapshot["last_vehicle_history_error"] is not None
    assert snapshot["last_vehicle_history_error"]["phase"] == "profile-match"
    assert snapshot["profile_unknown_session_count"] == 1


def test_estimate_for_profile_uses_closed_matching_sessions_and_excludes_weak_or_mismatched_history(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    first = archive.start_session(occupied_event(spot_id="estimate-a", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="estimate-a", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=first.session_id, profile_id="prof_repeat", profile_confidence=0.96)
    second = archive.start_session(occupied_event(spot_id="estimate-b", observed_at="2026-05-19T08:10:00Z"))
    archive.close_session(open_event(spot_id="estimate-b", observed_at="2026-05-19T09:15:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=second.session_id, profile_id="prof_repeat", profile_confidence=0.92)
    weak = archive.start_session(occupied_event(spot_id="estimate-weak", observed_at="2026-05-20T08:00:00Z"))
    archive.close_session(open_event(spot_id="estimate-weak", observed_at="2026-05-20T22:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=weak.session_id, profile_id="prof_repeat", profile_confidence=0.40)
    other = archive.start_session(occupied_event(spot_id="estimate-other", observed_at="2026-05-21T01:00:00Z"))
    archive.close_session(open_event(spot_id="estimate-other", observed_at="2026-05-21T23:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=other.session_id, profile_id="prof_other", profile_confidence=0.99)

    result = archive.estimate_for_profile("prof_repeat")

    assert result.status == "estimated"
    assert result.reason is None
    assert result.profile_id == "prof_repeat"
    assert result.sample_count == 2
    assert result.dwell_range is not None
    assert result.dwell_range.lower_seconds <= 3600
    assert result.dwell_range.upper_seconds >= 3900
    assert result.leave_time_window is not None
    assert result.leave_time_window.start_minute <= 9 * 60
    assert result.leave_time_window.end_minute >= 9 * 60 + 15


def test_estimate_for_profile_unknown_or_sparse_profile_returns_insufficient_history(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    closed = archive.start_session(occupied_event(spot_id="sparse", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="sparse", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=closed.session_id, profile_id="prof_repeat", profile_confidence=0.96)

    unknown = archive.estimate_for_profile(None)
    sparse = archive.estimate_for_profile("prof_repeat")

    assert unknown.status == "insufficient_history"
    assert unknown.reason == "unknown-profile"
    assert unknown.profile_id is None
    assert unknown.sample_count == 0
    assert sparse.status == "insufficient_history"
    assert sparse.reason == "insufficient-samples"
    assert sparse.profile_id == "prof_repeat"
    assert sparse.sample_count == 1


def test_estimate_for_session_uses_active_profile_but_never_counts_active_session(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    historical = archive.start_session(occupied_event(spot_id="historical", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="historical", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=historical.session_id, profile_id="prof_repeat", profile_confidence=0.96)
    active = archive.start_session(occupied_event(spot_id="current", observed_at="2026-05-19T08:00:00Z"))
    set_session_profile(tmp_path, archive_state="active", session_id=active.session_id, profile_id="prof_repeat", profile_confidence=1.0)

    result = archive.estimate_for_session(active.session_id)

    assert result.status == "insufficient_history"
    assert result.reason == "insufficient-samples"
    assert result.profile_id == "prof_repeat"
    assert result.sample_count == 1


def test_estimate_for_session_missing_or_unprofiled_active_session_returns_unknown_profile(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event(spot_id="unprofiled"))

    missing = archive.estimate_for_session("sess_missing")
    unprofiled = archive.estimate_for_session(active.session_id)

    assert missing.status == "insufficient_history"
    assert missing.reason == "unknown-profile"
    assert missing.profile_id is None
    assert missing.sample_count == 0
    assert unprofiled.status == "insufficient_history"
    assert unprofiled.reason == "unknown-profile"
    assert unprofiled.profile_id is None
    assert unprofiled.sample_count == 0


def test_estimate_helpers_preserve_closed_session_quarantine_and_module_convenience_api(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="valid-a", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="valid-a", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=first.session_id, profile_id="prof_repeat", profile_confidence=0.96)
    second = archive.start_session(occupied_event(spot_id="valid-b", observed_at="2026-05-19T08:00:00Z"))
    archive.close_session(open_event(spot_id="valid-b", observed_at="2026-05-19T09:05:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=second.session_id, profile_id="prof_repeat", profile_confidence=0.96)
    active = archive.start_session(occupied_event(spot_id="current", observed_at="2026-05-20T08:00:00Z"))
    set_session_profile(tmp_path, archive_state="active", session_id=active.session_id, profile_id="prof_repeat", profile_confidence=1.0)
    bad_path = tmp_path / "vehicle-history" / "sessions" / "closed" / "broken.json"
    bad_path.write_text("{not-json rtsp://camera.local access_token=supersecret raw_image_bytes")

    profile_result = archive.estimate_for_profile("prof_repeat")
    session_result = estimate_session_history(tmp_path, session_id=active.session_id)
    module_profile_result = estimate_profile_history(tmp_path, profile_id="prof_repeat")
    snapshot = archive.health_snapshot()

    assert profile_result.status == "estimated"
    assert profile_result.sample_count == 2
    assert session_result.status == "estimated"
    assert session_result.sample_count == 2
    assert module_profile_result.status == "estimated"
    assert module_profile_result.sample_count == 2
    assert not bad_path.exists()
    assert len(list((tmp_path / "vehicle-history" / "sessions" / "quarantine").glob("broken.json.corrupt-*"))) == 1
    assert snapshot["vehicle_history_failure_count"] == 1
    assert snapshot["last_vehicle_history_error"] is not None
    assert snapshot["last_vehicle_history_error"]["phase"] == "json-load"
    rendered = json.dumps(logger_records(stream))
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered
    assert "raw_image_bytes" not in rendered
