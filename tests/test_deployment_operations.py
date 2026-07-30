from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tarfile
import threading
import time

import pytest

from scripts import deployment_operations


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.fail_tar = False
        self.fail_validation = False
        self.rollback_image_exists = False
        self.active_image_id = "sha256:" + "a" * 64
        self.fail_recreate_once = False
        self.fail_start_once = False
        self.status_values: list[str] = []

    def run(
        self,
        command: tuple[str, ...] | list[str],
        *,
        capture: bool = False,
        cwd: Path | None = None,
        check: bool = True,
    ) -> str:
        call = tuple(command)
        self.calls.append(call)
        if call[:3] == ("docker", "image", "inspect"):
            if self.rollback_image_exists and "{{.Id}}" in call:
                return "sha256:" + "a" * 64
            return ""
        if call[:2] == ("docker", "inspect") and "{{.State.Running}}" in call:
            return "true"
        if call[:2] == ("docker", "inspect") and "{{.Image}}" in call:
            return self.active_image_id
        if call[:2] == ("docker", "inspect") and "{{.State.StartedAt}}" in call:
            return datetime.now(timezone.utc).isoformat()
        if call[:2] == ("git", "rev-parse"):
            return "b" * 40
        if call[:2] == ("git", "status"):
            return self.status_values.pop(0) if self.status_values else ""
        if call[:2] == ("docker", "run") and "tar" in call:
            if self.fail_tar:
                raise deployment_operations.DeploymentError("injected tar failure")
            mounts = [call[index + 1] for index, value in enumerate(call) if value == "--mount"]
            source = Path(mounts[0].split("src=", 1)[1].split(",", 1)[0])
            output_root = Path(mounts[1].split("src=", 1)[1].split(",", 1)[0])
            output = output_root / call[call.index("-cpf") + 1].removeprefix("/backup/")
            with tarfile.open(output, "w") as archive:
                for path in source.rglob("*"):
                    archive.add(path, arcname=path.relative_to(source))
            return ""
        if call and call[0] == "tar":
            if "-xpf" in call:
                destination = Path(call[call.index("-C") + 1])
                archive_path = Path(call[call.index("-xpf") + 1])
                with tarfile.open(archive_path, "r:") as archive:
                    archive.extractall(destination, filter="data")
                return ""
            output = Path(call[call.index("-cpf") + 1])
            with tarfile.open(output, "w") as archive:
                source = Path(call[call.index("-C") + 1])
                for path in source.rglob("*"):
                    archive.add(path, arcname=path.relative_to(source))
            return ""
        if call[:2] == ("docker", "run") and self.fail_validation:
            raise deployment_operations.DeploymentError("injected validation failure")
        return ""

    def compose(self, *arguments: str, capture: bool = False, check: bool = True) -> str:
        call = ("docker", "compose", *arguments)
        self.calls.append(call)
        if arguments[:2] == ("config", "--environment"):
            return f"MODEL_DIR={Path('models').resolve()}"
        if arguments[:3] == ("ps", "-q", deployment_operations.SERVICE):
            return "container-id"
        if arguments[:1] == ("up",) and self.fail_recreate_once:
            self.fail_recreate_once = False
            raise deployment_operations.DeploymentError("injected recreate failure")
        if arguments[:1] == ("start",) and self.fail_start_once:
            self.fail_start_once = False
            raise deployment_operations.DeploymentError("injected start failure")
        return ""


def _write_health(path: Path, timestamp: datetime, *, status: str = "ok") -> None:
    path.write_text(
        json.dumps(
            {
                "status": status,
                "updated_at": timestamp.isoformat(),
                "last_frame_at": timestamp.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    future = timestamp.timestamp() + 0.01
    os.utime(path, (future, future))


def _manifest(root: Path, name: str, targets: tuple[str, ...]) -> None:
    value = "".join(
        f"{deployment_operations._sha256(root / target)}  {target}\n"
        for target in targets
    )
    (root / name).write_text(value, encoding="utf-8")


def _bundle(root: Path) -> Path:
    bundle = root / "bundle"
    bundle.mkdir()
    for name, payload in (
        ("config.yaml", b"old-config"),
        (".env", b"old-env"),
        ("yolov8n.pt", b"old-model"),
    ):
        (bundle / name).write_bytes(payload)
    data = root / "archive-data"
    data.mkdir()
    (data / "state.json").write_text("{}", encoding="utf-8")
    with tarfile.open(bundle / "data.tar", "w") as archive:
        archive.add(data / "state.json", arcname="state.json")
    approved = deployment_operations._sha256(bundle / "yolov8n.pt")
    (bundle / "approved-model.sha256").write_text(
        f"{approved}  yolov8n.pt\n", encoding="utf-8"
    )
    (bundle / "image-id.txt").write_text(
        "backup-created-at=2026-07-30T00:00:00+00:00\n"
        f"source-revision={'b' * 40}\n"
        "rollback-image-tag=parking-spot-monitor:rollback-test\n"
        f"rollback-image-id=sha256:{'a' * 64}\n",
        encoding="utf-8",
    )
    _manifest(bundle, "yolov8n.pt.sha256", ("yolov8n.pt",))
    _manifest(bundle, "data.tar.sha256", ("data.tar",))
    _manifest(
        bundle,
        "bundle-files.sha256",
        ("config.yaml", ".env", "image-id.txt", "approved-model.sha256"),
    )
    return bundle


def test_freshness_wait_ignores_stale_health_before_running_healthcheck(
    tmp_path: Path,
) -> None:
    runner = FakeRunner()
    started = datetime.now(timezone.utc)
    health = tmp_path / "health.json"
    _write_health(health, started - timedelta(minutes=10))

    def publish_fresh_health() -> None:
        time.sleep(0.03)
        _write_health(health, started + timedelta(seconds=1))

    publisher = threading.Thread(target=publish_fresh_health)
    publisher.start()
    deployment_operations.wait_for_fresh_health(
        runner,
        started.isoformat(),
        tmp_path,
        timeout_seconds=1,
        poll_seconds=0.01,
    )
    publisher.join()

    healthchecks = [
        call for call in runner.calls if "parking_spot_monitor.healthcheck" in call
    ]
    assert len(healthchecks) == 1


def test_stale_health_times_out_without_running_one_shot_healthcheck(
    tmp_path: Path,
) -> None:
    runner = FakeRunner()
    started = datetime.now(timezone.utc)
    _write_health(tmp_path / "health.json", started - timedelta(minutes=10))

    with pytest.raises(deployment_operations.DeploymentError, match="no healthy"):
        deployment_operations.wait_for_fresh_health(
            runner,
            started.isoformat(),
            tmp_path,
            timeout_seconds=0.03,
            poll_seconds=0.005,
        )

    assert not any("parking_spot_monitor.healthcheck" in call for call in runner.calls)


def test_backup_tags_running_image_and_restarts_after_archive_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "models").mkdir()
    model = tmp_path / "models/yolov8n.pt"
    model.write_bytes(b"model")
    config_file = tmp_path / "operator-config.yaml"
    config_file.write_text("config", encoding="utf-8")
    (tmp_path / ".env").write_text("environment", encoding="utf-8")
    (tmp_path / "data").mkdir()
    runner = FakeRunner()
    runner.fail_tar = True

    with pytest.raises(deployment_operations.DeploymentError, match="tar failure"):
        deployment_operations.backup_operation(
            runner,
            tmp_path / "backup",
            "parking-spot-monitor:rollback-test",
            deployment_operations._sha256(model),
            config_file=config_file,
        )

    assert (
        "docker",
        "image",
        "tag",
        "sha256:" + "a" * 64,
        "parking-spot-monitor:rollback-test",
    ) in runner.calls
    stop = runner.calls.index(("docker", "compose", "stop", deployment_operations.SERVICE))
    start = runner.calls.index(("docker", "compose", "start", deployment_operations.SERVICE))
    assert stop < start
    assert not (tmp_path / "backup").exists()
    assert not list(tmp_path.glob(".backup.partial-*"))


def test_rollback_validation_failure_removes_secret_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(deployment_operations.tempfile, "tempdir", str(tmp_path))
    bundle = _bundle(tmp_path)
    (tmp_path / "models").mkdir()
    (tmp_path / "models/yolov8n.pt").write_bytes(b"active-model")
    (tmp_path / "config.yaml").write_bytes(b"active-config")
    (tmp_path / ".env").write_bytes(b"active-env")
    (tmp_path / "data").mkdir()
    runner = FakeRunner()
    runner.fail_validation = True
    runner.rollback_image_exists = True

    with pytest.raises(deployment_operations.DeploymentError, match="validation failure"):
        deployment_operations.rollback_operation(runner, bundle, tmp_path / "data")

    assert not list(tmp_path.glob("parking-rollback-*"))
    assert (tmp_path / "config.yaml").read_bytes() == b"active-config"
    assert (tmp_path / ".env").read_bytes() == b"active-env"
    assert not any(call[:4] == ("docker", "compose", "stop", deployment_operations.SERVICE) for call in runner.calls)


def test_upgrade_rechecks_cleanliness_around_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        deployment_operations, "wait_for_fresh_health", lambda *_args, **_kwargs: None
    )
    runner = FakeRunner()
    revision = "b" * 40

    deployment_operations.upgrade_operation(
        runner, revision, "parking-spot-monitor:rollback-test", tmp_path
    )

    status_indices = [
        index
        for index, call in enumerate(runner.calls)
        if call == ("git", "status", "--porcelain")
    ]
    build_index = runner.calls.index(
        ("docker", "compose", "build", deployment_operations.SERVICE)
    )
    recreate_index = runner.calls.index(
        (
            "docker",
            "compose",
            "up",
            "-d",
            "--no-build",
            "--force-recreate",
            deployment_operations.SERVICE,
        )
    )
    assert len(status_indices) == 4
    assert status_indices[1] < build_index < status_indices[2] < status_indices[3] < recreate_index


def test_rollback_recreate_failure_restores_prior_files_and_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        deployment_operations, "wait_for_fresh_health", lambda *_args, **_kwargs: None
    )
    bundle = _bundle(tmp_path)
    (tmp_path / "models").mkdir()
    model = tmp_path / "models/yolov8n.pt"
    model.write_bytes(b"active-model")
    (tmp_path / "config.yaml").write_bytes(b"active-config")
    (tmp_path / ".env").write_bytes(b"active-env")
    (tmp_path / "data").mkdir()
    runner = FakeRunner()
    runner.rollback_image_exists = True
    runner.active_image_id = "sha256:" + "c" * 64
    runner.fail_recreate_once = True

    with pytest.raises(deployment_operations.DeploymentError, match="recreate failure"):
        deployment_operations.rollback_operation(runner, bundle, tmp_path / "data")

    assert (tmp_path / "config.yaml").read_bytes() == b"active-config"
    assert (tmp_path / ".env").read_bytes() == b"active-env"
    assert model.read_bytes() == b"active-model"
    assert (
        "docker",
        "image",
        "tag",
        "sha256:" + "c" * 64,
        deployment_operations.IMAGE_TAG,
    ) in runner.calls
    assert runner.calls.count(
        (
            "docker", "compose", "up", "-d", "--no-build", "--force-recreate",
            deployment_operations.SERVICE,
        )
    ) == 2


def test_restore_start_failure_reactivates_preserved_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        deployment_operations, "wait_for_fresh_health", lambda *_args, **_kwargs: None
    )
    bundle = _bundle(tmp_path)
    data = tmp_path / "data"
    data.mkdir()
    (data / "current.txt").write_text("current", encoding="utf-8")
    runner = FakeRunner()
    runner.fail_start_once = True

    with pytest.raises(deployment_operations.DeploymentError, match="start failure"):
        deployment_operations.restore_data_operation(runner, bundle, data)

    assert (data / "current.txt").read_text(encoding="utf-8") == "current"
    assert not list(tmp_path.glob("data.pre-restore.*"))
    assert list(tmp_path.glob("data.failed-restore.*"))
