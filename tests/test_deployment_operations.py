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
        self.model_dirs: dict[Path, Path] = {}
        self.compose_environments: list[dict[str, str] | None] = []

    def run(
        self,
        command: tuple[str, ...] | list[str],
        *,
        capture: bool = False,
        cwd: Path | None = None,
        check: bool = True,
        environment: dict[str, str] | None = None,
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

    def compose(
        self,
        *arguments: str,
        capture: bool = False,
        check: bool = True,
        environment: dict[str, str] | None = None,
    ) -> str:
        call = ("docker", "compose", *arguments)
        self.calls.append(call)
        self.compose_environments.append(environment)
        operation = arguments
        env_file: Path | None = None
        if operation[:1] == ("--env-file",):
            env_file = Path(operation[1])
            operation = operation[2:]
        if operation[:2] == ("config", "--environment"):
            if env_file is not None and env_file in self.model_dirs:
                return f"MODEL_DIR={self.model_dirs[env_file]}"
            return f"MODEL_DIR={Path('models').resolve()}"
        if operation[:3] == ("ps", "-q", deployment_operations.SERVICE):
            return "container-id"
        if operation[:1] == ("up",) and self.fail_recreate_once:
            self.fail_recreate_once = False
            raise deployment_operations.DeploymentError("injected recreate failure")
        if operation[:1] == ("start",) and self.fail_start_once:
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
    monkeypatch.setattr(
        deployment_operations, "wait_for_fresh_health", lambda *_args, **_kwargs: None
    )
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
    stop = runner.calls.index(
        (
            "docker", "compose", "--env-file", ".env", "stop",
            deployment_operations.SERVICE,
        )
    )
    start = runner.calls.index(
        (
            "docker", "compose", "--env-file", ".env", "start",
            deployment_operations.SERVICE,
        )
    )
    assert stop < start
    assert not (tmp_path / "backup").exists()
    assert not list(tmp_path.glob(".backup.partial-*"))


def test_backup_requires_parent_to_preexist_before_any_mutation(tmp_path: Path) -> None:
    runner = FakeRunner()
    parent = tmp_path / "missing-backup-parent"

    with pytest.raises(
        deployment_operations.DeploymentError,
        match="backup parent must pre-exist as a non-symlink directory",
    ):
        deployment_operations.backup_operation(
            runner,
            parent / "backup",
            "parking-spot-monitor:rollback-test",
            "a" * 64,
        )

    assert not parent.exists()
    assert runner.calls == []


@pytest.mark.parametrize("unsafe_kind", ("symlink", "writable"))
def test_backup_rejects_unprotected_parent_before_any_mutation(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    runner = FakeRunner()
    parent = tmp_path / "backup-parent"
    if unsafe_kind == "symlink":
        protected = tmp_path / "protected"
        protected.mkdir(mode=0o700)
        parent.symlink_to(protected, target_is_directory=True)
    else:
        parent.mkdir(mode=0o777)
        parent.chmod(0o777)

    with pytest.raises(
        deployment_operations.DeploymentError,
        match="backup parent must pre-exist as a non-symlink directory",
    ):
        deployment_operations.backup_operation(
            runner,
            parent / "backup",
            "parking-spot-monitor:rollback-test",
            "a" * 64,
        )

    assert runner.calls == []


def test_backup_fsyncs_archive_and_bundle_before_durable_publish(
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
    (tmp_path / "data/state.json").write_text("{}", encoding="utf-8")
    runner = FakeRunner()
    monkeypatch.setattr(
        deployment_operations, "wait_for_fresh_health", lambda *_args, **_kwargs: None
    )
    real_fsync = os.fsync
    real_rename = os.rename
    events: list[str] = []

    def record_fsync(descriptor: int) -> None:
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target.name == "data.tar":
            events.append("archive-fsync")
        elif target.name.startswith(".backup.partial-"):
            events.append("stage-fsync")
        elif target == tmp_path:
            events.append("parent-fsync")
        real_fsync(descriptor)

    def record_rename(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
        if Path(destination) == tmp_path / "backup":
            events.append("publish-rename")
        real_rename(source, destination)

    monkeypatch.setattr(os, "fsync", record_fsync)
    monkeypatch.setattr(os, "rename", record_rename)

    deployment_operations.backup_operation(
        runner,
        tmp_path / "backup",
        "parking-spot-monitor:rollback-test",
        deployment_operations._sha256(model),
        config_file=config_file,
    )

    assert events.index("archive-fsync") < events.index("stage-fsync")
    assert events.index("stage-fsync") < events.index("publish-rename")
    assert events.index("publish-rename") < events.index("parent-fsync")


def test_backup_requires_fresh_health_after_restarting_service(
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
    health_calls: list[tuple[str, Path, Path | None]] = []

    def record_health(
        _runner: FakeRunner,
        started_at: str,
        data_dir: Path,
        *,
        env_file: Path | None = None,
        **_kwargs: object,
    ) -> None:
        health_calls.append((started_at, data_dir, env_file))

    monkeypatch.setattr(deployment_operations, "wait_for_fresh_health", record_health)

    deployment_operations.backup_operation(
        runner,
        tmp_path / "backup",
        "parking-spot-monitor:rollback-test",
        deployment_operations._sha256(model),
        config_file=config_file,
    )

    assert len(health_calls) == 1
    assert health_calls[0][1:] == (Path("data"), Path(".env"))
    assert any(
        call[:2] == ("docker", "inspect") and "{{.State.StartedAt}}" in call
        for call in runner.calls
    )


def test_backup_health_failure_prevents_success_after_bundle_publish(
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

    def fail_health(*_args: object, **_kwargs: object) -> None:
        raise deployment_operations.DeploymentError("injected fresh health failure")

    monkeypatch.setattr(deployment_operations, "wait_for_fresh_health", fail_health)

    with pytest.raises(
        deployment_operations.DeploymentError,
        match="bundle was published.*did not become healthy",
    ):
        deployment_operations.backup_operation(
            runner,
            tmp_path / "backup",
            "parking-spot-monitor:rollback-test",
            deployment_operations._sha256(model),
            config_file=config_file,
        )

    assert (tmp_path / "backup").is_dir()


@pytest.mark.parametrize(
    ("failure_mode", "secondary_message"),
    (
        ("health", "injected restart health failure"),
        ("start", "injected start failure"),
    ),
)
def test_backup_preserves_archive_error_when_restart_or_health_also_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
    secondary_message: str,
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
    runner.fail_start_once = failure_mode == "start"

    def fail_health(*_args: object, **_kwargs: object) -> None:
        if failure_mode == "health":
            raise deployment_operations.DeploymentError(secondary_message)

    monkeypatch.setattr(deployment_operations, "wait_for_fresh_health", fail_health)

    with pytest.raises(deployment_operations.DeploymentError, match="tar failure") as caught:
        deployment_operations.backup_operation(
            runner,
            tmp_path / "backup",
            "parking-spot-monitor:rollback-test",
            deployment_operations._sha256(model),
            config_file=config_file,
        )

    assert "backup completed" not in str(caught.value)
    assert getattr(caught.value, "__notes__", ()) == [
        "service restart/health verification also failed (DeploymentError)"
    ]
    assert secondary_message not in caught.value.__notes__[0]


def test_main_reports_secondary_backup_recovery_failure_without_secret_note(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    failure = deployment_operations.DeploymentError("injected archive failure")
    failure.add_note(
        "service restart/health verification also failed: token=private-value"
    )

    def fail_backup(*_args: object, **_kwargs: object) -> None:
        raise failure

    monkeypatch.setattr(deployment_operations, "backup_operation", fail_backup)

    result = deployment_operations.main(
        (
            "backup",
            "--backup-dir",
            str(tmp_path / "backup"),
            "--rollback-tag",
            "parking-spot-monitor:rollback-test",
            "--approved-model-sha256",
            "a" * 64,
        )
    )

    captured = capsys.readouterr()
    assert result == 2
    assert "injected archive failure" in captured.err
    assert "service restart/health verification also failed" in captured.err
    assert "private-value" not in captured.err
    assert "token=" not in captured.err


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
            "docker", "compose", "--env-file", ".env", "up", "-d", "--no-build", "--force-recreate",
            deployment_operations.SERVICE,
        )
    ) == 2


def test_rollback_uses_bundled_model_dir_and_external_environment_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODEL_DIR", str(tmp_path / "wrong-shell-models"))
    monkeypatch.setenv("MATRIX_ACCESS_TOKEN", "wrong-shell-token")
    monkeypatch.setattr(
        deployment_operations, "wait_for_fresh_health", lambda *_args, **_kwargs: None
    )
    bundle = _bundle(tmp_path)
    active_model_dir = tmp_path / "active-models"
    rollback_model_dir = tmp_path / "rollback-models"
    active_model_dir.mkdir()
    rollback_model_dir.mkdir()
    rollback_environment = (
        f"MODEL_DIR={rollback_model_dir}\nMATRIX_ACCESS_TOKEN=rollback-token\n"
    ).encode()
    (bundle / ".env").write_bytes(rollback_environment)
    _manifest(
        bundle,
        "bundle-files.sha256",
        ("config.yaml", ".env", "image-id.txt", "approved-model.sha256"),
    )
    (active_model_dir / "yolov8n.pt").write_bytes(b"active-model")
    (rollback_model_dir / "yolov8n.pt").write_bytes(b"unrelated-model")
    config_file = tmp_path / "operator-config.yaml"
    config_file.write_bytes(b"active-config")
    env_file = tmp_path / "operator.env"
    env_file.write_text(
        f"MODEL_DIR={active_model_dir}\nMATRIX_ACCESS_TOKEN=active-token\n",
        encoding="utf-8",
    )
    (tmp_path / "data").mkdir()
    runner = FakeRunner()
    runner.rollback_image_exists = True
    runner.model_dirs[env_file] = active_model_dir
    runner.model_dirs[bundle / ".env"] = rollback_model_dir

    deployment_operations.rollback_operation(
        runner,
        bundle,
        tmp_path / "data",
        config_file=config_file,
        env_file=env_file,
    )

    assert config_file.read_bytes() == b"old-config"
    assert env_file.read_bytes() == rollback_environment
    assert (active_model_dir / "yolov8n.pt").read_bytes() == b"active-model"
    assert (rollback_model_dir / "yolov8n.pt").read_bytes() == b"old-model"
    assert (
        "docker",
        "compose",
        "--env-file",
        str(env_file),
        "up",
        "-d",
        "--no-build",
        "--force-recreate",
        deployment_operations.SERVICE,
    ) in runner.calls
    assert runner.compose_environments
    assert all(
        environment is not None
        and "MODEL_DIR" not in environment
        and "MATRIX_ACCESS_TOKEN" not in environment
        for environment in runner.compose_environments
    )


def test_rollback_failure_restores_both_distinct_model_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        deployment_operations, "wait_for_fresh_health", lambda *_args, **_kwargs: None
    )
    bundle = _bundle(tmp_path)
    active_model_dir = tmp_path / "active-models"
    rollback_model_dir = tmp_path / "rollback-models"
    active_model_dir.mkdir()
    rollback_model_dir.mkdir()
    active_model = active_model_dir / "yolov8n.pt"
    prior_rollback_model = rollback_model_dir / "yolov8n.pt"
    active_model.write_bytes(b"active-model")
    prior_rollback_model.write_bytes(b"unrelated-model")
    config_file = tmp_path / "operator-config.yaml"
    config_file.write_bytes(b"active-config")
    env_file = tmp_path / "operator.env"
    env_file.write_bytes(b"active-env")
    (tmp_path / "data").mkdir()
    runner = FakeRunner()
    runner.rollback_image_exists = True
    runner.fail_recreate_once = True
    runner.model_dirs[env_file] = active_model_dir
    runner.model_dirs[bundle / ".env"] = rollback_model_dir

    with pytest.raises(deployment_operations.DeploymentError, match="recreate failure"):
        deployment_operations.rollback_operation(
            runner,
            bundle,
            tmp_path / "data",
            config_file=config_file,
            env_file=env_file,
        )

    assert config_file.read_bytes() == b"active-config"
    assert env_file.read_bytes() == b"active-env"
    assert active_model.read_bytes() == b"active-model"
    assert prior_rollback_model.read_bytes() == b"unrelated-model"


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
