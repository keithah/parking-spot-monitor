#!/usr/bin/env python3
"""Transactional backup, upgrade, rollback, and data-restore operations."""

from __future__ import annotations

import argparse
from contextlib import suppress
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from typing import Mapping, Sequence


SERVICE = "parking-spot-monitor"
IMAGE_TAG = "parking-spot-monitor:local"
HEALTH_STATUSES = frozenset({"ok", "starting", "degraded"})
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ENV_ASSIGNMENT_PATTERN = re.compile(
    r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*="
)


class DeploymentError(RuntimeError):
    """A deployment precondition or recovery operation failed."""


class Runner:
    def run(
        self,
        command: Sequence[str],
        *,
        capture: bool = False,
        cwd: Path | None = None,
        check: bool = True,
        environment: Mapping[str, str] | None = None,
    ) -> str:
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            check=False,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            env=dict(environment) if environment is not None else None,
        )
        if check and completed.returncode:
            detail = completed.stderr.strip() if capture and completed.stderr else ""
            raise DeploymentError(
                f"command failed ({completed.returncode}): {' '.join(command)}"
                + (f": {detail}" if detail else "")
            )
        return completed.stdout.strip() if capture and completed.stdout else ""

    def compose(
        self,
        *arguments: str,
        capture: bool = False,
        check: bool = True,
        environment: Mapping[str, str] | None = None,
    ) -> str:
        return self.run(
            ("docker", "compose", *arguments),
            capture=capture,
            check=check,
            environment=environment,
        )


def _require_regular_file(path: Path, label: str) -> None:
    try:
        value = path.lstat()
    except OSError as exc:
        raise DeploymentError(f"{label} is unavailable") from exc
    if not stat.S_ISREG(value.st_mode) or path.is_symlink():
        raise DeploymentError(f"{label} must be a non-symlink regular file")


def _copy_private(source: Path, destination: Path, mode: int = 0o600) -> None:
    _require_regular_file(source, str(source))
    if destination.exists() or destination.is_symlink():
        raise DeploymentError(f"refusing to overwrite staged file: {destination}")
    with source.open("rb") as source_handle:
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            mode,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as destination_handle:
                shutil.copyfileobj(source_handle, destination_handle, 1024 * 1024)
                destination_handle.flush()
                os.fsync(destination_handle.fileno())
        finally:
            os.close(descriptor)
    os.chmod(destination, mode)


def _atomic_install(source: Path, destination: Path, mode: int) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.deploy-{os.getpid()}")
    if temporary.exists() or temporary.is_symlink():
        raise DeploymentError(f"stale deployment temporary exists: {temporary}")
    try:
        _copy_private(source, temporary, mode)
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()


def _durable_unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_private(path: Path, value: str) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        payload = value.encode("utf-8")
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _checksum_manifest(root: Path, names: Sequence[str]) -> str:
    return "".join(f"{_sha256(root / name)}  {name}\n" for name in names)


def _create_data_archive(
    runner: Runner,
    data_dir: Path,
    destination: Path,
    image_id: str,
) -> None:
    if not data_dir.is_dir() or data_dir.is_symlink():
        raise DeploymentError("data source must be a non-symlink directory")
    descriptor = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    os.close(descriptor)
    runner.run(
        (
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--mount",
            f"type=bind,src={data_dir.resolve()},dst=/source,readonly",
            "--mount",
            f"type=bind,src={destination.parent.resolve()},dst=/backup",
            image_id,
            "tar",
            "--format=pax",
            "-C",
            "/source",
            "-cpf",
            f"/backup/{destination.name}",
            ".",
        )
    )
    _require_regular_file(destination, "data archive")
    os.chmod(destination, 0o600)


def _verify_manifest(root: Path, manifest_name: str, allowed: set[str]) -> None:
    manifest = root / manifest_name
    _require_regular_file(manifest, manifest_name)
    seen: set[str] = set()
    for line in manifest.read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        if separator != "  " or not SHA256_PATTERN.fullmatch(digest):
            raise DeploymentError(f"invalid checksum record in {manifest_name}")
        if name not in allowed or name in seen or Path(name).name != name:
            raise DeploymentError(f"unexpected checksum target in {manifest_name}")
        _require_regular_file(root / name, name)
        if _sha256(root / name) != digest:
            raise DeploymentError(f"checksum mismatch: {name}")
        seen.add(name)
    if seen != allowed:
        raise DeploymentError(f"incomplete checksum manifest: {manifest_name}")


def _bundle_metadata(root: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (root / "image-id.txt").read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key and value and key not in values:
            values[key] = value
    return values


def verify_bundle(root: Path) -> dict[str, str]:
    if not root.is_dir() or root.is_symlink():
        raise DeploymentError("rollback bundle must be a non-symlink directory")
    _verify_manifest(root, "yolov8n.pt.sha256", {"yolov8n.pt"})
    _verify_manifest(root, "data.tar.sha256", {"data.tar"})
    _verify_manifest(
        root,
        "bundle-files.sha256",
        {"config.yaml", ".env", "image-id.txt", "approved-model.sha256"},
    )
    _require_regular_file(root / "data.tar", "data.tar")
    _validate_archive(root / "data.tar")
    approved_line = (root / "approved-model.sha256").read_text(
        encoding="utf-8"
    ).strip()
    approved_digest, separator, approved_name = approved_line.partition("  ")
    if (
        separator != "  "
        or approved_name != "yolov8n.pt"
        or not SHA256_PATTERN.fullmatch(approved_digest)
        or approved_digest != _sha256(root / "yolov8n.pt")
    ):
        raise DeploymentError("approved model checksum does not match the bundled model")
    values = _bundle_metadata(root)
    for required in (
        "backup-created-at",
        "source-revision",
        "rollback-image-tag",
        "rollback-image-id",
    ):
        if not values.get(required):
            raise DeploymentError(f"bundle metadata is missing {required}")
    return values


def _validate_archive(path: Path) -> None:
    try:
        with tarfile.open(path, "r:") as archive:
            for member in archive:
                item = PurePosixPath(member.name)
                if item.is_absolute() or ".." in item.parts:
                    raise DeploymentError("data archive contains an unsafe path")
                if member.issym() or member.islnk() or not (
                    member.isdir() or member.isfile()
                ):
                    raise DeploymentError("data archive contains a link or special file")
    except (tarfile.TarError, OSError) as exc:
        raise DeploymentError("data archive is unreadable") from exc


def _compose(
    runner: Runner,
    *arguments: str,
    env_file: Path | None = None,
    capture: bool = False,
    check: bool = True,
) -> str:
    prefix = ("--env-file", str(env_file)) if env_file is not None else ()
    environment = None
    if env_file is not None:
        environment = dict(os.environ)
        try:
            with env_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    match = ENV_ASSIGNMENT_PATTERN.match(line)
                    if match is not None:
                        environment.pop(match.group(1), None)
        except (OSError, UnicodeError) as exc:
            raise DeploymentError("selected environment file is unreadable") from exc
    return runner.compose(
        *prefix,
        *arguments,
        capture=capture,
        check=check,
        environment=environment,
    )


def _model_dir(runner: Runner, env_file: Path | None = None) -> Path:
    configured = os.environ.get("MODEL_DIR") if env_file is None else None
    if configured is None:
        environment = _compose(
            runner, "config", "--environment", env_file=env_file, capture=True
        )
        configured = next(
            (
                line.removeprefix("MODEL_DIR=")
                for line in environment.splitlines()
                if line.startswith("MODEL_DIR=")
            ),
            "./models",
        )
    return Path(configured or "./models").resolve()


def _running_container(
    runner: Runner, env_file: Path | None = None
) -> tuple[str, str]:
    container_id = _compose(
        runner, "ps", "-q", SERVICE, env_file=env_file, capture=True
    )
    if not container_id:
        raise DeploymentError("service container is unavailable")
    running = runner.run(
        ("docker", "inspect", container_id, "--format", "{{.State.Running}}"),
        capture=True,
    )
    if running != "true":
        raise DeploymentError("service container is not running")
    image_id = runner.run(
        ("docker", "inspect", container_id, "--format", "{{.Image}}"),
        capture=True,
    )
    if not image_id.startswith("sha256:"):
        raise DeploymentError("running container image identity is unavailable")
    return container_id, image_id


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def wait_for_fresh_health(
    runner: Runner,
    started_at: str,
    data_dir: Path,
    *,
    timeout_seconds: float = 180,
    poll_seconds: float = 2,
    env_file: Path | None = None,
) -> None:
    started = _parse_time(started_at)
    if started is None:
        raise DeploymentError("container start timestamp is invalid")
    health_path = data_dir / "health.json"
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            payload = json.loads(health_path.read_text(encoding="utf-8"))
            updated = _parse_time(payload.get("updated_at"))
            frame = _parse_time(payload.get("last_frame_at"))
            status_value = payload.get("status")
            artifact_new = datetime.fromtimestamp(
                health_path.stat().st_mtime, timezone.utc
            ) > started
            if (
                isinstance(payload, dict)
                and status_value in HEALTH_STATUSES
                and updated is not None
                and updated > started
                and frame is not None
                and frame > started
                and artifact_new
            ):
                _compose(
                    runner,
                    "exec",
                    "-T",
                    SERVICE,
                    "python",
                    "-m",
                    "parking_spot_monitor.healthcheck",
                    "--health-file",
                    "/data/health.json",
                    "--max-age-seconds",
                    "120",
                    env_file=env_file,
                )
                return
        except (OSError, json.JSONDecodeError, AttributeError):
            pass
        time.sleep(poll_seconds)
    raise DeploymentError("no healthy successful frame newer than container start")


def _started_at(runner: Runner, env_file: Path | None = None) -> str:
    container_id, _ = _running_container(runner, env_file)
    return runner.run(
        ("docker", "inspect", container_id, "--format", "{{.State.StartedAt}}"),
        capture=True,
    )


def backup_operation(
    runner: Runner,
    backup_dir: Path,
    rollback_tag: str,
    approved_model_sha256: str,
    *,
    data_dir: Path = Path("data"),
    env_file: Path = Path(".env"),
    config_file: Path = Path("config.yaml"),
) -> None:
    if backup_dir.exists() or backup_dir.is_symlink():
        raise DeploymentError("backup destination already exists")
    if not SHA256_PATTERN.fullmatch(approved_model_sha256):
        raise DeploymentError("approved model SHA-256 must be 64 lowercase hex characters")
    _require_regular_file(env_file, "environment")
    model = _model_dir(runner, env_file) / "yolov8n.pt"
    for path, label in ((config_file, "config"), (env_file, "environment"), (model, "model")):
        _require_regular_file(path, label)
    if _sha256(model) != approved_model_sha256:
        raise DeploymentError("model does not match the operator-approved SHA-256")
    existing = runner.run(
        ("docker", "image", "inspect", rollback_tag, "--format", "{{.Id}}"),
        capture=True,
        check=False,
    )
    if existing:
        raise DeploymentError("rollback image tag already exists")
    _, running_image_id = _running_container(runner, env_file)
    runner.run(("docker", "image", "tag", running_image_id, rollback_tag))
    stopped = False
    parent = backup_dir.parent.resolve()
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{backup_dir.name}.partial-", dir=parent
    ) as temporary_name:
        stage = Path(temporary_name)
        os.chmod(stage, 0o700)
        try:
            stopped = True
            _compose(runner, "stop", SERVICE, env_file=env_file)
            _copy_private(config_file, stage / "config.yaml")
            _copy_private(env_file, stage / ".env")
            _copy_private(model, stage / "yolov8n.pt")
            _create_data_archive(
                runner, data_dir, stage / "data.tar", running_image_id
            )
            created_at = datetime.now(timezone.utc).isoformat()
            source_revision = runner.run(("git", "rev-parse", "HEAD"), capture=True)
            _write_private(
                stage / "image-id.txt",
                f"backup-created-at={created_at}\nsource-revision={source_revision}\n"
                f"rollback-image-tag={rollback_tag}\nrollback-image-id={running_image_id}\n"
                "environment-copy=.env\n",
            )
            _write_private(
                stage / "approved-model.sha256",
                f"{approved_model_sha256}  yolov8n.pt\n",
            )
            _write_private(
                stage / "yolov8n.pt.sha256",
                _checksum_manifest(stage, ("yolov8n.pt",)),
            )
            _write_private(
                stage / "data.tar.sha256",
                _checksum_manifest(stage, ("data.tar",)),
            )
            _write_private(
                stage / "bundle-files.sha256",
                _checksum_manifest(
                    stage,
                    ("config.yaml", ".env", "image-id.txt", "approved-model.sha256"),
                ),
            )
            verify_bundle(stage)
            os.rename(stage, backup_dir)
        finally:
            if stopped:
                try:
                    _compose(runner, "start", SERVICE, env_file=env_file)
                except DeploymentError as exc:
                    raise DeploymentError("backup completed but the service did not restart") from exc


def _require_clean(runner: Runner) -> None:
    if runner.run(("git", "status", "--porcelain"), capture=True):
        raise DeploymentError("refusing deployment from a dirty worktree")


def upgrade_operation(runner: Runner, revision: str, rollback_tag: str, data_dir: Path) -> None:
    _require_clean(runner)
    runner.run(("git", "fetch", "--prune", "origin"))
    runner.run(("git", "cat-file", "-e", f"{revision}^{{commit}}"))
    runner.run(("git", "checkout", "--detach", revision))
    resolved = runner.run(("git", "rev-parse", "HEAD"), capture=True)
    expected = runner.run(("git", "rev-parse", f"{revision}^{{commit}}"), capture=True)
    if resolved != expected:
        raise DeploymentError("checked-out revision does not match reviewed revision")
    runner.run(("docker", "image", "inspect", rollback_tag))
    runner.run(("python3", "-m", "compileall", "-q", "parking_spot_monitor", "src", "scripts", "tests"))
    runner.run(("python3", "-m", "pytest", "-q"))
    runner.run(("python3", "-I", "scripts/lock_dependencies.py", "--check"))
    _require_clean(runner)
    runner.compose("config", "--quiet")
    runner.compose("build", SERVICE)
    _require_clean(runner)
    runner.compose(
        "run", "--rm", SERVICE,
        "python", "-m", "parking_spot_monitor",
        "--config", "/config/config.yaml", "--data-dir", "/data", "--validate-config",
    )
    _require_clean(runner)
    runner.compose("up", "-d", "--no-build", "--force-recreate", SERVICE)
    wait_for_fresh_health(runner, _started_at(runner), data_dir)


def rollback_operation(
    runner: Runner,
    rollback_dir: Path,
    data_dir: Path,
    config_file: Path = Path("config.yaml"),
    env_file: Path = Path(".env"),
) -> None:
    metadata = verify_bundle(rollback_dir)
    rollback_tag = metadata["rollback-image-tag"]
    rollback_image = metadata["rollback-image-id"]
    actual = runner.run(
        ("docker", "image", "inspect", rollback_tag, "--format", "{{.Id}}"),
        capture=True,
    )
    if actual != rollback_image:
        raise DeploymentError("rollback image tag does not match bundle identity")
    _require_regular_file(env_file, "active environment")
    active_model_dir = _model_dir(runner, env_file)
    rollback_model_dir = _model_dir(runner, rollback_dir / ".env")
    active_model = active_model_dir / "yolov8n.pt"
    rollback_model = rollback_model_dir / "yolov8n.pt"
    _require_regular_file(active_model, "active model")
    rollback_model_existed = rollback_model != active_model and rollback_model.exists()
    if rollback_model_existed:
        _require_regular_file(rollback_model, "existing rollback-target model")
    _, active_image = _running_container(runner, env_file)
    service_stopped = False
    switched = False
    with tempfile.TemporaryDirectory(prefix="parking-rollback-") as temporary_name:
        stage = Path(temporary_name)
        os.chmod(stage, 0o700)
        (stage / "models").mkdir(mode=0o700)
        _copy_private(rollback_dir / "config.yaml", stage / "config.yaml")
        _copy_private(rollback_dir / ".env", stage / ".env")
        _copy_private(rollback_dir / "yolov8n.pt", stage / "models/yolov8n.pt")
        _copy_private(config_file, stage / "active-config.yaml")
        _copy_private(env_file, stage / "active.env")
        _copy_private(active_model, stage / "active-yolov8n.pt")
        if rollback_model_existed:
            _copy_private(rollback_model, stage / "rollback-target-yolov8n.pt")
        runner.run(
            (
                "docker", "run", "--rm", "--env-file", str(stage / ".env"),
                "--mount", f"type=bind,src={stage / 'config.yaml'},dst=/config/config.yaml,readonly",
                "--mount", f"type=bind,src={stage / 'models'},dst=/models,readonly",
                "--mount", f"type=bind,src={data_dir.resolve()},dst=/data",
                rollback_image, "python", "-m", "parking_spot_monitor",
                "--config", "/config/config.yaml", "--data-dir", "/data", "--validate-config",
            )
        )
        try:
            service_stopped = True
            _compose(runner, "stop", SERVICE, env_file=env_file)
            switched = True
            _atomic_install(stage / "config.yaml", config_file, 0o600)
            _atomic_install(stage / ".env", env_file, 0o600)
            _atomic_install(stage / "models/yolov8n.pt", rollback_model, 0o644)
            runner.run(("docker", "image", "tag", rollback_image, IMAGE_TAG))
            _compose(
                runner,
                "up", "-d", "--no-build", "--force-recreate", SERVICE,
                env_file=env_file,
            )
            wait_for_fresh_health(
                runner, _started_at(runner, env_file), data_dir, env_file=env_file
            )
        except BaseException:
            if service_stopped:
                _compose(runner, "stop", SERVICE, env_file=env_file, check=False)
                if switched:
                    _atomic_install(stage / "active-config.yaml", config_file, 0o600)
                    _atomic_install(stage / "active.env", env_file, 0o600)
                    _atomic_install(stage / "active-yolov8n.pt", active_model, 0o644)
                    if rollback_model != active_model:
                        if rollback_model_existed:
                            _atomic_install(
                                stage / "rollback-target-yolov8n.pt",
                                rollback_model,
                                0o644,
                            )
                        else:
                            _durable_unlink(rollback_model)
                runner.run(("docker", "image", "tag", active_image, IMAGE_TAG))
                _compose(
                    runner,
                    "up", "-d", "--no-build", "--force-recreate", SERVICE,
                    env_file=env_file,
                )
                wait_for_fresh_health(
                    runner, _started_at(runner, env_file), data_dir, env_file=env_file
                )
            raise


def restore_data_operation(runner: Runner, rollback_dir: Path, data_dir: Path) -> Path:
    verify_bundle(rollback_dir)
    parent = data_dir.resolve().parent
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    preserved = parent / f"{data_dir.name}.pre-restore.{timestamp}"
    if preserved.exists():
        raise DeploymentError("preserved data destination already exists")
    service_stopped = False
    activated = False
    with tempfile.TemporaryDirectory(prefix=f".{data_dir.name}.restore-", dir=parent) as temporary_name:
        restored = Path(temporary_name)
        runner.run(
            ("tar", "--no-same-owner", "-C", str(restored), "-xpf", str(rollback_dir / "data.tar"))
        )
        try:
            service_stopped = True
            runner.compose("stop", SERVICE)
            os.rename(data_dir, preserved)
            os.rename(restored, data_dir)
            activated = True
            runner.compose("start", SERVICE)
            wait_for_fresh_health(runner, _started_at(runner), data_dir)
            return preserved
        except BaseException:
            if service_stopped:
                runner.compose("stop", SERVICE, check=False)
                if activated and data_dir.exists():
                    failed = parent / f"{data_dir.name}.failed-restore.{timestamp}.{os.getpid()}"
                    os.rename(data_dir, failed)
                if preserved.exists():
                    os.rename(preserved, data_dir)
                runner.compose("start", SERVICE)
                wait_for_fresh_health(runner, _started_at(runner), data_dir)
            raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    backup = subparsers.add_parser("backup")
    backup.add_argument("--backup-dir", required=True, type=Path)
    backup.add_argument("--rollback-tag", required=True)
    backup.add_argument("--approved-model-sha256", required=True)
    backup.add_argument("--data-dir", default=Path("data"), type=Path)
    backup.add_argument("--env-file", default=Path(".env"), type=Path)
    backup.add_argument("--config-file", default=Path("config.yaml"), type=Path)
    upgrade = subparsers.add_parser("upgrade")
    upgrade.add_argument("--reviewed-revision", required=True)
    upgrade.add_argument("--rollback-tag", required=True)
    upgrade.add_argument("--data-dir", default=Path("data"), type=Path)
    rollback = subparsers.add_parser("rollback")
    rollback.add_argument("--rollback-dir", required=True, type=Path)
    rollback.add_argument("--data-dir", default=Path("data"), type=Path)
    rollback.add_argument("--config-file", default=Path("config.yaml"), type=Path)
    rollback.add_argument("--env-file", default=Path(".env"), type=Path)
    restore = subparsers.add_parser("restore-data")
    restore.add_argument("--rollback-dir", required=True, type=Path)
    restore.add_argument("--data-dir", default=Path("data"), type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    runner = Runner()
    try:
        if args.operation == "backup":
            backup_operation(
                runner,
                args.backup_dir,
                args.rollback_tag,
                args.approved_model_sha256,
                data_dir=args.data_dir,
                env_file=args.env_file,
                config_file=args.config_file,
            )
        elif args.operation == "upgrade":
            upgrade_operation(runner, args.reviewed_revision, args.rollback_tag, args.data_dir)
        elif args.operation == "rollback":
            rollback_operation(
                runner,
                args.rollback_dir,
                args.data_dir,
                args.config_file,
                args.env_file,
            )
        else:
            preserved = restore_data_operation(runner, args.rollback_dir, args.data_dir)
            print(f"preserved pre-restore data at {preserved}")
    except (DeploymentError, OSError) as exc:
        print(f"deployment operation failed: {exc}", file=sys.stderr)
        return 2
    return 0


def _interrupt(_signal_number: int, _frame: object) -> None:
    raise DeploymentError("deployment operation interrupted")


if __name__ == "__main__":
    for signal_name in ("SIGHUP", "SIGINT", "SIGTERM"):
        if hasattr(signal, signal_name):
            signal.signal(getattr(signal, signal_name), _interrupt)
    raise SystemExit(main())
