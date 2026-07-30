from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import pytest

from tests.dependency_lock_helpers import (
    compiled_output_path,
    compiled_requirement,
    copy_lock_inputs,
    load_lock_module,
    lock_pin_versions,
    remove_lock_requirement,
    valid_compiled,
)


def test_toolchain_pin_change_uses_authenticated_two_phase_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    current_paths = [root / destination for _, destination in module.COMMANDS]
    before = {path.name: path.read_bytes() for path in current_paths}
    old_versions = lock_pin_versions(before["requirements-build.lock"].decode())
    build_input = root / "requirements-build.txt"
    build_input.write_text(
        build_input.read_text(encoding="utf-8").replace(
            "pip-tools==7.5.0", "pip-tools==7.5.1"
        ),
        encoding="utf-8",
    )

    installed_versions = old_versions
    monkeypatch.setattr(
        module.importlib.metadata,
        "version",
        lambda name: installed_versions[module._canonical_name(name)],
    )

    def fake_run(command, *, cwd, check, env):
        source = command[-1]
        compiled_output_path(root, command).write_text(
            valid_compiled(source, build_pip_tools="7.5.1"),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    module.stage_build_lock(root)

    next_lock = root / module.NEXT_BUILD_LOCK
    staged_text = next_lock.read_text(encoding="utf-8")
    assert "pip-tools==7.5.1" in staged_text
    assert {path.name: path.read_bytes() for path in current_paths} == before
    installed_versions = lock_pin_versions(staged_text)

    module.generate_locks(root)

    assert not next_lock.exists()
    assert (root / "requirements-build.lock").read_text(encoding="utf-8") == staged_text
    assert module.check_locks(root) == 0


def test_stale_build_input_requires_authenticated_next_lock_before_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    build_input = root / "requirements-build.txt"
    build_input.write_text(
        build_input.read_text(encoding="utf-8").replace(
            "pip-tools==7.5.0", "pip-tools==7.5.1"
        ),
        encoding="utf-8",
    )
    expected = {
        module._canonical_name(line.split("==", 1)[0]): line.split("==", 1)[1]
        for line in build_input.read_text(encoding="utf-8").splitlines()
    }
    monkeypatch.setattr(
        module.importlib.metadata,
        "version",
        lambda name: expected[module._canonical_name(name)],
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("resolver ran before authentication"),
    )

    with pytest.raises(RuntimeError, match="stage.*build lock|next build lock"):
        module.generate_locks(root)


def test_build_lock_staging_rejects_unsupported_input_before_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    versions = lock_pin_versions(
        (root / "requirements-build.lock").read_text(encoding="utf-8")
    )
    (root / "requirements-build.txt").write_text(
        "pip-tools @ https://untrusted.invalid/pip-tools.whl\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        module.importlib.metadata,
        "version",
        lambda name: versions[module._canonical_name(name)],
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("resolver ran for unsupported input"),
    )

    with pytest.raises(RuntimeError, match="unsupported dependency requirement"):
        module.stage_build_lock(root)


@pytest.mark.parametrize(
    "replacement",
    ["", "pip-tools>=7.5.0\n"],
)
def test_build_lock_staging_requires_exact_pip_tools_pin_before_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: str
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    versions = lock_pin_versions(
        (root / "requirements-build.lock").read_text(encoding="utf-8")
    )
    build_input = root / "requirements-build.txt"
    build_input.write_text(
        build_input.read_text(encoding="utf-8").replace(
            "pip-tools==7.5.0\n", replacement
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        module.importlib.metadata,
        "version",
        lambda name: versions[module._canonical_name(name)],
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("resolver ran without exact pip-tools pin"),
    )

    with pytest.raises(RuntimeError, match="exact.*pip-tools|pip-tools.*exact"):
        module.stage_build_lock(root)


@pytest.mark.parametrize(
    "missing_package",
    ["pip-tools", "click", "packaging", "pyproject-hooks"],
)
def test_build_lock_staging_rejects_missing_trusted_closure_before_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing_package: str
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    current_lock = root / "requirements-build.lock"
    original = current_lock.read_text(encoding="utf-8")
    versions = lock_pin_versions(original)
    current_lock.write_text(
        remove_lock_requirement(original, missing_package), encoding="utf-8"
    )
    monkeypatch.setattr(
        module.importlib.metadata,
        "version",
        lambda name: versions[module._canonical_name(name)],
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("resolver ran with incomplete closure"),
    )

    with pytest.raises(RuntimeError, match="trusted.*closure|closure.*missing"):
        module.stage_build_lock(root)


def test_build_lock_staging_rejects_untrusted_extra_before_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    current_lock = root / "requirements-build.lock"
    text = current_lock.read_text(encoding="utf-8") + compiled_requirement(
        "unexpected-tool"
    )
    current_lock.write_text(text, encoding="utf-8")
    versions = lock_pin_versions(text)
    monkeypatch.setattr(
        module.importlib.metadata,
        "version",
        lambda name: versions[module._canonical_name(name)],
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("resolver ran with untrusted extra"),
    )

    with pytest.raises(RuntimeError, match="trusted.*closure|unexpected"):
        module.stage_build_lock(root)


def test_build_lock_staging_rejects_renamed_closure_member_before_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    current_lock = root / "requirements-build.lock"
    text = current_lock.read_text(encoding="utf-8").replace(
        "click==8.4.2", "cluck==8.4.2"
    )
    current_lock.write_text(text, encoding="utf-8")
    versions = lock_pin_versions(text)
    monkeypatch.setattr(
        module.importlib.metadata,
        "version",
        lambda name: versions[module._canonical_name(name)],
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("resolver ran with renamed closure member"),
    )

    with pytest.raises(RuntimeError, match="trusted.*closure|missing|unexpected"):
        module.stage_build_lock(root)


def test_generation_sanitizes_indexes_and_publishes_explicit_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    monkeypatch.setattr(module, "_validate_build_tool", lambda _root: None)
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run(command, *, cwd, check, env):
        source = command[-1]
        compiled_output_path(root, command).write_text(
            valid_compiled(source), encoding="utf-8"
        )
        calls.append((command, env))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    monkeypatch.setenv("PIP_INDEX_URL", "https://untrusted.invalid/simple")
    monkeypatch.setenv("PIP_EXTRA_INDEX_URL", "https://also-untrusted.invalid/simple")
    monkeypatch.setenv("PIP_FIND_LINKS", "/untrusted/wheels")
    monkeypatch.setenv("PIP_NO_INDEX", "1")
    monkeypatch.setenv("PIP_TRUSTED_HOST", "untrusted.invalid")

    module.generate_locks(root)

    assert len(calls) == len(module.COMMANDS)
    for command, environment in calls:
        assert command[:4] == [sys.executable, "-I", "-m", "piptools"]
        assert any(
            argument
            in (
                "--index-url=https://pypi.org/simple",
                "--index-url=https://download.pytorch.org/whl/cpu",
            )
            for argument in command
        )
        for name in (
            "PIP_INDEX_URL",
            "PIP_EXTRA_INDEX_URL",
            "PIP_FIND_LINKS",
            "PIP_NO_INDEX",
            "PIP_TRUSTED_HOST",
        ):
            assert name not in environment
        assert environment["PIP_CONFIG_FILE"] == os.devnull
        assert "PYTHONPATH" not in environment
        assert environment["PYTHONSAFEPATH"] == "1"
    assert (
        root.joinpath("requirements-runtime.lock")
        .read_text(encoding="utf-8")
        .splitlines()[3]
        == "--index-url https://pypi.org/simple"
    )
    assert (
        root.joinpath("requirements-detector.lock")
        .read_text(encoding="utf-8")
        .splitlines()[3:5]
        == [
            "--index-url https://pypi.org/simple",
            "--extra-index-url https://download.pytorch.org/whl/cpu",
        ]
    )


@pytest.mark.parametrize(
    "bad_output",
    [
        "httpx==1.0\n",
        (
            "httpx==1.0\n"
            f"    --hash=sha256:{'a' * 64}\n"
        ),
        (
            "httpx==1.0 \\\n"
            f"    --hash=sha256:{'a' * 64}\n"
            f"    --hash=sha256:{'b' * 64}\n"
        ),
        (
            "httpx==1.0 \\\n"
            f"    --hash=sha256:{'a' * 64} \\\n"
        ),
        (
            "httpx>=1.0 \\\n"
            f"    --hash=sha256:{'a' * 64}\n"
        ),
        (
            "--index-url https://unexpected.invalid/simple\n\n"
            + compiled_requirement("httpx")
        ),
    ],
)
def test_malformed_successful_compiler_output_preserves_both_locks_and_cleans_temps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad_output: str
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    monkeypatch.setattr(module, "_validate_build_tool", lambda _root: None)
    before = {
        name: root.joinpath(name).read_bytes()
        for name in ("requirements-runtime.lock", "requirements-detector.lock")
    }

    def fake_run(command, *, cwd, check, env):
        source = command[-1]
        text = bad_output if source == "requirements.txt" else valid_compiled(source)
        compiled_output_path(root, command).write_text(text, encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="invalid generated dependency lock"):
        module.generate_locks(root)

    after = {
        name: root.joinpath(name).read_bytes()
        for name in ("requirements-runtime.lock", "requirements-detector.lock")
    }
    assert after == before
    assert list(root.glob(".*.lock.*.tmp")) == []


def test_compiler_failure_preserves_both_locks_and_cleans_temps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    monkeypatch.setattr(module, "_validate_build_tool", lambda _root: None)
    before = {
        name: root.joinpath(name).read_bytes()
        for name in ("requirements-runtime.lock", "requirements-detector.lock")
    }

    def fake_run(command, *, cwd, check, env):
        if command[-1] == "requirements.txt":
            compiled_output_path(root, command).write_text(
                valid_compiled(command[-1]), encoding="utf-8"
            )
            return SimpleNamespace(returncode=0)
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        module.generate_locks(root)

    after = {
        name: root.joinpath(name).read_bytes()
        for name in ("requirements-runtime.lock", "requirements-detector.lock")
    }
    assert after == before
    assert list(root.glob(".*.lock.*.tmp")) == []

@pytest.mark.parametrize("first_destination_exists", [True, False])
def test_second_publish_failure_restores_prior_pair_and_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    first_destination_exists: bool,
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    monkeypatch.setattr(module, "_validate_build_tool", lambda _root: None)
    destinations = [root / destination for _, destination in module.COMMANDS]
    if not first_destination_exists:
        destinations[0].unlink(missing_ok=True)
    before = {
        path.name: path.read_bytes() if path.exists() else None for path in destinations
    }

    def fake_run(command, *, cwd, check, env):
        compiled_output_path(root, command).write_text(
            valid_compiled(command[-1]), encoding="utf-8"
        )
        return SimpleNamespace(returncode=0)

    real_replace = module.os.replace
    replace_count = 0

    def fail_second_replace(source, destination):
        nonlocal replace_count
        replace_count += 1
        if replace_count == 2:
            raise OSError("injected second publish failure")
        return real_replace(source, destination)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    monkeypatch.setattr(module.os, "replace", fail_second_replace)

    with pytest.raises(OSError, match="second publish"):
        module.generate_locks(root)

    after = {
        path.name: path.read_bytes() if path.exists() else None for path in destinations
    }
    assert after == before


def test_cleanup_failure_reports_without_masking_primary_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = copy_lock_inputs(tmp_path)
    module = load_lock_module()
    digest = module.compute_source_digest(root)

    def fake_run(command, *, cwd, check, env):
        compiled_output_path(root, command).write_text(
            "malformed compiler output\n", encoding="utf-8"
        )
        return SimpleNamespace(returncode=0)

    real_unlink = module.Path.unlink

    def fail_temp_unlink(path, *args, **kwargs):
        if path.name.startswith(".requirements-runtime.lock."):
            raise OSError("injected cleanup failure")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    monkeypatch.setattr(module.Path, "unlink", fail_temp_unlink)

    with pytest.raises(RuntimeError, match="invalid generated dependency lock") as caught:
        module._compile_lock(
            root,
            "requirements.txt",
            "requirements-runtime.lock",
            digest,
        )

    assert any("cleanup failure" in note for note in getattr(caught.value, "__notes__", ()))
    assert "cleanup warning" in capsys.readouterr().err


def test_rollback_temp_cleanup_failure_does_not_mask_replace_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_lock_module()
    destination = tmp_path / "requirements-runtime.lock"
    destination.write_bytes(b"new")
    real_unlink = module.Path.unlink

    def fail_replace(source, target):
        raise OSError("injected rollback replace failure")

    def fail_rollback_unlink(path, *args, **kwargs):
        if ".rollback." in path.name:
            raise OSError("injected rollback cleanup failure")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(module.os, "replace", fail_replace)
    monkeypatch.setattr(module.Path, "unlink", fail_rollback_unlink)

    with pytest.raises(OSError, match="rollback replace") as caught:
        module._restore_destination(destination, ("file", b"old", 0o644))

    assert any("cleanup failure" in note for note in getattr(caught.value, "__notes__", ()))
    assert "cleanup warning" in capsys.readouterr().err
