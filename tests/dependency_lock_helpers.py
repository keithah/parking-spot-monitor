from __future__ import annotations

import importlib.util
import os
from collections.abc import Sequence
from pathlib import Path
import shutil
import subprocess
import sys
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]
LOCK_SCRIPT = ROOT / "scripts" / "lock_dependencies.py"
LOCK_PATHS = (
    ROOT / "requirements-build.lock",
    ROOT / "requirements-runtime.lock",
    ROOT / "requirements-detector.lock",
)


def run_check(root: Path) -> subprocess.CompletedProcess[str]:
    environment = {
        **os.environ,
        "PIP_NO_INDEX": "1",
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PYTHONNOUSERSITE": "1",
    }
    return subprocess.run(
        [
            sys.executable,
            "-I",
            str(root / "scripts" / "lock_dependencies.py"),
            "--check",
        ],
        cwd=root,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def copy_lock_inputs(tmp_path: Path) -> Path:
    root = tmp_path / "repository"
    (root / "scripts").mkdir(parents=True)
    for relative in (
        "requirements.txt",
        "requirements-detector.txt",
        "requirements-build.txt",
        "requirements-build.lock",
        "pyproject.toml",
        "requirements-runtime.lock",
        "requirements-detector.lock",
        "scripts/lock_dependencies.py",
        "scripts/dependency_lock_validation.py",
    ):
        source = ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return root


def requirement_blocks(text: str) -> list[str]:
    lines = text.splitlines()
    blocks: list[str] = []
    current: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line.startswith((" ", "\t")) and stripped.startswith("--"):
            continue
        if not line.startswith((" ", "\t")):
            if current:
                blocks.append("\n".join(current))
            current = [stripped]
        elif current:
            current.append(stripped)
    if current:
        blocks.append("\n".join(current))
    return blocks


def lock_pin_versions(text: str) -> dict[str, str]:
    return {
        block.split("==", 1)[0]: block.split("==", 1)[1].split()[0]
        for block in requirement_blocks(text)
    }


def remove_lock_requirement(text: str, package: str) -> str:
    lines = text.splitlines(keepends=True)
    start = next(
        index for index, line in enumerate(lines) if line.startswith(f"{package}==")
    )
    end = start + 1
    while end < len(lines) and lines[end].startswith((" ", "\t")):
        end += 1
    return "".join(lines[:start] + lines[end:])


def load_lock_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("lock_dependencies", LOCK_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def compiled_requirement(name: str, version: str = "1.0") -> str:
    return f"{name}=={version} \\\n    --hash=sha256:{'a' * 64}\n"


def compiled_output_path(root: Path, command: Sequence[str]) -> Path:
    output = next(
        argument.split("=", 1)[1]
        for argument in command
        if argument.startswith("--output-file=")
    )
    return root / output


def valid_compiled(source: str, build_pip_tools: str = "7.5.0") -> str:
    if source in {"requirements.txt", "requirements-build.txt"}:
        # pip-tools suppresses its default PyPI directive even when supplied
        # explicitly; the generator must make the reviewed install source explicit.
        directives = ""
        requirements = (
            (
                ("httpx", "0.28.1"),
                ("pillow", "12.3.0"),
                ("pydantic", "2.13.4"),
                ("pyyaml", "6.0.3"),
            )
            if source == "requirements.txt"
            else (
                ("build", "1.5.0"),
                ("click", "8.4.2"),
                ("packaging", "26.2"),
                ("pip", "24.0"),
                ("pip-tools", build_pip_tools),
                ("pyproject-hooks", "1.2.0"),
                ("setuptools", "83.0.0"),
                ("wheel", "0.47.0"),
            )
        )
    else:
        directives = (
            "--index-url https://download.pytorch.org/whl/cpu\n"
            "--extra-index-url https://pypi.org/simple\n\n"
        )
        requirements = (
            ("torch", "2.7.1+cpu"),
            ("torchvision", "0.22.1+cpu"),
            ("ultralytics", "8.4.60"),
        )
    return directives + "".join(
        compiled_requirement(name, version) for name, version in requirements
    )
