"""Cross-process transaction lock for one local outbox file."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import stat
from typing import Iterator

from parking_monitor.outbox_models import OutboxPersistenceError


@contextmanager
def outbox_transaction(path: Path) -> Iterator[None]:
    """Serialize the complete read/merge/write transaction for ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f".{path.name}.lock")
    try:
        descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except OSError as exc:
        raise OutboxPersistenceError("failed to lock local outbox") from exc
    try:
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OutboxPersistenceError("local outbox lock must be a regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except OSError as exc:
            raise OutboxPersistenceError("failed to lock local outbox") from exc
        yield
    finally:
        os.close(descriptor)
