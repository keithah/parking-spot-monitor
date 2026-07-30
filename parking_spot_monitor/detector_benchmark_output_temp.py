from __future__ import annotations

import os
import secrets
import stat
from dataclasses import dataclass


@dataclass(frozen=True)
class OwnedTemporary:
    name: str
    descriptor: int
    device: int
    inode: int


def create_owned_temporary(
    parent_descriptor: int,
    output_name: str,
) -> OwnedTemporary:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    for _attempt in range(128):
        name = f".{output_name}.tmp-{secrets.token_hex(16)}"
        try:
            descriptor = os.open(
                name,
                flags,
                0o600,
                dir_fd=parent_descriptor,
            )
        except FileExistsError:
            continue
        try:
            item = os.fstat(descriptor)
            if not stat.S_ISREG(item.st_mode):
                raise ValueError("benchmark output temporary must be a regular file")
            return OwnedTemporary(
                name=name,
                descriptor=descriptor,
                device=item.st_dev,
                inode=item.st_ino,
            )
        except BaseException:
            os.close(descriptor)
            try:
                os.unlink(name, dir_fd=parent_descriptor)
            except FileNotFoundError:
                pass
            raise
    raise ValueError("unable to reserve a unique benchmark output temporary")
