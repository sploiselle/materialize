# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Various utilities"""

from __future__ import annotations

import json
import os
import pathlib
import random
import subprocess
from enum import Enum
from pathlib import Path
from threading import Thread
from typing import TypeVar

import zstandard

MZ_ROOT = Path(os.environ["MZ_ROOT"])


def nonce(digits: int) -> str:
    return "".join(random.choice("0123456789abcdef") for _ in range(digits))


T = TypeVar("T")


def all_subclasses(cls: type[T]) -> set[type[T]]:
    """Returns a recursive set of all subclasses of a class"""
    sc = cls.__subclasses__()
    return set(sc).union([subclass for c in sc for subclass in all_subclasses(c)])


NAUGHTY_STRINGS = None


def naughty_strings() -> list[str]:
    # Naughty strings taken from https://github.com/minimaxir/big-list-of-naughty-strings
    # Under MIT license, Copyright (c) 2015-2020 Max Woolf
    global NAUGHTY_STRINGS
    if not NAUGHTY_STRINGS:
        with open(MZ_ROOT / "misc" / "python" / "materialize" / "blns.json") as f:
            NAUGHTY_STRINGS = json.load(f)
    return NAUGHTY_STRINGS


class YesNoOnce(Enum):
    YES = 1
    NO = 2
    ONCE = 3


class PropagatingThread(Thread):
    def run(self):
        self.exc = None
        try:
            self.ret = self._target(*self._args, **self._kwargs)  # type: ignore
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exc:
            raise self.exc
        if hasattr(self, "ret"):
            return self.ret


def decompress_zst_to_directory(
    zst_file_path: str, destination_dir_path: str
) -> list[str]:
    """
    :return: file paths in destination dir
    """
    input_file = pathlib.Path(zst_file_path)
    output_paths = []

    with open(input_file, "rb") as compressed:
        decompressor = zstandard.ZstdDecompressor()
        output_path = pathlib.Path(destination_dir_path) / input_file.stem
        output_paths.append(str(output_path))
        with open(output_path, "wb") as destination:
            decompressor.copy_stream(compressed, destination)

    return output_paths


def ensure_dir_exists(path_to_dir: str) -> None:
    subprocess.run(
        [
            "mkdir",
            "-p",
            f"{path_to_dir}",
        ],
        check=True,
    )
