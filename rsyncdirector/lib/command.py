# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import logging
import multiprocessing
from rsyncdirector.lib.enums import RunResult
from invoke import run
from typing import List


class Command(object):
    def __init__(
        self,
        logger: logging.Logger,
        result_queue: multiprocessing.Queue,
        command: str,
        args: List[str] | None = None,
    ):
        self.logger = logger
        self.result_queue = result_queue
        self.command = command
        self.args = args

    def run(self) -> None:
        args = " ".join(self.args) if self.args else None
        cmd = f"{self.command} {args}" if args else self.command
        self.logger.info(f"running command; cmd={cmd}")
        result = run(cmd, warn=True)
        run_result = RunResult.FAIL
        if result and result.ok:
            run_result = RunResult.SUCCESS
        self.result_queue.put((run_result, result))
