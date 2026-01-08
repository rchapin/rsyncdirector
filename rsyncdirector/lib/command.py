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
        args: List[str],
    ):
        self.logger = logger
        self.result_queue = result_queue
        self.command = command
        self.args = args

    def run(self) -> None:
        args = " ".join(self.args)
        cmd = f"{self.command} {args}"
        self.logger.info(f"running command; cmd={cmd}")
        result = run(cmd, warn=True)
        run_result = RunResult.SUCCESS
        if result.failed:
            run_result = RunResult.FAIL
        self.result_queue.put((run_result, result))
