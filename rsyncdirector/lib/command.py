import logging
from invoke import run
from typing import List


class Command(object):
    def __init__(self, logger: logging.Logger, command: str, args: List[str]):
        self.logger = logger
        self.command = command
        self.args = args

    def run(self) -> None:
        args = " ".join(self.args)
        cmd = f"{self.command} {args}"
        self.logger.info(f"running command; cmd={cmd}")
        run(cmd)
