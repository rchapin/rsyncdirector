# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import os
import sys
import psutil
from abc import ABC, abstractmethod
from pathlib import Path
from fabric import Connection
from typing import Tuple
from rsyncdirector.lib.logging import Logger


class PidFile(ABC):

    def __init__(self, logger: Logger, pid: int, path: str) -> None:
        self.logger = logger.bind(pid=pid, path=path)
        self.pid = pid
        self.path = path

    @abstractmethod
    def delete(self) -> bool:
        pass

    @abstractmethod
    def does_pid_file_exist(self) -> Tuple[bool, str]:
        pass

    @abstractmethod
    def does_process_with_pid_exist(self, pid: int) -> bool:
        pass

    @abstractmethod
    def write(self) -> bool:
        file_exists, existing_pid_str = self.does_pid_file_exist()
        existing_pid = self.__parse_pid(existing_pid_str)

        should_write_pid = False
        if file_exists and existing_pid != 0:
            # Check to see if, for some reason that this is already our pid.
            if existing_pid == self.pid:
                should_write_pid = True
                self.logger.info("existing local pid file contains our pid, overwriting it")
            else:
                """
                If the existing pid is not ours, we need to verify that there is a running process
                with the pid that is in the current pid file.
                """
                if not self.does_process_with_pid_exist(existing_pid):
                    self.logger.info(
                        "found existing pid file with existing pid that maps to non-existant process",
                        existing_pid=existing_pid,
                    )
                    should_write_pid = True

        else:
            # There is no existing file or there is no valid pid in that file.
            should_write_pid = True

        return should_write_pid

    def __parse_pid(self, pid_str: str) -> int:
        try:
            existing_pid = int(pid_str)
            return existing_pid
        except Exception as e:
            self.logger.warning("unparseable pid", pid_str=pid_str)
            return 0


class PidFileLocal(PidFile):

    def __init__(self, logger: Logger, pid: int, path: str) -> None:
        super().__init__(logger, pid, path)

    def delete(self) -> bool:
        os.remove(self.path)
        return True

    def does_pid_file_exist(self) -> Tuple[bool, str]:
        path = Path(self.path)
        if path.is_dir():
            self.logger.warning("pid path is a directory for some reason, deleting it")
            path.rmdir()
            return False, ""

        file_exists = False
        existing_pid = ""
        if path.is_file():
            # Read the existing file and get the pid if there is one
            file_exists = True
            existing_pid = ""
            with open(self.path, "r") as fh:
                existing_pid = fh.read().strip()

        return file_exists, existing_pid

    def does_process_with_pid_exist(self, pid: int) -> bool:
        return psutil.pid_exists(pid)

    def write(self) -> bool:
        should_write_pid = super().write()

        if should_write_pid:
            with open(self.path, "w") as fh:
                bytes_written = fh.write(str(self.pid))
                if bytes_written > 0:
                    return True

        return False


class PidFileRemote(PidFile):

    def __init__(self, logger: Logger, pid: int, path: str, conn: Connection) -> None:
        super().__init__(logger, pid, path)
        self.conn = conn

    def delete(self) -> bool:
        result = self.conn.run(f"rm -f {self.path}")
        if not result.ok:
            self.logger.fatal("deleting pid file", result=result)
            sys.exit(1)
        return True

    def does_pid_file_exist(self) -> Tuple[bool, str]:
        result = self.conn.run(
            f'if [ -f "{self.path}" ]; then echo "1"; else echo "0"; fi', warn=True, hide=True
        )
        if not result.ok:
            self.logger.error("checking for existing pid file", result=result)
            return False, ""
        stdout = result.stdout
        file_exists = True if stdout.strip() == "1" else False

        if file_exists is False:
            return False, ""

        result = self.conn.run(f"cat {self.path}", warn=True, hide=True)
        if not result.ok:
            self.logger.error("cat'ing pid file path", result=result)
            return False, ""

        existing_pid = result.stdout
        return True, existing_pid.strip()

    def does_process_with_pid_exist(self, pid: int) -> bool:
        result = self.conn.run(f"ps -p {pid}", warn=True, hide=True)
        if not result.ok:
            # There is no process with this pid
            return False
        return True

    def write(self) -> bool:
        should_write_pid = super().write()
        if should_write_pid:
            result = self.conn.run(f"echo {self.pid} > {self.path}")
            if not result.ok:
                self.logger.error("writing remote pid file", result=result)
                return False
            return True
        return False
