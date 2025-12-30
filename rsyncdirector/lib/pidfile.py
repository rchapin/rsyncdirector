import logging
import os
import sys
import psutil
from abc import ABC, abstractmethod
from pathlib import Path
from fabric import Connection
from typing import Tuple


class PidFile(ABC):

    def __init__(self, logger: logging.Logger, pid: int, path: str) -> None:
        self.logger = logger
        self.pid = pid
        self.path = path

    @abstractmethod
    def delete(self) -> bool:
        pass

    @abstractmethod
    def does_pid_file_exist(self) -> Tuple[bool, int]:
        pass

    @abstractmethod
    def does_process_with_pid_exist(self) -> bool:
        pass

    @abstractmethod
    def write(self) -> bool:
        file_exists, existing_pid_str = self.does_pid_file_exist()
        existing_pid = None
        if existing_pid_str != "":
            try:
                existing_pid = int(existing_pid_str)
            except Exception as e:
                # If this fails, we will assume that there isn't a valid process running
                self.logger.warning(
                    f"pid file contained unparseable int; self.path={self.path}, existing_pid={existing_pid}"
                )
                existing_pid = None

        should_write_pid = False
        if file_exists and existing_pid:
            # Check to see if, for some reason that this is already our pid.
            if existing_pid == self.pid:
                should_write_pid = True
                self.logger.info(
                    f"existing local pid file contains our pid, overwriting it; path={self.path}, pid={self.pid}"
                )
            else:
                """
                If the existing pid is not ours, we need to verify that there is a running process
                with the pid that is in the current pid file.
                """
                if not self.does_process_with_pid_exist(existing_pid):
                    self.logger.info(
                        f"found existing pid file with existing pid that maps to non-existant process; path={self.path}, existing_pid={existing_pid}"
                    )
                    should_write_pid = True

        else:
            # There is no existing file or there is no valid pid in that file.
            should_write_pid = True

        return should_write_pid


class PidFileLocal(PidFile):

    def __init__(self, logger: logging.Logger, pid: int, path: str) -> None:
        super().__init__(logger, pid, path)

    def delete(self) -> bool:
        os.remove(self.path)
        return True

    def does_pid_file_exist(self) -> Tuple[bool, str]:
        path = Path(self.path)
        if path.is_dir():
            self.logger.warning(
                f"pid path is a directory for some reason, deleting it; self.path={self.path}"
            )
            path.rmdir()
            return False, ""

        file_exists = None
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

    def __init__(self, logger: logging.Logger, pid: int, path: str, conn: Connection) -> None:
        super().__init__(logger, pid, path)
        self.conn = conn

    def delete(self) -> bool:
        result = self.conn.run(f"rm -f {self.path}")
        if not result.ok:
            self.logger.fatal(f"deleting pid file; path={self.path}, result={result}")
            sys.exit(1)
        return True

    def does_pid_file_exist(self) -> Tuple[bool, str]:
        result = self.conn.run(
            f'if [ -f "{self.path}" ]; then echo "1"; else echo "0"; fi', warn=True, hide=True
        )
        if not result.ok:
            self.logger.error(f"checking for existing pid file; path={self.path}, result={result}")
            return False, 0
        stdout = result.stdout
        file_exists = True if stdout.strip() == "1" else False

        if file_exists is False:
            return False, 0

        result = self.conn.run(f"cat {self.path}", warn=True, hide=True)
        if not result.ok:
            self.logger.error(f"cat'ing pid file path; path={self.path}, result={result}")
            return False, 0

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
                self.logger.error(
                    f"writing remote pid file; pid={self.pid}, path={self.path}, result={result}"
                )
                return False
            return True
