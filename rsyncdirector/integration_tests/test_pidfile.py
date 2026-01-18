# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import logging
import os
import psutil
import random
import sys
from fabric import Connection
from pathlib import Path
from rsyncdirector.lib.pidfile import PidFile, PidFileLocal, PidFileRemote
from rsyncdirector.integration_tests.it_base import ITBase
from rsyncdirector.integration_tests.int_test_utils import IntegrationTestUtils, ContainerType
from typing import Callable, Tuple

logging.basicConfig(
    format="%(asctime)s,%(levelname)s,%(module)s,%(message)s", level=logging.INFO, stream=sys.stdout
)
logger = logging.getLogger(__name__)

FILE_NAME = "pid_file.txt"
PID = 647
REMOTE_PATH = "/var/tmp/pid_file.txt"

ExecRemoteFunc = Callable[[Connection], None]


class ITPidFile(ITBase):

    def setUp(self):
        logger.info("Running setup")
        self.setup_base()
        IntegrationTestUtils.restart_docker_containers(self.test_configs)

    def tearDown(self):
        IntegrationTestUtils.stop_docker_containers(self.test_configs)

    #
    # Local --------------------------------------------------------------------
    #
    def test_local_write_and_delete(self):
        pid_file, path = self.__get_pid_file_local(pid=PID)
        wrote_pid = pid_file.write()
        self.assertTrue(wrote_pid, "did not return true when writing pid file")
        self.__validate_local_pid_file(True, path, PID)

        deleted_pid = pid_file.delete()
        self.assertTrue(deleted_pid, "did not return true when deleting pid file")
        self.__validate_local_pid_file(False, path, PID)

    def test_local_write_pid_finds_existing_file_with_our_pid(self):
        # Write out a pid file with our pid.
        our_pid = os.getpid()
        self.__write_test_pid_local(our_pid)
        pid_file, path = self.__get_pid_file_local()
        wrote_pid = pid_file.write()
        self.assertTrue(wrote_pid, "did not return true when writing pid file")
        self.__validate_local_pid_file(True, path, our_pid)

    def test_local_write_pid_finds_empty_pid_file(self):
        """
        Tests that when we encounter an existing pid file that is empty that we overwrite it with
        our pid.
        """
        # Touch an empty pid file
        self.__write_test_pid_local(empty=True)
        pid_file, path = self.__get_pid_file_local(pid=PID)
        wrote_pid = pid_file.write()
        self.assertTrue(wrote_pid, "did not return true when writing pid file")
        self.__validate_local_pid_file(True, path, PID)

    def test_local_write_finds_pid_file_with_another_running_pid(self):
        """
        Tests that we return the correct value when we find an existing pid file with a pid for a
        currently running process.
        """
        # Write out a pid file with a pid that we know maps to a running process.
        self.__write_test_pid_local(1)
        pid_file, path = self.__get_pid_file_local()
        wrote_pid = pid_file.write()
        self.assertFalse(wrote_pid, "did not return false when writing pid file")
        # The pid file should still have the existing pid number in it.
        self.__validate_local_pid_file(True, path, 1)

    def test_local_write_finds_pid_file_with_non_existent_pid(self):
        """
        Tests that we return the correct value when we find an existing pid file with a pid for a
        non-existent process.
        """
        # Write out a pid file with a pid that we know does not exist.
        non_existant_pid = self.__find_available_pid_local()
        self.__write_test_pid_local(non_existant_pid)
        pid_file, path = self.__get_pid_file_local(PID)
        wrote_pid = pid_file.write()
        self.assertTrue(wrote_pid)
        # The pid file should still have the existing pid number in it.
        self.__validate_local_pid_file(True, path, PID)

    #
    # Remote -------------------------------------------------------------------
    #
    def test_remote_write_and_delete(self):
        def test_func(conn: Connection) -> None:
            pid_file = self.__get_pid_file_remote(conn)
            wrote_pid = pid_file.write()
            self.assertTrue(wrote_pid)
            self.__validate_remote_pid_file(
                conn=conn,
                file_should_exist=True,
                expected_pid=PID,
            )

            deleted_pid = pid_file.delete()
            self.assertTrue(deleted_pid, "did return true when deleting pid file")
            self.__validate_remote_pid_file(conn=conn, file_should_exist=False)

        self.__exec_remote_test(test_func)

    def test_remote_write_pid_finds_existing_file_with_our_pid(self):
        def test_func(conn: Connection) -> None:
            pid = os.getpid()
            self.__write_test_pid_remote(conn, pid)
            pid_file = self.__get_pid_file_remote(conn, pid)
            wrote_pid = pid_file.write()
            self.assertTrue(wrote_pid)
            self.__validate_remote_pid_file(
                conn=conn,
                file_should_exist=True,
                expected_pid=pid,
            )

        self.__exec_remote_test(test_func)

    def test_remote_write_pid_finds_empty_pid_file(self):
        def test_func(conn: Connection) -> None:
            # Touch an empty pid file.
            result = conn.run(f"touch {REMOTE_PATH}")
            if not result.ok:
                self.fail(f"writing test pid file; result={result}")

            pid_file = self.__get_pid_file_remote(conn)
            wrote_pid = pid_file.write()
            self.assertTrue(wrote_pid)
            self.__validate_remote_pid_file(
                conn=conn,
                file_should_exist=True,
                expected_pid=PID,
            )

        self.__exec_remote_test(test_func)

    def test_remote_write_finds_pid_file_with_another_running_pid(self):
        def test_func(conn: Connection) -> None:
            # Write out a pid file with a pid that we know maps to a running process.
            result = conn.run(f"echo 1 > {REMOTE_PATH}")
            if not result.ok:
                self.fail(f"writing test pid file; result={result}")

            pid_file = self.__get_pid_file_remote(conn)
            wrote_pid = pid_file.write()
            self.assertFalse(wrote_pid)
            self.__validate_remote_pid_file(
                conn=conn,
                file_should_exist=True,
                expected_pid=1,
            )

        self.__exec_remote_test(test_func)

    def test_remote_write_finds_pid_file_with_non_existent_pid(self):
        def test_func(conn: Connection) -> None:
            # Write out a pid file with a pid that does not map to a running process.
            non_existant_pid = self.__find_available_pid_remote(conn)
            result = conn.run(f"echo {non_existant_pid} > {REMOTE_PATH}")
            if not result.ok:
                self.fail(f"writing test pid file; result={result}")

            pid_file = self.__get_pid_file_remote(conn)
            wrote_pid = pid_file.write()
            self.assertTrue(wrote_pid)
            self.__validate_remote_pid_file(
                conn=conn,
                file_should_exist=True,
                expected_pid=PID,
            )

        self.__exec_remote_test(test_func)

    #
    # Helpers ------------------------------------------------------------------
    #
    # Local --------------------------------------------------------------------
    def __get_pid_file_local(self, pid: int | None = None) -> Tuple[PidFile, str]:
        path = os.path.join(self.test_configs["pid_dir"], FILE_NAME)
        pid = PID if pid is not None else os.getpid()
        return (
            PidFileLocal(
                logger=logger,
                path=path,
                pid=pid,
            ),
            path,
        )

    def __write_test_pid_local(self, pid: int | None = None, empty: bool = False):
        path = os.path.join(self.test_configs["pid_dir"], FILE_NAME)
        if empty:
            file_path = Path(path)
            file_path.touch()
            return

        if pid:
            with open(path, "w") as fh:
                fh.write(str(pid))
                fh.flush()
        else:
            Path(path).touch()
        return path

    def __validate_local_pid_file(
        self, file_should_exist: bool, expected_path: str, expected_pid: int | None = None
    ) -> None:
        p = Path(expected_path)
        is_file = p.is_file()
        if file_should_exist:
            self.assertTrue(is_file, "expected pid file did not exist")
            with p.open(mode="r") as fh:
                actual_pid = fh.read().strip()
                self.assertEqual(
                    expected_pid, int(actual_pid), "expected pid was not in the pid file"
                )
            return

        self.assertFalse(is_file, "did not expect a pid file but it was there")

    def __find_available_pid_local(self) -> int:
        while True:
            retval = random.randint(2, 32000)
            if not psutil.pid_exists(retval):
                return retval

        return -1

    # Remote -------------------------------------------------------------------
    def __find_available_pid_remote(self, conn: Connection) -> int:
        while True:
            retval = random.randint(2, 32000)
            result = conn.run(f"ps --pid {retval}", warn=True, hide=False)
            if not result.ok:
                return retval

        return -1

    def __get_pid_file_remote(self, conn: Connection, pid: int | None = None) -> PidFile:
        # FIXME: broken logic here
        pid = PID if pid is None else os.getpid()
        return PidFileRemote(
            logger=logger,
            path=REMOTE_PATH,
            pid=pid,
            conn=conn,
        )

    def __write_test_pid_remote(self, conn: Connection, pid: int | None = None):
        result = conn.run(f"echo {pid} > {REMOTE_PATH}")
        if not result.ok:
            self.fail(f"writing test pid file; result={result}")

    def __exec_remote_test(self, test_func: ExecRemoteFunc) -> None:
        conn = IntegrationTestUtils.get_test_docker_conn(self.test_configs, ContainerType.REMOTE)
        try:
            test_func(conn)
        finally:
            if conn is not None:
                conn.close()

    def __validate_remote_pid_file(
        self,
        conn: Connection,
        file_should_exist: bool,
        expected_pid: int | None = None,
    ) -> None:
        result = conn.run(f"cat {REMOTE_PATH}", warn=True, hide=True)
        if file_should_exist:
            if not result.ok:
                self.fail(
                    f"expected remote file did not exist; REMOTE_PATH={REMOTE_PATH}, result={result}"
                )
                return
            actual_pid = result.stdout
            actual_pid = actual_pid.strip()
            self.assertEqual(expected_pid, int(actual_pid))
            return

        if not file_should_exist:
            self.assertFalse(result.ok)
