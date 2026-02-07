# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import os
import shutil
import unittest
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Callable, List, Set, Tuple

import psutil

from rsyncdirector.integration_tests.int_test_utils import ContainerType, IntegrationTestUtils
from rsyncdirector.integration_tests.metrics_scraper import MetricsScraper, MetricsScraperCfg
from rsyncdirector.lib import logging
from rsyncdirector.lib.config import JobType
from rsyncdirector.lib.logging import Logger

DOCKER_SSH_WAIT_TIME = 1


@dataclass
class ExpectedFile:
    path: str
    bytes: int


class ExpectedDir(object):
    def __init__(
        self, path: str, files: List[ExpectedFile] | None = None, dirs: List[str] | None = None
    ) -> None:
        self.path = path
        self.files = files
        self.dirs = dirs


class ExpectedData(object):
    def __init__(self, job_type: JobType):
        self.job_type = job_type
        self.files: List[ExpectedFile] = []
        self.dirs: List[ExpectedDir] = []


class ITBase(unittest.TestCase):

    def setUp(self):
        self.logger = logging.get_logger(
            name="rsyncdirector-inttest",
            log_level="INFO",
            const_kvs=dict(process="rsyncdirector-inttest"),
            cache_logger=False,
            force_reconfig=True,
        )
        self.logger.info("Running setup")
        self.setup_base()
        IntegrationTestUtils.restart_docker_containers(self.logger, self.test_configs)

        metrics_scraper_cfg = MetricsScraperCfg(
            addr=self.test_configs["metrics_scraper_target_addr"],
            port=self.test_configs["metrics_scraper_target_port"],
            scrape_interval=timedelta(seconds=0.25),
            conn_timeout=timedelta(seconds=1),
        )
        self.metrics_scraper = MetricsScraper(metrics_scraper_cfg, self.logger)
        self.metrics_scraper.start()

    def get_default_test_data(self, job_type: JobType) -> Tuple[ExpectedData, str]:
        expected_data = ExpectedData(job_type)
        source_data_dir = os.path.join(self.test_configs["test_data_dir"], "d1")
        f1_bytes = IntegrationTestUtils.create_test_file(source_data_dir, "f1.txt", 256)
        f1_expected_path = None

        match job_type:
            case JobType.LOCAL:
                f1_expected_path = os.path.join(
                    os.path.sep, self.test_configs["test_local_sync_target_dir"], "d1", "f1.txt"
                )
            case JobType.REMOTE:
                f1_expected_path = os.path.join("/data/d1/", "f1.txt")
            case _:
                self.fail(f"unknown JobType; job_type={job_type}")

        expected_data.files.append(ExpectedFile(path=f1_expected_path, bytes=f1_bytes))
        return expected_data, source_data_dir

    def setup_base(self) -> None:
        self.logger.info("Running setup_base")
        self.test_configs = IntegrationTestUtils.get_test_configs(self.logger)
        self.waitfor_timeout_seconds = float(self.test_configs["waitfor_timeout_seconds"])
        self.waitfor_poll_interval = float(self.test_configs["waitfor_poll_interval"])

        # Clean any test dirs if they exist and then recreate them.
        test_dirs = [
            self.test_configs["config_dir"],
            self.test_configs["lock_dir"],
            self.test_configs["block_dir"],
            self.test_configs["pid_dir"],
            self.test_configs["test_data_dir"],
            self.test_configs["test_local_sync_target_dir"],
        ]
        for d in test_dirs:
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d, exist_ok=True)

        # Ensure that there isn't a dangling test process already listening on that port
        self.kill_process_listening_on_port(int(self.test_configs["metrics_scraper_target_port"]))

    def kill_process_listening_on_port(self, port: int) -> None:
        found_process = False
        for conn in psutil.net_connections(kind="inet"):
            if conn.laddr and conn.laddr.port == port:
                pid = conn.pid
                if pid:
                    found_process = True
                    try:
                        process = psutil.Process(pid)
                        self.logger.info(
                            "Terminating existing process listing on the given port",
                            process_name=process.name(),
                            pid=pid,
                            port=port,
                        )
                        process.kill()
                        process.wait(timeout=3)
                        self.logger.info("process terminated", process_name=process.name(), pid=pid)
                    except psutil.NoSuchProcess as e:
                        self.logger.info("process is no longer extant", pid=pid, error=e)
                        return None
                    except Exception as e:
                        self.fail(
                            f"error terminating process; process.name={process.name()}, pid={pid}, port={port}"
                        )
                else:
                    break

        if not found_process:
            self.logger.info("no process found listening on port", port=port)

    GetFileSize = Callable[[ExpectedFile], Tuple[bool, int]]
    ListSubDirs = Callable[[str], List[str]]
    FindFiles = Callable[[str], List[str]]

    def validate_post_conditions(self, expected_data: ExpectedData) -> None:
        self.logger.info("Validating post conditions")

        match expected_data.job_type:
            case JobType.LOCAL:
                self.validate_local_post_conditions(expected_data)
            case JobType.REMOTE:
                self.validate_remote_post_conditions(expected_data)
            case _:
                self.fail(f"unknown JobType; job_type={expected_data.job_type}")

    def validate_local_post_conditions(self, expected_data: ExpectedData) -> None:
        def get_file_size(expected_file: ExpectedFile) -> Tuple[bool, int]:
            try:
                retval = os.path.getsize(expected_file.path)
                return True, retval
            except Exception as e:
                return False, 0

        def list_sub_dirs(path: str) -> List[str]:
            p = Path(path)
            retval = [entry.name for entry in p.iterdir() if entry.is_dir()]
            return retval

        def find_files(path: str) -> List[str]:
            p = Path(path)
            retval = [entry.name for entry in p.iterdir() if entry.is_file()]
            return retval

        self.validate_expected_dirs(expected_data.dirs, list_sub_dirs, find_files)
        self.validate_expected_files(expected_data.files, get_file_size)

    def validate_remote_post_conditions(self, expected_data: ExpectedData) -> None:
        conn = None
        try:

            def run_cmd(cmd: str) -> List[str]:
                retval: List[str] = []
                if conn:
                    result = conn.run(cmd, warn=True, hide=True)
                    if result.ok:
                        for line in result.stdout.strip().split("\n"):
                            if line != "":
                                retval.append(line)
                    else:
                        self.fail(f"error running command; cmd={cmd}, result={result}")
                return retval

            def get_file_size(expected_file: ExpectedFile) -> Tuple[bool, int]:
                if conn:
                    result = conn.run(f"stat -c '%s' {expected_file.path}", warn=True, hide=True)
                    if result.ok:
                        return True, int(result.stdout.strip())
                return False, 0

            def list_sub_dirs(path: str) -> List[str]:
                return run_cmd(f"find {path} -maxdepth 1 -type d")

            def find_files(path: str) -> List[str]:
                return run_cmd(f"find {path} -maxdepth 1 -type f")

            conn = IntegrationTestUtils.get_test_docker_conn(
                self.test_configs, ContainerType.TARGET
            )

            self.validate_expected_dirs(expected_data.dirs, list_sub_dirs, find_files)
            self.validate_expected_files(expected_data.files, get_file_size)
        finally:
            if conn is not None:
                conn.close()

    def validate_expected_files(
        self,
        expected_files: List[ExpectedFile],
        get_file_size: GetFileSize,
    ) -> None:
        for expected_file in expected_files:
            # Get the size from an expected path
            found_file, actual_bytes = get_file_size(expected_file)
            if found_file == True:
                self.assertEqual(
                    expected_file.bytes,
                    actual_bytes,
                    f"expected_file.path={expected_file.path} expected_file.bytes={expected_file.bytes} != actual_bytes={actual_bytes}",
                )
            else:
                self.fail(f"expected_file.path={expected_file.path} was not found")

    def validate_expected_dirs(
        self,
        expected_dirs: List[ExpectedDir],
        list_sub_dirs: ListSubDirs,
        find_files: FindFiles,
    ) -> None:
        for expected_dir in expected_dirs:
            sub_dirs = list_sub_dirs(expected_dir.path)
            actual_sub_dirs: Set[str] = set()
            for dir in sub_dirs:
                # Don't include the parent directory in the results
                if expected_dir.path == dir:
                    continue
                tokens = dir.split(os.path.sep)
                actual_sub_dirs.add(tokens[-1])

            expected_sub_dir_num = len(expected_dir.dirs) if expected_dir.dirs is not None else 0
            self.assertEqual(expected_sub_dir_num, len(actual_sub_dirs))

            if expected_dir.dirs is not None:
                for expected_sub_dir in expected_dir.dirs:
                    if expected_sub_dir not in actual_sub_dirs:
                        self.fail(
                            f"expected_sub_dir was not present in dir; expected_sub_dir={expected_sub_dir}, dir={expected_dir.path}"
                        )
                    actual_sub_dirs.remove(expected_sub_dir)

            self.assertEqual(
                0,
                len(actual_sub_dirs),
                f"found actual dir(s) that we did not expect; actual_dirs={actual_sub_dirs}",
            )

            actual_files = find_files(expected_dir.path)
            expected_files_num = len(expected_dir.files) if expected_dir.files is not None else 0
            self.assertEqual(
                expected_files_num,
                len(actual_files),
                f"expected number of files not present in dir; dir={expected_dir.path}, "
                f"expected_files_num={expected_dir.files}, len(actual_files)={len(actual_files)}, "
                f"actual_files={actual_files}",
            )
