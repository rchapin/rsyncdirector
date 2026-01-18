# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import logging
import os
import stat
import sys
import rsyncdirector.lib.config as cfg
from datetime import timedelta
from rsyncdirector.integration_tests.it_base import ITBase, ExpectedData, ExpectedDir, ExpectedFile
from rsyncdirector.integration_tests.int_test_utils import (
    IntegrationTestUtils,
    ContainerType,
    Job,
    LockFile,
    RemoteLockFile,
    BlocksOn,
    BlocksOnRemote,
    SyncAction,
    CommandAction,
    RSYNC_ID_DEFAULT,
)
from rsyncdirector.integration_tests.metrics_scraper import (
    Metric,
    MetricsScraper,
    MetricsScraperCfg,
    MetricsConditions,
    WaitFor,
)
from rsyncdirector.lib.rsyncdirector import RsyncDirector
from rsyncdirector.lib.config import JobType
from typing import Sequence

logging.basicConfig(
    format="%(asctime)s,%(levelname)s,%(module)s,%(message)s", level=logging.INFO, stream=sys.stdout
)
logger = logging.getLogger(__name__)

RUNONCE_ENV_VAR_KEY = f"{cfg.ENV_VAR_PREFIX}_{cfg.ENV_VAR_RUNONCE}"
RUNONCE_ENV_VAR = {RUNONCE_ENV_VAR_KEY: "1"}

TEST_SHELL_SCRIPT = """#!/bin/bash
df
ls -al /var/tmp/
"""


class ITRsyncDirector(ITBase):

    def setUp(self):
        logger.info("Running setup")
        self.setup_base()
        IntegrationTestUtils.restart_docker_containers(self.test_configs)

        metrics_scraper_cfg = MetricsScraperCfg(
            addr=self.test_configs["metrics_scraper_target_addr"],
            port=self.test_configs["metrics_scraper_target_port"],
            scrape_interval=timedelta(seconds=0.25),
            conn_timeout=timedelta(seconds=1),
        )
        self.metrics_scraper = MetricsScraper(metrics_scraper_cfg, logger)
        self.metrics_scraper.start()

    def tearDown(self):
        self.metrics_scraper.shutdown()
        IntegrationTestUtils.stop_docker_containers(self.test_configs)

    def run_rsyncdirector(self, override_configs: dict[str, str] | None = None) -> RsyncDirector:
        config_path = os.path.join(
            self.test_configs["config_dir"], self.test_configs["config_file"]
        )
        os.environ[cfg.CONFIG_ENV_VAR_KEY] = config_path
        if override_configs is not None:
            for k, v in override_configs.items():
                os.environ[k] = v

        logger.info("Starting RsyncDirector")
        rsyncdirector = RsyncDirector(logger)
        rsyncdirector.start()

        # Unset any envs so that we do not pollute any following tests
        if override_configs is not None:
            for k in override_configs:
                os.environ.pop(k, None)

        return rsyncdirector

    def test_will_shutdown_when_finding_an_existing_pid_file_that_is_not_our_pid(self):
        """
        Tests that we will shutdown without executing any commands when we find an existing pid file
        that does not contain our pid.
        """
        job_type = JobType.REMOTE
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)

        # Write out the pid file with a pid that will not be the same as the running process.
        pid_file_name = f"rsyncdirector-{RSYNC_ID_DEFAULT}.pid"
        pid_file_path = os.path.join(self.test_configs["pid_dir"], pid_file_name)
        with open(pid_file_path, "w") as fh:
            fh.write("1")

        # Generate some test data that we will include in the configs but expect NOT to be rsynced.
        d1 = os.path.join(self.test_configs["test_data_dir"], "d1")
        IntegrationTestUtils.create_test_file(d1, "f1.txt", 256)

        # We don't expect anything to have been written into the /data dir so we define ExpectedData
        # specifying the path of the directory we expect to be empty.
        expected_data = ExpectedData(job_type)
        expected_data.dirs.append(ExpectedDir(path=os.path.join(os.path.sep, "data")))

        app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
        job = app_configs.jobs[0]
        job.actions = [
            SyncAction(
                action="sync",
                id=f"sync-of-{d1}",
                source=d1,
                dest="/data",
                opts=["-av", "--delete"],
            )
        ]

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)

        rsyncdirector = self.run_rsyncdirector()
        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_sync_to_remote_no_blocks(self):
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)

        # Generate some test data that we will rsync to the docker container, and build a dict that
        # stores paths and sizes of the files.
        job_type = JobType.REMOTE
        expected_data = ExpectedData(job_type)

        d1 = os.path.join(self.test_configs["test_data_dir"], "d1")
        f1_bytes = IntegrationTestUtils.create_test_file(d1, "f1.txt", 256)
        f1_expected_path = os.path.join("/data/d1/", "f1.txt")
        expected_data.files.append(ExpectedFile(path=f1_expected_path, bytes=f1_bytes))

        d2 = os.path.join(self.test_configs["test_data_dir"], "d1", "d2")
        f2_bytes = IntegrationTestUtils.create_test_file(d2, "f2.txt", 1024)
        f2_expected_path = os.path.join("/data/d1/d2", "f2.txt")
        expected_data.files.append(ExpectedFile(path=f2_expected_path, bytes=f2_bytes))

        app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
        job = app_configs.jobs[0]
        job.actions = [
            SyncAction(
                action="sync",
                id=f"sync-of-{d1}",
                source=d1,
                dest="/data",
                opts=["-av", "--delete"],
            ),
        ]

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)

        rsyncdirector = self.run_rsyncdirector()
        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_sync_to_local_no_blocks(self):
        job_type = JobType.LOCAL
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)
        expected_data, source_data_dir = self.get_default_test_data(job_type)

        app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
        job = app_configs.jobs[0]
        job.actions = [
            SyncAction(
                action="sync",
                id=f"sync-of{source_data_dir}",
                source=source_data_dir,
                dest=self.test_configs["test_local_sync_target_dir"],
                opts=["-av", "--delete"],
            )
        ]

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)

        rsyncdirector = self.run_rsyncdirector()
        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_waits_for_remote_block_condition_on_another_host(self):
        """
        Test that it will wait for a remote block condition on a host other than the host to which
        we are syncing data.
        """
        job_type = JobType.REMOTE
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)

        # Generate some test data that we will rsync to the docker container, and build a dict that
        # stores paths and sizes of the files.
        expected_data = ExpectedData(job_type)

        d1 = os.path.join(self.test_configs["test_data_dir"], "d1")
        f1_bytes = IntegrationTestUtils.create_test_file(d1, "f1.txt", 256)
        f1_expected_path = os.path.join("/data/d1/", "f1.txt")
        expected_data.files.append(ExpectedFile(path=f1_expected_path, bytes=f1_bytes))

        remote_block_file_path = os.path.join(os.path.sep, "var", "tmp", "blockfile")
        conn = None
        try:
            conn = IntegrationTestUtils.get_test_docker_conn(
                self.test_configs, ContainerType.REMOTE
            )
            result = conn.run(f'echo "647" > {remote_block_file_path}', warn=True, hide=True)
            if not result.ok:
                self.fail("unable to create test lockfile on remote test host")

            app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
            job = app_configs.jobs[0]
            job.actions = [
                SyncAction(
                    action="sync",
                    id="sync-of-{d1}",
                    source=d1,
                    dest="/data",
                    opts=["-av", "--delete"],
                ),
            ]
            job.blocks_on = [
                BlocksOnRemote(
                    type="remote",
                    path=remote_block_file_path,
                    wait_time=1,
                    user="root",
                    host="localhost",
                    port=self.test_configs["container_remote_port"],
                    private_key_path=self.test_configs["ssh_identity_file"],
                ),
            ]

            IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
            rsyncdirector = self.run_rsyncdirector()

            metrics_conditions = MetricsConditions(
                metrics=[
                    Metric(name="blocked_total", labels={"job_id": "local_to_container"}, value=1.0)
                ]
            )
            WaitFor.metrics(
                logger=logger,
                metrics_scraper=self.metrics_scraper,
                conditions=metrics_conditions,
                timeout=timedelta(seconds=self.waitfor_timeout_seconds),
                poll_interval=timedelta(seconds=self.waitfor_poll_interval),
            )
            # Once we see that we have been blocked at least once, remove the lock file and the
            # rsyncdirector should continue executing the job.
            result = conn.run(f"rm {remote_block_file_path}", warn=True, hide=True)
            if not result.ok:
                self.fail("unable to remove test lockfile on remote test host")

        except Exception as e:
            self.fail(f"error occurred setting up block on remote host; e={e}")
        finally:
            if conn is not None:
                conn.close()

        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_waits_for_local_block_condition(self):
        """
        Test sync blocking on a local lock file until it is removed.
        """
        job_type = JobType.REMOTE
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)
        expected_data, source_data_dir = self.get_default_test_data(job_type)

        # Create a lock file that we should wait until it has been removed before syncing the data.
        local_block_file_path = os.path.join(
            os.path.sep, self.test_configs["block_dir"], "blockfile"
        )
        with open(local_block_file_path, "w") as fh:
            fh.write("647")

        app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
        job = app_configs.jobs[0]
        job.actions = [
            SyncAction(
                action="sync",
                id=f"sync-of-{source_data_dir}",
                source=source_data_dir,
                dest="/data",
                opts=["-av", "--delete"],
            )
        ]
        job.blocks_on = [
            BlocksOn(
                type="local",
                path=local_block_file_path,
                wait_time=1,
            ),
        ]

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
        rsyncdirector = self.run_rsyncdirector()

        metrics_conditions = MetricsConditions(
            metrics=[
                Metric(name="blocked_total", labels={"job_id": "local_to_container"}, value=1.0)
            ]
        )
        WaitFor.metrics(
            logger=logger,
            metrics_scraper=self.metrics_scraper,
            conditions=metrics_conditions,
            timeout=timedelta(seconds=self.waitfor_timeout_seconds),
            poll_interval=timedelta(seconds=self.waitfor_poll_interval),
        )
        # Once we see that we have been blocked at least once, remove the lock file and the
        # rsyncdirector should continue executing the job.
        logger.info(f"removing local block file; local_block_file_path={local_block_file_path}")
        os.remove(local_block_file_path)

        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_waits_for_multiple_block_conditions(self):
        job_type = JobType.REMOTE
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)
        expected_data, source_data_dir = self.get_default_test_data(job_type)
        conn = None

        try:
            # Create a lock file on the localhost and on the remote host to which we will sync data that
            # we should wait until they have BOTH been removed before syncing the data.
            existing_pid = "647"
            local_block_file_path = os.path.join(
                os.path.sep, self.test_configs["block_dir"], "blockfile"
            )
            with open(local_block_file_path, "w") as fh:
                fh.write(existing_pid)

            remote_block_file_path = os.path.join(os.path.sep, "var", "tmp", "blockfile")
            conn = IntegrationTestUtils.get_test_docker_conn(
                self.test_configs, ContainerType.REMOTE
            )
            result = conn.run(
                f"echo {existing_pid} > {remote_block_file_path}", warn=True, hide=True
            )
            if not result.ok:
                self.fail("unable to create test lockfile on remote test host")

            job_id = "local_to_container_multiple_blocks"
            app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
            job = app_configs.jobs[0]
            job.id = job_id
            job.actions = [
                SyncAction(
                    action="sync",
                    id=f"sync of {source_data_dir}",
                    source=source_data_dir,
                    dest="/data",
                    opts=["-av", "--delete"],
                )
            ]
            job.blocks_on = [
                BlocksOnRemote(
                    type="remote",
                    path=remote_block_file_path,
                    wait_time=1,
                    user="root",
                    host="localhost",
                    port=self.test_configs["container_remote_port"],
                    private_key_path=self.test_configs["ssh_identity_file"],
                ),
                BlocksOn(
                    path=local_block_file_path,
                    type="local",
                    wait_time=1,
                ),
            ]

            IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
            rsyncdirector = self.run_rsyncdirector()

            # Once we see that we have been blocked at least once, remove the remote lock file and then
            # we should block on the local lock file.
            metrics_conditions = MetricsConditions(
                metrics=[Metric(name="blocked_total", labels={"job_id": job_id}, value=1.0)]
            )
            WaitFor.metrics(
                logger=logger,
                metrics_scraper=self.metrics_scraper,
                conditions=metrics_conditions,
                timeout=timedelta(seconds=self.waitfor_timeout_seconds),
                poll_interval=timedelta(seconds=self.waitfor_poll_interval),
            )
            logger.info(
                f"removing remote block file; remote_block_file_path={remote_block_file_path}"
            )
            result = conn.run(f"rm {remote_block_file_path}", warn=True, hide=True)
            if not result.ok:
                self.fail("unable to remove test lockfile on remote test host")

            # Once we see that we have been blocked once more, remove the lock file and the
            # rsyncdirector should continue executing the job.
            metrics_conditions = MetricsConditions(
                metrics=[Metric(name="blocked_total", labels={"job_id": job_id}, value=2.0)]
            )
            WaitFor.metrics(
                logger=logger,
                metrics_scraper=self.metrics_scraper,
                conditions=metrics_conditions,
                timeout=timedelta(seconds=self.waitfor_timeout_seconds),
                poll_interval=timedelta(seconds=self.waitfor_poll_interval),
            )
            logger.info(f"removing local block file; local_block_file_path={local_block_file_path}")
            os.remove(local_block_file_path)

        except Exception as e:
            self.fail(f"error occurred setting up block on remote host; e={e}")
        finally:
            if conn is not None:
                conn.close()

        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_creates_local_and_remote_lock_files(self):
        job_type = JobType.REMOTE
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)

        expected_data = ExpectedData(job_type)
        source_data_dir = os.path.join(self.test_configs["test_data_dir"], "d1")
        f1_bytes = IntegrationTestUtils.create_test_file(source_data_dir, "f1.txt", 1024 * 1024)
        f1_expected_path = os.path.join("/data/d1/", "f1.txt")
        expected_data.files.append(ExpectedFile(path=f1_expected_path, bytes=f1_bytes))

        conn = None
        try:
            conn = IntegrationTestUtils.get_test_docker_conn(
                self.test_configs, ContainerType.REMOTE
            )

            # Create an app config that will result in the creation of a local and a remote lock
            # file. We create a "large" test file that we will sync and then throttle the bandwidth
            # to give us a chance to validate the lock files before the sync finishes.  This test
            # could be considered a bit of a race condition, but this is the best and least
            # complicated way that I figured out how to go about it.
            existing_pid = "647"
            local_block_file_path = os.path.join(
                os.path.sep, self.test_configs["block_dir"], "blockfile"
            )

            local_lock_file_path = os.path.join(
                os.path.sep, self.test_configs["lock_dir"], "lockfile"
            )
            remote_lock_file_path = os.path.join(os.path.sep, "var", "tmp", "lockfile")
            app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
            job = app_configs.jobs[0]
            job.id = "local_to_container_multi_lock_files"
            job.lock_files = [
                LockFile(
                    type="local",
                    path=local_lock_file_path,
                ),
                RemoteLockFile(
                    type="remote",
                    path=remote_lock_file_path,
                    user="root",
                    host="localhost",
                    port=self.test_configs["container_remote_port"],
                    private_key_path=self.test_configs["ssh_identity_file"],
                ),
            ]
            job.actions = [
                SyncAction(
                    action="sync",
                    id=f"sync-of-{source_data_dir}",
                    source=source_data_dir,
                    dest="/data",
                    opts=["-av", "--delete", "--bwlimit=2"],
                )
            ]
            job.blocks_on = [
                BlocksOn(
                    type="local",
                    path=local_block_file_path,
                    wait_time=1,
                ),
            ]

            IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
            rsyncdirector = self.run_rsyncdirector()

            # Wait until we see metrics that indicate that we have written two lock files.
            metrics_conditions = MetricsConditions(
                metrics=[
                    Metric(
                        name="lock_files",
                        labels={"job_id": "local_to_container_multi_lock_files"},
                        value=2.0,
                    )
                ]
            )
            WaitFor.metrics(
                logger=logger,
                metrics_scraper=self.metrics_scraper,
                conditions=metrics_conditions,
                timeout=timedelta(seconds=self.waitfor_timeout_seconds),
                poll_interval=timedelta(seconds=self.waitfor_poll_interval),
            )
            logger.info("verified metrics for two lock files")

            # Confirm that both of the lock files exist
            our_pid = os.getpid()
            with open(local_lock_file_path, "r") as fh:
                local_lock_file_pid = fh.read().strip()
                self.assertEqual(our_pid, int(local_lock_file_pid))

            result = conn.run(f"cat {remote_lock_file_path}", warn=True, hide=True)
            if not result.ok:
                self.fail("unable to create test lockfile on remote test host")
            remote_lock_file_pid = result.stdout
            remote_lock_file_pid = remote_lock_file_pid.strip()
            self.assertEqual(our_pid, int(remote_lock_file_pid))

        except Exception as e:
            self.fail(f"error occurred setting up block on remote host; e={e}")
        finally:
            if conn is not None:
                conn.close()

        rsyncdirector.shutdown()
        rsyncdirector.join(timeout=5.0)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_runs_a_scheduled_job(self):
        job_id = "local_to_local_runs_via_schedule"
        job_type = JobType.LOCAL
        expected_data, source_data_dir = self.get_default_test_data(job_type)

        # Create some more test data in additional directories
        source_data_dir_d2 = os.path.join(source_data_dir, "d2")
        source_data_dir_d3 = os.path.join(source_data_dir_d2, "d3")
        f2_bytes = IntegrationTestUtils.create_test_file(source_data_dir_d3, "f2.txt", 647)
        f2_expected_path = os.path.join(
            os.path.sep, self.test_configs["test_local_sync_target_dir"], "d1", "d2", "d3", "f2.txt"
        )
        expected_data.files.append(ExpectedFile(path=f2_expected_path, bytes=f2_bytes))

        rsync_id = "scheduled_test"
        app_configs = IntegrationTestUtils.get_app_configs(
            test_configs=self.test_configs,
            job_type=job_type,
            rsync_id=rsync_id,
        )
        job = app_configs.jobs[0]
        job.id = job_id
        job.actions = [
            SyncAction(
                action="sync",
                id=f"sync-of-{source_data_dir}",
                source=source_data_dir,
                dest=self.test_configs["test_local_sync_target_dir"],
                opts=["-av", "--delete"],
            )
        ]

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
        rsyncdirector = self.run_rsyncdirector()

        metrics_conditions = MetricsConditions(
            metrics=[Metric(name="runs_completed_total", labels={"rsync_id": rsync_id}, value=1.0)]
        )
        # Wait for 90 seconds because the cron should run within 60.
        WaitFor.metrics(
            logger=logger,
            metrics_scraper=self.metrics_scraper,
            conditions=metrics_conditions,
            timeout=timedelta(seconds=90),
            poll_interval=timedelta(seconds=self.waitfor_poll_interval),
        )

        rsyncdirector.shutdown()
        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)

    def test_runs_multiple_jobs(self):
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)
        job_type = JobType.LOCAL
        expected_data = ExpectedData(job_type)

        source_data_dir_data1 = os.path.join(self.test_configs["test_data_dir"], "data1")
        f1_bytes = IntegrationTestUtils.create_test_file(source_data_dir_data1, "f1.txt", 728)
        f1_expected_path = os.path.join(
            self.test_configs["test_local_sync_target_dir"], "data1", "f1.txt"
        )
        expected_data.files.append(ExpectedFile(path=f1_expected_path, bytes=f1_bytes))

        source_data_dir_data2 = os.path.join(self.test_configs["test_data_dir"], "data2")
        f2_bytes = IntegrationTestUtils.create_test_file(source_data_dir_data2, "f2.txt", 963)
        f2_expected_path = os.path.join(
            self.test_configs["test_local_sync_target_dir"], "data2", "f2.txt"
        )
        expected_data.files.append(ExpectedFile(path=f2_expected_path, bytes=f2_bytes))

        rsync_id = "multi_job_test"
        job_id_1 = "job_id_1"
        app_configs = IntegrationTestUtils.get_app_configs(
            test_configs=self.test_configs,
            job_type=job_type,
            rsync_id=rsync_id,
        )
        job1 = app_configs.jobs[0]
        job1.id = job_id_1
        job1.actions = [
            SyncAction(
                action="sync",
                id=f"sync-of-{source_data_dir_data1}",
                source=source_data_dir_data1,
                dest=self.test_configs["test_local_sync_target_dir"],
                opts=["-av", "--delete"],
            ),
        ]
        job_id_2 = "job_id_2"
        app_configs.jobs = list(app_configs.jobs)
        app_configs.jobs.append(
            Job(
                id=job_id_2,
                type="local",
                lock_files=[
                    LockFile(
                        type="local",
                        path=os.path.join(self.test_configs["lock_dir"], "lock_file_2"),
                    ),
                ],
                actions=[
                    SyncAction(
                        action="sync",
                        id=f"sync-of-{source_data_dir_data2}",
                        source=source_data_dir_data2,
                        dest=self.test_configs["test_local_sync_target_dir"],
                        opts=["-av", "--delete"],
                    ),
                ],
                blocks_on=None,
            )
        )

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
        rsyncdirector = self.run_rsyncdirector()
        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_interruptable_wait(self):
        """
        Tests that we can interrupt "sleeps" if/when we need to shutdown.  Run with a configuration
        that honors a block file that we will never remove and then confirm that we can shutdown and
        exit the "sleep".  We will use the same underlying implementation to "sleep" in the rest of
        the code and this is just one code path that execercises the interruptable sleep
        implementation.
        """
        job_type = JobType.REMOTE
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)
        _, source_data_dir = self.get_default_test_data(job_type)

        remote_block_file_path = os.path.join(os.path.sep, "var", "tmp", "blockfile")
        conn = None
        try:
            conn = IntegrationTestUtils.get_test_docker_conn(
                self.test_configs, ContainerType.REMOTE
            )
            result = conn.run(f'echo "647" > {remote_block_file_path}', warn=True, hide=True)
            if not result.ok:
                self.fail("unable to create test block file on remote test host")

            app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
            job = app_configs.jobs[0]
            job.id = "interruptable_wait"
            job.actions = [
                SyncAction(
                    action="sync",
                    id=f"sync-of-{source_data_dir}",
                    source=source_data_dir,
                    dest="/data",
                    opts=["-av", "--delete"],
                ),
            ]
            # Block and wait without a timeout.
            job.blocks_on = [
                BlocksOnRemote(
                    type="remote",
                    path=remote_block_file_path,
                    wait_time=5,
                    user="root",
                    host="localhost",
                    port=self.test_configs["container_remote_port"],
                    private_key_path=self.test_configs["ssh_identity_file"],
                ),
            ]

            IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
            rsyncdirector = self.run_rsyncdirector()

            metrics_conditions = MetricsConditions(
                metrics=[
                    Metric(name="blocked_total", labels={"job_id": "interruptable_wait"}, value=1.0)
                ]
            )
            WaitFor.metrics(
                logger=logger,
                metrics_scraper=self.metrics_scraper,
                conditions=metrics_conditions,
                timeout=timedelta(seconds=self.waitfor_timeout_seconds),
                poll_interval=timedelta(seconds=self.waitfor_poll_interval),
            )
            # Once we see that we have been blocked at least once, call shutdown.  If it shuts down
            # the test will fall through and pass.
            rsyncdirector.shutdown()

        except Exception as e:
            self.fail(f"error occurred setting up block on remote host; e={e}")
        finally:
            if conn is not None:
                conn.close()

        rsyncdirector.join(timeout=5.0)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_when_block_times_out_it_exits_the_job(self):
        """
        Tests that when a blocks_on config times out that it exists the job for it to be
        rescheduled.
        """
        job_type = JobType.REMOTE
        _, source_data_dir = self.get_default_test_data(job_type)

        # We don't expect anything to have been written into the /data dir so we define ExpectedData
        # specifying the path of the directory we expect to be empty.
        expected_data = ExpectedData(job_type)
        expected_data.dirs.append(ExpectedDir(path=os.path.join(os.path.sep, "data")))

        remote_block_file_path = os.path.join(os.path.sep, "var", "tmp", "blockfile")
        conn = None
        try:
            conn = IntegrationTestUtils.get_test_docker_conn(
                self.test_configs, ContainerType.REMOTE
            )
            result = conn.run(f'echo "647" > {remote_block_file_path}', warn=True, hide=True)
            if not result.ok:
                self.fail("unable to create test block file on remote test host")

            job_id = "block_timeout"
            app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
            job = app_configs.jobs[0]
            job.id = job_id
            job.actions = [
                SyncAction(
                    action="sync",
                    id=f"sync-of-{source_data_dir}",
                    source=source_data_dir,
                    dest="/data",
                    opts=["-av", "--delete"],
                ),
            ]
            # Block and wait for 1s with a 3s timeout.
            job.blocks_on = [
                BlocksOnRemote(
                    type="remote",
                    path=remote_block_file_path,
                    wait_time=1,
                    user="root",
                    host="localhost",
                    port=self.test_configs["container_remote_port"],
                    private_key_path=self.test_configs["ssh_identity_file"],
                    timeout=4,
                ),
            ]

            IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
            rsyncdirector = self.run_rsyncdirector()

            metrics_conditions = MetricsConditions(
                metrics=[
                    Metric(
                        name="job_skipped_for_block_timeout_total",
                        labels={"job_id": job_id},
                        value=1.0,
                    )
                ]
            )
            # Wait for 90 seconds because the cron should run within 60.
            WaitFor.metrics(
                logger=logger,
                metrics_scraper=self.metrics_scraper,
                conditions=metrics_conditions,
                timeout=timedelta(seconds=90),
                poll_interval=timedelta(seconds=self.waitfor_poll_interval),
            )
            rsyncdirector.shutdown()

        except Exception as e:
            self.fail(f"error occurred setting up block on remote host; e={e}")
        finally:
            if conn is not None:
                conn.close()

        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)

    def test_block_and_lock_on_same_file(self):
        """
        Test sync blocking and locking on the same file.
        """
        job_type = JobType.REMOTE
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)
        expected_data, source_data_dir = self.get_default_test_data(job_type)

        # Create a lock file that we should wait until it has been removed before syncing the data.
        # We will start by writing a pid that cannot possiibly be owned by the current process.
        lock_file_path = os.path.join(os.path.sep, self.test_configs["lock_dir"], "lockfile")
        with open(lock_file_path, "w") as fh:
            fh.write("1")

        app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
        job_id = "locks_and_blocks_on_same_file"
        job = app_configs.jobs[0]
        job.id = job_id
        job.actions = [
            SyncAction(
                action="sync",
                id=f"sync-of-{source_data_dir}",
                source=source_data_dir,
                dest="/data",
                opts=["-av", "--delete"],
            ),
        ]
        job.lock_files = [
            LockFile(
                type="local",
                path=lock_file_path,
            ),
        ]
        job.blocks_on = [
            BlocksOn(
                type="local",
                path=lock_file_path,
                wait_time=1,
            ),
        ]

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)
        rsyncdirector = self.run_rsyncdirector()

        metrics_conditions = MetricsConditions(
            metrics=[Metric(name="blocked_total", labels={"job_id": job_id}, value=1.0)]
        )
        WaitFor.metrics(
            logger=logger,
            metrics_scraper=self.metrics_scraper,
            conditions=metrics_conditions,
            timeout=timedelta(seconds=self.waitfor_timeout_seconds),
            poll_interval=timedelta(seconds=self.waitfor_poll_interval),
        )
        # Once we see that we have been blocked at least once, remove the lock file and the
        # rsyncdirector should continue executing the job.
        logger.info(f"removing local block file; local_block_file_path={lock_file_path}")
        os.remove(lock_file_path)

        # Wait until we see metrics that indicate that we have written the lock file.
        metrics_conditions = MetricsConditions(
            metrics=[
                Metric(
                    name="lock_files",
                    labels={"job_id": job_id},
                    value=1.0,
                )
            ]
        )
        WaitFor.metrics(
            logger=logger,
            metrics_scraper=self.metrics_scraper,
            conditions=metrics_conditions,
            timeout=timedelta(seconds=self.waitfor_timeout_seconds),
            poll_interval=timedelta(seconds=self.waitfor_poll_interval),
        )

        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_run_sync_and_command_actions(self):
        """
        Tests that we can intersperse running commands with syncs and run commands of different
        types.
        """
        job_type = JobType.REMOTE
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)
        expected_data, source_data_dir = self.get_default_test_data(job_type)

        # A shell script that we will call that runs a few commands.
        test_shell_script_path = os.path.join(self.test_configs["config_dir"], "test.sh")
        with open(test_shell_script_path, "w") as fh:
            fh.write(TEST_SHELL_SCRIPT)
        current_mode = os.stat(test_shell_script_path).st_mode
        updated_mode = current_mode | stat.S_IXUSR
        os.chmod(test_shell_script_path, updated_mode)

        ssh_args = [
            "-p",
            self.test_configs["container_target_port"],
            "-i",
            self.test_configs["ssh_identity_file"],
            f"root@{self.test_configs["test_host"]}",
        ]
        mkdir_args = [*ssh_args, "mkdir", "/tarballs"]
        tar_args = [*ssh_args, "tar", "-czvf", "/tarballs/data.tar.gz", "/data"]

        app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
        job = app_configs.jobs[0]
        job.actions = [
            # First, execute a sync action to rsync data to the remote host.
            SyncAction(
                action="sync",
                id=f"sync-of-{source_data_dir}",
                source=source_data_dir,
                dest="/data",
                opts=["-av", "--delete"],
            ),
            # Run a command, calling a shell script with no arguments to test that we can omit the "args" key.
            CommandAction(
                action="command",
                id="df",
                command=test_shell_script_path,
            ),
            # Run a single command.
            CommandAction(
                action="command",
                id="df",
                command="df",
            ),
            # Then run a command to ssh to the host and create a tarball directory.
            CommandAction(
                action="command",
                id="create-remote-tarball-dir",
                command="ssh",
                args=mkdir_args,
            ),
            # Then run a command to ssh to the host and tar up the data directory into the newly
            # created tarball dir.
            CommandAction(
                action="command",
                id="create-remote-tarball",
                command="ssh",
                args=tar_args,
            ),
        ]

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)

        rsyncdirector = self.run_rsyncdirector()
        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)

        # Verify that the tarball was created in the expected new directory.
        conn = None
        try:
            conn = IntegrationTestUtils.get_test_docker_conn(
                self.test_configs, ContainerType.TARGET
            )
            cmd = "stat /tarballs/data.tar.gz"
            result = conn.run(cmd)
            if result.ok:
                for line in result.stdout.strip().split("\n"):
                    if "Size:" in line:
                        tokens = line.split()
                        self.assertTrue(len(tokens) >= 2)
                        self.assertEqual("Size:", tokens[0])
                        self.assertFalse("", tokens[1])
                        bytes = int(tokens[1])
                        self.assertTrue(bytes > 0)
            else:
                self.fail(f"error running command; cmd={cmd}, result={result}")
        finally:
            if conn:
                conn.close()

        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)

    def test_failed_action_aborts_job(self):
        job_type = JobType.LOCAL
        IntegrationTestUtils.set_env_vars(RUNONCE_ENV_VAR)
        dest_dir = self.test_configs["test_local_sync_target_dir"]
        _, source_data_dir = self.get_default_test_data(job_type)
        # We don't expect anything to have been written into the destination dir so we define
        # ExpectedData specifying the path of the directory we expect to be empty.
        expected_data = ExpectedData(job_type)
        expected_data.dirs.append(ExpectedDir(path=dest_dir))

        app_configs = IntegrationTestUtils.get_app_configs(self.test_configs, job_type)
        job = app_configs.jobs[0]
        job.actions = [
            CommandAction(
                action="command",
                id="bogus-command",
                command="completely-bogus-command",
                args=["this", "will", "error", "out"],
            ),
            SyncAction(
                action="sync",
                id=f"sync-of-{source_data_dir}",
                source=source_data_dir,
                dest=self.test_configs["test_local_sync_target_dir"],
                opts=["-av", "--delete"],
            ),
        ]

        IntegrationTestUtils.write_app_configs(self.test_configs, app_configs)

        rsyncdirector = self.run_rsyncdirector()
        rsyncdirector.join(timeout=5.0)
        self.validate_post_conditions(expected_data)
        IntegrationTestUtils.unset_env_vars(RUNONCE_ENV_VAR)
