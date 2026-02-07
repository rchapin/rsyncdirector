# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import multiprocessing
import os
import queue
import sys
import time
import traceback
from datetime import datetime, timedelta
from enum import Enum
from importlib.metadata import PackageNotFoundError, version
from threading import Event, Thread
from typing import Any, Dict, List, Tuple

from apscheduler import events
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fabric import Connection
from invoke import run

import rsyncdirector.lib.config as cfg
import rsyncdirector.lib.metrics as metrics
from rsyncdirector.lib.command import Command
from rsyncdirector.lib.config import BlocksOnType, LockFileType
from rsyncdirector.lib.enums import RunResult
from rsyncdirector.lib.logging import Logger
from rsyncdirector.lib.metrics import Metrics
from rsyncdirector.lib.pidfile import PidFileLocal, PidFileRemote
from rsyncdirector.lib.rsync import Rsync


class LockFileAction(Enum):
    DELETE = "delete"
    WRITE = "write"


class RsyncDirector(Thread):

    JOB_ID_RUNONCE = "runonce"

    METRICS_DEFAULT_ADDR = "0.0.0.0"
    METRICS_DEFAULT_PORT = "9090"
    METRICS_DEFAULT_STARTUP_TIMEOUT_SECONDS = float(10)
    METRICS_DEFAULT_STARTUP_RETRY_WAIT_SECONDS = float(2)
    METRICS_DEFAULT_STARTUP_NUM_RETRIES = 3

    PID_FILE_DIR_DEFAULT = "/etc/rsyncdirector"

    SUB_PROCESS_JOIN_SECONDS = float(5)

    def __init__(self, logger: Logger, env_vars: Dict):
        Thread.__init__(self)
        # Reference to the subprocess in which we will run the actual rsync command
        self.process = None
        self.metrics = None
        self.pid_file = None
        self.scheduled_job_running = False
        self.version = RsyncDirector.__get_app_version()

        """
        The shutdown_flag is a threading event object that indicates whether the thread should be
        terminated.
        """
        self.shutdown_flag = Event()
        # An event object to be used for interruptable "sleeping".
        self.wait_event = Event()

        if cfg.CONFIG_ENV_VAR_KEY not in env_vars:
            raise Exception(
                "Required env var defining path to config file is not defined; "
                f"export {cfg.CONFIG_ENV_VAR_KEY} pointing to path of a valid config file"
            )
        self.config_file_path = env_vars[cfg.CONFIG_ENV_VAR_KEY]
        self.dryrun = (
            True
            if cfg.DRYRUN_ENV_VAR_KEY in env_vars and env_vars[cfg.DRYRUN_ENV_VAR_KEY] == "1"
            else False
        )
        self.runonce = (
            True
            if cfg.RUNONCE_ENV_VAR_KEY in env_vars and env_vars[cfg.RUNONCE_ENV_VAR_KEY] == "1"
            else False
        )

        # TODO: validate configs
        self.configs = cfg.Config.load_configs(self.config_file_path)
        self.rsync_id = self.configs["rsync_id"]
        self.logger = logger.bind(rsync_id=self.rsync_id, version=self.version)

        self.pid = os.getpid()
        pid_filename = RsyncDirector.__get_pid_file_name(self.rsync_id)
        pid_file_dir = (
            self.configs["pid_file_dir"]
            if self.configs["pid_file_dir"]
            else RsyncDirector.PID_FILE_DIR_DEFAULT
        )
        pid_path = os.path.join(pid_file_dir, pid_filename)
        self.pid_file = PidFileLocal(logger=self.logger, pid=self.pid, path=pid_path)
        if not self.pid_file.write():
            self.logger.error("unable to write pidfile, shutting down", pid_path=pid_path)
            metrics.PID_FILE_ERR.inc()
            self.shutdown()
            return
        self.logger = self.logger.bind(pid=self.pid)

        self.scheduler = BackgroundScheduler()
        self.scheduler.add_listener(self.__event_listener)
        self.cron_schedule = self.configs["cron_schedule"]

    def __block(self, logger: Logger, job_id: str, blocks_on_conf: dict) -> bool:
        """
        Block execution of the job is there is a blocks_on configuration for the job and if the
        block condition is satisfied.

        If there is a blocks_on configuration for the job and there is a block file, this thread
        will spin-lock based on the wait and timeout configurations in the blocks_on configuration.
        If there is no timeout, it will block until the block indefinitely condition is removed.  If
        there is a timeout and the block condition is not removed it will return False, to indicate
        that the job is still blocked and we should not continue processing the job.

        Args:
            job_id (str): The job id for which we are blocking
            blocks_on_conf (dict): The blocks_on configs for the job

        Returns:
            bool: Whether or not we should continue processing the job, or if the job execution should be terminated.
        """
        for block in blocks_on_conf:
            # Only continue blocking if we do not get shutdown while attempting to process the
            # blocks.
            timeout_threshold = None
            timeout = None
            if "timeout" in block:
                timeout = timedelta(seconds=int(block["timeout"]))
                timeout_threshold = datetime.now() + timeout

            while True and self.is_shutdown() == False:
                # Get the type and see if we are blocked.
                blocks_on_type = BlocksOnType.get_enum_value_from_string(block["type"])
                logger = logger.bind(blocks_on_conf=block)
                blocked = False

                match blocks_on_type:
                    case BlocksOnType.LOCAL:
                        blocked = RsyncDirector.__is_blocked_local(block, logger)
                    case BlocksOnType.REMOTE:
                        blocked = RsyncDirector.__is_blocked_remote(block, logger)
                    case _:
                        logger.fatal("Unknown blocks_on type")
                        sys.exit(1)

                if not blocked:
                    logger.info("block has been satisfied")
                    break

                if blocked:
                    # Have we timedout?
                    if timeout_threshold and timeout:
                        timeout_delta = datetime.now() - timeout_threshold
                        if timeout_delta.total_seconds() > timeout.total_seconds():
                            logger.info("block has timedout")
                            return False

                    wait_time = block["wait_time"]
                    logger.info("job is blocked")
                    metrics.BLOCKED_COUNTER.labels(job_id).inc()
                    self.__interruptable_wait(wait_time)

        # Once all blocks have been satisfied or if there are no blocks configured just return True
        # so as not to block job processing.
        return True

    def __event_listener(self, event):
        def list_jobs():
            for job in self.scheduler.get_jobs():
                self.logger.info(
                    "scheduled for next run",
                    apscheduler_job_id=job.id,
                    next_run=job.next_run_time.isoformat(),
                )

        if self.runonce and event.code == events.EVENT_JOB_EXECUTED:
            self.logger.info("runonce job finished, exiting")
            if not self.is_shutdown():
                self.shutdown()
                return

        if event.code == events.EVENT_JOB_ADDED:
            self.logger.info("scheduler event: EVENT_JOB_ADDED")
            list_jobs()

        if event.code == events.EVENT_JOB_EXECUTED and event.job_id == RsyncDirector.JOB_ID_RUNONCE:
            self.logger.info("runonce job finished")
            list_jobs()

    def __exec_job(self):
        self.logger.info("exec_job starting")
        if self.scheduled_job_running:
            self.logger.warning("Unable to execute multiple jobs simultaneously, exiting exec_job")
            return
        self.scheduled_job_running = True

        for job in self.configs["jobs"]:
            if self.is_shutdown():
                self.logger.info("We have been shutdown, exiting jobs execution loop")
                break
            with metrics.JOB_DURATION.labels(job["id"]).time():
                self.__run_job(job)

        self.scheduled_job_running = False
        self.logger.info("exec_job finished")
        metrics.RUNS_COMPLETED.labels(self.rsync_id).inc()

    @staticmethod
    def __exec_process_command(
        logger: Logger, result_queue: multiprocessing.Queue, command: str, args: List[str]
    ) -> None:
        cmd = Command(logger, result_queue, command, args)
        cmd.run()

    @staticmethod
    def __exec_process_rsync(
        logger: Logger, result_queue: multiprocessing.Queue, job: Dict, sync: Dict
    ) -> None:
        user = job["user"] if "user" in job else None
        host = job["host"] if "host" in job else None
        port = job["port"] if "port" in job else None
        private_key_path = job["private_key_path"] if "private_key_path" in job else None

        # TODO: this can get removed once we validate and clean-up the configs on start-up.
        job_type = cfg.JobType[job["type"].upper()]
        rsync = Rsync(logger, result_queue, job_type, sync, user, host, port, private_key_path)
        rsync.run()

    @staticmethod
    def __get_connection(config: Dict) -> Connection:
        retval = None

        port = config["port"] if "port" in config else "22"
        if "private_key_path" in config:
            retval = Connection(
                host=config["host"],
                user=config["user"],
                port=port,
                connect_kwargs=dict(key_filename=config["private_key_path"]),
            )
        else:
            retval = Connection(
                host=config["host"],
                user=config["user"],
                port=port,
            )

        return retval

    @staticmethod
    def __get_pid_file_name(id: str) -> str:
        return f"rsyncdirector-{id}.pid"

    def __get_process_command(
        self, logger: Logger, action: Dict
    ) -> Tuple[multiprocessing.Process, multiprocessing.Queue]:
        result_queue = multiprocessing.Queue()
        args = action["args"] if "args" in action else None
        process = multiprocessing.Process(
            target=RsyncDirector.__exec_process_command,
            args=(
                logger,
                result_queue,
                action["command"],
                args,
            ),
        )
        return process, result_queue

    def __get_process_rsync(
        self, logger: Logger, job: Dict, action: Dict
    ) -> Tuple[multiprocessing.Process, multiprocessing.Queue]:
        result_queue = multiprocessing.Queue()
        sync = {
            "source": action["source"],
            "dest": action["dest"],
            "opts": action["opts"],
        }
        process = multiprocessing.Process(
            target=RsyncDirector.__exec_process_rsync,
            args=(
                logger,
                result_queue,
                job,
                sync,
            ),
        )
        return process, result_queue

    @staticmethod
    def __get_app_version() -> str:
        retval = "unknown"
        try:
            retval = version("rsyncdirector")
        except PackageNotFoundError:
            pass
        return retval

    @staticmethod
    def __is_blocked_local(blocks_on_conf: Dict, logger: Logger) -> bool:
        path = blocks_on_conf["path"]
        if os.path.exists(path):
            block_file_pid = None
            with open(path, "r") as fh:
                block_file_pid = fh.read().strip()
            logger.info(
                "local block file exists",
                block_file_pid=block_file_pid,
            )
            return True

        return False

    def __interruptable_wait(self, wait_time: int) -> None:
        self.logger.info("interruptable wait_event waiting", wait_time=wait_time)
        was_set = self.wait_event.wait(timeout=wait_time)
        self.logger.info("wait_event complete", was_interrupted=was_set)
        self.wait_event.clear()

    def __interrupt_wait(self) -> None:
        if self.wait_event is None:
            self.logger.info("interrupt_wait: no current wait_event to interrupt")
            return
        if self.wait_event is not None:
            self.logger.info("interrupt_wait: calling set() to interrupt wait_event")
            self.wait_event.set()

    @staticmethod
    def __is_blocked_remote(blocks_on_conf: Dict, logger: Logger):
        retval = False
        conn = None
        try:
            conn = RsyncDirector.__get_connection(blocks_on_conf)
            path = blocks_on_conf["path"]

            # Does the block file exist?
            result = conn.run(f"ls {path}", warn=True, hide=True)
            if result.ok == True:
                """
                The file does exist.  Read the contents of it to get the pid of the process that is
                still running to enrich the log message.
                """
                retval = True
                result = conn.run(f"cat {path}", warn=True, hide=True)
                if result.ok == False:
                    logger.error(
                        "unable to read the contents of remote block file",
                        stdout=result.stdout,
                        stderr=result.stdout,
                    )
                    metrics.BLOCK_FILE_ERR.inc()
                else:
                    block_file_pid = result.stdout.strip()
                    logger.info(
                        "remote block file exists",
                        block_file_pid=block_file_pid,
                    )
        except Exception as e:
            logger.error("checking for remote block", exception=e)
            metrics.BLOCK_FILE_ERR.inc()
            traceback.print_exc()
        finally:
            if conn is not None:
                conn.close()
        return retval

    def __lock_files(
        self, logger: Logger, job_id: str, lock_files: List[Dict], lock_file_action: LockFileAction
    ) -> None:
        for lock_file in lock_files:
            lock_file_type = LockFileType.get_enum_value_from_string(lock_file["type"])
            file_path = lock_file["path"]
            logger = logger.bind(lock_file=lock_file, lock_file_action=lock_file_action)
            conn = None

            # A 'lock file' is nothing more than a file that contains the pid of the process that
            # 'owns' the lock file, so we use the existing PidFile implementation which encapsulates
            # all of that logic.
            pid_file = None
            try:
                match lock_file_type:
                    case LockFileType.LOCAL:
                        pid_file = PidFileLocal(logger=logger, pid=self.pid, path=file_path)
                    case LockFileType.REMOTE:
                        conn = RsyncDirector.__get_connection(lock_file)
                        pid_file = PidFileRemote(
                            logger=logger, pid=self.pid, path=file_path, conn=conn
                        )
                    case _:
                        logger.fatal("Unknown lock_file.type")
                        sys.exit(1)

                match lock_file_action:
                    case LockFileAction.DELETE:
                        if pid_file.delete() is not True:
                            logger.fatal("unable to delete lock file")
                            sys.exit(1)
                        metrics.LOCK_FILES.labels(job_id).dec()
                    case LockFileAction.WRITE:
                        if pid_file.write() is not True:
                            self.logger.fatal("unable to write lock file")
                            sys.exit(1)
                        metrics.LOCK_FILES.labels(job_id).inc()
                        pass
                    case _:
                        logger.fatal("Unknown lock_file_action")
                        sys.exit(1)

            except Exception as e:
                logger.fatal("writing lock file", exception=e)
                sys.exit(1)
            finally:
                if conn is not None:
                    conn.close()

    def __run_job(self, job):
        job_id = job["id"]
        logger = self.logger.bind(job_id=job_id, job_type=job["type"])

        if "blocks_on" in job:
            with metrics.BLOCKED_DURATION.labels(job["id"]).time():
                continue_processing_job = self.__block(logger, job_id, job["blocks_on"])
                if not continue_processing_job:
                    logger.info("block condition was not removed, not continuing processing job")
                    metrics.JOB_SKIPPED_FOR_BLOCK_TIMEOUT_COUNTER.labels(job_id).inc()
                    return

        # Write lock files in a try block so that we can ensure to delete them.
        has_lock_files = True if "lock_files" in job else False
        try:
            if has_lock_files:
                self.__lock_files(logger, job["id"], job["lock_files"], LockFileAction.WRITE)

            logger.info("Executing actions for job")
            if "actions" not in job:
                raise Exception(f"no actions defined in job; job={job}")

            for action in job["actions"]:
                action_id = action["id"]
                logger = logger.bind(action=action["action"], action_id=action_id)
                if self.is_shutdown():
                    logger.info("We have been shutdown, exiting __run_job, action execution loop")
                    break

                # We run the action commands in a separate process altogether so that we can kill it
                # if we need to.
                result_queue = None
                match action["action"]:
                    case "sync":
                        self.process, result_queue = self.__get_process_rsync(logger, job, action)
                    case "command":
                        self.process, result_queue = self.__get_process_command(logger, action)
                    case _:
                        logger.fatal("Unknown acton", action=action)
                        sys.exit(1)

                try:
                    self.process.start()

                    # We MUST read from queue BEFORE joining to avoid deadlock. If the child process
                    # writes to the queue and the pipe buffer fills, it will block forever if we do
                    # not consume from the queue first.  Because we do not know how long the process
                    # will take and do not want to impose an arbitrary timeout we will poll the
                    # queue with a short timeout so we can detect if the process exits without
                    # sending a message, while still waiting as long as needed for the result.
                    result_msg = None
                    if result_queue:
                        while self.process.is_alive():
                            try:
                                result_msg = result_queue.get(block=True, timeout=1.0)
                                # We received a message, exit the loop polling the queue.
                                break
                            except queue.Empty:
                                # If the queue is empty only continue looping if the process is
                                # still alive.
                                if not self.process.is_alive():
                                    break
                                # Process still running, continue waiting . . . .
                                continue
                            except Exception as e:
                                logger.error("reading from result queue failed", exception=e)
                                metrics.ACTION_EXECUTION_ERR.labels(job_id, action_id).inc()
                                break

                        # Process exited, try one final non-blocking read in case message arrived
                        # just as process was exiting.
                        if result_msg is None and not result_queue.empty():
                            try:
                                result_msg = result_queue.get(block=False)
                            except:
                                logger.error("reading from result queue failed", exception=e)
                                metrics.JOB_ABORTED_FOR_FAILED_ACTION_ERR.labels(
                                    job_id, action_id
                                ).inc()
                                return

                    # Now it's safe to join since we've consumed the queue
                    self.process.join()

                    if self.process.exitcode != 0:
                        logger.error("Process failed", exit_code=self.process.exitcode)
                        metrics.JOB_ABORTED_FOR_FAILED_ACTION_ERR.labels(job_id, action_id).inc()
                        return

                    if not result_msg:
                        logger.error("reading from result queue failed, no result_msg")
                        metrics.JOB_ABORTED_FOR_FAILED_ACTION_ERR.labels(job_id, action_id).inc()
                        return

                    # Process the result message.
                    if result_msg:
                        run_result = result_msg[0]
                        if run_result == RunResult.FAIL:
                            result = result_msg[1]
                            logger.error(
                                "action failed, exiting job",
                                result_stdout=result.stdout.strip(),
                                result_sterr=result.stderr.strip(),
                                result_return_code=result.return_code,
                                result=result_msg,
                            )
                            metrics.JOB_ABORTED_FOR_FAILED_ACTION_ERR.labels(
                                job_id, action_id
                            ).inc()
                            return
                        logger.info("action suceeded", result=result_msg)

                except Exception as e:
                    err_type = type(e).__name__
                    err_msg = str(e)
                    logger.error("running job", error_type=err_type, err_msg=err_msg)
                    metrics.JOB_ABORTED_FOR_EXCEPTION_ERR.labels(job_id, action_id).inc()
                    return
                finally:
                    self.process.close()
                    self.process = None

        finally:
            if has_lock_files:
                self.__lock_files(logger, job["id"], job["lock_files"], LockFileAction.DELETE)

    def __schedule_cron_job(self):
        self.logger.info("Scheduling cron job", schedule=self.cron_schedule)
        self.scheduler.add_job(
            func=self.__exec_job, trigger=CronTrigger.from_crontab(self.cron_schedule)
        )

    def __start_metrics(self) -> None:
        addr = RsyncDirector.METRICS_DEFAULT_ADDR
        port = RsyncDirector.METRICS_DEFAULT_PORT
        startup_timeout_seconds = RsyncDirector.METRICS_DEFAULT_STARTUP_TIMEOUT_SECONDS
        startup_retry_wait_seconds = RsyncDirector.METRICS_DEFAULT_STARTUP_RETRY_WAIT_SECONDS
        startup_num_retries = RsyncDirector.METRICS_DEFAULT_STARTUP_NUM_RETRIES
        if "metrics" in self.configs:
            metrics = self.configs["metrics"]
            if "addr" in metrics:
                addr = metrics["addr"]
            if "port" in metrics:
                port = metrics["port"]
            if "startup_timeout_seconds" in metrics:
                startup_timeout_seconds = float(metrics["startup_timeout_seconds"])
            if "startup_retry_wait_seconds" in metrics:
                startup_retry_wait_seconds = float(metrics["startup_retry_wait_seconds"])
            if "startup_retry_limit" in metrics:
                startup_num_retries = int(metrics["startup_retry_limit"])

        self.metrics = Metrics(logger=self.logger, addr=addr, port=port)
        self.__initialize_metrics()
        self.metrics.start(startup_timeout_seconds, startup_retry_wait_seconds, startup_num_retries)

    def __initialize_metrics(self) -> None:
        metrics.BLOCK_FILE_ERR.inc(0)
        metrics.PID_FILE_ERR.inc(0)
        metrics.RUNS_COMPLETED.labels(self.rsync_id).inc(0)

        for job in self.configs["jobs"]:
            job_id = job["id"]
            metrics.BLOCKED_COUNTER.labels(job_id).inc(0)
            metrics.JOB_SKIPPED_FOR_BLOCK_TIMEOUT_COUNTER.labels(job_id).inc(0)
            metrics.JOB_SKIPPED_FOR_BLOCK_TIMEOUT_COUNTER.labels(job_id).inc(0)

            for action in job["actions"]:
                action_id = action["id"]
                metrics.ACTION_EXECUTION_ERR.labels(job_id, action_id).inc(0)
                metrics.JOB_ABORTED_FOR_EXCEPTION_ERR.labels(job_id, action_id).inc(0)
                metrics.JOB_ABORTED_FOR_FAILED_ACTION_ERR.labels(job_id, action_id).inc(0)
                metrics.JOB_ABORTED_FOR_FAILED_PROCESS_ERR.labels(job_id, action_id).inc(0)

    # ##########################################################################
    # Public methods and funcs

    def is_shutdown(self):
        return self.shutdown_flag.is_set()

    def run(self):
        if self.is_shutdown():
            return

        self.__start_metrics()
        self.scheduler.start()
        if self.runonce:
            self.schedule_runonce_job()
        else:
            self.__schedule_cron_job()

        """
        Loop indefinitely until we are shutdown.  Once shutdown we execute the cleanup/shutdown code
        below and exit.
        """
        while not self.is_shutdown():
            time.sleep(0.5)

        # Kill any running sub-processes.
        if self.process is not None:
            try:
                if self.process.is_alive():
                    self.logger.info("killing rsync process", process=self.process)
                    self.process.kill()
                    self.process.join(RsyncDirector.SUB_PROCESS_JOIN_SECONDS)
            except Exception as e:
                self.logger.warning("terminating process", exception=e)

        # Stop the scheduler.
        if self.scheduler.running:
            current_jobs = self.scheduler.get_jobs()
            self.logger.info("Shutting down scheduler", current_jobs=current_jobs)
            try:
                self.scheduler.shutdown(wait=False)
            except Exception as e:
                self.logger.info("Caught exception shutting down the scheduler", exception=e)

        self.logger.info("RsyncDirector exiting run")

    def schedule_runonce_job(self):
        if not self.scheduled_job_running:
            self.logger.info("Scheduling a runonce job")
            self.scheduler.add_job(
                max_instances=1, id=RsyncDirector.JOB_ID_RUNONCE, func=self.__exec_job
            )

    def shutdown(self):
        self.logger.info("Shutting down")
        self.shutdown_flag.set()
        self.__interrupt_wait()

        max_checks = 5
        num_checks = 0
        while self.scheduled_job_running == True:
            if num_checks >= max_checks:
                self.logger.error(
                    f"exceeded max_checks while waiting for scheduled job to finish; max_checks={max_checks}"
                )
                break
            self.logger.info("Waiting for scheduled job to finish")
            time.sleep(1)
            num_checks += 1

        if self.metrics is not None:
            self.metrics.stop()

        if self.pid_file is not None:
            self.pid_file.delete()
