import logging
import multiprocessing
import os
import sys
import time
import traceback
import rsyncdirector.lib.config as cfg
from apscheduler import events
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
from rsyncdirector.lib.config import BlocksOnType, LockFileType
from rsyncdirector.lib.envvars import EnvVars
from rsyncdirector.lib.pidfile import PidFileLocal, PidFileRemote
from rsyncdirector.lib.rsync import Rsync
from rsyncdirector.lib.metrics import (
    Metrics,
    BLOCKED_COUNTER,
    BLOCKED_DURATION,
    LOCK_FILES,
    JOB_DURATION,
    JOB_SKIPPED_FOR_BLOCK_TIMEOUT_COUNTER,
    RUNS_COMPLETED,
)
from rsyncdirector.lib.utils import Utils
from enum import Enum
from fabric import Connection
from invoke import run
from threading import Event, Thread
from typing import List, Dict


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

    PID_FILE_DIR_DEFAULT = "/var/run/rsyncdirector"

    SUB_PROCESS_JOIN_SECONDS = float(5)

    def __init__(self, logger: logging.Logger):
        Thread.__init__(self)
        self.logger = logger
        # Reference to the subprocess in which we will run the actual rsync command
        self.process = None
        self.metrics = None
        self.pid_file = None
        self.scheduled_job_running = False

        """
        The shutdown_flag is a threading event object that indicates whether the thread should be
        terminated.
        """
        self.shutdown_flag = Event()
        # An event object to be used for interruptable "sleeping".
        self.wait_event = Event()

        env_vars = EnvVars.get_env_vars(cfg.ENV_VAR_PREFIX)
        if cfg.CONFIG_ENV_VAR_KEY not in env_vars:
            raise Exception(
                "Required env var defining path to config file is not defined; "
                f"export {cfg.CONFIG_ENV_VAR_KEY} pointing to path of a valid config file"
            )
        self.config = env_vars[cfg.CONFIG_ENV_VAR_KEY]
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
        if cfg.LOGLEVEL_ENV_VAR_KEY in env_vars:
            # FIXME: Updating the log level isn't working yet.
            level = env_vars[cfg.LOGLEVEL_ENV_VAR_KEY].upper()
            logger.setLevel(level=level)

        # TODO: validate configs
        self.configs = cfg.Config.load_configs(self.config)
        self.rsync_id = self.configs["rsync_id"]

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
            self.logger.error(f"unable to write pidfile, shutting down; pid_path={pid_path}")
            self.shutdown()
            return

        self.scheduler = BackgroundScheduler()
        self.scheduler.add_listener(self.__event_listener)
        self.cron_schedule = self.configs["cron_schedule"]

    def __block(self, job_id: str, blocks_on_conf: dict) -> bool:
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
                blocked = False
                match blocks_on_type:
                    case BlocksOnType.LOCAL:
                        blocked = RsyncDirector.__is_blocked_local(block, self.logger)
                    case BlocksOnType.REMOTE:
                        blocked = RsyncDirector.__is_blocked_remote(block, self.logger)
                    case _:
                        self.logger.fatal(f"Unknown blocks_on type; type={blocks_on_type}")
                        sys.exit(1)

                if not blocked:
                    self.logger.info(f"block has been satisfied; job_id={job_id}, block={block}")
                    break

                if blocked:
                    # Have we timedout?
                    if timeout_threshold and timeout:
                        timeout_delta = datetime.now() - timeout_threshold
                        if timeout_delta.total_seconds() > timeout.total_seconds():
                            self.logger.info(f"block has timedout; job_id={job_id}, block={block}")
                            return False

                    wait_time = block["wait_time"]
                    self.logger.info(f"job is blocked; job_id={job_id}, wait_time={wait_time}")
                    BLOCKED_COUNTER.labels(job_id).inc()
                    self.__interruptable_wait(wait_time)

        # Once all blocks have been satisfied or if there are no blocks configured just return True
        # so as not to block job processing.
        return True

    def __event_listener(self, event):
        def list_jobs():
            for job in self.scheduler.get_jobs():
                self.logger.info(f"Job id={job.id}, scheduled for next run at {job.next_run_time}")

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
            self.logger.warning(f"Unable to execute multiple jobs simultaneously, exiting exec_job")
            return
        self.scheduled_job_running = True

        for job in self.configs["jobs"]:
            if self.is_shutdown():
                self.logger.info(f"We have been shutdown, exiting jobs execution loop")
                break
            with JOB_DURATION.labels(job["id"]).time():
                self.__run_job(job)

        self.scheduled_job_running = False
        self.logger.info("exec_job finished")
        RUNS_COMPLETED.labels(self.rsync_id).inc()

    @staticmethod
    def __exec_process(logger, job, sync):
        user = job["user"] if "user" in job else None
        host = job["host"] if "host" in job else None
        port = job["port"] if "port" in job else None
        private_key_path = job["private_key_path"] if "private_key_path" in job else None

        # TODO: this can get removed once we validate and clean-up the configs on start-up.
        job_type = cfg.JobType[job["type"].upper()]

        rsync = Rsync(logger, job_type, sync, user, host, port, private_key_path)
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

    def __is_blocked_local(blocks_on_conf: Dict, logger: logging.Logger) -> bool:
        path = blocks_on_conf["path"]
        if os.path.exists(path):
            block_file_pid = None
            with open(path, "r") as fh:
                block_file_pid = fh.read().strip()
            logger.info(
                f"local block file exists; block_on_conf={blocks_on_conf}, "
                + f"block_file_pid={block_file_pid}"
            )
            return True

        return False

    def __interruptable_wait(self, wait_time: int) -> None:
        self.logger.info(f"interruptable wait_event waiting; wait_time={wait_time}")
        was_set = self.wait_event.wait(timeout=wait_time)
        self.logger.info(f"wait_event complete; was_set={was_set}")
        self.wait_event.clear()

    def __interrupt_wait(self) -> None:
        if self.wait_event is None:
            self.logger.info("interrupt_wait: no current wait_event to interrupt")
            return
        if self.wait_event is not None:
            self.logger.info("interrupt_wait: calling set() to interrupt wait_event")
            self.wait_event.set()

    @staticmethod
    def __is_blocked_remote(blocks_on_conf, logger):
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
                        f"unable to read the contents of remote block file; "
                        + f"path={path}, "
                        + f"stdout={result.stdout}, stderr={result.stdout}"
                    )
                else:
                    block_file_pid = result.stdout.strip()
                    logger.info(
                        f"remote block file exists; block_on_conf={blocks_on_conf}, "
                        + f"block_file_pid={block_file_pid}"
                    )
        except Exception as e:
            logger.error(f"checking for remote block; e={e}")
            traceback.print_exc()
        finally:
            if conn is not None:
                conn.close()
        return retval

    def __lock_files(
        self, job_id: str, lock_files: List[Dict], lock_file_action: LockFileAction
    ) -> None:
        for lock_file in lock_files:
            lock_file_type = LockFileType.get_enum_value_from_string(lock_file["type"])
            file_path = lock_file["path"]
            conn = None

            # A 'lock file' is nothing more than a file that contains the pid of the process that
            # 'owns' the lock file, so we use the existing PidFile implementation which encapsulates
            # all of that logic.
            pid_file = None
            try:
                match lock_file_type:
                    case LockFileType.LOCAL:
                        pid_file = PidFileLocal(logger=self.logger, pid=self.pid, path=file_path)
                    case LockFileType.REMOTE:
                        conn = RsyncDirector.__get_connection(lock_file)
                        pid_file = PidFileRemote(
                            logger=self.logger, pid=self.pid, path=file_path, conn=conn
                        )
                    case _:
                        self.logger.fatal(f"Unknown lock_file.type; type={lock_file_type}")
                        sys.exit(1)

                match lock_file_action:
                    case LockFileAction.DELETE:
                        if pid_file.delete() is not True:
                            self.logger.fatal(
                                f"unable to delete lock file; lock_file_type={lock_file_type}, file_path={file_path}"
                            )
                            sys.exit(1)
                        LOCK_FILES.labels(job_id).dec()
                    case LockFileAction.WRITE:
                        if pid_file.write() is not True:
                            self.logger.fatal(
                                f"unable to write lock file; lock_file_type={lock_file_type}, file_path={file_path}"
                            )
                            sys.exit(1)
                        LOCK_FILES.labels(job_id).inc()
                        pass
                    case _:
                        self.logger.fatal(
                            f"Unknown lock_file_action; lock_file_action={lock_file_action}"
                        )
                        sys.exit(1)

            except Exception as e:
                self.logger.fatal(f"writing lock file; lock_file={lock_file}, e={e}")
                sys.exit(1)
            finally:
                if conn is not None:
                    conn.close()

    def __run_job(self, job):
        job_id = job["id"]
        if "blocks_on" in job:
            with BLOCKED_DURATION.labels(job["id"]).time():
                continue_processing_job = self.__block(job_id, job["blocks_on"])
                if not continue_processing_job:
                    self.logger.info(
                        f"block condition was not removed, not continuing processing job; job_id={job_id}"
                    )
                    JOB_SKIPPED_FOR_BLOCK_TIMEOUT_COUNTER.labels(job_id).inc()
                    return

        # Write lock files in a try block so that we can ensure to delete them.
        has_lock_files = True if "lock_files" in job else False
        try:
            if has_lock_files:
                self.__lock_files(job["id"], job["lock_files"], LockFileAction.WRITE)

            self.logger.info(f"Executing syncs for job; job={job}")
            for sync in job["syncs"]:
                if self.is_shutdown():
                    self.logger.info(
                        f"We have been shutdown, exiting __run_job, sync execution loop"
                    )
                    break

                # We run the rsync command in a separate process altogether so that we can kill it
                # if we need to.
                self.logger.info(f"Executing sync; sync={sync}")
                self.process = multiprocessing.Process(
                    target=RsyncDirector.__exec_process,
                    args=(
                        self.logger,
                        job,
                        sync,
                    ),
                )
                try:
                    self.process.start()
                    self.process.join()
                except Exception as e:
                    err_type = type(e).__name__
                    err_msg = str(e)
                    self.logger.error(f"running job: error_type={err_type}, err_msg={err_msg}")
                finally:
                    self.process.close()
                    self.process = None

        finally:
            if has_lock_files:
                self.__lock_files(job["id"], job["lock_files"], LockFileAction.DELETE)

    def __schedule_cron_job(self):
        self.logger.info(f"Scheduling cron job with schedule={self.cron_schedule}")
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
        self.metrics.start(startup_timeout_seconds, startup_retry_wait_seconds, startup_num_retries)

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
                    self.logger.info(f"killing rsync process; process={self.process}")
                    self.process.kill()
                    self.process.join(RsyncDirector.SUB_PROCESS_JOIN_SECONDS)
            except Exception as e:
                self.logger.warning(f"terminating process; e={e}")

        # Stop the scheduler.
        if self.scheduler.running:
            self.logger.info("Shutting down scheduler")
            current_jobs = self.scheduler.get_jobs()
            self.logger.info(f"current_jobs={current_jobs}")
            try:
                self.scheduler.shutdown(wait=False)
            except Exception as e:
                self.logger.info(f"Caught exception shutting down the scheduler; e={e}")

        self.logger.info("RsyncDirector exiting run")

    def schedule_runonce_job(self):
        if not self.scheduled_job_running:
            self.logger.info(f"Scheduling a runonce job")
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
