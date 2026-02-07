from flask import Flask
from prometheus_client import Counter, Gauge, Histogram, Metric, generate_latest, start_http_server
from rsyncdirector.lib.utils import Utils
from rsyncdirector.lib.logging import Logger
import time

NAME = "metrics"
PREFIX = "rsyncdirector"

RUNS_COMPLETED = Counter(
    f"{PREFIX}_runs_completed", "Number of runs completed", labelnames=["rsync_id"]
)

JOB_DURATION = Histogram(
    name=f"{PREFIX}_job_duration_seconds",
    documentation="Duration of job in seconds",
    labelnames=["job_id"],
    buckets=(
        0.01,
        0.1,
        0.5,
        1,
        10,
        30,
        60,
        300,
        600,
        1800,
        3600,
        7200,
        14400,
        28800,
        86400,
        172800,
        604800,
        1209600,
    ),
)

JOB_SKIPPED_FOR_BLOCK_TIMEOUT_COUNTER = Counter(
    f"{PREFIX}_job_skipped_for_block_timeout",
    "Number of times a job is skipped because a block timedout",
    labelnames=["job_id"],
)

JOB_ABORTED_FOR_FAILED_PROCESS_ERR = Counter(
    f"{PREFIX}_job_aborted_for_failed_process_err",
    "Number of times a job is aborted because of a failed process",
    labelnames=["job_id", "action_id"],
)

JOB_ABORTED_FOR_FAILED_ACTION_ERR = Counter(
    f"{PREFIX}_job_aborted_for_failed_action_err",
    "Number of times a job is aborted because of a failed command",
    labelnames=["job_id", "action_id"],
)

JOB_ABORTED_FOR_EXCEPTION_ERR = Counter(
    f"{PREFIX}_job_aborted_for_exception_err",
    "Number of times a job is aborted because of an exception thrown by running the command",
    labelnames=["job_id", "action_id"],
)

BLOCKED_COUNTER = Counter(
    f"{PREFIX}_blocked", "Number of times a job is blocked", labelnames=["job_id"]
)

BLOCKED_DURATION = Histogram(
    f"{PREFIX}_blocked_seconds",
    "Time blocked in seconds",
    labelnames=["job_id"],
    buckets=(
        0.01,
        0.1,
        0.5,
        1,
        10,
        30,
        60,
        300,
        600,
        1800,
        3600,
        7200,
        14400,
        28800,
        86400,
        172800,
        604800,
        1209600,
    ),
)

LOCK_FILES = Gauge(
    f"{PREFIX}_lock_files", "Number of currently existing lock files", labelnames=["job_id"]
)


class Metrics(object):
    def __init__(self, logger: Logger, addr: str, port: str) -> None:
        self.logger = logger.bind(address=addr, port=port)
        self.addr = addr
        self.port = port
        self.app = Flask(NAME)
        self.thread = None
        self.running = False

    def index(self):
        return "Metrics server is running"

    def metrics(self):
        return generate_latest(), 200, {"Content-Type": "text/plain; charset=utf-8"}

    def start(self, timeout: float, wait_time: float, num_retries: int):
        if not self.running:
            self.running = True
            self.prometheus_httpd, self.prometheus_thread = start_http_server(
                port=int(self.port), addr=self.addr
            )

            # We use "localhost" for the host because it is possible that we are configured to
            # listen on "0.0.0.0" and while that is a valid configuration to listen on a socket, it
            # is NOT valid for a URL.
            Utils.wait_for_http_service(
                logger=self.logger,
                host="localhost",
                port=int(self.port),
                path="/metrics",
                wait_time=wait_time,
                num_retries=num_retries,
                timeout=timeout,
            )

            self.logger.info("Metrics http server started")
        else:
            self.logger.info("Metrics http server already running")

    def stop(self):
        if self.running:
            self.prometheus_httpd.shutdown()
            self.prometheus_httpd.server_close()
            self.prometheus_thread.join()
            self.running = False
            self.logger.info("Metrics http server stopped")
        else:
            self.logger.info("Metrics http is not running")
