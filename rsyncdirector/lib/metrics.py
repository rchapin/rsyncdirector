from flask import Flask
from typing import Any, Callable, Iterable, Sequence
from prometheus_client import Counter, Gauge, Histogram, Metric, generate_latest, start_http_server
from rsyncdirector.lib.utils import Utils
import logging
import time

NAME = "metrics"

RUNS_COMPLETED = Counter("runs_completed", "Number of runs completed", labelnames=["rsync_id"])

JOB_DURATION = Histogram(
    name="job_duration_seconds",
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
    "job_skipped_for_block_timeout",
    "Number of times a job is skipped because a block timedout",
    labelnames=["job_id"],
)

BLOCKED_COUNTER = Counter("blocked", "Number of times a job is blocked", labelnames=["job_id"])

BLOCKED_DURATION = Histogram(
    "blocked_seconds",
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

LOCK_FILES = Gauge("lock_files", "Number of currently existing lock files", labelnames=["job_id"])


class Metrics(object):
    def __init__(self, logger: logging.Logger, addr: str, port: str) -> None:
        self.logger = logger
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

            self.logger.info(f"Metrics http server started on http://{self.addr}:{self.port}")
        else:
            self.logger.info(
                f"Metrics http server already running on http://{self.addr}:{self.port}"
            )

    def stop(self):
        if self.running:
            self.prometheus_httpd.shutdown()
            self.prometheus_httpd.server_close()
            self.prometheus_thread.join()
            self.running = False
            self.logger.info("Metrics http server stopped")
        else:
            self.logger.info("Metrics http is not running")
