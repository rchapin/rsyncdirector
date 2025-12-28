# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import copy
import requests
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from logging import Logger
from threading import Event, Lock, Thread
from typing import Dict, List, Optional, Tuple


@dataclass
class MetricsScraperCfg:
    addr: str
    port: str
    scrape_interval: timedelta
    conn_timeout: timedelta


class MetricsScraper(Thread):

    def __init__(self, cfg: MetricsScraperCfg, logger: Logger):
        Thread.__init__(self)

        self._cfg = cfg
        self.logger = logger
        self._shutdown_flag = Event()
        self.metrics_lock = Lock()
        self.metrics = None

    @staticmethod
    def get_labels_key(labels: dict) -> str:
        if len(labels) == 0:
            return ""

        # FIXME: add type params to this
        keys = []
        for k, v in labels.items():
            keys.append(k)
        keys.sort()

        # FIXME: add type params to this
        pairs = []
        for k in keys:
            pairs.append(f"{k}__{labels[k]}")

        return "|".join(pairs)

    def get_metrics(self) -> dict:
        self.metrics_lock.acquire_lock()
        retval = copy.deepcopy(self.metrics)
        self.metrics_lock.release_lock()
        return retval

    def __set_metrics(self, metrics: dict) -> None:
        self.metrics_lock.acquire_lock()
        self.metrics = metrics
        self.metrics_lock.release_lock()

    def run(self):
        err_count = 0
        while not self._shutdown_flag.is_set():
            try:
                url = f"http://{self._cfg.addr}:{self._cfg.port}/metrics"
                response = requests.get(
                    url,
                    timeout=self._cfg.conn_timeout.total_seconds(),
                )
                response.raise_for_status()
                metrics = MetricsScraper.parse_metrics(response.text, self.logger)
                response.close()
                self.metrics_lock.acquire_lock()
                self.metrics = metrics
                self.metrics_lock.release_lock()
            except Exception as e:
                err_count = err_count + 1
                # We never re-raise the exception, just keep trying until we are shutdown.
                if err_count % 10 == 0:
                    self.logger.info(f"metrics scraper exception; e={e}")

            time.sleep(self._cfg.scrape_interval.total_seconds())
        self.logger.info("exiting run....")

    @staticmethod
    def parse_metrics(metrics: str, logger: Logger) -> dict:
        lines = metrics.split(sep="\n")
        retval = {}

        for line in lines:
            if line == "":
                continue

            if not line.startswith("#"):
                tokens = line.split()
                assert len(tokens) == 2

                name_tokens = tokens[0].split("{")
                assert len(name_tokens) > 0
                name = name_tokens[0]

                val = None
                try:
                    val = float(tokens[1])
                except Exception as e:
                    logger.error(
                        f"unable to cast value, 2nd token, to a float; tokens[1]={tokens[1]}"
                    )
                    continue

                labels_dict = MetricsScraper.get_labels(line)
                labels = (
                    f"::{MetricsScraper.get_labels_key(labels_dict)}"
                    if len(labels_dict) > 0
                    else ""
                )
                key = f"{name}{labels}"
                retval[key] = val

        return retval

    @staticmethod
    def get_labels(line: str) -> dict:
        retval = {}
        labels_start_index = None
        labels_end_index = None
        try:
            labels_start_index = line.index("{")
            labels_end_index = line.index("}")
        except Exception as e:
            return retval

        if labels_start_index == 0 or labels_end_index == 0:
            return retval

        labels_source_str = line[labels_start_index + 1 : labels_end_index]
        if len(labels_source_str) == 0:
            return retval
        labels_tokens = labels_source_str.split(sep=",")
        if len(labels_tokens) == 0:
            return retval

        for labels_token in labels_tokens:
            tokens = labels_token.split("=")
            if len(tokens) == 0:
                return retval

            k = tokens[0].strip()
            v = tokens[1].replace('"', "").strip()
            retval[k] = v

        return retval

    @staticmethod
    def __get_label_key(labels: dict) -> str:
        retval = ""

        return retval

    def shutdown(self):
        self.logger.info("metrics scraper shutting down")
        self._shutdown_flag.set()


@dataclass
class Metric:
    name: str
    value: float
    labels: Optional[dict] = None


@dataclass
class MetricsConditions:
    metrics: List[Metric]


class WaitFor(object):

    @staticmethod
    def metrics(
        logger: Logger,
        metrics_scraper: MetricsScraper,
        conditions: MetricsConditions,
        timeout: timedelta,
        poll_interval: timedelta,
        logging_silence_factor: int = 1000,
    ) -> None:

        # Build a dict of the expected metrics with the same key:value pair format as the metrics
        # that we will get from the scraper.
        expected_metrics = {}
        for metric in conditions.metrics:
            labels_key = (
                "" if metric.labels is None else MetricsScraper.get_labels_key(metric.labels)
            )
            key = metric.name if labels_key == "" else f"{metric.name}::{labels_key}"
            expected_metrics[key] = metric.value

        timeout_threshold = datetime.now() + timeout
        iteration = 0
        while True:
            if iteration % 50000 == 0:
                logger.info("waiting for metrics condition to be satisfied....")
            iteration += 1
            timeout_delta = datetime.now() - timeout_threshold
            if timeout_delta.total_seconds() > timeout.total_seconds():
                raise Exception("Timeout waiting for metrics")

            s = poll_interval.seconds
            time.sleep(s)
            metrics = metrics_scraper.get_metrics()
            if metrics == None:
                continue

            continue_waiting = True
            for expected_key, expected_val in expected_metrics.items():
                if expected_key in metrics:
                    actual_val = metrics[expected_key]
                    if float(expected_val) == actual_val:
                        continue_waiting = False
                        logger.info(
                            f"Found expected key and expected value; expected_key={expected_key}, "
                            f"expected_val={expected_val}, actual_val={actual_val}"
                        )
                    elif iteration % logging_silence_factor == 0:
                        logger.info(
                            f"Found expected key but not expected value; expected_key={expected_key}, "
                            f"expected_val={expected_val}, actual_val={actual_val}"
                        )
                    break

            if continue_waiting == False:
                break

    @staticmethod
    def to_metric_key_and_value(metric: Metric) -> Tuple[str, float]:
        labels_key = MetricsScraper.get_labels_key(metric.labels)
        key = metric.name if labels_key == "" else f"{metric.name}::{labels_key}"
        return "", 1.0
