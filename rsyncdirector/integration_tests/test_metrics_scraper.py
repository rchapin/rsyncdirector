# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import logging
import unittest
from dataclasses import dataclass
from rsyncdirector.lib import logging
from rsyncdirector.integration_tests.metrics_scraper import MetricsScraper

INPUT = """
# HELP python_gc_objects_collected_total Objects collected during gc
# TYPE python_gc_objects_collected_total counter
python_gc_objects_collected_total{generation="0"} 907.0
python_gc_objects_collected_total{generation="1"} 403.0
python_gc_objects_collected_total{generation="2"} 5.0

# HELP python_info Python platform information
# TYPE python_info gauge
python_info{implementation="CPython",major="3",minor="11",patchlevel="6",version="3.11.6"} 1.0

# HELP process_virtual_memory_bytes Virtual memory size in bytes.
# TYPE process_virtual_memory_bytes gauge
process_virtual_memory_bytes 5.30120704e+08

# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 0.52

# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter

# HELP my_test_counter_total Just a test
# TYPE my_test_counter_total counter
my_test_counter_total 1.0

# HELP my_test_counter_created Just a test
# TYPE my_test_counter_created gauge
my_test_counter_created 1.741126426815654e+09

# HELP my_float_histogram Test float histogram
# TYPE my_float_histogram histogram
my_float_histogram_bucket{le="0.01"} 0.0
my_float_histogram_bucket{le="0.25"} 0.0
my_float_histogram_bucket{le="0.5"} 0.0
my_float_histogram_bucket{le="1.0"} 1.0
my_float_histogram_bucket{le="2.0"} 1.0
my_float_histogram_bucket{le="5.0"} 1.0
my_float_histogram_bucket{le="10.0"} 1.0
my_float_histogram_bucket{le="+Inf"} 1.0
my_float_histogram_count 1.0
my_float_histogram_sum 0.75
"""


class MetricsScraperTest(unittest.TestCase):
    def setUp(self):
        self.logger = logging.get_logger("metrics-scraper-inttest", "INFO")

    def test_get_labels(self):
        test_cases = [
            {
                "name": "should parse line and return labels dict",
                "input_data": """python_info{implementation="CPython",major="3",minor="11",patchlevel="6",version="3.11.6"} 1.0""",
                "expected": {
                    "implementation": "CPython",
                    "major": "3",
                    "minor": "11",
                    "patchlevel": "6",
                    "version": "3.11.6",
                },
            },
            {
                "name": "there is only one left brace",
                "input_data": '''there is only one {key="value"''',
                "expected": {},
            },
            {
                "name": "there is only one right brace",
                "input_data": '''there is only one key="value}"''',
                "expected": {},
            },
        ]

        for tc in test_cases:
            name = tc["name"]
            input_data = tc["input_data"]
            expected_output = tc["expected"]
            with self.subTest(name, input_data=input_data, expected_output=expected_output):
                actual_output = MetricsScraper.get_labels(input_data)
                self.assertEqual(expected_output, actual_output)

    def test_get_labels_key(self):
        @dataclass
        class TestData:
            name: str
            input: dict
            expected: str

        test_cases = [
            TestData(
                name="properly sorts multi-labels",
                input={
                    "major": "3",
                    "patchlevel": "6",
                    "minor": "11",
                    "version": "3.11.6",
                    "implementation": "CPython",
                },
                expected="implementation__CPython|major__3|minor__11|patchlevel__6|version__3.11.6",
            ),
            TestData(
                name="empty string for empty dict",
                input={},
                expected="",
            ),
            TestData(
                name="single entry label",
                input={"foo": "blah"},
                expected="foo__blah",
            ),
        ]

        for tc in test_cases:
            name = tc.name
            input_data = tc.input
            expected = tc.expected
            with self.subTest(name, input_data=input_data, expected=expected):
                actual = MetricsScraper.get_labels_key(input_data)
                self.assertEqual(expected, actual)

    def test_parse_metrics(self):
        test_cases = [
            {
                "name": "happy path",
                "input_data": INPUT,
                "expected": {
                    "python_gc_objects_collected_total::generation__0": float(907.0),
                    "python_gc_objects_collected_total::generation__1": float(403.0),
                    "python_gc_objects_collected_total::generation__2": float(5.0),
                    "python_info::implementation__CPython|major__3|minor__11|patchlevel__6|version__3.11.6": float(
                        1.0
                    ),
                    "process_virtual_memory_bytes": float(5.30120704e08),
                    "process_cpu_seconds_total": float(0.52),
                    "my_test_counter_total": float(1.0),
                    "my_test_counter_created": float(1.741126426815654e09),
                    "my_float_histogram_bucket::le__0.01": float(0.0),
                    "my_float_histogram_bucket::le__0.25": float(0.0),
                    "my_float_histogram_bucket::le__0.5": float(0.0),
                    "my_float_histogram_bucket::le__1.0": float(1.0),
                    "my_float_histogram_bucket::le__2.0": float(1.0),
                    "my_float_histogram_bucket::le__5.0": float(1.0),
                    "my_float_histogram_bucket::le__10.0": float(1.0),
                    "my_float_histogram_bucket::le__+Inf": float(1.0),
                    "my_float_histogram_count": float(1.0),
                    "my_float_histogram_sum": float(0.75),
                },
            },
        ]

        for tc in test_cases:
            name = tc["name"]
            input_data = tc["input_data"]
            expected = tc["expected"]
            with self.subTest(name, input_data=input_data, expected=expected):
                actual = MetricsScraper.parse_metrics(input_data, self.logger)
                for actual_k, actual_v in actual.items():
                    if not actual_k in expected:
                        self.fail(f"actual key was not in expected; actual_k={actual_k}")
                        continue
                    expected_v = expected[actual_k]
                    self.assertEqual(expected_v, actual_v)
                    del expected[actual_k]
                self.assertTrue(
                    len(expected) == 0,
                    f"did not find expected entries for all expected, remainaing expected={expected}",
                )
