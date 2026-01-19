from dataclasses import dataclass
import unittest
from rsyncdirector.lib.utils import Utils


@dataclass
class RemoveWsAndSpecialChars:
    name: str
    input: str
    expected: str


class UtilsTest(unittest.TestCase):
    def test_remove_whitespace_and_special_chars(self):
        test_cases = [
            RemoveWsAndSpecialChars("empty input", "", ""),
            RemoveWsAndSpecialChars("all special chars", "!@#$%^", "______"),
            RemoveWsAndSpecialChars(
                "mix of special chars and spaces", "This is a test!", "This-is-a-test_"
            ),
        ]

        for test_case in test_cases:
            with self.subTest(msg=test_case.name, test_case=test_case):
                actual = Utils.remove_whitespace_and_special_chars(test_case.input)
                self.assertEqual(test_case.expected, actual)
