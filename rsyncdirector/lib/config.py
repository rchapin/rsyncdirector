# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

from enum import Enum
from typing import Dict
import yaml

ENV_VAR_PREFIX = "RSYNCMANANGER"
ENV_VAR_CONFIG = "CONFIG"
ENV_VAR_DRYRUN = "DRYRUN"
ENV_VAR_LOGLEVEL = "LOGLEVEL"
ENV_VAR_RUNONCE = "RUNONCE"

CONFIG_ENV_VAR_KEY = f"{ENV_VAR_PREFIX}_{ENV_VAR_CONFIG}"
DRYRUN_ENV_VAR_KEY = f"{ENV_VAR_PREFIX}_{ENV_VAR_DRYRUN}"
LOGLEVEL_ENV_VAR_KEY = f"{ENV_VAR_PREFIX}_{ENV_VAR_LOGLEVEL}"
RUNONCE_ENV_VAR_KEY = f"{ENV_VAR_PREFIX}_{ENV_VAR_RUNONCE}"


class JobType(Enum):
    LOCAL = 1
    REMOTE = 2


class BlocksOnType(Enum):
    LOCAL = "local"
    REMOTE = "remote"

    @staticmethod
    def get_enum_value_from_string(value_string: str):
        """
        Convert a string to the corresponding BlocksOnType enum value.
        :param value_string: The string representation of the enum value.
        :return: The corresponding BlocksOnType enum value or None if not found.
        """
        try:
            return BlocksOnType(value_string)
        except ValueError:
            return None


class LockFileType(Enum):
    LOCAL = "local"
    REMOTE = "remote"

    @staticmethod
    def get_enum_value_from_string(value_string: str):
        """
        Convert a string to the corresponding BlocksOnType enum value.
        :param value_string: The string representation of the enum value.
        :return: The corresponding BlocksOnType enum value or None if not found.
        """
        try:
            return LockFileType(value_string)
        except ValueError:
            return None


class Config(object):

    @staticmethod
    def load_configs(config: str) -> Dict:
        with open(config, "r") as fh:
            return yaml.load(fh, Loader=yaml.FullLoader)
