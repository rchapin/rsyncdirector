# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import os
from typing import Dict

class EnvVars(object):

    @staticmethod
    def get_env_vars(prefix: str) -> Dict:
        retval = {}
        for k, v in os.environ.items():
            if prefix in k:
                retval[k] = v
        return retval
