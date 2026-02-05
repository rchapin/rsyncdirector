# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import signal
import time
from functools import partial
from rsyncdirector.lib import config
from rsyncdirector.lib.envvars import EnvVars
from rsyncdirector.lib import logging
from rsyncdirector.lib.logging import Logger
from rsyncdirector.lib.rsyncdirector import RsyncDirector
from types import FrameType


def signal_handler(
    logger: Logger, rsyncdirector: RsyncDirector, signal_number: int, _frame: FrameType | None
) -> None:
    if signal_number == signal.SIGHUP:
        logger.info("Executing run_once job after catching signal", signal_number=signal_number)
        rsyncdirector.schedule_runonce_job()
    else:
        logger.info("Shutting down after catching signal", signal_number=signal_number)
        rsyncdirector.shutdown()


def main():
    env_vars = EnvVars.get_env_vars(config.ENV_VAR_PREFIX)
    log_level = env_vars[config.ENV_VAR_LOGLEVEL] if config.ENV_VAR_LOGLEVEL in env_vars else "INFO"
    logger = logging.get_logger(
        name="rsyncdirector",
        log_level=log_level,
        const_kvs=dict(process="rsyncdirector"),
    )

    try:
        rsyncdirector = RsyncDirector(logger, env_vars)

        # Register signal handlers to properly shutdown the application.
        handler = partial(signal_handler, logger, rsyncdirector)
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGHUP, handler)

        rsyncdirector.start()

        while not rsyncdirector.is_shutdown():
            time.sleep(0.5)

    except Exception as e:
        # TODO: add a stat
        logger.exception("exception from main", exception=e)

    if rsyncdirector:
        rsyncdirector.join(timeout=5.0)
    logger.info("Exiting main")


if __name__ == "__main__":
    main()
