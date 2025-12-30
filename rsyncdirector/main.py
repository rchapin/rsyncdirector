# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import sys
import logging
import signal
import time
from rsyncdirector.lib.rsyncdirector import RsyncDirector
from threading import Lock

# For the time-being, we are just logging to the console
logging.basicConfig(
    format="%(asctime)s,%(levelname)s,%(module)s,[%(threadName)s],%(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)
rsyncdirector = None


def signal_handler(signal_number, _frame):
    global rsyncdirector

    if signal_number == signal.SIGHUP:
        logger.info(f"Executing run_once job after catching signal; signal_number={signal_number}")
        rsyncdirector.schedule_runonce_job()
    else:
        logger.info(f"Shutting down after catching signal; signal_number={signal_number}")
        rsyncdirector.shutdown()


def main():
    # Register signal handlers to properly shutdown the application.
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

    global rsyncdirector
    try:
        rsyncdirector = RsyncDirector(logger)
        rsyncdirector.start()

        while not rsyncdirector.is_shutdown():
            time.sleep(0.5)

    except Exception as e:
        # Dump the entire stack trace as we do not expect this case.
        logger.exception(e)

    if rsyncdirector:
        rsyncdirector.join(timeout=5.0)
    logger.info("Exiting main")


if __name__ == "__main__":
    main()
