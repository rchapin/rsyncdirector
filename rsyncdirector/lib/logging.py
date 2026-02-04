import sys
import logging
import structlog
from typing import Union, Any
from structlog.stdlib import BoundLogger

Logger = Union[BoundLogger, Any]


def get_logger(name: str, log_level: str) -> Logger:
    """
    Configures and returns a structlog instance.
    """
    # Setup the standard library sink
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )

    # 2. Configure structlog if it hasn't been configured yet
    if not structlog.is_configured():
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.JSONRenderer(),
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        return structlog.get_logger(name)
