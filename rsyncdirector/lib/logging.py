import sys
import logging
import structlog
from typing import Union, Any
from structlog.stdlib import BoundLogger

Logger = Union[BoundLogger, Any]


def get_logger(name: str, log_level: str) -> Logger:
    # These run for BOTH thise code and third-party library logs.
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso", key="@timestamp"),
        structlog.processors.add_log_level,
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
        structlog.processors.EventRenamer("message"),
    ]

    if not structlog.is_configured():
        structlog.configure(
            processors=shared_processors
            + [
                # Prepare the data for the final formatter
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    # We create a Formatter that renders everything as JSON, regardless of how other libraries are
    # outputting logs.
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=shared_processors,
    )

    # Hijack the root logger to ensure we remove any existing handlers.
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, log_level.upper()))

    return structlog.get_logger(name)
