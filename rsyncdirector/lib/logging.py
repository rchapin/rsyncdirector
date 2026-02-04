import io
import sys
import logging
import structlog
from typing import Union, Any
from structlog.stdlib import BoundLogger

Logger = Union[BoundLogger, Any]


def get_logger(
    name: str, log_level: str, cache_logger: bool = True, force_reconfig: bool = False
) -> Logger:
    if force_reconfig:
        structlog.reset_defaults()
        # Also clear handlers from the root logger so we don't duplicate them
        logging.getLogger().handlers.clear()

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
            cache_logger_on_first_use=cache_logger,
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

    logger = structlog.get_logger(name)
    return logger.bind(process="rsyncdirector")


class LogStreamer(io.TextIOBase):
    """A file-like object that redirects writes to a logger."""

    def __init__(self, logger: Logger, component: str):
        self.logger = logger
        self.buffer = ""

    def write(self, message: str) -> int:
        # Sub-processes often stream in chunks, not full lines so we buffer until we see a newline.
        self.buffer += message
        if "\n" in self.buffer:
            lines = self.buffer.split("\n")

            # Log all complete lines
            for line in lines[:-1]:
                clean_line = line.strip()
                if clean_line:
                    # This triggers the JSON output with all of the provided metadata.
                    self.logger.info(clean_line)

            # Keep the partial line for the next write
            self.buffer = lines[-1]
        return len(message)

    def flush(self):
        if self.buffer.strip():
            self.logger.info(self.buffer.strip())
            self.buffer = ""
