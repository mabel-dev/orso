import logging
import os
from functools import lru_cache

from orso.logging.add_level import add_logging_level
from orso.logging.google_cloud_logger import GoogleLogger
from orso.logging.levels import LEVELS
from orso.logging.levels import LEVELS_TO_STRING
from orso.logging.log_formatter import LogFormatter

LOG_NAME: str = "DEFAULT"
LOG_FORMAT: str = "\001BOLD_CYANm%(name)s\001OFFm | %(levelname)-8s | %(asctime)s | \001PINKm%(funcName)s()\001OFFm | \001YELLOWm%(filename)s\001OFFm:\001PURPLEm%(lineno)s\001OFFm | %(message)s"


def set_log_name(log_name: str):
    global LOG_NAME
    LOG_NAME = log_name
    get_logger.cache_clear()


@lru_cache(1)
def get_logger() -> logging.Logger:
    """
    Use Python's native logging - we created a named logger so we can make sure
    only the logs related to our jobs are included (other modules also use the
    Python's logging module).
    """

    if GoogleLogger.supported():
        return GoogleLogger()  # type:ignore

    logger = logging.getLogger(LOG_NAME)

    # default is to log WARNING and above
    logger.setLevel(int(os.environ.get("LOGGING_LEVEL", 25)))

    # add the TRACE, AUDIT and ALERT levels to the logger
    if not hasattr(logger, "audit"):
        add_logging_level("AUDIT", LEVELS.AUDIT)
    if not hasattr(logging, "alert"):
        add_logging_level("ALERT", LEVELS.ALERT)

    # override the existing handlers for these levels
    add_logging_level("DEBUG", LEVELS.DEBUG)
    add_logging_level("INFO", LEVELS.INFO)
    add_logging_level("WARNING", LEVELS.WARNING)
    add_logging_level("ERROR", LEVELS.ERROR)

    # configure the logger
    mabel_logging_handler = logging.StreamHandler()
    formatter = LogFormatter(
        logging.Formatter(LOG_FORMAT, datefmt="\001DATEm%Y-%m-%d \001TIMEm%H:%M:%S%z\001OFFm")
    )
    mabel_logging_handler.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(mabel_logging_handler)

    return logger
