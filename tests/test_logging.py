import os
import sys


sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso import logging


def test_logger():
    logger = logging.get_logger()

    logger.error("An Error Occurred")


def test_google_logger():
    logger = logging.google_cloud_logger.GoogleLogger()

    logger.write_event("Google", "Test")


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
