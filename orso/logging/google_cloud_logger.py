"""
https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry
"""
import atexit
import datetime
import logging
import os
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import orjson as json

from orso.logging.levels import LEVELS
from orso.logging.levels import LEVELS_TO_STRING
from orso.logging.log_formatter import LogFormatter

logging_seen_warnings: Dict[int, int] = {}


def fix_fields(obj: Any) -> Any:
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("UTF8")
    if isinstance(obj, dict):
        return {k: fix_fields(v) for k, v in obj.items()}
    return str(obj)


def fix_dict(obj: dict) -> dict:
    return {k: fix_fields(v) for k, v in obj.items()}


def report_suppressions(message):
    from .. import logging as ml

    record = logging_seen_warnings.get(hash(message))
    if record:
        ml.get_logger().warning(
            f'The following message was suppressed {record} time(s) - "{message}"'
        )


def log_it(payload):
    if isinstance(payload, dict):
        payload = json.dumps(fix_dict(payload))
    if isinstance(payload, bytes):
        payload = payload.decode()
    print(payload, flush=True)
    return payload


def extract_caller():
    import os.path
    import traceback

    frames = traceback.extract_stack()
    if len(frames) < 4:
        return "<unknown>", "<unknown>", -1
    frame = frames[len(frames) - 4]
    head, tail = os.path.split(frame.filename)
    return frame.name, tail, frame.lineno


class GoogleLogger(object):
    @staticmethod
    def supported():
        if os.environ.get("K_SERVICE", "") or "" != "":  # nosemgrep
            return True
        return False

    @staticmethod
    def write_event(
        message: Union[str, dict],
        system: Optional[str] = None,
        severity: Optional[int] = logging.DEBUG,
        spanId: Optional[str] = None,
    ):
        # supress duplicate warnings
        if severity == LEVELS.WARNING:  # warnings
            hashed = hash(str(message))
            if hashed in logging_seen_warnings:
                logging_seen_warnings[hashed] += 1
                return "suppressed"
            logging_seen_warnings[hashed] = 0
            atexit.register(report_suppressions, str(message))

        structured_log = {
            "severity": str(severity).split(".")[-1],
            "logging.googleapis.com/labels": {
                "system": system,
                "platform": os.environ.get("LOG_SINK"),
                "formatter": "MABEL",
            },
        }

        method, module, line = extract_caller()
        structured_log["logging.googleapis.com/sourceLocation"] = {
            "function": method,
            "file": module,
            "line": line,
        }  # type:ignore

        if spanId:
            structured_log["logging.googleapis.com/spanId"] = spanId

        if isinstance(message, dict):
            formatter = LogFormatter(None)
            message = formatter.clean_record(message, False)
            structured_log["message"] = str(message) + " *"  # We don't want valid JSON here
            structured_log.update(message)  # type:ignore
            return log_it(structured_log)
        else:
            structured_log["message"] = message
            return log_it(structured_log)

    def __init__(self):
        # rewrite one of the levels
        LEVELS_TO_STRING[LEVELS.AUDIT] = "NOTICE"

        self.level = int(os.environ.get("LOGGING_LEVEL", 25))
        self.debug = self.create_logger(LEVELS.DEBUG)
        self.info = self.create_logger(LEVELS.INFO)
        self.warning = self.create_logger(LEVELS.WARNING)
        self.error = self.create_logger(LEVELS.ERROR)
        self.audit = self.create_logger(LEVELS.AUDIT)
        self.alert = self.create_logger(LEVELS.ALERT)

    def create_logger(self, level):
        from .create_logger import LOG_NAME

        def base_logger(message):
            return GoogleLogger.write_event(message=message, system=LOG_NAME, severity=level)

        def do_nothing(message):
            pass

        if level > self.level:
            return base_logger
        else:
            return do_nothing

    def setLevel(self, level):
        self.level = level
        self.debug = self.create_logger(LEVELS.DEBUG)
        self.info = self.create_logger(LEVELS.INFO)
        self.warning = self.create_logger(LEVELS.WARNING)
        self.error = self.create_logger(LEVELS.ERROR)
        self.audit = self.create_logger(LEVELS.AUDIT)
        self.alert = self.create_logger(LEVELS.ALERT)

    def __call__(self, message):
        self.debug(message)
