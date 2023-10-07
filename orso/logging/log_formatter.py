import hashlib
import json
import logging
import os
import re
from typing import Dict

from orso.display import colorizer

# if we find a key which matches these strings, we hash the contents
KEYS_TO_SANITIZE = [
    r"password$",
    r"pwd$",
    r".*_secret$",
    r".*_key$",
    r"_token$",
    r"credentials",
]
COMPILED_KEYS_TO_SANITIZE = [
    re.compile(expression, re.IGNORECASE) for expression in KEYS_TO_SANITIZE
]

COLOR_EXCHANGES = {
    " ALERT    ": "\001BOLD_REDm ALERT    \001OFFm",
    " ERROR    ": "\001REDm ERROR    \001OFFm",
    " DEBUG    ": "\001GREENm DEBUG    \001OFFm",
    " AUDIT    ": "\001YELLOWm AUDIT    \001OFFm",
    " WARNING  ": "\001BOLD_REDm WARNING  \001OFFm",
    " INFO     ": "\001BOLD_WHITEm INFO     \001OFFm",
}

COLOR_CODES = {
    "KEY": "\001KEYm",
    "OFF": "\001OFFm",
    "PURPLE": "\001PURPLEm",
    "YELLOW": "\001YELLOWm",
    "VALUE": "\001VALUEm",
}

QUOTES_OR_BACKTICKS_RE = re.compile(r"(['`])(.*?)\1")


class LogFormatter(logging.Formatter):
    def __init__(self, orig_formatter, suppress_color: bool = False):
        """
        Remove sensitive data from records before saving to external logs. Note that
        the value is hashed using (SHA256) and only the first 8 characters of the
        hex-encoded hash are presented. This information allows values to be traced
        without disclosing the actual value.

        The Sanitizer can only sanitize dictionaries, it doesn't sanitize strings,
        which could contain sensitive information We use the message id as a salt to
        further protect sensitive information.

        Based On: https://github.com/joocer/cronicl/blob/main/cronicl/utils/sanitizer.py
        """
        self.orig_formatter = orig_formatter
        self.suppress_color = suppress_color

    def format(self, record):
        try:
            msg = self.orig_formatter.format(record)
        except:
            msg = record
        msg = self.sanitize_record(msg)
        if "://" in msg:
            msg = re.sub(r":\/\/(.*?)\@", r"://\001BOLD_PURLEm<redacted>\001OFFm", msg)
        return msg

    def _can_colorize(self) -> bool:
        """
        Determines if the environment supports colorization.

        Returns:
            bool: True if colorization is supported, False otherwise.
        """
        if self.suppress_color:
            return False

        colorterm = os.environ.get("COLORTERM", "").lower()
        term = os.environ.get("TERM", "").lower()
        return "yes" in colorterm or "true" in colorterm or "256" in term

    def color_code(self, record):
        if self._can_colorize():
            for k, v in COLOR_EXCHANGES.items():
                if k in record:
                    return record.replace(k, v)
        return record

    def __getattr__(self, attr):
        return getattr(self.orig_formatter, attr)

    def hash_it(self, value_to_hash: str) -> str:
        """
        Hash a value using SHA256.

        Parameters:
            value_to_hash: str
                The value to hash.

        Returns:
            str: The hashed value.
        """
        return hashlib.sha256(value_to_hash.encode()).hexdigest()[:8]

    def clean_record(self, dirty_record: Dict, colorize: bool = True) -> Dict:
        """
        Cleans a log record dictionary.

        Parameters:
            dirty_record: Dict
                The original log record.
            colorize: bool
                Whether to add color codes to the log.

        Returns:
            Dict: The cleaned log record.
        """
        colors = COLOR_CODES if colorize else {key: "" for key in COLOR_CODES}

        def color_value(match):
            return f"{match.group(1)}{colors['YELLOW']}{match.group(2)}{colors['VALUE']}{match.group(1)}"

        clean_record = {}
        for key, value in dirty_record.items():
            if isinstance(value, dict):
                value = self.clean_record(value, colorize)
            elif any(regex.match(key) for regex in COMPILED_KEYS_TO_SANITIZE):
                value = f"{colors['PURPLE']}<redacted:{self.hash_it(str(value))}>{colors['OFF']}"
            else:
                value = QUOTES_OR_BACKTICKS_RE.sub(color_value, str(value))

            clean_key = f"{colors['KEY']}{key}{colors['OFF']}"
            clean_record[clean_key] = f"{colors['VALUE']}{value}{colors['OFF']}"

        return clean_record

    def sanitize_record(self, record: str) -> str:
        """
        Sanitizes and optionally colorizes a log record.

        Parameters:
            record: str
                The original log record.

        Returns:
            str: The sanitized and optionally colorized log record.
        """
        record = self.color_code(record)
        parts = record.split("|")
        json_part = parts.pop()

        try:
            dirty_record = json.loads(json_part.encode("UTF8"))
            clean_record = self.clean_record(dirty_record)
            parts.append(" " + json.dumps(clean_record))

        except ValueError:
            json_part = re.sub(r"`([^`]*)`", r"`\001YELLOWm\1\001OFFm`", f"{json_part}")
            json_part = re.sub(r"'([^']*)'", r"'\001YELLOWm\1\001OFFm'", f"{json_part}")
            json_part = re.sub(r'"([^"]*)"', r"'\001YELLOWm\1\001OFFm'", f"{json_part}")
            parts.append(" " + json_part.strip() + " *")

        record = "|".join(parts)
        return colorizer(record, self._can_colorize())
