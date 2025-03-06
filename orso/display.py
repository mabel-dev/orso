# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import decimal
from collections import deque
from itertools import islice
from typing import Union

from orso.compute.compiled import calculate_data_width

# Background		#282a36	40 42 54	231° 15% 18%
# Current Line		#44475a	68 71 90	232° 14% 31%
# Foreground		#f8f8f2	248 248 242	60° 30% 96%
# Red		        #ff5555	255 85 85	0° 100% 67%
# Yellow		    #f1fa8c	241 250 140	65° 92% 76%

COLORS = {
    "\001OFFm": "\033[0m",  # Text Reset
    # Opteryx named colors
    "\001PUNCm": "\033[38;5;102m",
    "\001CRLFm": "\033[38;2;98;114;164m",
    "\001HEADm": "\033[1m",
    "\001VARCHARm": "\033[38;2;255;171;82m",  # orange
    "\001CONSTm": "\033[38;2;139;233;253m\033[3m",  # cyan, italic
    "\001NULLm": "\033[38;2;98;114;164m\033[3m",  # grey, italic
    "\001TYPEm": "\033[38;2;98;114;164m",  # grey,
    "\001VALUEm": "\033[38;2;139;233;253m",  # cyan
    "\001FLOATm": "\033[38;2;255;121;198m",  # pink
    "\001INTEGERm": "\033[38;2;189;147;249m",  # purple
    "\001DATEm": "\033[38;2;80;250;123m",  # green
    "\001TIMESTAMPm": "\033[38;2;80;250;123m",  # green
    "\001TIMEm": "\033[38;2;26;185;67m",  # non-std green
    "\001KEYm": "\033[38;2;189;147;249m",  # purple
    "\001BLOBm": "\033[38;2;241;250;140m",  # yellow
    "\001INTERVALm": "\033[38;2;255;85;85m",  # pink
    # an orange color - 222
    # a red color = 209
    # Regular Colors
    "\001BLACKm": "\033[0;30m",  # Black
    "\001REDm": "\033[38;5;203m",  # Red
    "\001GREENm": "\033[38;2;80;250;123m",  # Green
    "\001YELLOWm": "\033[38;5;228m",  # Yellow
    "\001BLUEm": "\033[0;34m",  # Blue
    "\001PURPLEm": "\033[38;2;189;147;249m",  # Purple
    "\001CYANm": "\033[38;2;139;233;253m",  # Cyan
    "\001WHITEm": "\033[0;37m",  # White
    "\001PINKm": "\033[38;2;255;121;198m",  # pink
    # Bold
    "\001BOLD_BLACKm": "\033[1;30m",  # Black
    "\001BOLD_REDm": "\033[1;31m",  # Red
    "\001BOLD_GREENm": "\033[1;32m",  # Green
    "\001BOLD_YELLOWm": "\033[1;33m",  # Yellow
    "\001BOLD_BLUEm": "\033[1;34m",  # Blue
    "\001BOLD_PURPLEm": "\033[1;35m",  # Purple
    "\001BOLD_CYANm": "\033[1;36m",  # Cyan
    "\001BOLD_WHITEm": "\033[1;37m",  # White
}


def colorizer(record, can_colorize=True):
    record = str(record)
    record = record.replace(r"\u0001", "\x01")
    if can_colorize:
        for k, v in COLORS.items():
            record = record.replace(k, v)
    else:
        for k, v in COLORS.items():  # pragma: no cover
            record = record.replace(k, "")

    return record


def html_table(dictset, limit: int = 5):  # pragma: no cover
    """
    Render the dictset as a HTML table.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to render
        limit: integer (optional)
            The maximum number of record to show in the table, defaults to 5

    Returns:
        string (HTML table)
    """

    def sanitize(htmlstring):
        ## some types need converting to a string first
        if isinstance(htmlstring, (list, tuple, set)) or hasattr(htmlstring, "as_list"):
            return "[ " + ", ".join([sanitize(i) for i in htmlstring]) + " ]"
        if hasattr(htmlstring, "items"):
            return sanitize("{ " + ", ".join([f'"{k}": {v}' for k, v in htmlstring.items()]) + " }")
        if not isinstance(htmlstring, str):
            return str(htmlstring)
        escapes = {'"': "&quot;", "'": "&#39;", "<": "&lt;", ">": "&gt;", "$": "&#x24;"}
        # This is done first to prevent escaping other escapes.
        htmlstring = htmlstring.replace("&", "&amp;")
        for seq, esc in escapes.items():
            htmlstring = htmlstring.replace(seq, esc)
        return htmlstring

    def _to_html_table(data, columns):
        yield '<table class="table table-sm">'
        for counter, record in enumerate(data):
            if counter == 0:
                yield '<thead class="thead-light"><tr>'
                yield "<td></td>"
                for column in columns:
                    yield f"<th>{sanitize(column)}<th>\n"
                yield "</tr></thead><tbody>"

            yield "<tr>"
            yield f"<td><bold>{counter}</bold></td>"
            for i, column in enumerate(columns):
                sanitized = sanitize(record[i])
                yield f"<td title='{sanitized}' style='max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{sanitized}<td>\n"
            yield "</tr>"

        yield "</tbody></table>"

    footer = f"\n<p>{dictset.rowcount} rows x {dictset.columncount} columns</p>"  # type:ignore

    return "".join(_to_html_table(dictset.head(limit), dictset.column_names)) + footer


def ascii_table(
    table,
    limit: int = 5,
    display_width: Union[bool, int] = True,
    max_column_width: int = 30,
    colorize: bool = True,
    top_and_tail: bool = True,
    show_types: bool = False,
):  # pragma: no cover
    """
    Render the dictset as a ASCII table.

    NOTE:
        This exhausts generators so is only recommended to be used on lists.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to render
        limit: integer (optional)
            The maximum number of record to show in the table, defaults to 5
        display_width: integer/boolean (optional)
            The maximum width of the table, if an integer, the number of characters,
            if a boolean, True uses the display width, False disables (5000)

    Returns:
        string (ASCII table)
    """
    from math import isnan

    import numpy

    from orso import DataFrame

    lazy_length = 0
    is_lazy = not isinstance(table._rows, list)

    # get the width of the display
    if isinstance(display_width, bool):
        if not display_width:  # pragma: no cover
            display_width = 5000
        else:
            import shutil

            display_width = shutil.get_terminal_size((80, 20))[0]
    # Extract head data
    if limit > 0 and not top_and_tail:
        if is_lazy:
            t = DataFrame(rows=[row for row in islice(table._rows, limit)], schema=table.schema)
        else:
            t = table.slice(length=limit)
    elif limit > 0 and top_and_tail:
        if not is_lazy and table.rowcount >= ((2 * limit) + 1):
            t = table.head(size=limit) + table.tail(size=limit)
        elif is_lazy:
            head = list(islice(table._rows, limit))
            tail_collector = deque(maxlen=limit)
            for lazy_length, entry in enumerate(table._rows):
                tail_collector.append(entry)
            tail = list(tail_collector)
            lazy_length += len(head) + 1
            t = DataFrame(rows=head + tail, schema=table.schema)
        else:
            t = table
    else:
        t = table

    # width of index column
    index_width = len(str(lazy_length + 1)) + 2 if is_lazy else len(str(len(table))) + 2

    def numpy_type_mapper(value):
        if isinstance(value, numpy.ndarray):
            return value.tolist()
        if isinstance(value, (numpy.timedelta64,)):
            from types import SimpleNamespace

            seconds = value / numpy.timedelta64(1000000000, "ns")
            return SimpleNamespace(
                months=0, days=int(seconds // 86400), nanoseconds=(seconds % 86400) * 1e9
            )
        if numpy.issubdtype(value.dtype, numpy.integer):
            return int(value)
        if numpy.issubdtype(value.dtype, numpy.floating):
            return float(value)
        if numpy.issubdtype(value.dtype, numpy.bool_):
            return bool(value)
        if numpy.issubdtype(value.dtype, numpy.ndarray):
            return list(value)

        return str(value)

    def type_formatter(value, width, type_):
        if isinstance(value, (numpy.generic, numpy.ndarray)):
            value = numpy_type_mapper(value)

        if value is None or (isinstance(value, float) and isnan(value)):
            return "\001NULLm" + "null".rjust(width)[:width] + "\001OFFm"
        if isinstance(value, bool):
            # bool is a superclass of int, do before the int test
            return "\001CONSTm" + str(value).rjust(width)[:width] + "\001OFFm"
        if isinstance(value, int):
            return "\001INTEGERm" + str(value).rjust(width)[:width] + "\001OFFm"
        if isinstance(value, (float, decimal.Decimal)):
            return "\001FLOATm" + str(value).rjust(width)[:width] + "\001OFFm"
        if isinstance(value, str):
            return "\001VARCHARm" + trunc_printable(str(value).ljust(width), width) + "\001OFFm"
        if isinstance(value, datetime.datetime):
            value = f"{value.strftime('%Y-%m-%d')} \001TIMEm{value.strftime('%H:%M:%S')}"
            return "\001DATEm" + trunc_printable(value.rjust(width), width) + "\001OFFm"
        if isinstance(value, datetime.date):
            value = f"{value.strftime('%Y-%m-%d')}"
            return "\001DATEm" + trunc_printable(value.rjust(width), width) + "\001OFFm"
        if isinstance(value, (bytes, bytearray)):
            return (
                "\001BLOBm"
                + trunc_printable(value.decode("utf-8").ljust(width), width)
                + "\001OFFm"
            )
        if isinstance(value, dict):
            value = (
                "\001PUNCm{"
                + "\001PUNCm, ".join(
                    f"'\001KEYm{k}\001PUNCm':'\001VALUEm{v}\001PUNCm'" for k, v in value.items()
                )
                + "}\001OFFm"
            )
            return trunc_printable(value, width)
        if hasattr(value, "days"):
            # MonthDayNano is a superclass of list, do before list
            if isinstance(value, datetime.timedelta):
                days = value.days
                months = 0
                seconds = value.microseconds / 1e6 + value.seconds
            else:
                days = value.days
                months = value.months
                seconds = value.nanoseconds / 1e9

            hours, seconds = divmod(seconds, 3600)
            minutes, seconds = divmod(seconds, 60)
            years, months = divmod(months, 12)
            parts = []
            if years:
                parts.append(f"{int(years)}y")
            if months:
                parts.append(f"{int(months)}mo")
            if days:
                parts.append(f"{int(days)}d")
            if hours:
                parts.append(f"{int(hours)}h")
            if minutes:
                parts.append(f"{int(minutes)}m")
            if seconds:
                parts.append(f"{seconds:.2f}s")
            value = f"\001INTERVALm{' '.join(parts)}\001OFFm"
            return trunc_printable(value, width)
        if isinstance(value, (list, tuple)):
            value = (
                "\001PUNCm['\001VALUEm"
                + "\001PUNCm', '\001VALUEm".join(map(str, value))
                + "\001PUNCm']\001OFFm"
            )
            return trunc_printable(value, width)
        return str(value).ljust(width)[:width]

    def character_width(symbol):
        import unicodedata

        return 2 if unicodedata.east_asian_width(symbol) in ("F", "N", "W") else 1

    def trunc_printable(value, width, full_line: bool = True):
        offset = 0
        emit = ""
        ignoring = False

        for char in value:
            if char == "\n":
                emit += "\001CRLFm↵\001VARCHARm"
                offset += 1
                continue
            if char == "\r":
                continue
            emit += char
            if char in ("\033", "\001"):
                ignoring = True
            if not ignoring:
                offset += character_width(char)
            if ignoring and char == "m":
                ignoring = False
            if not ignoring and offset >= width:
                return emit + "\001OFFm"
        line = emit + "\001OFFm"
        if full_line:
            return line + " " * (width - offset)
        return line

    def _inner():
        # Calculate width
        col_width = list(map(len, t.column_names))

        data_width = [calculate_data_width(t.collect(i)) for i in range(t.columncount)]
        from orso.schema import RelationSchema
        from orso.types import OrsoTypes

        if isinstance(t.schema, RelationSchema):
            col_types = []
            for column in t.schema.columns:
                if column.type == OrsoTypes.ARRAY and column.element_type is not None:
                    col_types.append(f"ARRAY<{column.element_type}>")
                elif column.type == OrsoTypes.DECIMAL and column.precision is not None:
                    col_types.append(f"DECIMAL({column.precision},{column.scale})")
                else:
                    col_types.append(str(column.type))
        else:
            col_types = [OrsoTypes._MISSING_TYPE] * len(t.schema)
        col_type_width = list(map(len, col_types)) if show_types else [0] * len(col_types)
        col_width = [
            min(max(cw, ctw, dw), max_column_width)
            for cw, ctw, dw in zip(col_width, col_type_width, data_width)
        ]

        # Print data
        yield ("┌" + ("─" * index_width) + "┬─" + "─┬─".join("─" * cw for cw in col_width) + "─┐")
        yield (
            "│"
            + (" " * index_width)
            + "│ "
            + " │ ".join(
                "\001HEADm" + v.center(w)[:w] + "\001OFFm"
                for v, w in zip(t.column_names, col_width)
            )
            + " │"
        )
        if show_types:
            yield (
                "│"
                + (" " * index_width)
                + "│ "
                + " │ ".join(
                    "\001TYPEm" + v.center(w)[:w] + "\001OFFm" for v, w in zip(col_types, col_width)
                )
                + " │"
            )
        yield ("╞" + ("═" * index_width) + "╪═" + "═╪═".join("═" * cw for cw in col_width) + "═╡")
        if is_lazy:
            offset = 1
            for i, row in enumerate(t):
                if i == limit and lazy_length > (2 * limit):
                    yield "..."
                    offset += lazy_length - 2 * limit
                formatted = [type_formatter(v, w, t) for v, w, t in zip(row, col_width, col_types)]
                yield (
                    "│\001TYPEm"
                    + str(i + offset).rjust(index_width - 1)
                    + "\001OFFm │ "
                    + " │ ".join(formatted)
                    + " │"
                )
        else:
            for i, row in enumerate(t):
                if top_and_tail and (table.rowcount > 2 * limit):
                    if i == limit:
                        yield "\001PUNCm...\001OFFm"
                    if i >= limit:
                        i += t.rowcount - (2 * limit)
                formatted = [type_formatter(v, w, t) for v, w, t in zip(row, col_width, col_types)]
                yield (
                    "│\001TYPEm"
                    + str(i + 1).rjust(index_width - 1)
                    + "\001OFFm │ "
                    + " │ ".join(formatted)
                    + " │"
                )
        yield ("└" + ("─" * index_width) + "┴─" + "─┴─".join("─" * cw for cw in col_width) + "─┘")

    return "\n".join(
        colorizer(trunc_printable(line, display_width, False), colorize) for line in _inner()
    )


def markdown(
    table,
    limit: int = 5,
    max_column_width: int = 30,
):  # pragma: no cover
    # Extract head data
    t = table.slice(length=limit) if limit > 0 else table

    # width of index column
    index_width = len(str(len(table)))

    # Calculate width
    col_width = list(map(len, t.column_names))
    data_width = [
        max(list(map(len, map(str, [p for p in h if p is not None]))) + [4])
        for h in (t.collect(i) for i in range(t.columncount))
    ]
    col_width = [min(max(cw, dw), max_column_width) for cw, dw in zip(col_width, data_width)]

    # Print data
    data = [t.row(i) for i in range(len(t))]
    yield (
        "| #"
        + (" " * (index_width - 2))
        + "| "
        + " | ".join(v.ljust(w)[:w] for v, w in zip(t.column_names, col_width))
        + " |"
    )
    yield ("|" + ("-" * index_width) + "|-" + "-|-".join("-" * cw for cw in col_width) + "-|")
    for i in range(len(data)):
        formatted = [str(v).rjust(w)[:w] for v, w in zip(data[i], col_width)]
        yield ("|" + str(i + 1).rjust(index_width - 1) + " | " + " | ".join(formatted) + " |")
