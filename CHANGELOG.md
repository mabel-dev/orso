# Changelog

## Unreleased (v2)

The full list of v2 changes is assembled at release; entries here so far:

- **Timestamps: UTC offsets are applied, never dropped.** `parse_iso`, and with it every
  `DATE`, `TIME` and `TIMESTAMP` parse, now normalises an offset to naive UTC:
  `2021-02-21T12:00:00+01:00` is `11:00`, where v1 gave `12:00`. An offset that cannot be
  applied, such as hours-only `+01`, now gives `None` (a failed parse) instead of being
  ignored. Fractional seconds are kept to the microsecond; v1 dropped them. This matches
  draken's ISO parser and opteryx-core's `CAST`. In PyPI opteryx, `CAST` of offset strings
  changes accordingly, and its `test_cast` expectation for `+01:00` moves from 12:00 to
  11:00, as opteryx-core's copy of that test already has.
