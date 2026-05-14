"""
quality_checks.py
Row-level DQ assertions for Silver transformers.
Each check logs violations but does NOT raise — bad rows are counted and
optionally written to a separate _rejected/ partition for investigation.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import DataFrame, functions as F

logger = logging.getLogger(__name__)


@dataclass
class DQResult:
    table: str
    rows_in: int
    rows_passed: int
    rows_rejected: int
    violations: list = field(default_factory=list)

    @property
    def rejection_rate(self) -> float:
        return (self.rows_rejected / self.rows_in * 100) if self.rows_in else 0.0

    def summary(self) -> str:
        lines = [
            f"[DQ] {self.table}",
            f"     rows_in={self.rows_in:,}  passed={self.rows_passed:,}  "
            f"rejected={self.rows_rejected:,}  ({self.rejection_rate:.2f}%)",
        ]
        for v in self.violations:
            lines.append(f"     ⚠  {v}")
        return "\n".join(lines)


def assert_no_nulls(df: DataFrame, cols: list, table: str) -> tuple[DataFrame, list]:
    """
    Remove rows where any of the listed columns is null.
    Returns (clean_df, violation_messages).
    """
    violations = []
    condition = F.lit(False)
    for col in cols:
        condition = condition | F.col(col).isNull()

    bad = df.filter(condition)
    bad_count = bad.count()

    if bad_count > 0:
        null_counts = {
            c: df.filter(F.col(c).isNull()).count()
            for c in cols
        }
        for c, cnt in null_counts.items():
            if cnt > 0:
                violations.append(f"null in required column '{c}': {cnt:,} rows")
                logger.warning("[%s] %s null in '%s' (%d rows)", table, "assert_no_nulls", c, cnt)

    return df.filter(~condition), violations


def assert_range(
    df: DataFrame,
    col: str,
    min_val: Optional[float],
    max_val: Optional[float],
    table: str,
) -> tuple[DataFrame, list]:
    """
    Remove rows where col falls outside [min_val, max_val].
    Pass None to skip either bound.
    """
    violations = []
    condition = F.lit(False)

    if min_val is not None:
        condition = condition | (F.col(col) < min_val)
    if max_val is not None:
        condition = condition | (F.col(col) > max_val)

    bad_count = df.filter(condition).count()
    if bad_count > 0:
        msg = f"out-of-range values in '{col}' [{min_val}, {max_val}]: {bad_count:,} rows"
        violations.append(msg)
        logger.warning("[%s] assert_range: %s", table, msg)

    return df.filter(~condition), violations


def assert_unique(df: DataFrame, keys: list, table: str) -> list:
    """
    Check for duplicate rows on the given key columns.
    Does NOT remove rows — just reports. Deduplication is handled by silver_utils.deduplicate().
    Returns violation messages.
    """
    violations = []
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    dupes = total - distinct

    if dupes > 0:
        msg = f"duplicate keys {keys}: {dupes:,} duplicate rows"
        violations.append(msg)
        logger.warning("[%s] assert_unique: %s", table, msg)

    return violations


def run_checks(df: DataFrame, table: str, checks: list) -> tuple[DataFrame, DQResult]:
    """
    Run a list of check callables against the DataFrame, accumulating violations.
    Each check callable must accept df and return (df, violations).

    Usage:
        checks = [
            lambda d: assert_no_nulls(d, ["site_id", "date"], table),
            lambda d: assert_range(d, "occupancy", 0, None, table),
        ]
        clean_df, result = run_checks(df, "db/site_occupancy", checks)
    """
    rows_in = df.count()
    violations = []

    for check in checks:
        df, check_violations = check(df)
        violations.extend(check_violations)

    rows_passed = df.count()
    rows_rejected = rows_in - rows_passed

    result = DQResult(
        table=table,
        rows_in=rows_in,
        rows_passed=rows_passed,
        rows_rejected=rows_rejected,
        violations=violations,
    )
    logger.info(result.summary())
    return df, result