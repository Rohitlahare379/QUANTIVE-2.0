"""
Test Matrix — Continuous Aggregate (CAGG) Boundaries & Scheduling (Tests 33 to 38).
Verifies affected aggregate window calculations across 5m, 15m, 1h, 4h, 1d timeframes,
exact boundary alignment, and downstream CaggRefreshJob registration.
"""

import pytest
from datetime import datetime, timezone, timedelta
from app.services.cagg_refresh import compute_cagg_bucket_alignment


def test_33_affected_5m_bucket():
    """
    Test 33: Repair 10:01 -> 10:07 aligns to 5m buckets 10:00 -> 10:10.
    """
    start = datetime(2026, 1, 1, 10, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 10, 7, tzinfo=timezone.utc)

    aligned_start, aligned_end = compute_cagg_bucket_alignment(start, end, timeframe="5m")

    assert aligned_start == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert aligned_end == datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)


def test_34_affected_15m_bucket():
    """
    Test 34: Repair 10:07 -> 10:22 aligns to 15m buckets 10:00 -> 10:30.
    """
    start = datetime(2026, 1, 1, 10, 7, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 10, 22, tzinfo=timezone.utc)

    aligned_start, aligned_end = compute_cagg_bucket_alignment(start, end, timeframe="15m")

    assert aligned_start == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert aligned_end == datetime(2026, 1, 1, 10, 30, tzinfo=timezone.utc)


def test_35_affected_1h_bucket():
    """
    Test 35: Repair 10:15 -> 11:05 aligns to 1h buckets 10:00 -> 12:00.
    """
    start = datetime(2026, 1, 1, 10, 15, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 11, 5, tzinfo=timezone.utc)

    aligned_start, aligned_end = compute_cagg_bucket_alignment(start, end, timeframe="1h")

    assert aligned_start == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert aligned_end == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_36_affected_4h_bucket():
    """
    Test 36: Repair 02:30 -> 05:15 aligns to 4h buckets 00:00 -> 08:00.
    """
    start = datetime(2026, 1, 1, 2, 30, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 5, 15, tzinfo=timezone.utc)

    aligned_start, aligned_end = compute_cagg_bucket_alignment(start, end, timeframe="4h")

    assert aligned_start == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert aligned_end == datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)


def test_37_affected_1d_bucket():
    """
    Test 37: Repair Jan 1 15:00 -> Jan 2 08:00 aligns to 1d buckets Jan 1 00:00 -> Jan 3 00:00.
    """
    start = datetime(2026, 1, 1, 15, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, 8, 0, tzinfo=timezone.utc)

    aligned_start, aligned_end = compute_cagg_bucket_alignment(start, end, timeframe="1d")

    assert aligned_start == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert aligned_end == datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc)


def test_38_multiple_affected_timeframes_union():
    """
    Test 38: Union alignment encompassing all continuous aggregates (5m, 15m, 1h, 4h, 1d).
    A repair within Jan 1 10:01 -> 10:07 aligns to [Jan 1 00:00 -> Jan 2 00:00], ensuring
    every affected aggregate tier is refreshed completely.
    """
    start = datetime(2026, 1, 1, 10, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 10, 7, tzinfo=timezone.utc)

    aligned_start, aligned_end = compute_cagg_bucket_alignment(start, end, timeframe=None)

    assert aligned_start == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert aligned_end == datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)
