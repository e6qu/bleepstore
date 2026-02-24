"""Unit tests for the range request header parser.

Tests the parse_range_header function from object.py in isolation.
"""

import pytest

from bleepstore.errors import InvalidRange
from bleepstore.handlers.object import parse_range_header


class TestParseRangeHeader:
    """Tests for parse_range_header()."""

    def test_basic_range(self):
        """bytes=0-4 returns (0, 4)."""
        assert parse_range_header("bytes=0-4", 100) == (0, 4)

    def test_range_from_start(self):
        """bytes=0-99 returns full file."""
        assert parse_range_header("bytes=0-99", 100) == (0, 99)

    def test_range_first_byte(self):
        """bytes=0-0 returns first byte only."""
        assert parse_range_header("bytes=0-0", 100) == (0, 0)

    def test_suffix_range(self):
        """bytes=-5 returns last 5 bytes."""
        assert parse_range_header("bytes=-5", 100) == (95, 99)

    def test_suffix_range_larger_than_file(self):
        """bytes=-200 with file size 100 returns full file."""
        assert parse_range_header("bytes=-200", 100) == (0, 99)

    def test_open_ended_range(self):
        """bytes=10- returns from byte 10 to end."""
        assert parse_range_header("bytes=10-", 100) == (10, 99)

    def test_open_ended_last_byte(self):
        """bytes=99- returns last byte only."""
        assert parse_range_header("bytes=99-", 100) == (99, 99)

    def test_clamp_end_to_total(self):
        """bytes=50-200 with file size 100 clamps end to 99."""
        assert parse_range_header("bytes=50-200", 100) == (50, 99)

    def test_none_for_no_header(self):
        """Empty string returns None."""
        assert parse_range_header("", 100) is None

    def test_none_for_non_bytes_unit(self):
        """Non-bytes range unit returns None."""
        assert parse_range_header("pages=1-5", 100) is None

    def test_none_for_multi_range(self):
        """Multiple ranges return None (unsupported)."""
        assert parse_range_header("bytes=0-4, 10-14", 100) is None

    def test_invalid_start_past_end_raises(self):
        """bytes=100- with file size 100 raises InvalidRange."""
        with pytest.raises(InvalidRange):
            parse_range_header("bytes=100-", 100)

    def test_invalid_start_greater_than_end_raises(self):
        """bytes=10-5 raises InvalidRange."""
        with pytest.raises(InvalidRange):
            parse_range_header("bytes=10-5", 100)

    def test_invalid_suffix_zero_raises(self):
        """bytes=-0 raises InvalidRange."""
        with pytest.raises(InvalidRange):
            parse_range_header("bytes=-0", 100)

    def test_zero_length_file_open_ended_raises(self):
        """bytes=0- with file size 0 raises InvalidRange."""
        with pytest.raises(InvalidRange):
            parse_range_header("bytes=0-", 0)

    def test_none_for_none_input(self):
        """None input returns None."""
        assert parse_range_header(None, 100) is None

    def test_none_for_malformed(self):
        """Malformed range returns None."""
        assert parse_range_header("bytes=abc", 100) is None

    def test_start_equals_end(self):
        """bytes=5-5 returns single byte."""
        assert parse_range_header("bytes=5-5", 100) == (5, 5)

    def test_suffix_with_small_file(self):
        """bytes=-3 with file size 3 returns full file."""
        assert parse_range_header("bytes=-3", 3) == (0, 2)

    def test_suffix_with_file_size_1(self):
        """bytes=-1 with file size 1 returns single byte."""
        assert parse_range_header("bytes=-1", 1) == (0, 0)
