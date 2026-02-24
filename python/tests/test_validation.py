"""Tests for S3 input validation functions (Stage 1b)."""

import pytest

from bleepstore.errors import InvalidArgument, InvalidBucketName, KeyTooLongError
from bleepstore.validation import validate_bucket_name, validate_max_keys, validate_object_key


class TestValidateBucketName:
    """Tests for validate_bucket_name()."""

    # -- Valid names ----------------------------------------------------------

    def test_valid_simple(self):
        """A simple lowercase alphanumeric name passes."""
        validate_bucket_name("my-bucket")

    def test_valid_three_chars(self):
        """Minimum length (3 chars) is accepted."""
        validate_bucket_name("abc")

    def test_valid_63_chars(self):
        """Maximum length (63 chars) is accepted."""
        validate_bucket_name("a" * 63)

    def test_valid_with_dots(self):
        """Names with dots (but no consecutive dots) are accepted."""
        validate_bucket_name("my.bucket.name")

    def test_valid_with_hyphens(self):
        """Names with hyphens are accepted."""
        validate_bucket_name("my-bucket-123")

    def test_valid_all_digits(self):
        """Names that are all digits are accepted (as long as not an IP)."""
        validate_bucket_name("123456")

    # -- Invalid names --------------------------------------------------------

    def test_too_short(self):
        """Names shorter than 3 characters are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("ab")

    def test_too_long(self):
        """Names longer than 63 characters are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("a" * 64)

    def test_uppercase(self):
        """Names with uppercase characters are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("MyBucket")

    def test_starts_with_hyphen(self):
        """Names starting with a hyphen are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("-my-bucket")

    def test_ends_with_hyphen(self):
        """Names ending with a hyphen are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("my-bucket-")

    def test_ip_address(self):
        """Names formatted as IP addresses are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("192.168.1.1")

    def test_xn_prefix(self):
        """Names starting with 'xn--' are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("xn--bucket")

    def test_s3alias_suffix(self):
        """Names ending with '-s3alias' are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("mybucket-s3alias")

    def test_ol_s3_suffix(self):
        """Names ending with '--ol-s3' are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("mybucket--ol-s3")

    def test_consecutive_dots(self):
        """Names with consecutive periods are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("my..bucket")

    def test_underscore(self):
        """Names with underscores are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("my_bucket")

    def test_empty_string(self):
        """Empty string is rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("")

    def test_single_char(self):
        """Single character is rejected (too short)."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("a")

    def test_special_characters(self):
        """Names with special characters are rejected."""
        with pytest.raises(InvalidBucketName):
            validate_bucket_name("my@bucket!")


class TestValidateObjectKey:
    """Tests for validate_object_key()."""

    def test_valid_short_key(self):
        """A short key is accepted."""
        validate_object_key("hello.txt")

    def test_valid_key_with_slashes(self):
        """Keys with slashes are accepted."""
        validate_object_key("path/to/my/file.txt")

    def test_valid_at_limit(self):
        """A key of exactly 1024 bytes is accepted."""
        validate_object_key("a" * 1024)

    def test_too_long_ascii(self):
        """A key exceeding 1024 bytes (ASCII) is rejected."""
        with pytest.raises(KeyTooLongError):
            validate_object_key("a" * 1025)

    def test_too_long_multibyte(self):
        """A key whose UTF-8 encoding exceeds 1024 bytes is rejected.

        Each CJK character encodes to 3 bytes in UTF-8, so 342 characters
        produce 1026 bytes.
        """
        # Each char is 3 bytes in UTF-8
        long_key = "\u4e00" * 342  # 342 * 3 = 1026 bytes
        with pytest.raises(KeyTooLongError):
            validate_object_key(long_key)

    def test_valid_multibyte_at_limit(self):
        """A key whose UTF-8 encoding is exactly 1024 bytes is accepted.

        341 * 3 = 1023 bytes, plus one ASCII char = 1024 bytes.
        """
        key = "\u4e00" * 341 + "a"  # 341*3 + 1 = 1024 bytes
        validate_object_key(key)


class TestValidateMaxKeys:
    """Tests for validate_max_keys()."""

    def test_valid_zero(self):
        """Zero is a valid max-keys value."""
        assert validate_max_keys("0") == 0

    def test_valid_1000(self):
        """1000 (the maximum) is valid."""
        assert validate_max_keys("1000") == 1000

    def test_valid_middle(self):
        """A value in the middle of the range is valid."""
        assert validate_max_keys("500") == 500

    def test_negative(self):
        """Negative values are rejected."""
        with pytest.raises(InvalidArgument):
            validate_max_keys("-1")

    def test_over_1000(self):
        """Values above 1000 are rejected."""
        with pytest.raises(InvalidArgument):
            validate_max_keys("1001")

    def test_not_a_number(self):
        """Non-numeric strings are rejected."""
        with pytest.raises(InvalidArgument):
            validate_max_keys("abc")

    def test_float_string(self):
        """Float strings are rejected (must be integer)."""
        with pytest.raises(InvalidArgument):
            validate_max_keys("10.5")

    def test_empty_string(self):
        """Empty string is rejected."""
        with pytest.raises(InvalidArgument):
            validate_max_keys("")
