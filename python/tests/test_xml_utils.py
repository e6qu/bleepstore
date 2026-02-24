"""Tests for S3 XML rendering utilities."""

from bleepstore.xml_utils import render_error


class TestRenderError:
    """Tests for render_error() function."""

    def test_basic_error(self):
        """render_error produces valid S3 error XML."""
        xml = render_error(
            code="NoSuchBucket",
            message="The specified bucket does not exist.",
            resource="/mybucket",
            request_id="AABBCCDD11223344",
        )
        assert '<?xml version="1.0" encoding="UTF-8"?>' in xml
        assert "<Error>" in xml
        assert "</Error>" in xml
        assert "<Code>NoSuchBucket</Code>" in xml
        assert "<Message>The specified bucket does not exist.</Message>" in xml
        assert "<Resource>/mybucket</Resource>" in xml
        assert "<RequestId>AABBCCDD11223344</RequestId>" in xml

    def test_error_with_extra_fields(self):
        """render_error includes extra fields."""
        xml = render_error(
            code="NoSuchBucket",
            message="The specified bucket does not exist.",
            resource="/mybucket",
            request_id="AABB",
            extra_fields={"BucketName": "mybucket"},
        )
        assert "<BucketName>mybucket</BucketName>" in xml

    def test_error_without_resource(self):
        """render_error omits Resource when empty."""
        xml = render_error(
            code="InternalError",
            message="Internal Error",
        )
        assert "<Resource>" not in xml
        assert "<RequestId>" not in xml

    def test_error_escapes_xml_chars(self):
        """render_error properly escapes XML special characters."""
        xml = render_error(
            code="InvalidArgument",
            message='Value must be < 10 & > 0 for "test"',
            resource="/bucket/<key>",
        )
        assert "&lt;" in xml
        assert "&amp;" in xml

    def test_error_no_namespace(self):
        """Error XML must NOT have an xmlns namespace attribute."""
        xml = render_error(code="InternalError", message="test")
        assert "xmlns" not in xml
