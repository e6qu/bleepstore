"""E2E tests for S3 multipart upload operations."""

import hashlib

import pytest
from botocore.exceptions import ClientError

# 5 MiB minimum part size
MIN_PART_SIZE = 5 * 1024 * 1024


@pytest.mark.multipart_ops
class TestMultipartUpload:
    def test_basic_multipart_upload(self, s3_client, created_bucket):
        """Complete a basic multipart upload with 2 parts."""
        key = "multipart.bin"

        # Initiate
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key, ContentType="application/octet-stream"
        )
        upload_id = create_resp["UploadId"]
        assert upload_id

        try:
            # Upload parts (each >= 5MB except last)
            part1_data = b"A" * MIN_PART_SIZE
            part2_data = b"B" * 1024  # Last part can be smaller

            part1 = s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=part1_data,
            )
            part2 = s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=2,
                Body=part2_data,
            )

            # Complete
            resp = s3_client.complete_multipart_upload(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": 1, "ETag": part1["ETag"]},
                        {"PartNumber": 2, "ETag": part2["ETag"]},
                    ]
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "ETag" in resp
            # Multipart ETag has dash: "hash-N"
            assert "-" in resp["ETag"]

            # Verify object
            get_resp = s3_client.get_object(Bucket=created_bucket, Key=key)
            content = get_resp["Body"].read()
            assert content == part1_data + part2_data

        except Exception:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )
            raise

    def test_upload_part_etag(self, s3_client, created_bucket):
        """Each part's ETag should be the MD5 of the part data."""
        key = "etag-part.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            data = b"X" * MIN_PART_SIZE
            expected_md5 = hashlib.md5(data).hexdigest()

            part = s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=data,
            )
            etag = part["ETag"].strip('"')
            assert etag == expected_md5
        finally:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )

    def test_overwrite_part(self, s3_client, created_bucket):
        """Uploading same part number overwrites previous."""
        key = "overwrite-part.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            # Upload part 1 twice with different data
            data_v1 = b"V1" * (MIN_PART_SIZE // 2)
            data_v2 = b"V2" * (MIN_PART_SIZE // 2)

            s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=data_v1,
            )
            part_v2 = s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=data_v2,
            )

            # Complete with the latest ETag
            s3_client.complete_multipart_upload(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [{"PartNumber": 1, "ETag": part_v2["ETag"]}]
                },
            )

            # Verify it's the second version
            resp = s3_client.get_object(Bucket=created_bucket, Key=key)
            assert resp["Body"].read() == data_v2
        except Exception:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )
            raise


@pytest.mark.multipart_ops
class TestAbortMultipartUpload:
    def test_abort_upload(self, s3_client, created_bucket):
        """Abort a multipart upload."""
        key = "abort.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        # Upload a part
        s3_client.upload_part(
            Bucket=created_bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=b"A" * MIN_PART_SIZE,
        )

        # Abort
        resp = s3_client.abort_multipart_upload(
            Bucket=created_bucket, Key=key, UploadId=upload_id
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    def test_abort_nonexistent_upload(self, s3_client, created_bucket):
        """Abort non-existent upload returns NoSuchUpload."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket,
                Key="nope.bin",
                UploadId="nonexistent-upload-id",
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchUpload"


@pytest.mark.multipart_ops
class TestListMultipartUploads:
    def test_list_uploads(self, s3_client, created_bucket):
        """List in-progress multipart uploads."""
        # Create two uploads
        up1 = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key="upload1.bin"
        )
        up2 = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key="upload2.bin"
        )

        try:
            resp = s3_client.list_multipart_uploads(Bucket=created_bucket)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            upload_ids = [u["UploadId"] for u in resp.get("Uploads", [])]
            assert up1["UploadId"] in upload_ids
            assert up2["UploadId"] in upload_ids
        finally:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket,
                Key="upload1.bin",
                UploadId=up1["UploadId"],
            )
            s3_client.abort_multipart_upload(
                Bucket=created_bucket,
                Key="upload2.bin",
                UploadId=up2["UploadId"],
            )

    def test_list_uploads_with_prefix(self, s3_client, created_bucket):
        """List uploads filtered by prefix."""
        up1 = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key="data/file1.bin"
        )
        up2 = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key="logs/file2.bin"
        )

        try:
            resp = s3_client.list_multipart_uploads(
                Bucket=created_bucket, Prefix="data/"
            )
            keys = [u["Key"] for u in resp.get("Uploads", [])]
            assert "data/file1.bin" in keys
            assert "logs/file2.bin" not in keys
        finally:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket,
                Key="data/file1.bin",
                UploadId=up1["UploadId"],
            )
            s3_client.abort_multipart_upload(
                Bucket=created_bucket,
                Key="logs/file2.bin",
                UploadId=up2["UploadId"],
            )


@pytest.mark.multipart_ops
class TestListParts:
    def test_list_parts(self, s3_client, created_bucket):
        """List parts of an in-progress upload."""
        key = "list-parts.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            # Upload 3 parts
            for i in range(1, 4):
                s3_client.upload_part(
                    Bucket=created_bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=i,
                    Body=b"X" * MIN_PART_SIZE,
                )

            resp = s3_client.list_parts(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert len(resp["Parts"]) == 3
            for part in resp["Parts"]:
                assert "PartNumber" in part
                assert "ETag" in part
                assert "Size" in part
                assert "LastModified" in part
        finally:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )


@pytest.mark.multipart_ops
class TestMultipartUploadErrors:
    def test_complete_with_invalid_part_order(self, s3_client, created_bucket):
        """Parts must be in ascending order."""
        key = "order.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            part1 = s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=b"A" * MIN_PART_SIZE,
            )
            part2 = s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=2,
                Body=b"B" * 1024,
            )

            with pytest.raises(ClientError) as exc_info:
                s3_client.complete_multipart_upload(
                    Bucket=created_bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [
                            {"PartNumber": 2, "ETag": part2["ETag"]},
                            {"PartNumber": 1, "ETag": part1["ETag"]},
                        ]
                    },
                )
            assert exc_info.value.response["Error"]["Code"] == "InvalidPartOrder"
        finally:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )

    def test_complete_with_wrong_etag(self, s3_client, created_bucket):
        """Wrong ETag in complete returns InvalidPart."""
        key = "wrong-etag.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            s3_client.upload_part(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=b"A" * MIN_PART_SIZE,
            )

            with pytest.raises(ClientError) as exc_info:
                s3_client.complete_multipart_upload(
                    Bucket=created_bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [
                            {
                                "PartNumber": 1,
                                "ETag": '"0000000000000000000000000000000"',
                            }
                        ]
                    },
                )
            assert exc_info.value.response["Error"]["Code"] == "InvalidPart"
        finally:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )

    def test_upload_to_nonexistent_upload_id(self, s3_client, created_bucket):
        """Uploading to invalid upload ID returns NoSuchUpload."""
        with pytest.raises(ClientError) as exc_info:
            s3_client.upload_part(
                Bucket=created_bucket,
                Key="nope.bin",
                UploadId="fake-upload-id",
                PartNumber=1,
                Body=b"data",
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchUpload"


@pytest.mark.multipart_ops
class TestUploadPartCopy:
    def test_upload_part_copy(self, s3_client, created_bucket):
        """Copy a part from an existing object into a multipart upload."""
        # Create a source object large enough to use as a part
        source_data = b"S" * MIN_PART_SIZE
        s3_client.put_object(
            Bucket=created_bucket, Key="copy-source.bin", Body=source_data
        )

        # Initiate multipart upload
        key = "part-copy-dest.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            # Copy the source object as part 1
            copy_resp = s3_client.upload_part_copy(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                CopySource=f"{created_bucket}/copy-source.bin",
            )
            assert "CopyPartResult" in copy_resp
            assert "ETag" in copy_resp["CopyPartResult"]

            # Complete the upload
            resp = s3_client.complete_multipart_upload(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {
                            "PartNumber": 1,
                            "ETag": copy_resp["CopyPartResult"]["ETag"],
                        }
                    ]
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify the content matches the source
            get_resp = s3_client.get_object(Bucket=created_bucket, Key=key)
            assert get_resp["Body"].read() == source_data
        except Exception:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )
            raise

    def test_upload_part_copy_with_range(self, s3_client, created_bucket):
        """Copy a byte range from source into a part."""
        # Create a source object
        source_data = b"R" * MIN_PART_SIZE * 2
        s3_client.put_object(
            Bucket=created_bucket, Key="range-source.bin", Body=source_data
        )

        # Initiate multipart upload
        key = "part-copy-range.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            # Copy a range from the source as part 1
            range_end = MIN_PART_SIZE - 1
            copy_resp = s3_client.upload_part_copy(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                CopySource=f"{created_bucket}/range-source.bin",
                CopySourceRange=f"bytes=0-{range_end}",
            )
            assert "CopyPartResult" in copy_resp
            assert "ETag" in copy_resp["CopyPartResult"]

            # Complete the upload
            resp = s3_client.complete_multipart_upload(
                Bucket=created_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {
                            "PartNumber": 1,
                            "ETag": copy_resp["CopyPartResult"]["ETag"],
                        }
                    ]
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify the content is only the specified range
            get_resp = s3_client.get_object(Bucket=created_bucket, Key=key)
            content = get_resp["Body"].read()
            assert len(content) == MIN_PART_SIZE
            assert content == source_data[:MIN_PART_SIZE]
        except Exception:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )
            raise

    def test_upload_part_copy_nonexistent_source(self, s3_client, created_bucket):
        """Copy from a non-existent source object returns NoSuchKey."""
        key = "part-copy-nosrc.bin"
        create_resp = s3_client.create_multipart_upload(
            Bucket=created_bucket, Key=key
        )
        upload_id = create_resp["UploadId"]

        try:
            with pytest.raises(ClientError) as exc_info:
                s3_client.upload_part_copy(
                    Bucket=created_bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=1,
                    CopySource=f"{created_bucket}/nonexistent-source.bin",
                )
            assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"
        finally:
            s3_client.abort_multipart_upload(
                Bucket=created_bucket, Key=key, UploadId=upload_id
            )
