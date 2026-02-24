//! S3 XML response rendering.
//!
//! All S3 API responses are XML-encoded.  This module provides helpers
//! that produce the correct XML payloads using `quick-xml`.

use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;
use std::io::Cursor;

// ── Error response ──────────────────────────────────────────────────

/// Render an S3 `<Error>` XML document.
///
/// ```xml
/// <?xml version="1.0" encoding="UTF-8"?>
/// <Error>
///   <Code>NoSuchBucket</Code>
///   <Message>The specified bucket does not exist</Message>
///   <Resource>/mybucket</Resource>
///   <RequestId>abcd-1234</RequestId>
/// </Error>
/// ```
pub fn render_error(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    write_simple_element_group(
        &mut writer,
        "Error",
        &[
            ("Code", code),
            ("Message", message),
            ("Resource", resource),
            ("RequestId", request_id),
        ],
    );

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── ListAllMyBucketsResult ──────────────────────────────────────────

/// Render the `<ListAllMyBucketsResult>` response for `GET /`.
///
/// `buckets` is a list of `(name, creation_date)` pairs.
pub fn render_list_buckets_result(
    owner_id: &str,
    owner_display: &str,
    buckets: &[(&str, &str)],
) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    let root = BytesStart::new("ListAllMyBucketsResult")
        .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
    writer.write_event(Event::Start(root)).expect("start root");

    // <Owner>
    write_simple_element_group(
        &mut writer,
        "Owner",
        &[("ID", owner_id), ("DisplayName", owner_display)],
    );

    // <Buckets>
    writer
        .write_event(Event::Start(BytesStart::new("Buckets")))
        .expect("start Buckets");
    for (name, date) in buckets {
        write_simple_element_group(
            &mut writer,
            "Bucket",
            &[("Name", name), ("CreationDate", date)],
        );
    }
    writer
        .write_event(Event::End(BytesEnd::new("Buckets")))
        .expect("end Buckets");

    writer
        .write_event(Event::End(BytesEnd::new("ListAllMyBucketsResult")))
        .expect("end root");

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── ListBucketResult (v2) ───────────────────────────────────────────

/// Represents a single object entry inside a list-objects response.
pub struct ObjectEntry<'a> {
    pub key: &'a str,
    pub last_modified: &'a str,
    pub etag: &'a str,
    pub size: u64,
    pub storage_class: &'a str,
}

/// Render `<ListBucketResult>` for ListObjectsV2.
#[allow(clippy::too_many_arguments)]
pub fn render_list_objects_result(
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    max_keys: u32,
    is_truncated: bool,
    key_count: u32,
    entries: &[ObjectEntry<'_>],
    common_prefixes: &[&str],
    continuation_token: Option<&str>,
    next_continuation_token: Option<&str>,
    start_after: Option<&str>,
) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    let root = BytesStart::new("ListBucketResult")
        .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
    writer.write_event(Event::Start(root)).expect("start root");

    write_text_element(&mut writer, "Name", bucket);
    write_text_element(&mut writer, "Prefix", prefix);
    if !delimiter.is_empty() {
        write_text_element(&mut writer, "Delimiter", delimiter);
    }
    write_text_element(&mut writer, "MaxKeys", &max_keys.to_string());
    write_text_element(&mut writer, "KeyCount", &key_count.to_string());
    write_text_element(
        &mut writer,
        "IsTruncated",
        if is_truncated { "true" } else { "false" },
    );

    if let Some(token) = continuation_token {
        write_text_element(&mut writer, "ContinuationToken", token);
    }
    if let Some(token) = next_continuation_token {
        write_text_element(&mut writer, "NextContinuationToken", token);
    }
    if let Some(sa) = start_after {
        if !sa.is_empty() {
            write_text_element(&mut writer, "StartAfter", sa);
        }
    }

    for entry in entries {
        writer
            .write_event(Event::Start(BytesStart::new("Contents")))
            .expect("start Contents");
        write_text_element(&mut writer, "Key", entry.key);
        write_text_element(&mut writer, "LastModified", entry.last_modified);
        write_text_element(&mut writer, "ETag", entry.etag);
        write_text_element(&mut writer, "Size", &entry.size.to_string());
        write_text_element(&mut writer, "StorageClass", entry.storage_class);
        writer
            .write_event(Event::End(BytesEnd::new("Contents")))
            .expect("end Contents");
    }

    for cp in common_prefixes {
        writer
            .write_event(Event::Start(BytesStart::new("CommonPrefixes")))
            .expect("start CommonPrefixes");
        write_text_element(&mut writer, "Prefix", cp);
        writer
            .write_event(Event::End(BytesEnd::new("CommonPrefixes")))
            .expect("end CommonPrefixes");
    }

    writer
        .write_event(Event::End(BytesEnd::new("ListBucketResult")))
        .expect("end root");

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── ListBucketResult (v1) ───────────────────────────────────────────

/// Render `<ListBucketResult>` for ListObjectsV1.
#[allow(clippy::too_many_arguments)]
pub fn render_list_objects_result_v1(
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    marker: &str,
    max_keys: u32,
    is_truncated: bool,
    entries: &[ObjectEntry<'_>],
    common_prefixes: &[&str],
    next_marker: Option<&str>,
) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    let root = BytesStart::new("ListBucketResult")
        .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
    writer.write_event(Event::Start(root)).expect("start root");

    write_text_element(&mut writer, "Name", bucket);
    write_text_element(&mut writer, "Prefix", prefix);
    write_text_element(&mut writer, "Marker", marker);
    if !delimiter.is_empty() {
        write_text_element(&mut writer, "Delimiter", delimiter);
    }
    write_text_element(&mut writer, "MaxKeys", &max_keys.to_string());
    write_text_element(
        &mut writer,
        "IsTruncated",
        if is_truncated { "true" } else { "false" },
    );

    if let Some(nm) = next_marker {
        write_text_element(&mut writer, "NextMarker", nm);
    }

    for entry in entries {
        writer
            .write_event(Event::Start(BytesStart::new("Contents")))
            .expect("start Contents");
        write_text_element(&mut writer, "Key", entry.key);
        write_text_element(&mut writer, "LastModified", entry.last_modified);
        write_text_element(&mut writer, "ETag", entry.etag);
        write_text_element(&mut writer, "Size", &entry.size.to_string());
        write_text_element(&mut writer, "StorageClass", entry.storage_class);
        writer
            .write_event(Event::End(BytesEnd::new("Contents")))
            .expect("end Contents");
    }

    for cp in common_prefixes {
        writer
            .write_event(Event::Start(BytesStart::new("CommonPrefixes")))
            .expect("start CommonPrefixes");
        write_text_element(&mut writer, "Prefix", cp);
        writer
            .write_event(Event::End(BytesEnd::new("CommonPrefixes")))
            .expect("end CommonPrefixes");
    }

    writer
        .write_event(Event::End(BytesEnd::new("ListBucketResult")))
        .expect("end root");

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── DeleteResult ────────────────────────────────────────────────────

/// Represents a single deleted object in a DeleteObjects response.
pub struct DeletedEntry<'a> {
    pub key: &'a str,
}

/// Represents a single error in a DeleteObjects response.
pub struct DeleteErrorEntry<'a> {
    pub key: &'a str,
    pub code: &'a str,
    pub message: &'a str,
}

/// Render `<DeleteResult>` for batch DeleteObjects.
pub fn render_delete_result(
    deleted: &[DeletedEntry<'_>],
    errors: &[DeleteErrorEntry<'_>],
    quiet: bool,
) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    let root = BytesStart::new("DeleteResult")
        .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
    writer.write_event(Event::Start(root)).expect("start root");

    // In quiet mode, only errors are included (no <Deleted> elements).
    if !quiet {
        for entry in deleted {
            writer
                .write_event(Event::Start(BytesStart::new("Deleted")))
                .expect("start Deleted");
            write_text_element(&mut writer, "Key", entry.key);
            writer
                .write_event(Event::End(BytesEnd::new("Deleted")))
                .expect("end Deleted");
        }
    }

    for entry in errors {
        writer
            .write_event(Event::Start(BytesStart::new("Error")))
            .expect("start Error");
        write_text_element(&mut writer, "Key", entry.key);
        write_text_element(&mut writer, "Code", entry.code);
        write_text_element(&mut writer, "Message", entry.message);
        writer
            .write_event(Event::End(BytesEnd::new("Error")))
            .expect("end Error");
    }

    writer
        .write_event(Event::End(BytesEnd::new("DeleteResult")))
        .expect("end root");

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── CopyObjectResult ────────────────────────────────────────────────

/// Render `<CopyObjectResult>` returned by `PUT` with `x-amz-copy-source`.
pub fn render_copy_object_result(etag: &str, last_modified: &str) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    write_simple_element_group(
        &mut writer,
        "CopyObjectResult",
        &[("ETag", etag), ("LastModified", last_modified)],
    );

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── InitiateMultipartUploadResult ───────────────────────────────────

/// Render `<InitiateMultipartUploadResult>`.
pub fn render_initiate_multipart_upload_result(bucket: &str, key: &str, upload_id: &str) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    write_simple_element_group(
        &mut writer,
        "InitiateMultipartUploadResult",
        &[("Bucket", bucket), ("Key", key), ("UploadId", upload_id)],
    );

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── CompleteMultipartUploadResult ───────────────────────────────────

/// Render `<CompleteMultipartUploadResult>`.
pub fn render_complete_multipart_upload_result(
    location: &str,
    bucket: &str,
    key: &str,
    etag: &str,
) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    write_simple_element_group(
        &mut writer,
        "CompleteMultipartUploadResult",
        &[
            ("Location", location),
            ("Bucket", bucket),
            ("Key", key),
            ("ETag", etag),
        ],
    );

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── LocationConstraint ──────────────────────────────────────────────

/// Render `<LocationConstraint>` for GetBucketLocation.
///
/// Per S3 spec, us-east-1 returns an empty `<LocationConstraint/>` (null),
/// not the string "us-east-1".
pub fn render_location_constraint(region: &str) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    if region == "us-east-1" || region.is_empty() {
        // Self-closing empty element with namespace.
        let elem = BytesStart::new("LocationConstraint")
            .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
        writer
            .write_event(Event::Empty(elem))
            .expect("empty LocationConstraint");
    } else {
        let elem = BytesStart::new("LocationConstraint")
            .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
        writer
            .write_event(Event::Start(elem))
            .expect("start LocationConstraint");
        writer
            .write_event(Event::Text(BytesText::new(region)))
            .expect("region text");
        writer
            .write_event(Event::End(BytesEnd::new("LocationConstraint")))
            .expect("end LocationConstraint");
    }

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── AccessControlPolicy ────────────────────────────────────────────

/// Render `<AccessControlPolicy>` XML for GetBucketAcl / GetObjectAcl.
pub fn render_access_control_policy(acl: &crate::metadata::store::Acl) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    let root = BytesStart::new("AccessControlPolicy")
        .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
    writer.write_event(Event::Start(root)).expect("start root");

    // <Owner>
    write_simple_element_group(
        &mut writer,
        "Owner",
        &[
            ("ID", &acl.owner.id),
            ("DisplayName", &acl.owner.display_name),
        ],
    );

    // <AccessControlList>
    writer
        .write_event(Event::Start(BytesStart::new("AccessControlList")))
        .expect("start AccessControlList");

    for grant in &acl.grants {
        writer
            .write_event(Event::Start(BytesStart::new("Grant")))
            .expect("start Grant");

        match &grant.grantee {
            crate::metadata::store::AclGrantee::CanonicalUser { id, display_name } => {
                // <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                let mut grantee_start = BytesStart::new("Grantee");
                grantee_start
                    .push_attribute(("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance"));
                grantee_start.push_attribute(("xsi:type", "CanonicalUser"));
                writer
                    .write_event(Event::Start(grantee_start))
                    .expect("start Grantee");

                write_text_element(&mut writer, "ID", id);
                write_text_element(&mut writer, "DisplayName", display_name);

                writer
                    .write_event(Event::End(BytesEnd::new("Grantee")))
                    .expect("end Grantee");
            }
            crate::metadata::store::AclGrantee::Group { uri } => {
                let mut grantee_start = BytesStart::new("Grantee");
                grantee_start
                    .push_attribute(("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance"));
                grantee_start.push_attribute(("xsi:type", "Group"));
                writer
                    .write_event(Event::Start(grantee_start))
                    .expect("start Grantee");

                write_text_element(&mut writer, "URI", uri);

                writer
                    .write_event(Event::End(BytesEnd::new("Grantee")))
                    .expect("end Grantee");
            }
        }

        write_text_element(&mut writer, "Permission", &grant.permission);

        writer
            .write_event(Event::End(BytesEnd::new("Grant")))
            .expect("end Grant");
    }

    writer
        .write_event(Event::End(BytesEnd::new("AccessControlList")))
        .expect("end AccessControlList");

    writer
        .write_event(Event::End(BytesEnd::new("AccessControlPolicy")))
        .expect("end root");

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── ListMultipartUploadsResult ──────────────────────────────────────

/// Represents a single upload entry in the ListMultipartUploads response.
pub struct UploadEntry<'a> {
    pub key: &'a str,
    pub upload_id: &'a str,
    pub initiated: &'a str,
    pub storage_class: &'a str,
    pub owner_id: &'a str,
    pub owner_display: &'a str,
}

/// Render `<ListMultipartUploadsResult>` for ListMultipartUploads.
#[allow(clippy::too_many_arguments)]
pub fn render_list_multipart_uploads_result(
    bucket: &str,
    key_marker: &str,
    upload_id_marker: &str,
    max_uploads: u32,
    is_truncated: bool,
    entries: &[UploadEntry<'_>],
    next_key_marker: Option<&str>,
    next_upload_id_marker: Option<&str>,
    prefix: &str,
) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    let root = BytesStart::new("ListMultipartUploadsResult")
        .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
    writer.write_event(Event::Start(root)).expect("start root");

    write_text_element(&mut writer, "Bucket", bucket);
    write_text_element(&mut writer, "KeyMarker", key_marker);
    write_text_element(&mut writer, "UploadIdMarker", upload_id_marker);
    if let Some(nkm) = next_key_marker {
        write_text_element(&mut writer, "NextKeyMarker", nkm);
    }
    if let Some(nuim) = next_upload_id_marker {
        write_text_element(&mut writer, "NextUploadIdMarker", nuim);
    }
    write_text_element(&mut writer, "MaxUploads", &max_uploads.to_string());
    write_text_element(
        &mut writer,
        "IsTruncated",
        if is_truncated { "true" } else { "false" },
    );
    if !prefix.is_empty() {
        write_text_element(&mut writer, "Prefix", prefix);
    }

    for entry in entries {
        writer
            .write_event(Event::Start(BytesStart::new("Upload")))
            .expect("start Upload");
        write_text_element(&mut writer, "Key", entry.key);
        write_text_element(&mut writer, "UploadId", entry.upload_id);

        // Initiator (same as Owner for simplicity)
        write_simple_element_group(
            &mut writer,
            "Initiator",
            &[("ID", entry.owner_id), ("DisplayName", entry.owner_display)],
        );

        // Owner
        write_simple_element_group(
            &mut writer,
            "Owner",
            &[("ID", entry.owner_id), ("DisplayName", entry.owner_display)],
        );

        write_text_element(&mut writer, "StorageClass", entry.storage_class);
        write_text_element(&mut writer, "Initiated", entry.initiated);
        writer
            .write_event(Event::End(BytesEnd::new("Upload")))
            .expect("end Upload");
    }

    writer
        .write_event(Event::End(BytesEnd::new("ListMultipartUploadsResult")))
        .expect("end root");

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── ListPartsResult ──────────────────────────────────────────────────

/// Represents a single part entry in the ListParts response.
pub struct PartEntry<'a> {
    pub part_number: u32,
    pub last_modified: &'a str,
    pub etag: &'a str,
    pub size: u64,
}

/// Render `<ListPartsResult>` for ListParts.
#[allow(clippy::too_many_arguments)]
pub fn render_list_parts_result(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number_marker: u32,
    max_parts: u32,
    is_truncated: bool,
    parts: &[PartEntry<'_>],
    next_part_number_marker: Option<u32>,
    storage_class: &str,
    owner_id: &str,
    owner_display: &str,
) -> String {
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .expect("xml decl");

    let root = BytesStart::new("ListPartsResult")
        .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]);
    writer.write_event(Event::Start(root)).expect("start root");

    write_text_element(&mut writer, "Bucket", bucket);
    write_text_element(&mut writer, "Key", key);
    write_text_element(&mut writer, "UploadId", upload_id);

    // Initiator (same as Owner for simplicity)
    write_simple_element_group(
        &mut writer,
        "Initiator",
        &[("ID", owner_id), ("DisplayName", owner_display)],
    );

    // Owner
    write_simple_element_group(
        &mut writer,
        "Owner",
        &[("ID", owner_id), ("DisplayName", owner_display)],
    );

    write_text_element(&mut writer, "StorageClass", storage_class);
    write_text_element(
        &mut writer,
        "PartNumberMarker",
        &part_number_marker.to_string(),
    );
    if let Some(npm) = next_part_number_marker {
        write_text_element(&mut writer, "NextPartNumberMarker", &npm.to_string());
    }
    write_text_element(&mut writer, "MaxParts", &max_parts.to_string());
    write_text_element(
        &mut writer,
        "IsTruncated",
        if is_truncated { "true" } else { "false" },
    );

    for part in parts {
        writer
            .write_event(Event::Start(BytesStart::new("Part")))
            .expect("start Part");
        write_text_element(&mut writer, "PartNumber", &part.part_number.to_string());
        write_text_element(&mut writer, "LastModified", part.last_modified);
        write_text_element(&mut writer, "ETag", part.etag);
        write_text_element(&mut writer, "Size", &part.size.to_string());
        writer
            .write_event(Event::End(BytesEnd::new("Part")))
            .expect("end Part");
    }

    writer
        .write_event(Event::End(BytesEnd::new("ListPartsResult")))
        .expect("end root");

    String::from_utf8(writer.into_inner().into_inner()).expect("valid utf-8")
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Write a `<tag>text</tag>` element.
fn write_text_element(writer: &mut Writer<Cursor<Vec<u8>>>, tag: &str, text: &str) {
    writer
        .write_event(Event::Start(BytesStart::new(tag)))
        .expect("start tag");
    writer
        .write_event(Event::Text(BytesText::new(text)))
        .expect("text");
    writer
        .write_event(Event::End(BytesEnd::new(tag)))
        .expect("end tag");
}

/// Write a parent element containing a flat list of child text elements.
///
/// ```xml
/// <parent>
///   <child1>value1</child1>
///   <child2>value2</child2>
/// </parent>
/// ```
fn write_simple_element_group(
    writer: &mut Writer<Cursor<Vec<u8>>>,
    parent: &str,
    children: &[(&str, &str)],
) {
    writer
        .write_event(Event::Start(BytesStart::new(parent)))
        .expect("start parent");
    for (tag, value) in children {
        write_text_element(writer, tag, value);
    }
    writer
        .write_event(Event::End(BytesEnd::new(parent)))
        .expect("end parent");
}
