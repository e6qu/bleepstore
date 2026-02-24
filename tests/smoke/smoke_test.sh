#!/usr/bin/env bash
#
# BleepStore Smoke Test — AWS CLI
#
# Quick validation that basic S3 operations work via the aws cli.
#
# Usage:
#   BLEEPSTORE_ENDPOINT=http://localhost:9000 ./smoke_test.sh
#
# Requires: aws cli v2, configured with BleepStore credentials

set -euo pipefail

ENDPOINT="${BLEEPSTORE_ENDPOINT:-http://localhost:9000}"
BUCKET="smoke-test-$(date +%s)"
AWS="aws --endpoint-url $ENDPOINT"

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

echo "BleepStore Smoke Test"
echo "Endpoint: $ENDPOINT"
echo "Bucket:   $BUCKET"
echo

cleanup() {
    echo
    echo "Cleaning up..."
    $AWS s3 rm "s3://$BUCKET" --recursive 2>/dev/null || true
    $AWS s3api delete-bucket --bucket "$BUCKET" 2>/dev/null || true
}
trap cleanup EXIT

# --- Bucket operations ---
echo "=== Bucket Operations ==="

$AWS s3api create-bucket --bucket "$BUCKET" 2>/dev/null && pass "create-bucket" || fail "create-bucket"
$AWS s3api head-bucket --bucket "$BUCKET" 2>/dev/null && pass "head-bucket" || fail "head-bucket"
$AWS s3api get-bucket-location --bucket "$BUCKET" >/dev/null 2>&1 && pass "get-bucket-location" || fail "get-bucket-location"
$AWS s3api list-buckets | grep -q "$BUCKET" && pass "list-buckets" || fail "list-buckets"

# --- Object operations ---
echo
echo "=== Object Operations ==="

# Create a test file
TMPFILE=$(mktemp)
echo "Hello, BleepStore!" > "$TMPFILE"

# PUT
$AWS s3 cp "$TMPFILE" "s3://$BUCKET/hello.txt" 2>/dev/null && pass "put-object (s3 cp)" || fail "put-object (s3 cp)"

# PUT via s3api
$AWS s3api put-object --bucket "$BUCKET" --key "api-test.txt" --body "$TMPFILE" >/dev/null 2>&1 && pass "put-object (s3api)" || fail "put-object (s3api)"

# GET
OUTFILE=$(mktemp)
$AWS s3 cp "s3://$BUCKET/hello.txt" "$OUTFILE" 2>/dev/null && pass "get-object (s3 cp)" || fail "get-object (s3 cp)"
diff -q "$TMPFILE" "$OUTFILE" >/dev/null && pass "get-object content match" || fail "get-object content match"

# HEAD
$AWS s3api head-object --bucket "$BUCKET" --key "hello.txt" >/dev/null 2>&1 && pass "head-object" || fail "head-object"

# LIST
$AWS s3 ls "s3://$BUCKET/" 2>/dev/null | grep -q "hello.txt" && pass "list-objects (s3 ls)" || fail "list-objects (s3 ls)"

# ListObjectsV2
$AWS s3api list-objects-v2 --bucket "$BUCKET" | grep -q "hello.txt" && pass "list-objects-v2" || fail "list-objects-v2"

# COPY
$AWS s3 cp "s3://$BUCKET/hello.txt" "s3://$BUCKET/hello-copy.txt" 2>/dev/null && pass "copy-object" || fail "copy-object"

# DELETE single
$AWS s3api delete-object --bucket "$BUCKET" --key "hello-copy.txt" 2>/dev/null && pass "delete-object" || fail "delete-object"

# DELETE multiple
$AWS s3api delete-objects --bucket "$BUCKET" --delete '{"Objects":[{"Key":"hello.txt"},{"Key":"api-test.txt"}]}' >/dev/null 2>&1 && pass "delete-objects" || fail "delete-objects"

# --- Multipart upload ---
echo
echo "=== Multipart Upload ==="

# Create a larger file (11 MB to require 2+ parts with 5MB minimum)
BIGFILE=$(mktemp)
dd if=/dev/urandom of="$BIGFILE" bs=1M count=11 2>/dev/null

# Use s3 cp which auto-selects multipart for large files
$AWS s3 cp "$BIGFILE" "s3://$BUCKET/big-file.bin" 2>/dev/null && pass "multipart upload (s3 cp)" || fail "multipart upload (s3 cp)"

# Verify download
BIGOUT=$(mktemp)
$AWS s3 cp "s3://$BUCKET/big-file.bin" "$BIGOUT" 2>/dev/null && pass "multipart download" || fail "multipart download"

if cmp -s "$BIGFILE" "$BIGOUT"; then
    pass "multipart content match"
else
    fail "multipart content match"
fi

# --- ACL operations ---
echo
echo "=== ACL Operations ==="

$AWS s3api put-object --bucket "$BUCKET" --key "acl-test.txt" --body "$TMPFILE" >/dev/null 2>&1
$AWS s3api get-object-acl --bucket "$BUCKET" --key "acl-test.txt" >/dev/null 2>&1 && pass "get-object-acl" || fail "get-object-acl"
$AWS s3api get-bucket-acl --bucket "$BUCKET" >/dev/null 2>&1 && pass "get-bucket-acl" || fail "get-bucket-acl"

# --- Error handling ---
echo
echo "=== Error Handling ==="

# NoSuchKey
$AWS s3api get-object --bucket "$BUCKET" --key "nonexistent" /dev/null 2>&1 && fail "nosuchkey (should fail)" || pass "nosuchkey returns error"

# NoSuchBucket
$AWS s3api head-bucket --bucket "nonexistent-bucket-xyz123" 2>&1 && fail "nosuchbucket (should fail)" || pass "nosuchbucket returns error"

# --- Summary ---
echo
rm -f "$TMPFILE" "$OUTFILE" "$BIGFILE" "$BIGOUT"
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
