// Package xmlutil provides helpers for rendering S3-compatible XML responses.
package xmlutil

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	s3err "github.com/bleepstore/bleepstore/internal/errors"
)

// s3NS is the S3 XML namespace URI used in all success response root elements.
const s3NS = "http://s3.amazonaws.com/doc/2006-03-01/"

// xmlHeader is the standard XML declaration prepended to all responses.
const xmlHeader = `<?xml version="1.0" encoding="UTF-8"?>` + "\n"

// ErrorResponse is the XML structure for S3 error responses.
// Note: Error XML has NO xmlns namespace (unlike success responses).
type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

// Owner represents an S3 bucket or object owner.
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// Bucket represents a single bucket in a ListBuckets response.
type Bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ListAllMyBucketsResult is the XML structure for ListBuckets responses.
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets []Bucket `xml:"Buckets>Bucket"`
}

// Object represents a single object in a list objects response.
type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *Owner `xml:"Owner,omitempty"`
}

// CommonPrefix represents a common prefix in a list objects response.
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// ListBucketResult is the XML structure for ListObjects (v1) responses.
type ListBucketResult struct {
	XMLName        xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	EncodingType   string         `xml:"EncodingType,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []Object       `xml:"Contents"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`
}

// ListBucketV2Result is the XML structure for ListObjectsV2 responses.
type ListBucketV2Result struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []Object       `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes"`
}

// CopyObjectResult is the XML structure for CopyObject responses.
type CopyObjectResult struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// InitiateMultipartUploadResult is the XML response for CreateMultipartUpload.
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// CompleteMultipartUploadResult is the XML response for CompleteMultipartUpload.
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// Part represents a single part in a multipart upload listing.
type Part struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

// ListPartsResult is the XML response for ListParts.
type ListPartsResult struct {
	XMLName              xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult"`
	Bucket               string   `xml:"Bucket"`
	Key                  string   `xml:"Key"`
	UploadID             string   `xml:"UploadId"`
	PartNumberMarker     int      `xml:"PartNumberMarker"`
	NextPartNumberMarker int      `xml:"NextPartNumberMarker"`
	MaxParts             int      `xml:"MaxParts"`
	IsTruncated          bool     `xml:"IsTruncated"`
	Parts                []Part   `xml:"Part"`
}

// Upload represents a single in-progress multipart upload.
type Upload struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiator Owner  `xml:"Initiator"`
	Owner     Owner  `xml:"Owner"`
	Initiated string `xml:"Initiated"`
}

// ListMultipartUploadsResult is the XML response for ListMultipartUploads.
type ListMultipartUploadsResult struct {
	XMLName            xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult"`
	Bucket             string         `xml:"Bucket"`
	KeyMarker          string         `xml:"KeyMarker"`
	UploadIDMarker     string         `xml:"UploadIdMarker"`
	NextKeyMarker      string         `xml:"NextKeyMarker"`
	NextUploadIDMarker string         `xml:"NextUploadIdMarker"`
	MaxUploads         int            `xml:"MaxUploads"`
	EncodingType       string         `xml:"EncodingType,omitempty"`
	IsTruncated        bool           `xml:"IsTruncated"`
	Uploads            []Upload       `xml:"Upload"`
	CommonPrefixes     []CommonPrefix `xml:"CommonPrefixes"`
}

// CopyPartResult is the XML response for UploadPartCopy.
type CopyPartResult struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyPartResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

// DeleteRequest is the XML structure for the Delete Objects request body.
type DeleteRequest struct {
	XMLName xml.Name           `xml:"Delete"`
	Quiet   bool               `xml:"Quiet"`
	Objects []DeleteRequestObj `xml:"Object"`
}

// DeleteRequestObj represents a single object to delete in a DeleteObjects request.
type DeleteRequestObj struct {
	Key string `xml:"Key"`
}

// DeleteResult is the XML response for DeleteObjects (multi-object delete).
type DeleteResult struct {
	XMLName xml.Name      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`
	Deleted []DeletedItem `xml:"Deleted"`
	Errors  []DeleteError `xml:"Error"`
}

// DeletedItem represents a successfully deleted object.
type DeletedItem struct {
	Key string `xml:"Key"`
}

// DeleteError represents a failed deletion in a multi-object delete.
type DeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// LocationConstraint is the XML response for GetBucketLocation.
type LocationConstraint struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
	Location string   `xml:",chardata"`
}

// AccessControlPolicy is the XML structure for ACL responses.
type AccessControlPolicy struct {
	XMLName           xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlPolicy"`
	Owner             Owner    `xml:"Owner"`
	AccessControlList ACL      `xml:"AccessControlList"`
}

// ACL holds the list of grants in an access control policy.
type ACL struct {
	Grants []Grant `xml:"Grant"`
}

// Grant represents a single ACL grant.
type Grant struct {
	Grantee    Grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

// Grantee represents the entity receiving an ACL grant.
// It uses a custom MarshalXML to produce the xmlns:xsi and xsi:type attributes
// that S3 clients expect.
type Grantee struct {
	XMLName     xml.Name `xml:"Grantee"`
	Type        string   `xml:"-"` // Rendered via custom MarshalXML
	ID          string   `xml:"ID,omitempty"`
	DisplayName string   `xml:"DisplayName,omitempty"`
	URI         string   `xml:"URI,omitempty"`
}

// MarshalXML customizes XML marshaling for Grantee to include the xmlns:xsi
// and xsi:type attributes expected by S3 clients.
func (g Grantee) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name = xml.Name{Local: "Grantee"}
	start.Attr = []xml.Attr{
		{Name: xml.Name{Local: "xmlns:xsi"}, Value: "http://www.w3.org/2001/XMLSchema-instance"},
		{Name: xml.Name{Local: "xsi:type"}, Value: g.Type},
	}

	// Define an alias type to avoid infinite recursion.
	type granteeContent struct {
		ID          string `xml:"ID,omitempty"`
		DisplayName string `xml:"DisplayName,omitempty"`
		URI         string `xml:"URI,omitempty"`
	}

	return e.EncodeElement(granteeContent{
		ID:          g.ID,
		DisplayName: g.DisplayName,
		URI:         g.URI,
	}, start)
}

// UnmarshalXML customizes XML unmarshaling for Grantee to extract the
// xsi:type attribute that S3 clients send.
func (g *Grantee) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Extract xsi:type from attributes.
	for _, attr := range start.Attr {
		if attr.Name.Local == "type" {
			g.Type = attr.Value
		}
	}

	// Decode child elements.
	type granteeContent struct {
		ID          string `xml:"ID"`
		DisplayName string `xml:"DisplayName"`
		URI         string `xml:"URI"`
	}
	var content granteeContent
	if err := d.DecodeElement(&content, &start); err != nil {
		return err
	}
	g.ID = content.ID
	g.DisplayName = content.DisplayName
	g.URI = content.URI
	return nil
}

// RenderError writes an S3 error XML response to the given ResponseWriter.
// The requestID parameter should match the x-amz-request-id header value.
func RenderError(w http.ResponseWriter, r *http.Request, s3Err *s3err.S3Error, resource string) {
	// Get the request ID that was set by the common headers middleware.
	requestID := w.Header().Get("x-amz-request-id")

	resp := ErrorResponse{
		Code:      s3Err.Code,
		Message:   s3Err.Message,
		Resource:  resource,
		RequestID: requestID,
	}
	writeXML(w, s3Err.HTTPStatus, resp)
}

// WriteErrorResponse is a convenience function that renders an S3 error
// using the request path as the resource.
func WriteErrorResponse(w http.ResponseWriter, r *http.Request, s3Err *s3err.S3Error) {
	RenderError(w, r, s3Err, r.URL.Path)
}

// RenderListBuckets writes a ListAllMyBucketsResult XML response.
func RenderListBuckets(w http.ResponseWriter, result *ListAllMyBucketsResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderListObjects writes a ListBucketResult XML response.
func RenderListObjects(w http.ResponseWriter, result *ListBucketResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderListObjectsV2 writes a ListBucketV2Result XML response.
func RenderListObjectsV2(w http.ResponseWriter, result *ListBucketV2Result) {
	writeXML(w, http.StatusOK, result)
}

// RenderCopyObject writes a CopyObjectResult XML response.
func RenderCopyObject(w http.ResponseWriter, result *CopyObjectResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderInitiateMultipartUpload writes an InitiateMultipartUploadResult XML response.
func RenderInitiateMultipartUpload(w http.ResponseWriter, result *InitiateMultipartUploadResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderCompleteMultipartUpload writes a CompleteMultipartUploadResult XML response.
func RenderCompleteMultipartUpload(w http.ResponseWriter, result *CompleteMultipartUploadResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderListParts writes a ListPartsResult XML response.
func RenderListParts(w http.ResponseWriter, result *ListPartsResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderListMultipartUploads writes a ListMultipartUploadsResult XML response.
func RenderListMultipartUploads(w http.ResponseWriter, result *ListMultipartUploadsResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderCopyPartResult writes a CopyPartResult XML response.
func RenderCopyPartResult(w http.ResponseWriter, result *CopyPartResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderDeleteResult writes a DeleteResult XML response.
func RenderDeleteResult(w http.ResponseWriter, result *DeleteResult) {
	writeXML(w, http.StatusOK, result)
}

// RenderLocationConstraint writes a LocationConstraint XML response.
func RenderLocationConstraint(w http.ResponseWriter, location string) {
	result := LocationConstraint{Location: location}
	writeXML(w, http.StatusOK, result)
}

// RenderAccessControlPolicy writes an AccessControlPolicy XML response.
func RenderAccessControlPolicy(w http.ResponseWriter, acp *AccessControlPolicy) {
	writeXML(w, http.StatusOK, acp)
}

// FormatTimeS3 formats a time.Time as an S3-compatible ISO 8601 string
// with millisecond precision (e.g., "2006-01-02T15:04:05.000Z").
func FormatTimeS3(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// FormatTimeHTTP formats a time.Time as an HTTP date per RFC 7231
// (e.g., "Mon, 02 Jan 2006 15:04:05 GMT").
func FormatTimeHTTP(t time.Time) string {
	return t.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
}

// EncodeKeyURL returns the URL-encoded version of the key if encodingType is "url",
// otherwise returns the key unchanged.
func EncodeKeyURL(key string, encodingType string) string {
	if encodingType != "url" {
		return key
	}
	return url.QueryEscape(key)
}

// writeXML marshals v as XML and writes it to w with the given HTTP status code.
func writeXML(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	io.WriteString(w, xmlHeader)
	enc := xml.NewEncoder(w)
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(w, "<!-- XML encoding error: %v -->", err)
	}
}
