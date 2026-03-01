package metadata

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/bleepstore/bleepstore/internal/config"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	firestoreTimeFormat = "2006-01-02T15:04:05.000Z"
)

type FirestoreStore struct {
	client     *firestore.Client
	collection string
}

func encodeKey(key string) string {
	return base64.URLEncoding.EncodeToString([]byte(key))
}

func decodeKey(encoded string) string {
	padding := 4 - len(encoded)%4
	if padding != 4 {
		encoded += strings.Repeat("=", padding)
	}
	decoded, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return encoded
	}
	return string(decoded)
}

func docIDBucket(bucket string) string {
	return "bucket_" + bucket
}

func docIDObject(bucket, key string) string {
	return "object_" + bucket + "_" + encodeKey(key)
}

func docIDUpload(uploadID string) string {
	return "upload_" + uploadID
}

func docIDPart(partNumber int) string {
	return fmt.Sprintf("part_%05d", partNumber)
}

func docIDCredential(accessKey string) string {
	return "cred_" + accessKey
}

func NewFirestoreStore(ctx context.Context, cfg *config.FirestoreConfig) (*FirestoreStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("firestore config is required")
	}

	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	client, err := firestore.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating firestore client: %w", err)
	}

	collection := cfg.Collection
	if collection == "" {
		collection = "bleepstore"
	}

	return &FirestoreStore{
		client:     client,
		collection: collection,
	}, nil
}

func (s *FirestoreStore) collectionRef() *firestore.CollectionRef {
	return s.client.Collection(s.collection)
}

func (s *FirestoreStore) Ping(ctx context.Context) error {
	_, err := s.collectionRef().Limit(1).Documents(ctx).Next()
	if err != nil && err != iterator.Done {
		return err
	}
	return nil
}

func (s *FirestoreStore) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func firestoreNow() string {
	return time.Now().UTC().Format(firestoreTimeFormat)
}

func (s *FirestoreStore) CreateBucket(ctx context.Context, bucket *BucketRecord) error {
	acl := "{}"
	if bucket.ACL != nil {
		acl = string(bucket.ACL)
	}

	docRef := s.collectionRef().Doc(docIDBucket(bucket.Name))
	_, err := docRef.Set(ctx, map[string]interface{}{
		"type":          "bucket",
		"name":          bucket.Name,
		"region":        bucket.Region,
		"owner_id":      bucket.OwnerID,
		"owner_display": bucket.OwnerDisplay,
		"acl":           acl,
		"created_at":    bucket.CreatedAt.UTC().Format(firestoreTimeFormat),
	})
	return err
}

func (s *FirestoreStore) GetBucket(ctx context.Context, name string) (*BucketRecord, error) {
	docRef := s.collectionRef().Doc(docIDBucket(name))
	doc, err := docRef.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("getting bucket: %w", err)
	}
	if !doc.Exists() {
		return nil, nil
	}

	return s.docToBucket(doc.Data()), nil
}

func (s *FirestoreStore) DeleteBucket(ctx context.Context, name string) error {
	docRef := s.collectionRef().Doc(docIDBucket(name))
	_, err := docRef.Delete(ctx)
	return err
}

func (s *FirestoreStore) ListBuckets(ctx context.Context, owner string) ([]BucketRecord, error) {
	query := s.collectionRef().Where("type", "==", "bucket")
	if owner != "" {
		query = query.Where("owner_id", "==", owner)
	}

	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("listing buckets: %w", err)
	}

	var buckets []BucketRecord
	for _, doc := range docs {
		buckets = append(buckets, *s.docToBucket(doc.Data()))
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func (s *FirestoreStore) BucketExists(ctx context.Context, name string) (bool, error) {
	docRef := s.collectionRef().Doc(docIDBucket(name))
	doc, err := docRef.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		return false, fmt.Errorf("checking bucket exists: %w", err)
	}
	return doc.Exists(), nil
}

func (s *FirestoreStore) UpdateBucketAcl(ctx context.Context, name string, acl json.RawMessage) error {
	docRef := s.collectionRef().Doc(docIDBucket(name))
	_, err := docRef.Update(ctx, []firestore.Update{
		{Path: "acl", Value: string(acl)},
	})
	return err
}

func (s *FirestoreStore) PutObject(ctx context.Context, obj *ObjectRecord) error {
	acl := "{}"
	if obj.ACL != nil {
		acl = string(obj.ACL)
	}
	userMeta := "{}"
	if obj.UserMetadata != nil {
		b, _ := json.Marshal(obj.UserMetadata)
		userMeta = string(b)
	}
	contentType := obj.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	storageClass := obj.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	data := map[string]interface{}{
		"type":          "object",
		"bucket":        obj.Bucket,
		"key":           obj.Key,
		"size":          obj.Size,
		"etag":          obj.ETag,
		"content_type":  contentType,
		"storage_class": storageClass,
		"acl":           acl,
		"user_metadata": userMeta,
		"last_modified": obj.LastModified.UTC().Format(firestoreTimeFormat),
	}

	if obj.ContentEncoding != "" {
		data["content_encoding"] = obj.ContentEncoding
	}
	if obj.ContentLanguage != "" {
		data["content_language"] = obj.ContentLanguage
	}
	if obj.ContentDisposition != "" {
		data["content_disposition"] = obj.ContentDisposition
	}
	if obj.CacheControl != "" {
		data["cache_control"] = obj.CacheControl
	}
	if obj.Expires != "" {
		data["expires"] = obj.Expires
	}

	docRef := s.collectionRef().Doc(docIDObject(obj.Bucket, obj.Key))
	_, err := docRef.Set(ctx, data)
	return err
}

func (s *FirestoreStore) GetObject(ctx context.Context, bucket, key string) (*ObjectRecord, error) {
	docRef := s.collectionRef().Doc(docIDObject(bucket, key))
	doc, err := docRef.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("getting object: %w", err)
	}
	if !doc.Exists() {
		return nil, nil
	}
	return s.docToObject(doc.Data()), nil
}

func (s *FirestoreStore) DeleteObject(ctx context.Context, bucket, key string) error {
	docRef := s.collectionRef().Doc(docIDObject(bucket, key))
	_, err := docRef.Delete(ctx)
	return err
}

func (s *FirestoreStore) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	docRef := s.collectionRef().Doc(docIDObject(bucket, key))
	doc, err := docRef.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		return false, fmt.Errorf("checking object exists: %w", err)
	}
	return doc.Exists(), nil
}

func (s *FirestoreStore) DeleteObjectsMeta(ctx context.Context, bucket string, keys []string) ([]string, []error) {
	if len(keys) == 0 {
		return nil, nil
	}

	var deleted []string
	var errs []error

	batch := s.client.Batch()
	for _, key := range keys {
		docRef := s.collectionRef().Doc(docIDObject(bucket, key))
		batch.Delete(docRef)
		deleted = append(deleted, key)
	}

	_, err := batch.Commit(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	return deleted, errs
}

func (s *FirestoreStore) UpdateObjectAcl(ctx context.Context, bucket, key string, acl json.RawMessage) error {
	docRef := s.collectionRef().Doc(docIDObject(bucket, key))
	_, err := docRef.Update(ctx, []firestore.Update{
		{Path: "acl", Value: string(acl)},
	})
	return err
}

func (s *FirestoreStore) ListObjects(ctx context.Context, bucket string, opts ListObjectsOptions) (*ListObjectsResult, error) {
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	startAfter := opts.StartAfter
	if opts.ContinuationToken != "" {
		startAfter = opts.ContinuationToken
	}
	if opts.Marker != "" && startAfter == "" {
		startAfter = opts.Marker
	}

	query := s.collectionRef().
		Where("type", "==", "object").
		Where("bucket", "==", bucket).
		OrderBy("key", firestore.Asc)

	if startAfter != "" {
		query = query.StartAfter(startAfter)
	}

	query = query.Limit(maxKeys + 1)

	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("listing objects: %w", err)
	}

	var items []ObjectRecord
	for _, doc := range docs {
		obj := s.docToObject(doc.Data())
		if opts.Prefix != "" && !strings.HasPrefix(obj.Key, opts.Prefix) {
			continue
		}
		items = append(items, *obj)
	}

	if opts.Delimiter == "" {
		isTruncated := len(items) > maxKeys
		if isTruncated {
			items = items[:maxKeys]
		}
		result := &ListObjectsResult{
			Objects:     items,
			IsTruncated: isTruncated,
		}
		if isTruncated && len(items) > 0 {
			lastKey := items[len(items)-1].Key
			result.NextMarker = lastKey
			result.NextContinuationToken = lastKey
		}
		return result, nil
	}

	var objects []ObjectRecord
	prefixSet := make(map[string]bool)

	for _, obj := range items {
		keyAfterPrefix := obj.Key
		if opts.Prefix != "" {
			keyAfterPrefix = obj.Key[len(opts.Prefix):]
		}

		delimIdx := strings.Index(keyAfterPrefix, opts.Delimiter)
		if delimIdx >= 0 {
			commonPrefix := opts.Prefix + keyAfterPrefix[:delimIdx+len(opts.Delimiter)]
			prefixSet[commonPrefix] = true
		} else {
			objects = append(objects, obj)
		}

		if len(objects)+len(prefixSet) >= maxKeys {
			break
		}
	}

	var commonPrefixes []string
	for p := range prefixSet {
		commonPrefixes = append(commonPrefixes, p)
	}
	sort.Strings(commonPrefixes)

	isTruncated := len(objects)+len(commonPrefixes) > maxKeys || len(items) > maxKeys

	result := &ListObjectsResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    isTruncated,
	}
	if isTruncated {
		var lastKey string
		if len(objects) > 0 {
			lastKey = objects[len(objects)-1].Key
		}
		if len(commonPrefixes) > 0 {
			lastPrefix := commonPrefixes[len(commonPrefixes)-1]
			if lastPrefix > lastKey {
				lastKey = lastPrefix
			}
		}
		result.NextMarker = lastKey
		result.NextContinuationToken = lastKey
	}

	return result, nil
}

func (s *FirestoreStore) CreateMultipartUpload(ctx context.Context, upload *MultipartUploadRecord) (string, error) {
	uploadID := upload.UploadID
	if uploadID == "" {
		var err error
		uploadID, err = generateUploadID()
		if err != nil {
			return "", err
		}
	}

	acl := "{}"
	if upload.ACL != nil {
		acl = string(upload.ACL)
	}
	userMeta := "{}"
	if upload.UserMetadata != nil {
		b, _ := json.Marshal(upload.UserMetadata)
		userMeta = string(b)
	}
	contentType := upload.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	storageClass := upload.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	data := map[string]interface{}{
		"type":          "upload",
		"upload_id":     uploadID,
		"bucket":        upload.Bucket,
		"key":           upload.Key,
		"content_type":  contentType,
		"storage_class": storageClass,
		"acl":           acl,
		"user_metadata": userMeta,
		"owner_id":      upload.OwnerID,
		"owner_display": upload.OwnerDisplay,
		"initiated_at":  upload.InitiatedAt.UTC().Format(firestoreTimeFormat),
	}

	if upload.ContentEncoding != "" {
		data["content_encoding"] = upload.ContentEncoding
	}
	if upload.ContentLanguage != "" {
		data["content_language"] = upload.ContentLanguage
	}
	if upload.ContentDisposition != "" {
		data["content_disposition"] = upload.ContentDisposition
	}
	if upload.CacheControl != "" {
		data["cache_control"] = upload.CacheControl
	}
	if upload.Expires != "" {
		data["expires"] = upload.Expires
	}

	docRef := s.collectionRef().Doc(docIDUpload(uploadID))
	_, err := docRef.Set(ctx, data)
	if err != nil {
		return "", fmt.Errorf("creating multipart upload: %w", err)
	}

	return uploadID, nil
}

func (s *FirestoreStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUploadRecord, error) {
	docRef := s.collectionRef().Doc(docIDUpload(uploadID))
	doc, err := docRef.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("getting multipart upload: %w", err)
	}
	if !doc.Exists() {
		return nil, nil
	}

	upload := s.docToUpload(doc.Data())
	if upload.Bucket != bucket || upload.Key != key {
		return nil, nil
	}
	return upload, nil
}

func (s *FirestoreStore) PutPart(ctx context.Context, part *PartRecord) error {
	uploadRef := s.collectionRef().Doc(docIDUpload(part.UploadID))
	partRef := uploadRef.Collection("parts").Doc(docIDPart(part.PartNumber))

	_, err := partRef.Set(ctx, map[string]interface{}{
		"type":          "part",
		"upload_id":     part.UploadID,
		"part_number":   part.PartNumber,
		"size":          part.Size,
		"etag":          part.ETag,
		"last_modified": part.LastModified.UTC().Format(firestoreTimeFormat),
	})
	return err
}

func (s *FirestoreStore) ListParts(ctx context.Context, uploadID string, opts ListPartsOptions) (*ListPartsResult, error) {
	maxParts := opts.MaxParts
	if maxParts <= 0 {
		maxParts = 1000
	}

	uploadRef := s.collectionRef().Doc(docIDUpload(uploadID))
	partsRef := uploadRef.Collection("parts")

	query := partsRef.OrderBy("part_number", firestore.Asc)

	if opts.PartNumberMarker > 0 {
		query = query.StartAfter(opts.PartNumberMarker)
	}

	query = query.Limit(maxParts + 1)

	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("listing parts: %w", err)
	}

	var parts []PartRecord
	for _, doc := range docs {
		parts = append(parts, *s.docToPart(doc.Data()))
	}

	isTruncated := len(parts) > maxParts
	if isTruncated {
		parts = parts[:maxParts]
	}

	result := &ListPartsResult{
		Parts:       parts,
		IsTruncated: isTruncated,
	}
	if isTruncated && len(parts) > 0 {
		result.NextPartNumberMarker = parts[len(parts)-1].PartNumber
	}

	return result, nil
}

func (s *FirestoreStore) GetPartsForCompletion(ctx context.Context, uploadID string, partNumbers []int) ([]PartRecord, error) {
	uploadRef := s.collectionRef().Doc(docIDUpload(uploadID))
	partsRef := uploadRef.Collection("parts")

	query := partsRef.OrderBy("part_number", firestore.Asc)

	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("getting parts: %w", err)
	}

	pnSet := make(map[int]bool)
	if len(partNumbers) > 0 {
		for _, pn := range partNumbers {
			pnSet[pn] = true
		}
	}

	var parts []PartRecord
	for _, doc := range docs {
		p := s.docToPart(doc.Data())
		if len(partNumbers) == 0 || pnSet[p.PartNumber] {
			parts = append(parts, *p)
		}
	}

	return parts, nil
}

func (s *FirestoreStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, obj *ObjectRecord) error {
	if err := s.PutObject(ctx, obj); err != nil {
		return fmt.Errorf("putting completed object: %w", err)
	}

	uploadRef := s.collectionRef().Doc(docIDUpload(uploadID))

	parts, _ := s.GetPartsForCompletion(ctx, uploadID, nil)

	batch := s.client.Batch()
	for _, part := range parts {
		partRef := uploadRef.Collection("parts").Doc(docIDPart(part.PartNumber))
		batch.Delete(partRef)
	}
	batch.Delete(uploadRef)

	_, err := batch.Commit(ctx)
	return err
}

func (s *FirestoreStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	uploadRef := s.collectionRef().Doc(docIDUpload(uploadID))

	parts, _ := s.GetPartsForCompletion(ctx, uploadID, nil)

	batch := s.client.Batch()
	for _, part := range parts {
		partRef := uploadRef.Collection("parts").Doc(docIDPart(part.PartNumber))
		batch.Delete(partRef)
	}
	batch.Delete(uploadRef)

	_, err := batch.Commit(ctx)
	return err
}

func (s *FirestoreStore) ListMultipartUploads(ctx context.Context, bucket string, opts ListUploadsOptions) (*ListUploadsResult, error) {
	maxUploads := opts.MaxUploads
	if maxUploads <= 0 {
		maxUploads = 1000
	}

	query := s.collectionRef().
		Where("type", "==", "upload").
		Where("bucket", "==", bucket)

	if opts.Prefix != "" {
		query = query.Where("key", ">=", opts.Prefix).
			Where("key", "<", opts.Prefix+"\uf8ff")
	}

	query = query.OrderBy("key", firestore.Asc).OrderBy("upload_id", firestore.Asc)

	if opts.KeyMarker != "" || opts.UploadIDMarker != "" {
		query = query.StartAfter(opts.KeyMarker, opts.UploadIDMarker)
	}

	query = query.Limit(maxUploads + 1)

	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("listing multipart uploads: %w", err)
	}

	var uploads []MultipartUploadRecord
	for _, doc := range docs {
		uploads = append(uploads, *s.docToUpload(doc.Data()))
	}

	isTruncated := len(uploads) > maxUploads
	if isTruncated {
		uploads = uploads[:maxUploads]
	}

	result := &ListUploadsResult{
		Uploads:     uploads,
		IsTruncated: isTruncated,
	}
	if isTruncated && len(uploads) > 0 {
		last := uploads[len(uploads)-1]
		result.NextKeyMarker = last.Key
		result.NextUploadIDMarker = last.UploadID
	}

	return result, nil
}

func (s *FirestoreStore) GetCredential(ctx context.Context, accessKeyID string) (*CredentialRecord, error) {
	docRef := s.collectionRef().Doc(docIDCredential(accessKeyID))
	doc, err := docRef.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("getting credential: %w", err)
	}
	if !doc.Exists() {
		return nil, nil
	}

	cred := s.docToCredential(doc.Data())
	if !cred.Active {
		return nil, nil
	}
	return cred, nil
}

func (s *FirestoreStore) PutCredential(ctx context.Context, cred *CredentialRecord) error {
	docRef := s.collectionRef().Doc(docIDCredential(cred.AccessKeyID))

	_, err := docRef.Set(ctx, map[string]interface{}{
		"type":          "credential",
		"access_key_id": cred.AccessKeyID,
		"secret_key":    cred.SecretKey,
		"owner_id":      cred.OwnerID,
		"display_name":  cred.DisplayName,
		"active":        cred.Active,
		"created_at":    cred.CreatedAt.UTC().Format(firestoreTimeFormat),
	})
	return err
}

func (s *FirestoreStore) ReapExpiredUploads(ttlSeconds int) ([]ExpiredUpload, error) {
	cutoff := time.Now().Add(-time.Duration(ttlSeconds) * time.Second).UTC().Format(firestoreTimeFormat)

	ctx := context.Background()

	query := s.collectionRef().
		Where("type", "==", "upload").
		Where("initiated_at", "<", cutoff)

	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("querying expired uploads: %w", err)
	}

	var reaped []ExpiredUpload
	for _, doc := range docs {
		upload := s.docToUpload(doc.Data())
		uploadID := upload.UploadID
		bucket := upload.Bucket
		key := upload.Key

		uploadRef := s.collectionRef().Doc(docIDUpload(uploadID))

		parts, _ := s.GetPartsForCompletion(ctx, uploadID, nil)

		batch := s.client.Batch()
		for _, part := range parts {
			partRef := uploadRef.Collection("parts").Doc(docIDPart(part.PartNumber))
			batch.Delete(partRef)
		}
		batch.Delete(uploadRef)
		batch.Commit(ctx)

		reaped = append(reaped, ExpiredUpload{
			UploadID:   uploadID,
			BucketName: bucket,
			ObjectKey:  key,
		})
	}

	return reaped, nil
}

func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getInt64FromMap(m map[string]interface{}, key string) int64 {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int64:
			return n
		case int:
			return int64(n)
		case float64:
			return int64(n)
		}
	}
	return 0
}

func getIntFromMap(m map[string]interface{}, key string) int {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		}
	}
	return 0
}

func getBoolFromMap(m map[string]interface{}, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func (s *FirestoreStore) docToBucket(m map[string]interface{}) *BucketRecord {
	createdAt, _ := time.Parse(firestoreTimeFormat, getStringFromMap(m, "created_at"))
	return &BucketRecord{
		Name:         getStringFromMap(m, "name"),
		Region:       getStringFromMap(m, "region"),
		OwnerID:      getStringFromMap(m, "owner_id"),
		OwnerDisplay: getStringFromMap(m, "owner_display"),
		ACL:          json.RawMessage(getStringFromMap(m, "acl")),
		CreatedAt:    createdAt,
	}
}

func (s *FirestoreStore) docToObject(m map[string]interface{}) *ObjectRecord {
	lastModified, _ := time.Parse(firestoreTimeFormat, getStringFromMap(m, "last_modified"))
	obj := &ObjectRecord{
		Bucket:             getStringFromMap(m, "bucket"),
		Key:                getStringFromMap(m, "key"),
		Size:               getInt64FromMap(m, "size"),
		ETag:               getStringFromMap(m, "etag"),
		ContentType:        getStringFromMap(m, "content_type"),
		ContentEncoding:    getStringFromMap(m, "content_encoding"),
		ContentLanguage:    getStringFromMap(m, "content_language"),
		ContentDisposition: getStringFromMap(m, "content_disposition"),
		CacheControl:       getStringFromMap(m, "cache_control"),
		Expires:            getStringFromMap(m, "expires"),
		StorageClass:       getStringFromMap(m, "storage_class"),
		ACL:                json.RawMessage(getStringFromMap(m, "acl")),
		LastModified:       lastModified,
	}
	userMeta := getStringFromMap(m, "user_metadata")
	if userMeta != "" && userMeta != "{}" {
		obj.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(userMeta), &obj.UserMetadata)
	}
	return obj
}

func (s *FirestoreStore) docToUpload(m map[string]interface{}) *MultipartUploadRecord {
	initiatedAt, _ := time.Parse(firestoreTimeFormat, getStringFromMap(m, "initiated_at"))
	upload := &MultipartUploadRecord{
		UploadID:           getStringFromMap(m, "upload_id"),
		Bucket:             getStringFromMap(m, "bucket"),
		Key:                getStringFromMap(m, "key"),
		ContentType:        getStringFromMap(m, "content_type"),
		ContentEncoding:    getStringFromMap(m, "content_encoding"),
		ContentLanguage:    getStringFromMap(m, "content_language"),
		ContentDisposition: getStringFromMap(m, "content_disposition"),
		CacheControl:       getStringFromMap(m, "cache_control"),
		Expires:            getStringFromMap(m, "expires"),
		StorageClass:       getStringFromMap(m, "storage_class"),
		ACL:                json.RawMessage(getStringFromMap(m, "acl")),
		OwnerID:            getStringFromMap(m, "owner_id"),
		OwnerDisplay:       getStringFromMap(m, "owner_display"),
		InitiatedAt:        initiatedAt,
	}
	userMeta := getStringFromMap(m, "user_metadata")
	if userMeta != "" && userMeta != "{}" {
		upload.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(userMeta), &upload.UserMetadata)
	}
	return upload
}

func (s *FirestoreStore) docToPart(m map[string]interface{}) *PartRecord {
	lastModified, _ := time.Parse(firestoreTimeFormat, getStringFromMap(m, "last_modified"))
	return &PartRecord{
		UploadID:     getStringFromMap(m, "upload_id"),
		PartNumber:   getIntFromMap(m, "part_number"),
		Size:         getInt64FromMap(m, "size"),
		ETag:         getStringFromMap(m, "etag"),
		LastModified: lastModified,
	}
}

func (s *FirestoreStore) docToCredential(m map[string]interface{}) *CredentialRecord {
	createdAt, _ := time.Parse(firestoreTimeFormat, getStringFromMap(m, "created_at"))
	return &CredentialRecord{
		AccessKeyID: getStringFromMap(m, "access_key_id"),
		SecretKey:   getStringFromMap(m, "secret_key"),
		OwnerID:     getStringFromMap(m, "owner_id"),
		DisplayName: getStringFromMap(m, "display_name"),
		Active:      getBoolFromMap(m, "active"),
		CreatedAt:   createdAt,
	}
}
