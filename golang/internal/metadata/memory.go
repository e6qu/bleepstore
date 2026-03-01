package metadata

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type MemoryStore struct {
	mu          sync.RWMutex
	buckets     map[string]*BucketRecord
	objects     map[string]map[string]*ObjectRecord
	uploads     map[string]*MultipartUploadRecord
	parts       map[string]map[int]*PartRecord
	credentials map[string]*CredentialRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		buckets:     make(map[string]*BucketRecord),
		objects:     make(map[string]map[string]*ObjectRecord),
		uploads:     make(map[string]*MultipartUploadRecord),
		parts:       make(map[string]map[int]*PartRecord),
		credentials: make(map[string]*CredentialRecord),
	}
}

func (s *MemoryStore) Ping(ctx context.Context) error {
	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}

func (s *MemoryStore) CreateBucket(ctx context.Context, bucket *BucketRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[bucket.Name]; exists {
		return fmt.Errorf("bucket already exists: %s", bucket.Name)
	}

	bucketCopy := *bucket
	if bucketCopy.ACL == nil {
		bucketCopy.ACL = json.RawMessage("{}")
	}
	s.buckets[bucket.Name] = &bucketCopy
	return nil
}

func (s *MemoryStore) GetBucket(ctx context.Context, name string) (*BucketRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucket, exists := s.buckets[name]
	if !exists {
		return nil, nil
	}
	bucketCopy := *bucket
	return &bucketCopy, nil
}

func (s *MemoryStore) DeleteBucket(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[name]; !exists {
		return fmt.Errorf("bucket not found: %s", name)
	}

	if objects, exists := s.objects[name]; exists && len(objects) > 0 {
		return fmt.Errorf("bucket not empty: %s", name)
	}

	for _, upload := range s.uploads {
		if upload.Bucket == name {
			return fmt.Errorf("bucket not empty: %s", name)
		}
	}

	delete(s.buckets, name)
	return nil
}

func (s *MemoryStore) ListBuckets(ctx context.Context, owner string) ([]BucketRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buckets []BucketRecord
	for _, bucket := range s.buckets {
		if bucket.OwnerID == owner {
			bucketCopy := *bucket
			buckets = append(buckets, bucketCopy)
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func (s *MemoryStore) BucketExists(ctx context.Context, name string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.buckets[name]
	return exists, nil
}

func (s *MemoryStore) UpdateBucketAcl(ctx context.Context, name string, acl json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucket, exists := s.buckets[name]
	if !exists {
		return fmt.Errorf("bucket not found: %s", name)
	}

	bucket.ACL = acl
	return nil
}

func (s *MemoryStore) PutObject(ctx context.Context, obj *ObjectRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[obj.Bucket]; !exists {
		return fmt.Errorf("bucket not found: %s", obj.Bucket)
	}

	if s.objects[obj.Bucket] == nil {
		s.objects[obj.Bucket] = make(map[string]*ObjectRecord)
	}

	objCopy := *obj
	if objCopy.ContentType == "" {
		objCopy.ContentType = "application/octet-stream"
	}
	if objCopy.StorageClass == "" {
		objCopy.StorageClass = "STANDARD"
	}
	if objCopy.ACL == nil {
		objCopy.ACL = json.RawMessage("{}")
	}
	if objCopy.UserMetadata == nil {
		objCopy.UserMetadata = make(map[string]string)
	}

	s.objects[obj.Bucket][obj.Key] = &objCopy
	return nil
}

func (s *MemoryStore) GetObject(ctx context.Context, bucket, key string) (*ObjectRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if bucketObjects, exists := s.objects[bucket]; exists {
		if obj, exists := bucketObjects[key]; exists {
			objCopy := *obj
			return &objCopy, nil
		}
	}
	return nil, nil
}

func (s *MemoryStore) DeleteObject(ctx context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if bucketObjects, exists := s.objects[bucket]; exists {
		delete(bucketObjects, key)
	}
	return nil
}

func (s *MemoryStore) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if bucketObjects, exists := s.objects[bucket]; exists {
		_, exists = bucketObjects[key]
		return exists, nil
	}
	return false, nil
}

func (s *MemoryStore) DeleteObjectsMeta(ctx context.Context, bucket string, keys []string) ([]string, []error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted []string
	var errs []error

	bucketObjects, exists := s.objects[bucket]
	if !exists {
		return keys, nil
	}

	for _, key := range keys {
		delete(bucketObjects, key)
		deleted = append(deleted, key)
	}

	return deleted, errs
}

func (s *MemoryStore) UpdateObjectAcl(ctx context.Context, bucket, key string, acl json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if bucketObjects, exists := s.objects[bucket]; exists {
		if obj, exists := bucketObjects[key]; exists {
			obj.ACL = acl
			return nil
		}
	}
	return fmt.Errorf("object not found: %s/%s", bucket, key)
}

func (s *MemoryStore) ListObjects(ctx context.Context, bucket string, opts ListObjectsOptions) (*ListObjectsResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

	var allObjects []ObjectRecord
	bucketObjects, exists := s.objects[bucket]
	if !exists {
		return &ListObjectsResult{}, nil
	}

	for _, obj := range bucketObjects {
		if opts.Prefix != "" && !strings.HasPrefix(obj.Key, opts.Prefix) {
			continue
		}
		if startAfter != "" && obj.Key <= startAfter {
			continue
		}
		objCopy := *obj
		allObjects = append(allObjects, objCopy)
	}

	sort.Slice(allObjects, func(i, j int) bool {
		return allObjects[i].Key < allObjects[j].Key
	})

	if opts.Delimiter == "" {
		isTruncated := len(allObjects) > maxKeys
		if isTruncated {
			allObjects = allObjects[:maxKeys]
		}
		result := &ListObjectsResult{
			Objects:     allObjects,
			IsTruncated: isTruncated,
		}
		if isTruncated && len(allObjects) > 0 {
			lastKey := allObjects[len(allObjects)-1].Key
			result.NextMarker = lastKey
			result.NextContinuationToken = lastKey
		}
		return result, nil
	}

	var objects []ObjectRecord
	prefixSet := make(map[string]bool)

	for _, obj := range allObjects {
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
	}

	var commonPrefixes []string
	for p := range prefixSet {
		commonPrefixes = append(commonPrefixes, p)
	}
	sort.Strings(commonPrefixes)

	totalEntries := len(objects) + len(commonPrefixes)
	isTruncated := totalEntries > maxKeys

	if isTruncated {
		type entry struct {
			key      string
			isPrefix bool
		}
		var entries []entry
		for _, obj := range objects {
			entries = append(entries, entry{key: obj.Key, isPrefix: false})
		}
		for _, p := range commonPrefixes {
			entries = append(entries, entry{key: p, isPrefix: true})
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].key < entries[j].key
		})

		if len(entries) > maxKeys {
			entries = entries[:maxKeys]
		}

		objects = nil
		prefixSet = make(map[string]bool)
		for _, e := range entries {
			if e.isPrefix {
				prefixSet[e.key] = true
			} else {
				for _, obj := range allObjects {
					if obj.Key == e.key {
						objects = append(objects, obj)
						break
					}
				}
			}
		}
		commonPrefixes = nil
		for p := range prefixSet {
			commonPrefixes = append(commonPrefixes, p)
		}
		sort.Strings(commonPrefixes)
	}

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

func (s *MemoryStore) CreateMultipartUpload(ctx context.Context, upload *MultipartUploadRecord) (string, error) {
	uploadID := upload.UploadID
	if uploadID == "" {
		var err error
		uploadID, err = generateUploadID()
		if err != nil {
			return "", err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[upload.Bucket]; !exists {
		return "", fmt.Errorf("bucket not found: %s", upload.Bucket)
	}

	uploadCopy := *upload
	uploadCopy.UploadID = uploadID
	if uploadCopy.ContentType == "" {
		uploadCopy.ContentType = "application/octet-stream"
	}
	if uploadCopy.StorageClass == "" {
		uploadCopy.StorageClass = "STANDARD"
	}
	if uploadCopy.ACL == nil {
		uploadCopy.ACL = json.RawMessage("{}")
	}
	if uploadCopy.UserMetadata == nil {
		uploadCopy.UserMetadata = make(map[string]string)
	}

	s.uploads[uploadID] = &uploadCopy
	return uploadID, nil
}

func (s *MemoryStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUploadRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	upload, exists := s.uploads[uploadID]
	if !exists || upload.Bucket != bucket || upload.Key != key {
		return nil, nil
	}

	uploadCopy := *upload
	return &uploadCopy, nil
}

func (s *MemoryStore) PutPart(ctx context.Context, part *PartRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.uploads[part.UploadID]; !exists {
		return fmt.Errorf("upload not found: %s", part.UploadID)
	}

	if s.parts[part.UploadID] == nil {
		s.parts[part.UploadID] = make(map[int]*PartRecord)
	}

	partCopy := *part
	s.parts[part.UploadID][part.PartNumber] = &partCopy
	return nil
}

func (s *MemoryStore) ListParts(ctx context.Context, uploadID string, opts ListPartsOptions) (*ListPartsResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	maxParts := opts.MaxParts
	if maxParts <= 0 {
		maxParts = 1000
	}

	uploadParts, exists := s.parts[uploadID]
	if !exists {
		return &ListPartsResult{}, nil
	}

	var parts []PartRecord
	for pn, part := range uploadParts {
		if pn <= opts.PartNumberMarker {
			continue
		}
		partCopy := *part
		parts = append(parts, partCopy)
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

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

func (s *MemoryStore) GetPartsForCompletion(ctx context.Context, uploadID string, partNumbers []int) ([]PartRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	uploadParts, exists := s.parts[uploadID]
	if !exists {
		return nil, nil
	}

	var parts []PartRecord
	for _, pn := range partNumbers {
		if part, exists := uploadParts[pn]; exists {
			partCopy := *part
			parts = append(parts, partCopy)
		}
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

func (s *MemoryStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, obj *ObjectRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.uploads[uploadID]; !exists {
		return fmt.Errorf("upload not found: %s", uploadID)
	}

	if s.objects[obj.Bucket] == nil {
		s.objects[obj.Bucket] = make(map[string]*ObjectRecord)
	}

	objCopy := *obj
	if objCopy.ContentType == "" {
		objCopy.ContentType = "application/octet-stream"
	}
	if objCopy.StorageClass == "" {
		objCopy.StorageClass = "STANDARD"
	}
	if objCopy.ACL == nil {
		objCopy.ACL = json.RawMessage("{}")
	}
	if objCopy.UserMetadata == nil {
		objCopy.UserMetadata = make(map[string]string)
	}

	s.objects[obj.Bucket][obj.Key] = &objCopy

	delete(s.parts, uploadID)
	delete(s.uploads, uploadID)

	return nil
}

func (s *MemoryStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	upload, exists := s.uploads[uploadID]
	if !exists || upload.Bucket != bucket || upload.Key != key {
		return fmt.Errorf("upload not found: %s", uploadID)
	}

	delete(s.parts, uploadID)
	delete(s.uploads, uploadID)

	return nil
}

func (s *MemoryStore) ListMultipartUploads(ctx context.Context, bucket string, opts ListUploadsOptions) (*ListUploadsResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	maxUploads := opts.MaxUploads
	if maxUploads <= 0 {
		maxUploads = 1000
	}

	var allUploads []MultipartUploadRecord
	for _, upload := range s.uploads {
		if upload.Bucket != bucket {
			continue
		}
		if opts.Prefix != "" && !strings.HasPrefix(upload.Key, opts.Prefix) {
			continue
		}
		if opts.KeyMarker != "" {
			if upload.Key < opts.KeyMarker {
				continue
			}
			if upload.Key == opts.KeyMarker && opts.UploadIDMarker != "" && upload.UploadID <= opts.UploadIDMarker {
				continue
			}
		}
		uploadCopy := *upload
		allUploads = append(allUploads, uploadCopy)
	}

	sort.Slice(allUploads, func(i, j int) bool {
		if allUploads[i].Key != allUploads[j].Key {
			return allUploads[i].Key < allUploads[j].Key
		}
		return allUploads[i].InitiatedAt.Before(allUploads[j].InitiatedAt)
	})

	isTruncated := len(allUploads) > maxUploads
	if isTruncated {
		allUploads = allUploads[:maxUploads]
	}

	result := &ListUploadsResult{
		Uploads:     allUploads,
		IsTruncated: isTruncated,
	}
	if isTruncated && len(allUploads) > 0 {
		last := allUploads[len(allUploads)-1]
		result.NextKeyMarker = last.Key
		result.NextUploadIDMarker = last.UploadID
	}

	return result, nil
}

func (s *MemoryStore) GetCredential(ctx context.Context, accessKeyID string) (*CredentialRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cred, exists := s.credentials[accessKeyID]
	if !exists {
		return nil, nil
	}

	credCopy := *cred
	return &credCopy, nil
}

func (s *MemoryStore) PutCredential(ctx context.Context, cred *CredentialRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	credCopy := *cred
	s.credentials[cred.AccessKeyID] = &credCopy
	return nil
}

func (s *MemoryStore) ReapExpiredUploads(ttlSeconds int) ([]ExpiredUpload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-time.Duration(ttlSeconds) * time.Second)
	var expired []ExpiredUpload

	for uploadID, upload := range s.uploads {
		if upload.InitiatedAt.Before(cutoff) {
			expired = append(expired, ExpiredUpload{
				UploadID:   uploadID,
				BucketName: upload.Bucket,
				ObjectKey:  upload.Key,
			})
			delete(s.parts, uploadID)
			delete(s.uploads, uploadID)
		}
	}

	return expired, nil
}

func generateMemoryUploadID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating upload ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}
