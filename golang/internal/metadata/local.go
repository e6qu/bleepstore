package metadata

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bleepstore/bleepstore/internal/config"
)

type jsonlEntry struct {
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
	Deleted  bool            `json:"_deleted,omitempty"`
	Key      string          `json:"key,omitempty"`
	UploadID string          `json:"upload_id,omitempty"`
	Bucket   string          `json:"bucket,omitempty"`
}

type localBucketEntry struct {
	jsonlEntry
	Name string `json:"name"`
}

type localObjectEntry struct {
	jsonlEntry
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type localUploadEntry struct {
	jsonlEntry
	UploadID string `json:"upload_id"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
}

type localPartEntry struct {
	jsonlEntry
	UploadID   string `json:"upload_id"`
	PartNumber int    `json:"part_number"`
}

type localCredentialEntry struct {
	jsonlEntry
	AccessKeyID string `json:"access_key_id"`
}

type LocalStore struct {
	mu          sync.RWMutex
	rootDir     string
	compactOn   bool
	buckets     map[string]*BucketRecord
	objects     map[string]map[string]*ObjectRecord
	uploads     map[string]*MultipartUploadRecord
	parts       map[string]map[int]*PartRecord
	credentials map[string]*CredentialRecord
}

func NewLocalStore(cfg *config.LocalMetaConfig) (*LocalStore, error) {
	if cfg == nil {
		cfg = &config.LocalMetaConfig{}
	}
	if cfg.RootDir == "" {
		cfg.RootDir = "./data/metadata"
	}

	if err := os.MkdirAll(cfg.RootDir, 0755); err != nil {
		return nil, fmt.Errorf("creating metadata directory: %w", err)
	}

	s := &LocalStore{
		rootDir:     cfg.RootDir,
		compactOn:   cfg.CompactOnStartup,
		buckets:     make(map[string]*BucketRecord),
		objects:     make(map[string]map[string]*ObjectRecord),
		uploads:     make(map[string]*MultipartUploadRecord),
		parts:       make(map[string]map[int]*PartRecord),
		credentials: make(map[string]*CredentialRecord),
	}

	if err := s.loadAll(); err != nil {
		return nil, fmt.Errorf("loading metadata: %w", err)
	}

	if s.compactOn {
		if err := s.compact(); err != nil {
			return nil, fmt.Errorf("compacting metadata: %w", err)
		}
	}

	return s, nil
}

func (s *LocalStore) loadAll() error {
	if err := s.loadBuckets(); err != nil {
		return err
	}
	if err := s.loadObjects(); err != nil {
		return err
	}
	if err := s.loadUploads(); err != nil {
		return err
	}
	if err := s.loadParts(); err != nil {
		return err
	}
	if err := s.loadCredentials(); err != nil {
		return err
	}
	return nil
}

func (s *LocalStore) loadBuckets() error {
	path := filepath.Join(s.rootDir, "buckets.jsonl")
	return s.loadJSONLFile(path, func(entry jsonlEntry) error {
		if entry.Deleted {
			return nil
		}
		var bucket BucketRecord
		if err := json.Unmarshal(entry.Data, &bucket); err != nil {
			return err
		}
		s.buckets[bucket.Name] = &bucket
		return nil
	})
}

func (s *LocalStore) loadObjects() error {
	path := filepath.Join(s.rootDir, "objects.jsonl")
	return s.loadJSONLFile(path, func(entry jsonlEntry) error {
		if entry.Deleted {
			return nil
		}
		var obj ObjectRecord
		if err := json.Unmarshal(entry.Data, &obj); err != nil {
			return err
		}
		if s.objects[obj.Bucket] == nil {
			s.objects[obj.Bucket] = make(map[string]*ObjectRecord)
		}
		s.objects[obj.Bucket][obj.Key] = &obj
		return nil
	})
}

func (s *LocalStore) loadUploads() error {
	path := filepath.Join(s.rootDir, "uploads.jsonl")
	return s.loadJSONLFile(path, func(entry jsonlEntry) error {
		if entry.Deleted {
			return nil
		}
		var upload MultipartUploadRecord
		if err := json.Unmarshal(entry.Data, &upload); err != nil {
			return err
		}
		s.uploads[upload.UploadID] = &upload
		return nil
	})
}

func (s *LocalStore) loadParts() error {
	path := filepath.Join(s.rootDir, "parts.jsonl")
	return s.loadJSONLFile(path, func(entry jsonlEntry) error {
		if entry.Deleted {
			return nil
		}
		var part PartRecord
		if err := json.Unmarshal(entry.Data, &part); err != nil {
			return err
		}
		if s.parts[part.UploadID] == nil {
			s.parts[part.UploadID] = make(map[int]*PartRecord)
		}
		s.parts[part.UploadID][part.PartNumber] = &part
		return nil
	})
}

func (s *LocalStore) loadCredentials() error {
	path := filepath.Join(s.rootDir, "credentials.jsonl")
	return s.loadJSONLFile(path, func(entry jsonlEntry) error {
		if entry.Deleted {
			return nil
		}
		var cred CredentialRecord
		if err := json.Unmarshal(entry.Data, &cred); err != nil {
			return err
		}
		s.credentials[cred.AccessKeyID] = &cred
		return nil
	})
}

func (s *LocalStore) loadJSONLFile(path string, handler func(jsonlEntry) error) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry jsonlEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if err := handler(entry); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (s *LocalStore) appendEntry(filename string, entry interface{}) error {
	path := filepath.Join(s.rootDir, filename)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = f.Write(append(data, '\n'))
	return err
}

func (s *LocalStore) compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Compact buckets
	if err := s.compactBuckets("buckets.jsonl", s.buckets); err != nil {
		return err
	}

	// Compact objects (flatten nested map first)
	flattenedObjects := make(map[string]*ObjectRecord)
	for _, bucketObjs := range s.objects {
		for k, v := range bucketObjs {
			flattenedObjects[k] = v
		}
	}
	if err := s.compactObjects("objects.jsonl", flattenedObjects); err != nil {
		return err
	}

	// Compact uploads
	if err := s.compactUploads("uploads.jsonl", s.uploads); err != nil {
		return err
	}

	// Compact parts (flatten nested map first)
	flattenedParts := make(map[string]*PartRecord)
	for uploadID, uploadParts := range s.parts {
		for pn, part := range uploadParts {
			key := fmt.Sprintf("%s-%d", uploadID, pn)
			flattenedParts[key] = part
		}
	}
	if err := s.compactParts("parts.jsonl", flattenedParts); err != nil {
		return err
	}

	// Compact credentials
	if err := s.compactCredentials("credentials.jsonl", s.credentials); err != nil {
		return err
	}

	return nil
}

func (s *LocalStore) compactBuckets(filename string, data map[string]*BucketRecord) error {
	return s.writeCompactFile(filename, func(f *os.File) error {
		for _, bucket := range data {
			entry := jsonlEntry{Type: "bucket"}
			var err error
			entry.Data, err = json.Marshal(bucket)
			if err != nil {
				return err
			}
			if err := writeJSONLLine(f, entry); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *LocalStore) compactObjects(filename string, data map[string]*ObjectRecord) error {
	return s.writeCompactFile(filename, func(f *os.File) error {
		for _, obj := range data {
			entry := jsonlEntry{Type: "object", Bucket: obj.Bucket, Key: obj.Key}
			var err error
			entry.Data, err = json.Marshal(obj)
			if err != nil {
				return err
			}
			if err := writeJSONLLine(f, entry); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *LocalStore) compactUploads(filename string, data map[string]*MultipartUploadRecord) error {
	return s.writeCompactFile(filename, func(f *os.File) error {
		for _, upload := range data {
			entry := jsonlEntry{Type: "upload", UploadID: upload.UploadID, Bucket: upload.Bucket, Key: upload.Key}
			var err error
			entry.Data, err = json.Marshal(upload)
			if err != nil {
				return err
			}
			if err := writeJSONLLine(f, entry); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *LocalStore) compactParts(filename string, data map[string]*PartRecord) error {
	return s.writeCompactFile(filename, func(f *os.File) error {
		for _, part := range data {
			entry := jsonlEntry{Type: "part", UploadID: part.UploadID}
			var err error
			entry.Data, err = json.Marshal(part)
			if err != nil {
				return err
			}
			if err := writeJSONLLine(f, entry); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *LocalStore) compactCredentials(filename string, data map[string]*CredentialRecord) error {
	return s.writeCompactFile(filename, func(f *os.File) error {
		for _, cred := range data {
			entry := jsonlEntry{Type: "credential"}
			var err error
			entry.Data, err = json.Marshal(cred)
			if err != nil {
				return err
			}
			if err := writeJSONLLine(f, entry); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *LocalStore) writeCompactFile(filename string, writeFunc func(*os.File) error) error {
	path := filepath.Join(s.rootDir, filename)
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if err := writeFunc(f); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	f.Close()

	return os.Rename(tmpPath, path)
}

func writeJSONLLine(f *os.File, entry jsonlEntry) error {
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = f.Write(append(line, '\n'))
	return err
}

func (s *LocalStore) Ping(ctx context.Context) error {
	return nil
}

func (s *LocalStore) Close() error {
	return nil
}

func (s *LocalStore) CreateBucket(ctx context.Context, bucket *BucketRecord) error {
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

	data, _ := json.Marshal(&bucketCopy)
	entry := jsonlEntry{Type: "bucket", Data: data}
	return s.appendEntry("buckets.jsonl", entry)
}

func (s *LocalStore) GetBucket(ctx context.Context, name string) (*BucketRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucket, exists := s.buckets[name]
	if !exists {
		return nil, nil
	}
	bucketCopy := *bucket
	return &bucketCopy, nil
}

func (s *LocalStore) DeleteBucket(ctx context.Context, name string) error {
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

	entry := jsonlEntry{Type: "bucket", Deleted: true, Key: name}
	return s.appendEntry("buckets.jsonl", entry)
}

func (s *LocalStore) ListBuckets(ctx context.Context, owner string) ([]BucketRecord, error) {
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

func (s *LocalStore) BucketExists(ctx context.Context, name string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.buckets[name]
	return exists, nil
}

func (s *LocalStore) UpdateBucketAcl(ctx context.Context, name string, acl json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucket, exists := s.buckets[name]
	if !exists {
		return fmt.Errorf("bucket not found: %s", name)
	}

	bucket.ACL = acl

	data, _ := json.Marshal(bucket)
	entry := jsonlEntry{Type: "bucket", Data: data}
	return s.appendEntry("buckets.jsonl", entry)
}

func (s *LocalStore) PutObject(ctx context.Context, obj *ObjectRecord) error {
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

	data, _ := json.Marshal(&objCopy)
	entry := jsonlEntry{Type: "object", Data: data, Bucket: obj.Bucket, Key: obj.Key}
	return s.appendEntry("objects.jsonl", entry)
}

func (s *LocalStore) GetObject(ctx context.Context, bucket, key string) (*ObjectRecord, error) {
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

func (s *LocalStore) DeleteObject(ctx context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if bucketObjects, exists := s.objects[bucket]; exists {
		delete(bucketObjects, key)
	}

	entry := jsonlEntry{Type: "object", Deleted: true, Bucket: bucket, Key: key}
	return s.appendEntry("objects.jsonl", entry)
}

func (s *LocalStore) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if bucketObjects, exists := s.objects[bucket]; exists {
		_, exists = bucketObjects[key]
		return exists, nil
	}
	return false, nil
}

func (s *LocalStore) DeleteObjectsMeta(ctx context.Context, bucket string, keys []string) ([]string, []error) {
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

		entry := jsonlEntry{Type: "object", Deleted: true, Bucket: bucket, Key: key}
		if err := s.appendEntry("objects.jsonl", entry); err != nil {
			errs = append(errs, err)
		}
	}

	return deleted, errs
}

func (s *LocalStore) UpdateObjectAcl(ctx context.Context, bucket, key string, acl json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if bucketObjects, exists := s.objects[bucket]; exists {
		if obj, exists := bucketObjects[key]; exists {
			obj.ACL = acl

			data, _ := json.Marshal(obj)
			entry := jsonlEntry{Type: "object", Data: data, Bucket: bucket, Key: key}
			return s.appendEntry("objects.jsonl", entry)
		}
	}
	return fmt.Errorf("object not found: %s/%s", bucket, key)
}

func (s *LocalStore) ListObjects(ctx context.Context, bucket string, opts ListObjectsOptions) (*ListObjectsResult, error) {
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

func (s *LocalStore) CreateMultipartUpload(ctx context.Context, upload *MultipartUploadRecord) (string, error) {
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

	data, _ := json.Marshal(&uploadCopy)
	entry := jsonlEntry{Type: "upload", Data: data, UploadID: uploadID, Bucket: upload.Bucket, Key: upload.Key}
	if err := s.appendEntry("uploads.jsonl", entry); err != nil {
		return "", err
	}

	return uploadID, nil
}

func (s *LocalStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUploadRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	upload, exists := s.uploads[uploadID]
	if !exists || upload.Bucket != bucket || upload.Key != key {
		return nil, nil
	}

	uploadCopy := *upload
	return &uploadCopy, nil
}

func (s *LocalStore) PutPart(ctx context.Context, part *PartRecord) error {
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

	data, _ := json.Marshal(&partCopy)
	entry := jsonlEntry{Type: "part", Data: data, UploadID: part.UploadID}
	return s.appendEntry("parts.jsonl", entry)
}

func (s *LocalStore) ListParts(ctx context.Context, uploadID string, opts ListPartsOptions) (*ListPartsResult, error) {
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

func (s *LocalStore) GetPartsForCompletion(ctx context.Context, uploadID string, partNumbers []int) ([]PartRecord, error) {
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

func (s *LocalStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, obj *ObjectRecord) error {
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

	objData, _ := json.Marshal(&objCopy)
	objEntry := jsonlEntry{Type: "object", Data: objData, Bucket: obj.Bucket, Key: obj.Key}
	if err := s.appendEntry("objects.jsonl", objEntry); err != nil {
		return err
	}

	uploadEntry := jsonlEntry{Type: "upload", Deleted: true, UploadID: uploadID, Bucket: bucket, Key: key}
	if err := s.appendEntry("uploads.jsonl", uploadEntry); err != nil {
		return err
	}

	delete(s.parts, uploadID)
	delete(s.uploads, uploadID)

	return nil
}

func (s *LocalStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	upload, exists := s.uploads[uploadID]
	if !exists || upload.Bucket != bucket || upload.Key != key {
		return fmt.Errorf("upload not found: %s", uploadID)
	}

	entry := jsonlEntry{Type: "upload", Deleted: true, UploadID: uploadID, Bucket: bucket, Key: key}
	if err := s.appendEntry("uploads.jsonl", entry); err != nil {
		return err
	}

	delete(s.parts, uploadID)
	delete(s.uploads, uploadID)

	return nil
}

func (s *LocalStore) ListMultipartUploads(ctx context.Context, bucket string, opts ListUploadsOptions) (*ListUploadsResult, error) {
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

func (s *LocalStore) GetCredential(ctx context.Context, accessKeyID string) (*CredentialRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cred, exists := s.credentials[accessKeyID]
	if !exists {
		return nil, nil
	}

	credCopy := *cred
	return &credCopy, nil
}

func (s *LocalStore) PutCredential(ctx context.Context, cred *CredentialRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	credCopy := *cred
	s.credentials[cred.AccessKeyID] = &credCopy

	data, _ := json.Marshal(&credCopy)
	entry := jsonlEntry{Type: "credential", Data: data}
	return s.appendEntry("credentials.jsonl", entry)
}

func (s *LocalStore) ReapExpiredUploads(ttlSeconds int) ([]ExpiredUpload, error) {
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

			entry := jsonlEntry{Type: "upload", Deleted: true, UploadID: uploadID, Bucket: upload.Bucket, Key: upload.Key}
			s.appendEntry("uploads.jsonl", entry)

			delete(s.parts, uploadID)
			delete(s.uploads, uploadID)
		}
	}

	return expired, nil
}
