package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/bleepstore/bleepstore/internal/config"
)

const (
	cosmosTimeFormat = "2006-01-02T15:04:05.000Z"
)

type CosmosStore struct {
	client    *azcosmos.ContainerClient
	database  string
	container string
}

func docIDBucketCosmos(bucket string) string {
	return "bucket_" + bucket
}

func docIDObjectCosmos(bucket, key string) string {
	return "object_" + bucket + "_" + key
}

func docIDUploadCosmos(uploadID string) string {
	return "upload_" + uploadID
}

func docIDPartCosmos(uploadID string, partNumber int) string {
	return fmt.Sprintf("part_%s_%05d", uploadID, partNumber)
}

func docIDCredentialCosmos(accessKey string) string {
	return "cred_" + accessKey
}

func NewCosmosStore(ctx context.Context, cfg *config.CosmosConfig) (*CosmosStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cosmos config is required")
	}
	if cfg.Endpoint == "" && cfg.MasterKey == "" {
		return nil, fmt.Errorf("cosmos endpoint or master key is required")
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("cosmos database name is required")
	}
	if cfg.Container == "" {
		return nil, fmt.Errorf("cosmos container name is required")
	}

	var cred azcosmos.KeyCredential
	if cfg.MasterKey != "" {
		var err error
		cred, err = azcosmos.NewKeyCredential(cfg.MasterKey)
		if err != nil {
			return nil, fmt.Errorf("creating cosmos key credential: %w", err)
		}
	}

	client, err := azcosmos.NewClientWithKey(cfg.Endpoint, cred, &azcosmos.ClientOptions{
		ClientOptions: policy.ClientOptions{},
	})
	if err != nil {
		return nil, fmt.Errorf("creating cosmos client: %w", err)
	}

	dbClient, err := client.NewDatabase(cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("getting database client: %w", err)
	}

	containerClient, err := dbClient.NewContainer(cfg.Container)
	if err != nil {
		return nil, fmt.Errorf("getting container client: %w", err)
	}

	return &CosmosStore{
		client:    containerClient,
		database:  cfg.Database,
		container: cfg.Container,
	}, nil
}

func (s *CosmosStore) Ping(ctx context.Context) error {
	_, err := s.client.Read(ctx, nil)
	return err
}

func (s *CosmosStore) Close() error {
	return nil
}

func cosmosNow() string {
	return time.Now().UTC().Format(cosmosTimeFormat)
}

type cosmosItem struct {
	ID                 string                 `json:"id"`
	Type               string                 `json:"type"`
	Name               string                 `json:"name,omitempty"`
	Region             string                 `json:"region,omitempty"`
	OwnerID            string                 `json:"owner_id,omitempty"`
	OwnerDisplay       string                 `json:"owner_display,omitempty"`
	ACL                string                 `json:"acl,omitempty"`
	CreatedAt          string                 `json:"created_at,omitempty"`
	Bucket             string                 `json:"bucket,omitempty"`
	Key                string                 `json:"key,omitempty"`
	Size               int64                  `json:"size,omitempty"`
	ETag               string                 `json:"etag,omitempty"`
	ContentType        string                 `json:"content_type,omitempty"`
	ContentEncoding    string                 `json:"content_encoding,omitempty"`
	ContentLanguage    string                 `json:"content_language,omitempty"`
	ContentDisposition string                 `json:"content_disposition,omitempty"`
	CacheControl       string                 `json:"cache_control,omitempty"`
	Expires            string                 `json:"expires,omitempty"`
	StorageClass       string                 `json:"storage_class,omitempty"`
	UserMetadata       string                 `json:"user_metadata,omitempty"`
	LastModified       string                 `json:"last_modified,omitempty"`
	DeleteMarker       bool                   `json:"delete_marker,omitempty"`
	UploadID           string                 `json:"upload_id,omitempty"`
	PartNumber         int                    `json:"part_number,omitempty"`
	InitiatedAt        string                 `json:"initiated_at,omitempty"`
	AccessKeyID        string                 `json:"access_key_id,omitempty"`
	SecretKey          string                 `json:"secret_key,omitempty"`
	DisplayName        string                 `json:"display_name,omitempty"`
	Active             bool                   `json:"active,omitempty"`
	Extra              map[string]interface{} `json:"-"`
}

func (s *CosmosStore) CreateBucket(ctx context.Context, bucket *BucketRecord) error {
	acl := "{}"
	if bucket.ACL != nil {
		acl = string(bucket.ACL)
	}

	item := &cosmosItem{
		ID:           docIDBucketCosmos(bucket.Name),
		Type:         "bucket",
		Name:         bucket.Name,
		Region:       bucket.Region,
		OwnerID:      bucket.OwnerID,
		OwnerDisplay: bucket.OwnerDisplay,
		ACL:          acl,
		CreatedAt:    bucket.CreatedAt.UTC().Format(cosmosTimeFormat),
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshaling bucket: %w", err)
	}

	_, err = s.client.CreateItem(ctx, azcosmos.NewPartitionKeyString("bucket"), data, nil)
	return err
}

func (s *CosmosStore) GetBucket(ctx context.Context, name string) (*BucketRecord, error) {
	resp, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKeyString("bucket"), docIDBucketCosmos(name), nil)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return nil, nil
		}
		return nil, fmt.Errorf("getting bucket: %w", err)
	}

	var item cosmosItem
	if err := json.Unmarshal(resp.Value, &item); err != nil {
		return nil, fmt.Errorf("unmarshaling bucket: %w", err)
	}

	createdAt, _ := time.Parse(cosmosTimeFormat, item.CreatedAt)
	return &BucketRecord{
		Name:         item.Name,
		Region:       item.Region,
		OwnerID:      item.OwnerID,
		OwnerDisplay: item.OwnerDisplay,
		ACL:          json.RawMessage(item.ACL),
		CreatedAt:    createdAt,
	}, nil
}

func (s *CosmosStore) DeleteBucket(ctx context.Context, name string) error {
	_, err := s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("bucket"), docIDBucketCosmos(name), nil)
	if err != nil && !strings.Contains(err.Error(), "NotFound") {
		return err
	}
	return nil
}

func (s *CosmosStore) ListBuckets(ctx context.Context, owner string) ([]BucketRecord, error) {
	query := "SELECT * FROM c WHERE c.type = 'bucket'"
	var params []azcosmos.QueryParameter
	if owner != "" {
		query += " AND c.owner_id = @owner_id"
		params = append(params, azcosmos.QueryParameter{Name: "@owner_id", Value: owner})
	}

	pager := s.client.NewQueryItemsPager(query, azcosmos.NewPartitionKeyString("bucket"), &azcosmos.QueryOptions{
		QueryParameters: params,
	})

	var buckets []BucketRecord
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing buckets: %w", err)
		}
		for _, item := range resp.Items {
			var ci cosmosItem
			if err := json.Unmarshal(item, &ci); err != nil {
				continue
			}
			createdAt, _ := time.Parse(cosmosTimeFormat, ci.CreatedAt)
			buckets = append(buckets, BucketRecord{
				Name:         ci.Name,
				Region:       ci.Region,
				OwnerID:      ci.OwnerID,
				OwnerDisplay: ci.OwnerDisplay,
				ACL:          json.RawMessage(ci.ACL),
				CreatedAt:    createdAt,
			})
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func (s *CosmosStore) BucketExists(ctx context.Context, name string) (bool, error) {
	_, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKeyString("bucket"), docIDBucketCosmos(name), nil)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *CosmosStore) UpdateBucketAcl(ctx context.Context, name string, acl json.RawMessage) error {
	resp, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKeyString("bucket"), docIDBucketCosmos(name), nil)
	if err != nil {
		return fmt.Errorf("reading bucket: %w", err)
	}

	var item cosmosItem
	if err := json.Unmarshal(resp.Value, &item); err != nil {
		return fmt.Errorf("unmarshaling bucket: %w", err)
	}

	item.ACL = string(acl)
	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshaling bucket: %w", err)
	}

	_, err = s.client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString("bucket"), docIDBucketCosmos(name), data, nil)
	return err
}

func (s *CosmosStore) PutObject(ctx context.Context, obj *ObjectRecord) error {
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

	item := &cosmosItem{
		ID:                 docIDObjectCosmos(obj.Bucket, obj.Key),
		Type:               "object",
		Bucket:             obj.Bucket,
		Key:                obj.Key,
		Size:               obj.Size,
		ETag:               obj.ETag,
		ContentType:        contentType,
		ContentEncoding:    obj.ContentEncoding,
		ContentLanguage:    obj.ContentLanguage,
		ContentDisposition: obj.ContentDisposition,
		CacheControl:       obj.CacheControl,
		Expires:            obj.Expires,
		StorageClass:       storageClass,
		ACL:                acl,
		UserMetadata:       userMeta,
		LastModified:       obj.LastModified.UTC().Format(cosmosTimeFormat),
		DeleteMarker:       obj.DeleteMarker,
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshaling object: %w", err)
	}

	_, err = s.client.UpsertItem(ctx, azcosmos.NewPartitionKeyString("object"), data, nil)
	return err
}

func (s *CosmosStore) GetObject(ctx context.Context, bucket, key string) (*ObjectRecord, error) {
	resp, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKeyString("object"), docIDObjectCosmos(bucket, key), nil)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return nil, nil
		}
		return nil, fmt.Errorf("getting object: %w", err)
	}

	var item cosmosItem
	if err := json.Unmarshal(resp.Value, &item); err != nil {
		return nil, fmt.Errorf("unmarshaling object: %w", err)
	}

	return s.itemToObject(&item), nil
}

func (s *CosmosStore) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("object"), docIDObjectCosmos(bucket, key), nil)
	if err != nil && !strings.Contains(err.Error(), "NotFound") {
		return err
	}
	return nil
}

func (s *CosmosStore) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKeyString("object"), docIDObjectCosmos(bucket, key), nil)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *CosmosStore) DeleteObjectsMeta(ctx context.Context, bucket string, keys []string) ([]string, []error) {
	if len(keys) == 0 {
		return nil, nil
	}

	var deleted []string
	var errs []error

	for _, key := range keys {
		_, err := s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("object"), docIDObjectCosmos(bucket, key), nil)
		if err != nil && !strings.Contains(err.Error(), "NotFound") {
			errs = append(errs, err)
			continue
		}
		deleted = append(deleted, key)
	}

	return deleted, errs
}

func (s *CosmosStore) UpdateObjectAcl(ctx context.Context, bucket, key string, acl json.RawMessage) error {
	resp, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKeyString("object"), docIDObjectCosmos(bucket, key), nil)
	if err != nil {
		return fmt.Errorf("reading object: %w", err)
	}

	var item cosmosItem
	if err := json.Unmarshal(resp.Value, &item); err != nil {
		return fmt.Errorf("unmarshaling object: %w", err)
	}

	item.ACL = string(acl)
	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshaling object: %w", err)
	}

	_, err = s.client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString("object"), docIDObjectCosmos(bucket, key), data, nil)
	return err
}

func (s *CosmosStore) ListObjects(ctx context.Context, bucket string, opts ListObjectsOptions) (*ListObjectsResult, error) {
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

	query := "SELECT * FROM c WHERE c.type = 'object' AND c.bucket = @bucket"
	params := []azcosmos.QueryParameter{
		{Name: "@bucket", Value: bucket},
	}

	if opts.Prefix != "" {
		query += " AND STARTSWITH(c.id, @prefix)"
		prefixID := "object_" + bucket + "_" + opts.Prefix
		params = append(params, azcosmos.QueryParameter{Name: "@prefix", Value: prefixID})
	}
	if startAfter != "" {
		query += " AND c.id > @start_after"
		params = append(params, azcosmos.QueryParameter{Name: "@start_after", Value: docIDObjectCosmos(bucket, startAfter)})
	}

	query += " ORDER BY c.id"

	pager := s.client.NewQueryItemsPager(query, azcosmos.NewPartitionKeyString("object"), &azcosmos.QueryOptions{
		QueryParameters: params,
		PageSizeHint:    int32(maxKeys + 1),
	})

	var items []ObjectRecord
	for pager.More() && len(items) <= maxKeys {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing objects: %w", err)
		}
		for _, item := range resp.Items {
			var ci cosmosItem
			if err := json.Unmarshal(item, &ci); err != nil {
				continue
			}
			items = append(items, *s.itemToObject(&ci))
			if len(items) > maxKeys {
				break
			}
		}
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

func (s *CosmosStore) CreateMultipartUpload(ctx context.Context, upload *MultipartUploadRecord) (string, error) {
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

	item := &cosmosItem{
		ID:                 docIDUploadCosmos(uploadID),
		Type:               "upload",
		UploadID:           uploadID,
		Bucket:             upload.Bucket,
		Key:                upload.Key,
		ContentType:        contentType,
		ContentEncoding:    upload.ContentEncoding,
		ContentLanguage:    upload.ContentLanguage,
		ContentDisposition: upload.ContentDisposition,
		CacheControl:       upload.CacheControl,
		Expires:            upload.Expires,
		StorageClass:       storageClass,
		ACL:                acl,
		UserMetadata:       userMeta,
		OwnerID:            upload.OwnerID,
		OwnerDisplay:       upload.OwnerDisplay,
		InitiatedAt:        upload.InitiatedAt.UTC().Format(cosmosTimeFormat),
	}

	data, err := json.Marshal(item)
	if err != nil {
		return "", fmt.Errorf("marshaling upload: %w", err)
	}

	_, err = s.client.CreateItem(ctx, azcosmos.NewPartitionKeyString("upload"), data, nil)
	if err != nil {
		return "", fmt.Errorf("creating multipart upload: %w", err)
	}

	return uploadID, nil
}

func (s *CosmosStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUploadRecord, error) {
	resp, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKeyString("upload"), docIDUploadCosmos(uploadID), nil)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return nil, nil
		}
		return nil, fmt.Errorf("getting multipart upload: %w", err)
	}

	var item cosmosItem
	if err := json.Unmarshal(resp.Value, &item); err != nil {
		return nil, fmt.Errorf("unmarshaling upload: %w", err)
	}

	upload := s.itemToUpload(&item)
	if upload.Bucket != bucket || upload.Key != key {
		return nil, nil
	}
	return upload, nil
}

func (s *CosmosStore) PutPart(ctx context.Context, part *PartRecord) error {
	item := &cosmosItem{
		ID:           docIDPartCosmos(part.UploadID, part.PartNumber),
		Type:         "upload",
		UploadID:     part.UploadID,
		PartNumber:   part.PartNumber,
		Size:         part.Size,
		ETag:         part.ETag,
		LastModified: part.LastModified.UTC().Format(cosmosTimeFormat),
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshaling part: %w", err)
	}

	_, err = s.client.UpsertItem(ctx, azcosmos.NewPartitionKeyString("upload"), data, nil)
	return err
}

func (s *CosmosStore) ListParts(ctx context.Context, uploadID string, opts ListPartsOptions) (*ListPartsResult, error) {
	maxParts := opts.MaxParts
	if maxParts <= 0 {
		maxParts = 1000
	}

	query := "SELECT * FROM c WHERE c.type = 'upload' AND STARTSWITH(c.id, @prefix)"
	params := []azcosmos.QueryParameter{
		{Name: "@prefix", Value: "part_" + uploadID + "_"},
	}

	if opts.PartNumberMarker > 0 {
		query += " AND c.id > @start_after"
		params = append(params, azcosmos.QueryParameter{
			Name: "@start_after", Value: docIDPartCosmos(uploadID, opts.PartNumberMarker),
		})
	}

	query += " ORDER BY c.id"

	pager := s.client.NewQueryItemsPager(query, azcosmos.NewPartitionKeyString("upload"), &azcosmos.QueryOptions{
		QueryParameters: params,
		PageSizeHint:    int32(maxParts + 1),
	})

	var parts []PartRecord
	for pager.More() && len(parts) <= maxParts {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing parts: %w", err)
		}
		for _, item := range resp.Items {
			var ci cosmosItem
			if err := json.Unmarshal(item, &ci); err != nil {
				continue
			}
			if ci.PartNumber > 0 {
				parts = append(parts, *s.itemToPart(&ci))
			}
			if len(parts) > maxParts {
				break
			}
		}
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

func (s *CosmosStore) GetPartsForCompletion(ctx context.Context, uploadID string, partNumbers []int) ([]PartRecord, error) {
	query := "SELECT * FROM c WHERE c.type = 'upload' AND STARTSWITH(c.id, @prefix)"
	params := []azcosmos.QueryParameter{
		{Name: "@prefix", Value: "part_" + uploadID + "_"},
	}

	pager := s.client.NewQueryItemsPager(query, azcosmos.NewPartitionKeyString("upload"), &azcosmos.QueryOptions{
		QueryParameters: params,
	})

	pnSet := make(map[int]bool)
	if len(partNumbers) > 0 {
		for _, pn := range partNumbers {
			pnSet[pn] = true
		}
	}

	var parts []PartRecord
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting parts: %w", err)
		}
		for _, item := range resp.Items {
			var ci cosmosItem
			if err := json.Unmarshal(item, &ci); err != nil {
				continue
			}
			if ci.PartNumber > 0 && (len(partNumbers) == 0 || pnSet[ci.PartNumber]) {
				parts = append(parts, *s.itemToPart(&ci))
			}
		}
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

func (s *CosmosStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, obj *ObjectRecord) error {
	if err := s.PutObject(ctx, obj); err != nil {
		return fmt.Errorf("putting completed object: %w", err)
	}

	parts, _ := s.GetPartsForCompletion(ctx, uploadID, nil)
	for _, part := range parts {
		_, _ = s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("upload"), docIDPartCosmos(uploadID, part.PartNumber), nil)
	}

	_, err := s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("upload"), docIDUploadCosmos(uploadID), nil)
	return err
}

func (s *CosmosStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	parts, _ := s.GetPartsForCompletion(ctx, uploadID, nil)
	for _, part := range parts {
		_, _ = s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("upload"), docIDPartCosmos(uploadID, part.PartNumber), nil)
	}

	_, err := s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("upload"), docIDUploadCosmos(uploadID), nil)
	return err
}

func (s *CosmosStore) ListMultipartUploads(ctx context.Context, bucket string, opts ListUploadsOptions) (*ListUploadsResult, error) {
	maxUploads := opts.MaxUploads
	if maxUploads <= 0 {
		maxUploads = 1000
	}

	query := "SELECT * FROM c WHERE c.type = 'upload' AND c.bucket = @bucket AND c.upload_id IS NOT NULL"
	params := []azcosmos.QueryParameter{
		{Name: "@bucket", Value: bucket},
	}

	if opts.Prefix != "" {
		query += " AND STARTSWITH(c.key, @prefix)"
		params = append(params, azcosmos.QueryParameter{Name: "@prefix", Value: opts.Prefix})
	}

	if opts.KeyMarker != "" {
		query += " AND (c.key > @key_marker OR (c.key = @key_marker AND c.upload_id > @upload_id_marker))"
		params = append(params,
			azcosmos.QueryParameter{Name: "@key_marker", Value: opts.KeyMarker},
			azcosmos.QueryParameter{Name: "@upload_id_marker", Value: opts.UploadIDMarker},
		)
	}

	query += " ORDER BY c.key, c.upload_id"

	pager := s.client.NewQueryItemsPager(query, azcosmos.NewPartitionKeyString("upload"), &azcosmos.QueryOptions{
		QueryParameters: params,
		PageSizeHint:    int32(maxUploads + 1),
	})

	var uploads []MultipartUploadRecord
	for pager.More() && len(uploads) <= maxUploads {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing multipart uploads: %w", err)
		}
		for _, item := range resp.Items {
			var ci cosmosItem
			if err := json.Unmarshal(item, &ci); err != nil {
				continue
			}
			if ci.UploadID != "" {
				uploads = append(uploads, *s.itemToUpload(&ci))
			}
			if len(uploads) > maxUploads {
				break
			}
		}
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

func (s *CosmosStore) GetCredential(ctx context.Context, accessKeyID string) (*CredentialRecord, error) {
	resp, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKeyString("credential"), docIDCredentialCosmos(accessKeyID), nil)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return nil, nil
		}
		return nil, fmt.Errorf("getting credential: %w", err)
	}

	var item cosmosItem
	if err := json.Unmarshal(resp.Value, &item); err != nil {
		return nil, fmt.Errorf("unmarshaling credential: %w", err)
	}

	if !item.Active {
		return nil, nil
	}

	createdAt, _ := time.Parse(cosmosTimeFormat, item.CreatedAt)
	return &CredentialRecord{
		AccessKeyID: item.AccessKeyID,
		SecretKey:   item.SecretKey,
		OwnerID:     item.OwnerID,
		DisplayName: item.DisplayName,
		Active:      item.Active,
		CreatedAt:   createdAt,
	}, nil
}

func (s *CosmosStore) PutCredential(ctx context.Context, cred *CredentialRecord) error {
	item := &cosmosItem{
		ID:          docIDCredentialCosmos(cred.AccessKeyID),
		Type:        "credential",
		AccessKeyID: cred.AccessKeyID,
		SecretKey:   cred.SecretKey,
		OwnerID:     cred.OwnerID,
		DisplayName: cred.DisplayName,
		Active:      cred.Active,
		CreatedAt:   cred.CreatedAt.UTC().Format(cosmosTimeFormat),
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshaling credential: %w", err)
	}

	_, err = s.client.UpsertItem(ctx, azcosmos.NewPartitionKeyString("credential"), data, nil)
	return err
}

func (s *CosmosStore) ReapExpiredUploads(ttlSeconds int) ([]ExpiredUpload, error) {
	cutoff := time.Now().Add(-time.Duration(ttlSeconds) * time.Second).UTC().Format(cosmosTimeFormat)

	ctx := context.Background()

	query := "SELECT * FROM c WHERE c.type = 'upload' AND c.upload_id IS NOT NULL AND c.initiated_at < @cutoff"
	params := []azcosmos.QueryParameter{
		{Name: "@cutoff", Value: cutoff},
	}

	pager := s.client.NewQueryItemsPager(query, azcosmos.NewPartitionKeyString("upload"), &azcosmos.QueryOptions{
		QueryParameters: params,
	})

	var reaped []ExpiredUpload
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("querying expired uploads: %w", err)
		}
		for _, item := range resp.Items {
			var ci cosmosItem
			if err := json.Unmarshal(item, &ci); err != nil {
				continue
			}

			uploadID := ci.UploadID
			bucket := ci.Bucket
			key := ci.Key

			parts, _ := s.GetPartsForCompletion(ctx, uploadID, nil)
			for _, part := range parts {
				_, _ = s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("upload"), docIDPartCosmos(uploadID, part.PartNumber), nil)
			}

			_, _ = s.client.DeleteItem(ctx, azcosmos.NewPartitionKeyString("upload"), docIDUploadCosmos(uploadID), nil)

			reaped = append(reaped, ExpiredUpload{
				UploadID:   uploadID,
				BucketName: bucket,
				ObjectKey:  key,
			})
		}
	}

	return reaped, nil
}

func (s *CosmosStore) itemToObject(item *cosmosItem) *ObjectRecord {
	lastModified, _ := time.Parse(cosmosTimeFormat, item.LastModified)
	obj := &ObjectRecord{
		Bucket:             item.Bucket,
		Key:                item.Key,
		Size:               item.Size,
		ETag:               item.ETag,
		ContentType:        item.ContentType,
		ContentEncoding:    item.ContentEncoding,
		ContentLanguage:    item.ContentLanguage,
		ContentDisposition: item.ContentDisposition,
		CacheControl:       item.CacheControl,
		Expires:            item.Expires,
		StorageClass:       item.StorageClass,
		ACL:                json.RawMessage(item.ACL),
		LastModified:       lastModified,
		DeleteMarker:       item.DeleteMarker,
	}
	if item.UserMetadata != "" && item.UserMetadata != "{}" {
		obj.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(item.UserMetadata), &obj.UserMetadata)
	}
	return obj
}

func (s *CosmosStore) itemToUpload(item *cosmosItem) *MultipartUploadRecord {
	initiatedAt, _ := time.Parse(cosmosTimeFormat, item.InitiatedAt)
	upload := &MultipartUploadRecord{
		UploadID:           item.UploadID,
		Bucket:             item.Bucket,
		Key:                item.Key,
		ContentType:        item.ContentType,
		ContentEncoding:    item.ContentEncoding,
		ContentLanguage:    item.ContentLanguage,
		ContentDisposition: item.ContentDisposition,
		CacheControl:       item.CacheControl,
		Expires:            item.Expires,
		StorageClass:       item.StorageClass,
		ACL:                json.RawMessage(item.ACL),
		OwnerID:            item.OwnerID,
		OwnerDisplay:       item.OwnerDisplay,
		InitiatedAt:        initiatedAt,
	}
	if item.UserMetadata != "" && item.UserMetadata != "{}" {
		upload.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(item.UserMetadata), &upload.UserMetadata)
	}
	return upload
}

func (s *CosmosStore) itemToPart(item *cosmosItem) *PartRecord {
	lastModified, _ := time.Parse(cosmosTimeFormat, item.LastModified)
	return &PartRecord{
		UploadID:     item.UploadID,
		PartNumber:   item.PartNumber,
		Size:         item.Size,
		ETag:         item.ETag,
		LastModified: lastModified,
	}
}
