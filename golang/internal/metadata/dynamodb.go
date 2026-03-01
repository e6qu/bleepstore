package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bleepstore/bleepstore/internal/config"
)

const (
	dynamoTimeFormat = "2006-01-02T15:04:05.000Z"
)

type DynamoDBStore struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoDBStore(cfg *config.DynamoDBConfig) (*DynamoDBStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("dynamodb config is required")
	}
	if cfg.Table == "" {
		return nil, fmt.Errorf("dynamodb table name is required")
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	var awsCfg aws.Config
	var err error

	if cfg.EndpointURL != "" {
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(region),
		)
		if err != nil {
			return nil, fmt.Errorf("loading aws config: %w", err)
		}
		awsCfg.BaseEndpoint = aws.String(cfg.EndpointURL)
	} else {
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(region),
		)
		if err != nil {
			return nil, fmt.Errorf("loading aws config: %w", err)
		}
	}

	client := dynamodb.NewFromConfig(awsCfg)

	return &DynamoDBStore{
		client:    client,
		tableName: cfg.Table,
	}, nil
}

func (s *DynamoDBStore) Ping(ctx context.Context) error {
	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(s.tableName),
	})
	return err
}

func (s *DynamoDBStore) Close() error {
	return nil
}

func pkBucket(bucket string) string {
	return "BUCKET#" + bucket
}

func pkObject(bucket, key string) string {
	return "OBJECT#" + bucket + "#" + key
}

func pkUpload(uploadID string) string {
	return "UPLOAD#" + uploadID
}

func pkCredential(accessKey string) string {
	return "CRED#" + accessKey
}

func skMetadata() string {
	return "#METADATA"
}

func skPart(partNumber int) string {
	return fmt.Sprintf("PART#%05d", partNumber)
}

func nowISO() string {
	return time.Now().UTC().Format(dynamoTimeFormat)
}

func (s *DynamoDBStore) CreateBucket(ctx context.Context, bucket *BucketRecord) error {
	acl := "{}"
	if bucket.ACL != nil {
		acl = string(bucket.ACL)
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item: map[string]types.AttributeValue{
			"pk":            &types.AttributeValueMemberS{Value: pkBucket(bucket.Name)},
			"sk":            &types.AttributeValueMemberS{Value: skMetadata()},
			"type":          &types.AttributeValueMemberS{Value: "bucket"},
			"name":          &types.AttributeValueMemberS{Value: bucket.Name},
			"region":        &types.AttributeValueMemberS{Value: bucket.Region},
			"owner_id":      &types.AttributeValueMemberS{Value: bucket.OwnerID},
			"owner_display": &types.AttributeValueMemberS{Value: bucket.OwnerDisplay},
			"acl":           &types.AttributeValueMemberS{Value: acl},
			"created_at":    &types.AttributeValueMemberS{Value: bucket.CreatedAt.UTC().Format(dynamoTimeFormat)},
		},
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	if err != nil {
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
			return fmt.Errorf("bucket already exists: %s", bucket.Name)
		}
		return fmt.Errorf("creating bucket: %w", err)
	}
	return nil
}

func (s *DynamoDBStore) GetBucket(ctx context.Context, name string) (*BucketRecord, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkBucket(name)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("getting bucket: %w", err)
	}
	if resp.Item == nil {
		return nil, nil
	}

	return s.itemToBucket(resp.Item), nil
}

func (s *DynamoDBStore) DeleteBucket(ctx context.Context, name string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkBucket(name)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
	})
	return err
}

func (s *DynamoDBStore) ListBuckets(ctx context.Context, owner string) ([]BucketRecord, error) {
	var buckets []BucketRecord

	var exclusiveStartKey map[string]types.AttributeValue
	for {
		input := &dynamodb.ScanInput{
			TableName:        aws.String(s.tableName),
			FilterExpression: aws.String("begins_with(pk, :prefix) AND sk = :meta"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":prefix": &types.AttributeValueMemberS{Value: "BUCKET#"},
				":meta":   &types.AttributeValueMemberS{Value: skMetadata()},
			},
		}
		if exclusiveStartKey != nil {
			input.ExclusiveStartKey = exclusiveStartKey
		}

		resp, err := s.client.Scan(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("listing buckets: %w", err)
		}

		for _, item := range resp.Items {
			b := s.itemToBucket(item)
			if owner == "" || b.OwnerID == owner {
				buckets = append(buckets, *b)
			}
		}

		if resp.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = resp.LastEvaluatedKey
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func (s *DynamoDBStore) BucketExists(ctx context.Context, name string) (bool, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkBucket(name)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
		ProjectionExpression: aws.String("pk"),
	})
	if err != nil {
		return false, fmt.Errorf("checking bucket exists: %w", err)
	}
	return resp.Item != nil, nil
}

func (s *DynamoDBStore) UpdateBucketAcl(ctx context.Context, name string, acl json.RawMessage) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkBucket(name)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
		UpdateExpression:          aws.String("SET acl = :acl"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":acl": &types.AttributeValueMemberS{Value: string(acl)}},
	})
	return err
}

func (s *DynamoDBStore) PutObject(ctx context.Context, obj *ObjectRecord) error {
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

	item := map[string]types.AttributeValue{
		"pk":            &types.AttributeValueMemberS{Value: pkObject(obj.Bucket, obj.Key)},
		"sk":            &types.AttributeValueMemberS{Value: skMetadata()},
		"type":          &types.AttributeValueMemberS{Value: "object"},
		"bucket":        &types.AttributeValueMemberS{Value: obj.Bucket},
		"key":           &types.AttributeValueMemberS{Value: obj.Key},
		"size":          &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", obj.Size)},
		"etag":          &types.AttributeValueMemberS{Value: obj.ETag},
		"content_type":  &types.AttributeValueMemberS{Value: contentType},
		"storage_class": &types.AttributeValueMemberS{Value: storageClass},
		"acl":           &types.AttributeValueMemberS{Value: acl},
		"user_metadata": &types.AttributeValueMemberS{Value: userMeta},
		"last_modified": &types.AttributeValueMemberS{Value: obj.LastModified.UTC().Format(dynamoTimeFormat)},
	}

	if obj.ContentEncoding != "" {
		item["content_encoding"] = &types.AttributeValueMemberS{Value: obj.ContentEncoding}
	}
	if obj.ContentLanguage != "" {
		item["content_language"] = &types.AttributeValueMemberS{Value: obj.ContentLanguage}
	}
	if obj.ContentDisposition != "" {
		item["content_disposition"] = &types.AttributeValueMemberS{Value: obj.ContentDisposition}
	}
	if obj.CacheControl != "" {
		item["cache_control"] = &types.AttributeValueMemberS{Value: obj.CacheControl}
	}
	if obj.Expires != "" {
		item["expires"] = &types.AttributeValueMemberS{Value: obj.Expires}
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      item,
	})
	return err
}

func (s *DynamoDBStore) GetObject(ctx context.Context, bucket, key string) (*ObjectRecord, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkObject(bucket, key)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("getting object: %w", err)
	}
	if resp.Item == nil {
		return nil, nil
	}
	return s.itemToObject(resp.Item), nil
}

func (s *DynamoDBStore) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkObject(bucket, key)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
	})
	return err
}

func (s *DynamoDBStore) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkObject(bucket, key)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
		ProjectionExpression: aws.String("pk"),
	})
	if err != nil {
		return false, fmt.Errorf("checking object exists: %w", err)
	}
	return resp.Item != nil, nil
}

func (s *DynamoDBStore) DeleteObjectsMeta(ctx context.Context, bucket string, keys []string) ([]string, []error) {
	if len(keys) == 0 {
		return nil, nil
	}

	var deleted []string
	var errs []error

	for i := 0; i < len(keys); i += 25 {
		end := i + 25
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		var writeRequests []types.WriteRequest
		for _, key := range batch {
			writeRequests = append(writeRequests, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: pkObject(bucket, key)},
						"sk": &types.AttributeValueMemberS{Value: skMetadata()},
					},
				},
			})
		}

		_, err := s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				s.tableName: writeRequests,
			},
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		deleted = append(deleted, batch...)
	}

	return deleted, errs
}

func (s *DynamoDBStore) UpdateObjectAcl(ctx context.Context, bucket, key string, acl json.RawMessage) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkObject(bucket, key)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
		UpdateExpression:          aws.String("SET acl = :acl"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":acl": &types.AttributeValueMemberS{Value: string(acl)}},
	})
	return err
}

func (s *DynamoDBStore) ListObjects(ctx context.Context, bucket string, opts ListObjectsOptions) (*ListObjectsResult, error) {
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

	prefixFilter := "OBJECT#" + bucket + "#"
	if opts.Prefix != "" {
		prefixFilter = pkObject(bucket, opts.Prefix)
	}

	var allObjects []ObjectRecord
	var exclusiveStartKey map[string]types.AttributeValue

	for len(allObjects) <= maxKeys {
		input := &dynamodb.ScanInput{
			TableName:        aws.String(s.tableName),
			FilterExpression: aws.String("begins_with(pk, :prefix) AND sk = :meta"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":prefix": &types.AttributeValueMemberS{Value: prefixFilter},
				":meta":   &types.AttributeValueMemberS{Value: skMetadata()},
			},
			Limit: aws.Int32(int32(maxKeys + 1)),
		}
		if exclusiveStartKey != nil {
			input.ExclusiveStartKey = exclusiveStartKey
		}

		resp, err := s.client.Scan(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("listing objects: %w", err)
		}

		for _, item := range resp.Items {
			obj := s.itemToObject(item)
			if opts.Prefix != "" && !strings.HasPrefix(obj.Key, opts.Prefix) {
				continue
			}
			if startAfter != "" && obj.Key <= startAfter {
				continue
			}
			allObjects = append(allObjects, *obj)
			if len(allObjects) > maxKeys {
				break
			}
		}

		if resp.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = resp.LastEvaluatedKey

		if len(allObjects) > maxKeys {
			break
		}
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

		if len(objects)+len(prefixSet) >= maxKeys {
			break
		}
	}

	var commonPrefixes []string
	for p := range prefixSet {
		commonPrefixes = append(commonPrefixes, p)
	}
	sort.Strings(commonPrefixes)

	isTruncated := len(objects)+len(commonPrefixes) > maxKeys || len(allObjects) > maxKeys

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

func (s *DynamoDBStore) CreateMultipartUpload(ctx context.Context, upload *MultipartUploadRecord) (string, error) {
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

	item := map[string]types.AttributeValue{
		"pk":            &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
		"sk":            &types.AttributeValueMemberS{Value: skMetadata()},
		"type":          &types.AttributeValueMemberS{Value: "upload"},
		"upload_id":     &types.AttributeValueMemberS{Value: uploadID},
		"bucket":        &types.AttributeValueMemberS{Value: upload.Bucket},
		"key":           &types.AttributeValueMemberS{Value: upload.Key},
		"content_type":  &types.AttributeValueMemberS{Value: contentType},
		"storage_class": &types.AttributeValueMemberS{Value: storageClass},
		"acl":           &types.AttributeValueMemberS{Value: acl},
		"user_metadata": &types.AttributeValueMemberS{Value: userMeta},
		"owner_id":      &types.AttributeValueMemberS{Value: upload.OwnerID},
		"owner_display": &types.AttributeValueMemberS{Value: upload.OwnerDisplay},
		"initiated_at":  &types.AttributeValueMemberS{Value: upload.InitiatedAt.UTC().Format(dynamoTimeFormat)},
	}

	if upload.ContentEncoding != "" {
		item["content_encoding"] = &types.AttributeValueMemberS{Value: upload.ContentEncoding}
	}
	if upload.ContentLanguage != "" {
		item["content_language"] = &types.AttributeValueMemberS{Value: upload.ContentLanguage}
	}
	if upload.ContentDisposition != "" {
		item["content_disposition"] = &types.AttributeValueMemberS{Value: upload.ContentDisposition}
	}
	if upload.CacheControl != "" {
		item["cache_control"] = &types.AttributeValueMemberS{Value: upload.CacheControl}
	}
	if upload.Expires != "" {
		item["expires"] = &types.AttributeValueMemberS{Value: upload.Expires}
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      item,
	})
	if err != nil {
		return "", fmt.Errorf("creating multipart upload: %w", err)
	}

	return uploadID, nil
}

func (s *DynamoDBStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUploadRecord, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("getting multipart upload: %w", err)
	}
	if resp.Item == nil {
		return nil, nil
	}

	upload := s.itemToUpload(resp.Item)
	if upload.Bucket != bucket || upload.Key != key {
		return nil, nil
	}
	return upload, nil
}

func (s *DynamoDBStore) PutPart(ctx context.Context, part *PartRecord) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item: map[string]types.AttributeValue{
			"pk":            &types.AttributeValueMemberS{Value: pkUpload(part.UploadID)},
			"sk":            &types.AttributeValueMemberS{Value: skPart(part.PartNumber)},
			"type":          &types.AttributeValueMemberS{Value: "part"},
			"upload_id":     &types.AttributeValueMemberS{Value: part.UploadID},
			"part_number":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", part.PartNumber)},
			"size":          &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", part.Size)},
			"etag":          &types.AttributeValueMemberS{Value: part.ETag},
			"last_modified": &types.AttributeValueMemberS{Value: part.LastModified.UTC().Format(dynamoTimeFormat)},
		},
	})
	return err
}

func (s *DynamoDBStore) ListParts(ctx context.Context, uploadID string, opts ListPartsOptions) (*ListPartsResult, error) {
	maxParts := opts.MaxParts
	if maxParts <= 0 {
		maxParts = 1000
	}

	var parts []PartRecord
	var exclusiveStartKey map[string]types.AttributeValue

	startSK := "PART#"
	if opts.PartNumberMarker > 0 {
		startSK = skPart(opts.PartNumberMarker + 1)
	}

	for len(parts) < maxParts+1 {
		input := &dynamodb.QueryInput{
			TableName:              aws.String(s.tableName),
			KeyConditionExpression: aws.String("pk = :pk AND sk >= :startSK"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":      &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
				":startSK": &types.AttributeValueMemberS{Value: startSK},
			},
			Limit: aws.Int32(int32(maxParts + 1)),
		}
		if exclusiveStartKey != nil {
			input.ExclusiveStartKey = exclusiveStartKey
		}

		resp, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("listing parts: %w", err)
		}

		for _, item := range resp.Items {
			if strings.HasPrefix(getString(item, "sk"), "PART#") {
				parts = append(parts, *s.itemToPart(item))
			}
		}

		if resp.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = resp.LastEvaluatedKey
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

func (s *DynamoDBStore) GetPartsForCompletion(ctx context.Context, uploadID string, partNumbers []int) ([]PartRecord, error) {
	if len(partNumbers) == 0 {
		return nil, nil
	}

	var parts []PartRecord
	var exclusiveStartKey map[string]types.AttributeValue

	for {
		input := &dynamodb.QueryInput{
			TableName:              aws.String(s.tableName),
			KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
				":prefix": &types.AttributeValueMemberS{Value: "PART#"},
			},
		}
		if exclusiveStartKey != nil {
			input.ExclusiveStartKey = exclusiveStartKey
		}

		resp, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("getting parts: %w", err)
		}

		for _, item := range resp.Items {
			parts = append(parts, *s.itemToPart(item))
		}

		if resp.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = resp.LastEvaluatedKey
	}

	pnSet := make(map[int]bool)
	for _, pn := range partNumbers {
		pnSet[pn] = true
	}

	var filtered []PartRecord
	for _, p := range parts {
		if pnSet[p.PartNumber] {
			filtered = append(filtered, p)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].PartNumber < filtered[j].PartNumber
	})

	return filtered, nil
}

func (s *DynamoDBStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, obj *ObjectRecord) error {
	if err := s.PutObject(ctx, obj); err != nil {
		return fmt.Errorf("putting completed object: %w", err)
	}

	parts, _ := s.GetPartsForCompletion(ctx, uploadID, nil)

	if len(parts) > 0 {
		for i := 0; i < len(parts); i += 25 {
			end := i + 25
			if end > len(parts) {
				end = len(parts)
			}
			batch := parts[i:end]

			var writeRequests []types.WriteRequest
			for _, p := range batch {
				writeRequests = append(writeRequests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
							"sk": &types.AttributeValueMemberS{Value: skPart(p.PartNumber)},
						},
					},
				})
			}

			_, _ = s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					s.tableName: writeRequests,
				},
			})
		}
	}

	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
	})

	return err
}

func (s *DynamoDBStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	parts, _ := s.GetPartsForCompletion(ctx, uploadID, nil)

	if len(parts) > 0 {
		for i := 0; i < len(parts); i += 25 {
			end := i + 25
			if end > len(parts) {
				end = len(parts)
			}
			batch := parts[i:end]

			var writeRequests []types.WriteRequest
			for _, p := range batch {
				writeRequests = append(writeRequests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
							"sk": &types.AttributeValueMemberS{Value: skPart(p.PartNumber)},
						},
					},
				})
			}

			_, _ = s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					s.tableName: writeRequests,
				},
			})
		}
	}

	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
	})

	return err
}

func (s *DynamoDBStore) ListMultipartUploads(ctx context.Context, bucket string, opts ListUploadsOptions) (*ListUploadsResult, error) {
	maxUploads := opts.MaxUploads
	if maxUploads <= 0 {
		maxUploads = 1000
	}

	var allUploads []MultipartUploadRecord
	var exclusiveStartKey map[string]types.AttributeValue

	filterExpr := "begins_with(pk, :upload_prefix) AND sk = :meta AND #bucket = :bucket"
	exprValues := map[string]types.AttributeValue{
		":upload_prefix": &types.AttributeValueMemberS{Value: "UPLOAD#"},
		":meta":          &types.AttributeValueMemberS{Value: skMetadata()},
		":bucket":        &types.AttributeValueMemberS{Value: bucket},
	}
	exprNames := map[string]string{"#bucket": "bucket"}

	if opts.Prefix != "" {
		filterExpr += " AND begins_with(#key, :prefix)"
		exprValues[":prefix"] = &types.AttributeValueMemberS{Value: opts.Prefix}
		exprNames["#key"] = "key"
	}

	for len(allUploads) < maxUploads+1 {
		input := &dynamodb.ScanInput{
			TableName:                 aws.String(s.tableName),
			FilterExpression:          aws.String(filterExpr),
			ExpressionAttributeValues: exprValues,
			ExpressionAttributeNames:  exprNames,
			Limit:                     aws.Int32(int32(maxUploads + 1)),
		}
		if exclusiveStartKey != nil {
			input.ExclusiveStartKey = exclusiveStartKey
		}

		resp, err := s.client.Scan(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("listing multipart uploads: %w", err)
		}

		for _, item := range resp.Items {
			allUploads = append(allUploads, *s.itemToUpload(item))
		}

		if resp.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = resp.LastEvaluatedKey
	}

	sort.Slice(allUploads, func(i, j int) bool {
		if allUploads[i].Key != allUploads[j].Key {
			return allUploads[i].Key < allUploads[j].Key
		}
		return allUploads[i].InitiatedAt.Before(allUploads[j].InitiatedAt)
	})

	if opts.KeyMarker != "" || opts.UploadIDMarker != "" {
		var filtered []MultipartUploadRecord
		passedMarker := opts.KeyMarker == ""
		for _, u := range allUploads {
			if !passedMarker {
				if u.Key > opts.KeyMarker || (u.Key == opts.KeyMarker && u.UploadID > opts.UploadIDMarker) {
					passedMarker = true
				}
			}
			if passedMarker {
				filtered = append(filtered, u)
			}
		}
		allUploads = filtered
	}

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

func (s *DynamoDBStore) GetCredential(ctx context.Context, accessKeyID string) (*CredentialRecord, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pkCredential(accessKeyID)},
			"sk": &types.AttributeValueMemberS{Value: skMetadata()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("getting credential: %w", err)
	}
	if resp.Item == nil {
		return nil, nil
	}

	return s.itemToCredential(resp.Item), nil
}

func (s *DynamoDBStore) PutCredential(ctx context.Context, cred *CredentialRecord) error {
	active := "true"
	if !cred.Active {
		active = "false"
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item: map[string]types.AttributeValue{
			"pk":            &types.AttributeValueMemberS{Value: pkCredential(cred.AccessKeyID)},
			"sk":            &types.AttributeValueMemberS{Value: skMetadata()},
			"type":          &types.AttributeValueMemberS{Value: "credential"},
			"access_key_id": &types.AttributeValueMemberS{Value: cred.AccessKeyID},
			"secret_key":    &types.AttributeValueMemberS{Value: cred.SecretKey},
			"owner_id":      &types.AttributeValueMemberS{Value: cred.OwnerID},
			"display_name":  &types.AttributeValueMemberS{Value: cred.DisplayName},
			"active":        &types.AttributeValueMemberBOOL{Value: cred.Active},
			"created_at":    &types.AttributeValueMemberS{Value: active},
		},
	})
	return err
}

func (s *DynamoDBStore) ReapExpiredUploads(ttlSeconds int) ([]ExpiredUpload, error) {
	cutoff := time.Now().Add(-time.Duration(ttlSeconds) * time.Second).UTC().Format(dynamoTimeFormat)

	var items []map[string]types.AttributeValue
	var exclusiveStartKey map[string]types.AttributeValue

	for {
		input := &dynamodb.ScanInput{
			TableName:        aws.String(s.tableName),
			FilterExpression: aws.String("begins_with(pk, :upload_prefix) AND sk = :meta AND initiated_at < :cutoff"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":upload_prefix": &types.AttributeValueMemberS{Value: "UPLOAD#"},
				":meta":          &types.AttributeValueMemberS{Value: skMetadata()},
				":cutoff":        &types.AttributeValueMemberS{Value: cutoff},
			},
		}
		if exclusiveStartKey != nil {
			input.ExclusiveStartKey = exclusiveStartKey
		}

		resp, err := s.client.Scan(context.Background(), input)
		if err != nil {
			return nil, fmt.Errorf("scanning expired uploads: %w", err)
		}

		items = append(items, resp.Items...)

		if resp.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = resp.LastEvaluatedKey
	}

	var reaped []ExpiredUpload
	for _, item := range items {
		upload := s.itemToUpload(item)
		uploadID := upload.UploadID
		bucket := upload.Bucket
		key := upload.Key

		parts, _ := s.GetPartsForCompletion(context.Background(), uploadID, nil)
		if len(parts) > 0 {
			for i := 0; i < len(parts); i += 25 {
				end := i + 25
				if end > len(parts) {
					end = len(parts)
				}
				batch := parts[i:end]

				var writeRequests []types.WriteRequest
				for _, p := range batch {
					writeRequests = append(writeRequests, types.WriteRequest{
						DeleteRequest: &types.DeleteRequest{
							Key: map[string]types.AttributeValue{
								"pk": &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
								"sk": &types.AttributeValueMemberS{Value: skPart(p.PartNumber)},
							},
						},
					})
				}

				_, _ = s.client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						s.tableName: writeRequests,
					},
				})
			}
		}

		_, _ = s.client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
			TableName: aws.String(s.tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pkUpload(uploadID)},
				"sk": &types.AttributeValueMemberS{Value: skMetadata()},
			},
		})

		reaped = append(reaped, ExpiredUpload{
			UploadID:   uploadID,
			BucketName: bucket,
			ObjectKey:  key,
		})
	}

	return reaped, nil
}

func getString(item map[string]types.AttributeValue, key string) string {
	if v, ok := item[key]; ok {
		if sv, ok := v.(*types.AttributeValueMemberS); ok {
			return sv.Value
		}
	}
	return ""
}

func getNInt(item map[string]types.AttributeValue, key string) int64 {
	if v, ok := item[key]; ok {
		if nv, ok := v.(*types.AttributeValueMemberN); ok {
			var n int64
			fmt.Sscanf(nv.Value, "%d", &n)
			return n
		}
	}
	return 0
}

func getNInt32(item map[string]types.AttributeValue, key string) int {
	if v, ok := item[key]; ok {
		if nv, ok := v.(*types.AttributeValueMemberN); ok {
			var n int
			fmt.Sscanf(nv.Value, "%d", &n)
			return n
		}
	}
	return 0
}

func getBool(item map[string]types.AttributeValue, key string) bool {
	if v, ok := item[key]; ok {
		if bv, ok := v.(*types.AttributeValueMemberBOOL); ok {
			return bv.Value
		}
	}
	return false
}

func (s *DynamoDBStore) itemToBucket(item map[string]types.AttributeValue) *BucketRecord {
	createdAt, _ := time.Parse(dynamoTimeFormat, getString(item, "created_at"))
	return &BucketRecord{
		Name:         getString(item, "name"),
		Region:       getString(item, "region"),
		OwnerID:      getString(item, "owner_id"),
		OwnerDisplay: getString(item, "owner_display"),
		ACL:          json.RawMessage(getString(item, "acl")),
		CreatedAt:    createdAt,
	}
}

func (s *DynamoDBStore) itemToObject(item map[string]types.AttributeValue) *ObjectRecord {
	lastModified, _ := time.Parse(dynamoTimeFormat, getString(item, "last_modified"))
	obj := &ObjectRecord{
		Bucket:             getString(item, "bucket"),
		Key:                getString(item, "key"),
		Size:               getNInt(item, "size"),
		ETag:               getString(item, "etag"),
		ContentType:        getString(item, "content_type"),
		ContentEncoding:    getString(item, "content_encoding"),
		ContentLanguage:    getString(item, "content_language"),
		ContentDisposition: getString(item, "content_disposition"),
		CacheControl:       getString(item, "cache_control"),
		Expires:            getString(item, "expires"),
		StorageClass:       getString(item, "storage_class"),
		ACL:                json.RawMessage(getString(item, "acl")),
		LastModified:       lastModified,
	}
	userMeta := getString(item, "user_metadata")
	if userMeta != "" && userMeta != "{}" {
		obj.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(userMeta), &obj.UserMetadata)
	}
	return obj
}

func (s *DynamoDBStore) itemToUpload(item map[string]types.AttributeValue) *MultipartUploadRecord {
	initiatedAt, _ := time.Parse(dynamoTimeFormat, getString(item, "initiated_at"))
	upload := &MultipartUploadRecord{
		UploadID:           getString(item, "upload_id"),
		Bucket:             getString(item, "bucket"),
		Key:                getString(item, "key"),
		ContentType:        getString(item, "content_type"),
		ContentEncoding:    getString(item, "content_encoding"),
		ContentLanguage:    getString(item, "content_language"),
		ContentDisposition: getString(item, "content_disposition"),
		CacheControl:       getString(item, "cache_control"),
		Expires:            getString(item, "expires"),
		StorageClass:       getString(item, "storage_class"),
		ACL:                json.RawMessage(getString(item, "acl")),
		OwnerID:            getString(item, "owner_id"),
		OwnerDisplay:       getString(item, "owner_display"),
		InitiatedAt:        initiatedAt,
	}
	userMeta := getString(item, "user_metadata")
	if userMeta != "" && userMeta != "{}" {
		upload.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(userMeta), &upload.UserMetadata)
	}
	return upload
}

func (s *DynamoDBStore) itemToPart(item map[string]types.AttributeValue) *PartRecord {
	lastModified, _ := time.Parse(dynamoTimeFormat, getString(item, "last_modified"))
	return &PartRecord{
		UploadID:     getString(item, "upload_id"),
		PartNumber:   getNInt32(item, "part_number"),
		Size:         getNInt(item, "size"),
		ETag:         getString(item, "etag"),
		LastModified: lastModified,
	}
}

func (s *DynamoDBStore) itemToCredential(item map[string]types.AttributeValue) *CredentialRecord {
	createdAt, _ := time.Parse(dynamoTimeFormat, getString(item, "created_at"))
	return &CredentialRecord{
		AccessKeyID: getString(item, "access_key_id"),
		SecretKey:   getString(item, "secret_key"),
		OwnerID:     getString(item, "owner_id"),
		DisplayName: getString(item, "display_name"),
		Active:      getBool(item, "active"),
		CreatedAt:   createdAt,
	}
}
