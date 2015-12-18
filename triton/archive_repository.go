package triton

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"time"
)

// ArchiveRepository manages reading and writing Archives
type ArchiveRepository struct {
	s3Service  S3Service
	s3Uploader S3UploaderService
	stream     string
	bucket     string
	client     string
}

func NewArchiveRepository(s3Service S3Service, s3Uploader S3UploaderService, bucket string, stream string, client string) *ArchiveRepository {
	return &ArchiveRepository{
		s3Service:  s3Service,
		s3Uploader: s3Uploader,
		bucket:     bucket,
		stream:     stream,
		client:     client,
	}
}

// Upload the archive for a stream at Time t
func (ar *ArchiveRepository) Upload(t time.Time, contents io.ReadCloser, metadata *StreamMetadata) (err error) {
	archiveKey := ArchiveKey{Stream: ar.stream, Time: t, Client: ar.client}
	_, err = ar.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(ar.bucket),
		Key:    aws.String(archiveKey.Path()),
		Body:   contents,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			return fmt.Errorf("Failed to upload: %v (%v)", awsErr.Code(), awsErr.Message())
		}
		return
	}
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(metadata)
	if err != nil {
		return
	}
	_, err = ar.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(ar.bucket),
		Key:    aws.String(archiveKey.MetadataPath()),
		Body:   ioutil.NopCloser(&buf),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			return fmt.Errorf("Failed to upload metadata: %v (%v)", awsErr.Code(), awsErr.Message())
		}
		return
	}
	return
}

// ArchivesAtDate lists all the archives for a stream stored at a UTC date represented by aDate
func (ar *ArchiveRepository) ArchivesAtDate(aDate time.Time) (result []StoreArchive, err error) {
	keyPrefix := ArchiveKey{Time: aDate, Stream: ar.stream, Client: ar.client}.PathPrefix()
	keys := []string{}
	err = ar.s3Service.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(ar.bucket),
		Prefix: aws.String(keyPrefix),
	}, func(output *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool) {
		for _, object := range output.Contents {
			keys = append(keys, *object.Key)
		}
		return true
	})
	if err != nil {
		return
	}
	sort.Sort(sort.StringSlice(keys))
	for _, key := range keys {
		if strings.HasSuffix(key, metadataSuffix) {
			continue
		}
		var sa StoreArchive
		sa, err = NewStoreArchive(ar.bucket, key, ar.s3Service)
		if err != nil {
			err = fmt.Errorf("failed to create store archive for %q: %s", key, err)
			return
		}
		result = append(result, sa)
	}
	return
}
