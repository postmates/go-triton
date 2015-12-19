package triton

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// An Uploader is just a simple wrapper around an S3manager. It just assumes
// default options, and that we will want to upload from some local file name
// to a remote file name.
type S3Uploader struct {
	uploader   *s3manager.Uploader
	bucketName string
}

func (s *S3Uploader) Upload(fileName, keyName string) (err error) {
	r, err := os.Open(fileName)
	if err != nil {
		return
	}

	log.Println("Uploading", fileName)

	ui := s3manager.UploadInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(keyName),
		Body:   r,
	}

	_, err = s.uploader.Upload(&ui)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			return fmt.Errorf("Failed to upload: %v (%v)", awsErr.Code(), awsErr.Message())
		}
		return
	} else {
		log.Println("Completed upload to", keyName)
	}
	return
}

func (s *S3Uploader) UploadData(r io.Reader, keyName string) (err error) {
	log.Println("Uploading", keyName)

	ui := s3manager.UploadInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(keyName),
		Body:   r,
	}

	_, err = s.uploader.Upload(&ui)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			return fmt.Errorf("Failed to upload: %v (%v)", awsErr.Code(), awsErr.Message())
		}
		return
	} else {
		log.Println("Completed upload to", keyName)
	}
	return
}
func NewUploader(c client.ConfigProvider, bucketName string) *S3Uploader {
	m := s3manager.NewUploader(c)

	u := S3Uploader{
		uploader:   m,
		bucketName: bucketName,
	}

	return &u
}
