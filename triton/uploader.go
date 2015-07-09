package triton

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

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

func NewUploader(svc *s3.S3, bucketName string) *S3Uploader {
	uo := &s3manager.UploadOptions{S3: svc}
	m := s3manager.NewUploader(uo)

	u := S3Uploader{
		uploader:   m,
		bucketName: bucketName,
	}

	return &u
}
