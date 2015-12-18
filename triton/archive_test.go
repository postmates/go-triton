package triton

import (
	"fmt"
	"io"
	"testing"
	"time"
)

var (
	streamTime    = time.Date(2015, time.August, 1, 0, 0, 0, 0, time.UTC)
	streamKeyPath = fmt.Sprintf("%04d%02d%02d/test_stream-archive-%d.tri", streamTime.Year(), streamTime.Month(), streamTime.Day(), streamTime.Unix())
)

func TestNewArchive(t *testing.T) {
	sa, err := NewStoreArchive("foo", streamKeyPath, nil)
	if err != nil {
		t.Fatal("Error creating sa:", err.Error())
	}

	if sa.Key != streamKeyPath {
		t.Error("Failed to store key")
	}

	if sa.StreamName != "test_stream" {
		t.Error("StreamName mismatch", sa.StreamName)
	}

	if sa.ClientName != "archive" {
		t.Error("Should have a client name")
	}
	if !sa.T.Equal(streamTime) {
		t.Errorf("Stream time mismatch: %s != %s", sa.T, streamTime)
	}

	if sa.Bucket != "foo" {
		t.Error("bucket name mismatch, %s != %s", sa.Bucket, "foo")
	}

}

func TestNewArchiveShard(t *testing.T) {
	sa, err := NewStoreArchive("foo", streamKeyPath, nil)
	if err != nil {
		t.Fatalf("Error creating sa", err)
	}

	if sa.StreamName != "test_stream" {
		t.Error("StreamName mismatch", sa.StreamName)
	}

	if sa.ClientName != "archive" {
		t.Error("Should have a client name")
	}

	if !sa.T.Equal(streamTime) {
		t.Errorf("StreamName mismatch %s != %s", sa.T, streamTime)
	}

}

func TestReadEmpty(t *testing.T) {
	sa, err := NewStoreArchive("foo", streamKeyPath, &nullS3Service{})
	if err != nil {
		t.Fatal("Error creating sa", err)
	}

	_, err = sa.ReadRecord()
	if err != io.EOF {
		t.Fatal("Should have EOF: ", err)
	}
}
