package triton

import (
	"io"
	"testing"
	"time"
)

func TestNewArchive(t *testing.T) {
	key := "20150801/test_stream-archive-123455.tri"
	sa, err := NewStoreArchive("foo", key, nil)
	if err != nil {
		t.Fatal("Error creating sa", err)
	}

	if sa.Key != key {
		t.Error("Failed to store key")
	}

	if sa.StreamName != "test_stream" {
		t.Error("StreamName mismatch", sa.StreamName)
	}

	if sa.ClientName != "archive" {
		t.Error("Should have a client name")
	}

	if sa.T != time.Date(2015, time.August, 1, 0, 0, 0, 0, time.UTC) {
		t.Error("StreamName mismatch", sa.StreamName)
	}

	if sa.Bucket != "foo" {
		t.Error("bucket name mismatch")
	}

	if sa.SortValue != 123455 {
		t.Error("Sort value mismatch")
	}
}

func TestNewArchiveShard(t *testing.T) {
	sa, err := NewStoreArchive("foo", "20150801/test_stream-store_test-123455.tri", nil)
	if err != nil {
		t.Fatal("Error creating sa", err)
	}

	if sa.StreamName != "test_stream" {
		t.Error("StreamName mismatch", sa.StreamName)
	}

	if sa.ClientName != "store_test" {
		t.Error("Should have a client name")
	}

	if sa.T != time.Date(2015, time.August, 1, 0, 0, 0, 0, time.UTC) {
		t.Error("StreamName mismatch", sa.StreamName)
	}

	if sa.SortValue != 123455 {
		t.Error("Sort value mismatch")
	}
}

func TestReadEmpty(t *testing.T) {
	sa, err := NewStoreArchive("foo", "20150801/test_stream-store_test-123455.tri", &nullS3Service{})
	if err != nil {
		t.Fatal("Error creating sa", err)
	}

	_, err = sa.ReadRecord()
	if err != io.EOF {
		t.Fatal("Should have EOF: ", err)
	}
}
