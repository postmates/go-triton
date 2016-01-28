package triton

import (
	"io"
	"testing"
	"time"
)

func TestNewArchive(t *testing.T) {
	key := "20150801/test_stream-archive-123455.tri"
	sa, err := newStoreArchive("foo", key, nil)
	if err != nil {
		t.Fatal("Error creating sa", err)
	}

	if sa.key != key {
		t.Error("Failed to store key")
	}

	if sa.streamName != "test_stream" {
		t.Error("StreamName mismatch", sa.streamName)
	}

	if sa.clientName != "archive" {
		t.Error("Should have a client name")
	}

	if sa.T != time.Date(2015, time.August, 1, 0, 0, 0, 0, time.UTC) {
		t.Error("StreamName mismatch", sa.streamName)
	}

	if sa.bucket != "foo" {
		t.Error("bucket name mismatch")
	}

	if sa.SortValue != 123455 {
		t.Error("Sort value mismatch")
	}
}

func TestNewArchiveShard(t *testing.T) {
	sa, err := newStoreArchive("foo", "20150801/test_stream-store_test-123455.tri", nil)
	if err != nil {
		t.Fatal("Error creating sa", err)
	}

	if sa.streamName != "test_stream" {
		t.Error("StreamName mismatch", sa.streamName)
	}

	if sa.clientName != "store_test" {
		t.Error("Should have a client name")
	}

	if sa.T != time.Date(2015, time.August, 1, 0, 0, 0, 0, time.UTC) {
		t.Error("StreamName mismatch", sa.streamName)
	}

	if sa.SortValue != 123455 {
		t.Error("Sort value mismatch")
	}
}

func TestReadEmpty(t *testing.T) {
	sa, err := newStoreArchive("foo", "20150801/test_stream-store_test-123455.tri", &nullS3Service{})
	if err != nil {
		t.Fatal("Error creating sa", err)
	}

	_, err = sa.Read(nil)
	if err != io.EOF {
		t.Fatal("Should have EOF: ", err)
	}
}
