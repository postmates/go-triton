package triton

import (
	"testing"
	"time"
)

func TestArchiveKeyPathCodec(t *testing.T) {
	aTime := time.Now()
	archiveKey := ArchiveKey{Time: aTime, Stream: "a", Client: "b"}
	archiveKey2, err := DecodeArchiveKey(archiveKey.Path())

	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}
	if !archiveKey.Equal(archiveKey2) {
		t.Fatalf("expecting %+v == %+v", archiveKey, archiveKey2)
	}

}
