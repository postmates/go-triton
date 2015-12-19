package triton

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ArchiveKey is a struct representing the path value for the Triton S3 keys
type ArchiveKey struct {
	Client string
	Stream string
	Time   time.Time
}

// Path encodes the ArchiveKey to a string path
func (a ArchiveKey) Path() string {
	return fmt.Sprintf("%04d%02d%02d/%s-%d.tri", a.Time.Year(), a.Time.Month(), a.Time.Day(), a.fullStreamName(), a.Time.Unix())
}

const (
	metadataSuffix = ".metadata"
)

// MetadataPath encodes the ArchiveKey to a string path with the metadata suffix applied
func (a ArchiveKey) MetadataPath() string {
	return a.Path() + metadataSuffix
}

// fullStreamName returns the full stream name (stream + "-" + client) if there is a client name or just stream
func (a ArchiveKey) fullStreamName() (stream string) {
	stream = a.Stream
	if a.Client != "" {
		stream += "-" + a.Client
	}
	return
}

// PathPrefix returns the string key prefix without the timestamp
func (a ArchiveKey) PathPrefix() string {
	return fmt.Sprintf("%04d%02d%02d/%s-", a.Time.Year(), a.Time.Month(), a.Time.Day(), a.fullStreamName())
}

func (a ArchiveKey) Equal(other ArchiveKey) (result bool) {
	if a.Stream != other.Stream {
		return false
	}
	if a.Time.Truncate(time.Second) != other.Time.Truncate(time.Second) {
		return false
	}
	if a.Client != other.Client {
		return false
	}
	return true
}

var archiveKeyPattern = regexp.MustCompile(`^/?(?P<day>\d{8})\/(?P<stream>.+)\-(?P<ts>\d+)\.tri$`)

// Decode an archive S3 key into an ArchiveKey
func DecodeArchiveKey(keyName string) (a ArchiveKey, err error) {
	res := archiveKeyPattern.FindStringSubmatch(keyName)
	if res == nil {
		err = fmt.Errorf("Invalid key name")
		return
	}
	ts, err := strconv.ParseInt(res[3], 10, 64)
	if err != nil {
		err = fmt.Errorf("Failed to parse timestamp value: %s", err.Error())
		return
	}
	a.Time = time.Unix(ts, 0)
	nameParts := strings.Split(res[2], "-")
	if len(nameParts) != 2 {
		err = fmt.Errorf("Failure parsing stream name: %v", res[2])
		return
	}
	a.Stream = nameParts[0]
	a.Client = nameParts[1]
	return
}
