package triton

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cenkalti/backoff"
	"github.com/tinylib/msgp/msgp"
)

// Writer represents a Kinesis writer configured to use a stream and
// partition key.
type Writer interface {
	WriteRecords(r ...Record) error
}

// NewWriter returns a Writer using the given configuration.
// The returned writer is a syncronous writer.
func NewWriter(config *StreamConfig) Writer {
	awsConfig := aws.NewConfig().WithRegion(config.RegionName)
	sess := session.New(awsConfig)
	svc := kinesis.New(sess)

	return &writer{
		config:         config,
		svc:            svc,
		maxBackoffWait: 2 * time.Minute,
	}
}

type writer struct {
	config *StreamConfig
	svc    KinesisService

	maxBackoffWait time.Duration
}

// WriteRecords synchronously writes data to the Kinesis stream.
//
// Writes to Kinesis are expected to take ~100ms. Upon error, WriteRecord
// will retry with exponential backoff. Care must be taken to protect against
// unwanted blocking.
func (w *writer) WriteRecords(rs ...Record) error {
	inputs := make([]*recordInput, len(rs))
	for i, r := range rs {
		ri, err := w.recordInputFromRecord(r)
		if err != nil {
			return err
		}
		inputs[i] = ri
	}

	return w.write(inputs...)
}

func (w *writer) write(inputs ...*recordInput) error {
	records := make([]*kinesis.PutRecordsRequestEntry, len(inputs))
	for i, input := range inputs {
		records[i] = &kinesis.PutRecordsRequestEntry{
			Data:         input.data,
			PartitionKey: aws.String(input.partition),
		}
	}

	params := &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: aws.String(w.config.StreamName),
	}

	writeOp := func() error {
		_, err := w.svc.PutRecords(params)
		return err
	}

	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = 100 * time.Millisecond
	eb.MaxElapsedTime = w.maxBackoffWait

	return backoff.Retry(writeOp, eb)
}

type recordInput struct {
	data      []byte
	partition string
}

func (w *writer) recordInputFromRecord(r Record) (*recordInput, error) {
	// extract partition value
	val, ok := r[w.config.PartitionKeyName]
	if !ok {
		errMsg := fmt.Sprintf("Partition key '%s' does not exist", w.config.PartitionKeyName)
		return nil, errors.New(errMsg)
	}

	partition := fmt.Sprintf("%v", val)

	// encode to msgpack
	buf := new(bytes.Buffer)
	encoder := msgp.NewWriter(buf)
	if err := encoder.WriteMapStrIntf(r); err != nil {
		return nil, err
	}
	encoder.Flush()

	ri := &recordInput{
		data:      buf.Bytes(),
		partition: partition,
	}
	return ri, nil
}
