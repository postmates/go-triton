# Triton - Management for a Kinesis Data Pipeline #

Triton is an opinionated set of tooling for building a data pipeline around an
AWS stack including [Kinesis](http://aws.amazon.com/kinesis/) and S3.

It provides the necessary glue for building real applications on top of the
type of architecture. 

## Overview ##

As your application collects data, write it to Kinesis streams as a series of
events. Other applications in your infrastructure read from this stream
providing a solid pattern for services to share data. 

Triton aims to provide a level of tooling, glue and utility to make this
ecosystem easy to use. Namely:

  1. Standardized, uniform method of determining what streams to write to. 
  1. Process (and standard format) for archiving stream data to S3.
  1. Higher-level APIs for processing streams.

### Configuration ###

Triton applications should share a configuration file for defining streams. Its
yaml formatted, and looks something like:

    my_stream:
      name: my_stream_v2
      partition_key: value
      region: us-west-1

The name configuration allows for a decoupling between application-level
and logical stream names. So your application may only know about a stream
named `user_activity` but the underlying AWS configured stream name may change.


### Serialization and Storage ###

Events in Triton are Serialized using [Message Pack](http://msgpack.org/)

When stored to S3, they are also framed and compressed using
[Snappy](https://code.google.com/p/snappy/source/browse/trunk/framing_format.txt)

This combination is good tradeoff between flexibility and performance.

Archive files in S3 are organized by date and time. For example:

    20150710/user_activity_prod-shardId-000000000000-1436553581.tri

This indicates the stream `user_activity_prod`, shard `shardId-000000000`
stored at unix timestamp `1436553581`. 

The date and time specify when the event was processed, not emitted. There is
no guarantee that each file will contain a specific hour of data.

### Stream Position ###

Triton uses an external database for clients to maintain their stream position.

For example, each time `triton store` writes data to S3, it updates the
database with the last sequence number for each shard in the stream it's
processing. In this way, if the process (or instance) dies, it can resume from
where it left off ensuring uninterrupted data stored in S3.

This checkpoint mechanism is available as a library.

## Usage ##

This repository includes a command line tool `triton` which provides some
tooling for using Kinesis streams.

### Storage ###

The `triton store` command runs a process to store a Kinesis stream into S3.

Events are batched together and uploaded to S3 in something close to hourly
files.

Example usage:


    $ export TRITON_CONFIG=/etc/triton.yaml
    $ export TRITON_DB="postgres://user:password@triton.rds.amazon.com"
    $ triton store --bucket=triton-prod --stream=user_activity


## Client Library ###


### Other Languages ###

The core of the Triton stack is built in Go. It's assumed that Triton will be
used with applications running in other languages besides go. With that in
mind, see also [triton-python](https://github.com/postmates/triton-python)

## Building ##

This package ships a single command: `triton`

Assuming a normal go workspace (placing this code in
`$GOROOT/src/github.com/postmates/go-triton`), you can use the Makefile:

    make

Standard go build commands of course also work.