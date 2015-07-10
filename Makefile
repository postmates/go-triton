.PHONY: triton
export GOPATH=$(shell echo $$PWD)

all: build/bin/triton

deps:
	go get -d triton/commands

triton: build deps
	go build -o build/triton src/triton/commands/triton.go

build:
	mkdir -p build

clean:
	rm -rf build pkg
