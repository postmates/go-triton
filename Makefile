.PHONY: triton
export GOPATH=$(shell echo $$PWD)

all: build/bin/triton

deps:
	go get -d ./

triton: build deps
	go build -o build/triton ./triton.go

build:
	mkdir -p build

clean:
	rm -rf build pkg src
