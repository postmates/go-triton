.PHONY: triton test deps

all: triton

test: deps
	go test github.com/postmates/go-triton/triton

deps:
	go get -d .

triton: build deps
	go build -o build/triton ./triton.go

build:
	mkdir -p build

clean:
	rm -rf build

cscope:
	find $$GOPATH/src -type f -iname "*.go"> cscope.files
	cscope -b -k
