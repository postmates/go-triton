.PHONY: triton test

all: test triton

test:
	go test github.com/postmates/postal-go-triton/triton

deps:
	go get -d .

triton: build deps
	go build -o build/triton ./triton.go

build:
	mkdir -p build

clean:
	rm -rf build pkg src
