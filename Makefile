BINARY=rockset-mongo
VERSION=0.0.1
BUILD=`git rev-parse HEAD`
PLATFORMS=darwin linux windows
ARCHITECTURES=amd64 arm64

# Setup linker flags option for build that interoperate with variable names in src code
LDFLAGS=-ldflags "-X main.VersionStr=${VERSION} -X main.GitCommit=${BUILD}"

default: build

all: clean build_all

build:
	go build ${LDFLAGS} -o ${BINARY}

build_all:
	mkdir -p bin
	$(foreach GOOS, $(PLATFORMS),\
	$(foreach GOARCH, $(ARCHITECTURES), $(shell export GOOS=$(GOOS); export GOARCH=$(GOARCH); go build $(LDFLAGS) -o bin/$(BINARY)-$(GOOS)-$(GOARCH)-$(VERSION) .)))

clean:
	go clean
	rm -rf bin

.PHONY: check clean install build_all all
