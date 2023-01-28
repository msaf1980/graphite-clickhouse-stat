all: test build

VERSION ?= $(shell git describe --abbrev=4 --dirty --always --tags)

# K6_STAT_DB_ADDR ?= "http://localhost:8123"
# K6_STAT_DB ?= "default"

DOCKER ?= docker
GO ?= go

SRCS:=$(shell find . -name '*.go' | grep -v 'vendor')

## help: Prints a list of available build targets.
help:
	echo "Usage: make <OPTIONS> ... <TARGETS>"
	echo ""
	echo "Available targets are:"
	echo ''
	sed -n 's/^##//p' ${PWD}/Makefile | column -t -s ':' | sed -e 's/^/ /'
	echo
	echo "Targets run by default are: `sed -n 's/^all: //p' ./Makefile | sed -e 's/ /, /g' | sed -e 's/\(.*\), /\1, and /'`"

## clean: Removes any previously created build artifacts.
clean:
	rm -f ./graphite-clickhouse-stat

build: FORCE
	GO111MODULE=on ${GO} build -ldflags '-X main.BuildVersion=$(VERSION)' ${PWD}/cmd/graphite-clickhouse-stat

## format: Applies Go formatting to code.
format:
	${GO} fmt ./...

## test: Executes any unit tests.
test:
	${GO} test -cover -race ./...

lint:
	golangci-lint run

FORCE:

.PHONY: build
