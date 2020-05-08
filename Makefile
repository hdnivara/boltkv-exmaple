GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
BIN_NAME=boltkv-example

LINTCMD=golangci-lint run
LINTOPTS=-E funlen \
	 -E lll \
	 -E nakedret \
	 -E gocritic \
	 -E godot \
	 -E golint \
	 -E stylecheck \
	 -E unconvert \
	 -E unparam
GOLINT=$(LINTCMD) $(LINTOPTS)


all: test build

pr: build lint testv clean

build:
	$(GOBUILD)

test:
	$(GOTEST) ./...

testv:
	$(GOTEST) -v ./...

lint:
	$(GOLINT)

clean:
	$(GOCLEAN)
	rm -f $(BIN_NAME)
