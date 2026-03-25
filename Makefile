.PHONY: clean test

mqsubscriber: go.* *.go
	go build -o $@ ./cmd/mqsubscriber

clean:
	rm -rf mqsubscriber dist/

test:
	go test -v ./...

install:
	go install github.com/fujiwara/mqsubscriber/cmd/mqsubscriber

dist:
	goreleaser build --snapshot --clean
