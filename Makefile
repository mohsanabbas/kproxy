.PHONY: all build test race vet bench fuzz tidy lint vuln cover clean

GO ?= go
PKG := ./...

all: vet test race

build:
	$(GO) build $(PKG)

vet:
	$(GO) vet $(PKG)

test:
	$(GO) test -count=1 -timeout 60s $(PKG)

race:
	$(GO) test -count=1 -race -timeout 90s $(PKG)

bench:
	$(GO) test -run=^$$ -bench=. -benchmem -benchtime=2s ./internal/proxy ./internal/plan ./internal/kwire

fuzz:
	$(GO) test -run=^$$ -fuzz=FuzzPrimitivesDontPanic -fuzztime=10s ./internal/kwire

tidy:
	$(GO) mod tidy

vuln:
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest
	$(shell $(GO) env GOPATH)/bin/govulncheck $(PKG)

cover:
	$(GO) test -count=1 -coverprofile=coverage.out $(PKG)
	$(GO) tool cover -func=coverage.out | tail -n 20

clean:
	rm -f coverage.out
	$(GO) clean $(PKG)
