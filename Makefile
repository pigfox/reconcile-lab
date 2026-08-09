# Recipe lines below are indented with TABS. Make requires it — spaces produce
# "missing separator" on every target.

.PHONY: test cover sweep clean

test:
	go test -race -shuffle=on ./...
	go vet ./...
	@test -z "$$(gofmt -l .)" || { echo "gofmt needs to run on:"; gofmt -l .; exit 1; }

# Coverage is measured on the simulation package, which is the code this
# repository authored. cmd/sweep is a driver whose only job is to print what the
# package computes; it has no behavior of its own worth asserting.
cover:
	go test -covermode=set -coverprofile=cover.out ./internal/reconcilelab/
	go tool cover -func=cover.out | tail -1

# sweep reproduces the contention table. It runs on the virtual clock, so the
# whole thing finishes in about a second and gives the same answer every time.
sweep:
	go run ./cmd/sweep

clean:
	rm -f cover.out
