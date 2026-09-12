# gosched — work-stealing scheduler with observable traces.
#
# Every target runs with the repository-local Go toolchain layout so that the
# commands in the README are exactly the commands CI/users would run.

GO ?= go
PKGS := ./...

.PHONY: all build vet fmt fmtcheck test test-race test-stress cover \
        bench bench-short results trace-report example-trace clean verify

all: build vet test

build:
	$(GO) build $(PKGS)

vet:
	$(GO) vet $(PKGS)

fmt:
	gofmt -l -w .

fmtcheck:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

# Fast unit test pass.
test:
	$(GO) test $(PKGS)

# Race detector over the whole tree. This is the gate the spec requires.
test-race:
	$(GO) test -race $(PKGS)

# Concurrency stability: many repetitions to shake out lost wakeups,
# double-execution and ring buffer races.
test-stress:
	$(GO) test -race -count=5 $(PKGS)

cover:
	$(GO) test -race -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -20

# Go benchmarks (throughput micro-benchmarks).
bench:
	$(GO) test -run XXX -bench=. -benchtime=2s ./bench/...

# The real measurement harness: full matrix written into results/.
results:
	$(GO) run ./cmd/gosched-bench -out ../results -reps 5

# Human-readable aggregation of a recorded trace.
trace-report:
	$(GO) run ./cmd/gosched-trace -in ../results/example-trace.json

# Regenerate the committed example trace + its report from a deterministic run.
example-trace:
	$(GO) run ./cmd/gosched-bench -mode example -out ../results

verify: build vet fmtcheck test-race test-stress

clean:
	rm -rf bin coverage.out
