## default to coverage
all: coverage

## show available targets
help:
	@printf "\nAvailable make targets:\n"
	@printf "  %-15s %s\n" "all" "default (runs coverage)"
	@printf "  %-15s %s\n" "bench" "run benchmarks"
	@printf "  %-15s %s\n" "clean" "remove generated files (coverage)"
	@printf "  %-15s %s\n" "coverage" "generate coverage and function report"
	@printf "  %-15s %s\n" "help" "show available targets (this message)"
	@printf "  %-15s %s\n" "htmlcoverage" "generate coverage and HTML report"
	@printf "  %-15s %s\n" "test" "run unit tests (verbose)"
	@printf "\n"

## run test
test:
	go test -v ./...

## generate coverage + html
htmlcoverage: make-coverage-path
	go test -coverprofile=coverage/coverage.out ./...
	go tool cover -html=coverage/coverage.out -o coverage/coverage.html

## generate coverage + func output
coverage: make-coverage-path
	go test -coverprofile=coverage/coverage.out ./...
	go tool cover -func=coverage/coverage.out 

## run benchmark
bench:
	go test -bench=. ./...

make-coverage-path:
	@mkdir -p coverage

## clean
clean:
	rm -rf coverage

.PHONY: make-coverage-path test clean coverage bench help
