.PHONY: help test short race matrix large dash build vet

help:
	@echo "make test    - run all Go tests including 43-case matrix"
	@echo "make short   - run fast tests only"
	@echo "make race    - run focused end-to-end tests with -race"
	@echo "make matrix  - generate reports/mptest-report.*"
	@echo "make large   - create/transfer/compare a 300 MiB random file"
	@echo "make dash    - start dashboard on 127.0.0.1:8090"

test:
	go test ./... -timeout 500s

short:
	go test -short ./...

race:
	go test -race ./tests -run 'Test(SmokeExactBytes|MiddleboxFallback|MaxSubflowsAndPriorityRefill|SlowPathWindowRescue|DisconnectRedirectAndAutoRejoin|HighLossHandshake)' -timeout 180s

matrix:
	mkdir -p reports
	go run ./cmd/mptest -matrix-mb 2 -large-mb 0 -json reports/mptest-report.json -markdown reports/mptest-report.md

large:
	mkdir -p reports
	go run ./cmd/mptest -skip-matrix -large-mb 300 -json reports/large-file-300m.json -markdown reports/large-file-300m.md

dash:
	go run ./cmd/mpdash -addr 127.0.0.1:8090 -size-mb 32

vet:
	go vet ./...

build:
	go build ./...
