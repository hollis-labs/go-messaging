.PHONY: check fmt vet lint test vuln

check: fmt vet lint test vuln

fmt:
	go fmt ./...

vet:
	go vet ./...
	@echo "✓ go vet: ok"

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not installed; see https://golangci-lint.run/usage/install/"; exit 1; }
	golangci-lint run --max-issues-per-linter=0 --max-same-issues=0

test:
	go test -race -count=1 ./...

vuln:
	@command -v govulncheck >/dev/null 2>&1 || go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck ./...
