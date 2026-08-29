.PHONY: build tidy test vet lint generate

tidy:
	@go mod tidy

# Regenerate protocol/schema_gen.go from protocol/schema.go.
# The arpack version is pinned via tools/tools.go.
generate:
	@go run github.com/edmand46/arpack/cmd/arpack -in protocol/schema.go -out-go protocol

build:
	@go build ./...

test:
	@go test ./...

vet:
	@go vet ./...

lint:
	@test -z "$$(gofmt -l .)" || (echo "unformatted files:" && gofmt -l . && exit 1)
