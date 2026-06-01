.PHONY: proto build clean run

BINARY=config-center

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/raft_transport.proto proto/config_service.proto

build:
	go build -o $(BINARY) ./cmd/config-center

clean:
	rm -f $(BINARY)
	rm -rf data/

run: build
	./$(BINARY)

test:
	go test ./... -v

tidy:
	go mod tidy
