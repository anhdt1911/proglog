install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--proto_path=.

test:
	go test -race ./...
