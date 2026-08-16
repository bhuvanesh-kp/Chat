build-chat:
	@go build -o ./bin/chat ./...

chat: build-chat
	@./bin/chat

test-chat-race:
	@go clean -testcache
	@go test -race ./...