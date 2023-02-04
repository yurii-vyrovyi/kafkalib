
.PHONY: deps
deps:
	go mod tidy

.PHONY: unit_test
unit_test:
	go test -count=1 -v ./... -parallel=4

.PHONY: lint
lint:
	golangci-lint run -v --config=./.golangci.yml


.PHONY: mock_gen
mock_gen:
	go generate ./...