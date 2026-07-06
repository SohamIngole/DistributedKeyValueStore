.PHONY: build test bench run-cluster clean

build:
	go build ./...

test:
	go test -race ./...

bench:
	go test -bench=. -benchmem ./...

run-cluster:
	docker-compose up --build

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

clean:
	docker-compose down -v
	rm -f *.aof coverage.out coverage.html