FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /DistributedKeyValueStore ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /coordinator ./cmd/coordinator

FROM scratch
COPY --from=builder /DistributedKeyValueStore /DistributedKeyValueStore
COPY --from=builder /coordinator /coordinator
EXPOSE 6379 6380 6381 6399 6400 7000
ENTRYPOINT ["/DistributedKeyValueStore"]