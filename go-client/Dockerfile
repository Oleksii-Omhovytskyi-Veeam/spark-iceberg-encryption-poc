# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Multi-stage build for the external Spark Connect Go client.
#   Stage 1: resolve deps + compile a static binary against spark-connect-go v35.
#   Stage 2: drop the binary onto a tiny base.
# go.mod pins only the direct dependency; `go mod tidy` resolves the transitive
# graph (grpc, arrow, protobuf, ...) and writes go.sum at build time, so the
# client always builds reproducibly from a single direct require.
# ---------------------------------------------------------------------------
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod main.go ./
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /go-client .

# ---------------------------------------------------------------------------
FROM debian:bookworm-slim
COPY --from=build /go-client /usr/local/bin/go-client
ENTRYPOINT ["/usr/local/bin/go-client"]
