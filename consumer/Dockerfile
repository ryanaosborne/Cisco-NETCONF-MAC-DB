FROM golang:1.22-alpine AS builder
WORKDIR /app
RUN apk add --no-cache protobuf-dev
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.32.0 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
COPY go.mod ./
COPY proto/ ./proto/
COPY consumer/ ./consumer/
RUN protoc --go_out=. --go-grpc_out=. proto/telemetry.proto
RUN go mod download && go mod tidy
RUN go build -o consumer ./consumer

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/consumer .
CMD ["./consumer"]