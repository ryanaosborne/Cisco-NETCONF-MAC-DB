FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY collector/ ./collector/
RUN GOFLAGS=-mod=mod go build -o collector ./collector

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/collector .
CMD ["./collector"]
