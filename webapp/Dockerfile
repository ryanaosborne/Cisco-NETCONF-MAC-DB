FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY webapp/ ./webapp/
RUN go mod download && go mod tidy
RUN go build -o webapp ./webapp

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/webapp .
EXPOSE 8888
CMD ["./webapp"]
