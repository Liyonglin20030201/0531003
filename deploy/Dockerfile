FROM golang:1.22-alpine AS builder

RUN apk add --no-cache gcc musl-dev rocksdb-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -o config-center ./cmd/config-center

FROM alpine:3.19
RUN apk add --no-cache rocksdb

WORKDIR /app
COPY --from=builder /app/config-center .

ENTRYPOINT ["./config-center"]
