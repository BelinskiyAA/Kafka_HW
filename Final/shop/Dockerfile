FROM golang:1.25.4-alpine3.22 AS builder

RUN apk add --no-cache ca-certificates tzdata
RUN apk update && apk add --no-cache gcc git libc-dev librdkafka librdkafka-dev

WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN  CGO_ENABLED=1 go build -tags musl -v -ldflags="-s -w" -o /usr/local/bin/producer ./cmd/Producer/...
RUN  CGO_ENABLED=1 go build -mod=mod -tags musl -v -ldflags="-s -w" -o /usr/local/bin/block ./cmd/Block/...
RUN  CGO_ENABLED=1 go build -tags musl -v -ldflags="-s -w" -o /usr/local/bin/filter ./cmd/Filter/...

FROM alpine:3.22

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/producer /usr/local/bin/producer
COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/block /usr/local/bin/block
COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/filter /usr/local/bin/filter

USER appuser
