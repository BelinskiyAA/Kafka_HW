FROM golang:1.25.4-alpine3.22 AS builder

RUN apk add --no-cache ca-certificates tzdata
RUN apk update && apk add --no-cache gcc git libc-dev librdkafka librdkafka-dev

WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN  CGO_ENABLED=1 go build -tags musl -v -ldflags="-s -w" -o /usr/local/bin/search ./cmd/Search/...
RUN  CGO_ENABLED=1 go build -mod=mod -tags musl -v -ldflags="-s -w" -o /usr/local/bin/recom ./cmd/Recom/...

FROM alpine:3.22

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/search /usr/local/bin/search
COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/recom /usr/local/bin/recom

USER appuser
