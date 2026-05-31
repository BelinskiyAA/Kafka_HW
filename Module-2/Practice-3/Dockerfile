FROM golang:1.25.4-alpine3.22 AS builder

RUN apk add --no-cache ca-certificates tzdata
RUN apk update && apk add --no-cache gcc git libc-dev librdkafka librdkafka-dev

WORKDIR /usr/src/app

COPY go.mod ./
RUN go mod download

COPY . .
RUN  CGO_ENABLED=1 go build -tags musl -v -ldflags="-s -w" -o /usr/local/bin/app ./cmd/service/...
RUN  CGO_ENABLED=1 go build -tags musl -v -ldflags="-s -w" -o /usr/local/bin/processor ./cmd/processor/...
RUN  CGO_ENABLED=1 go build -tags musl -v -ldflags="-s -w" -o /usr/local/bin/words ./cmd/words/...
RUN  CGO_ENABLED=1 go build -tags musl -v -ldflags="-s -w" -o /usr/local/bin/consumer ./cmd/consumer/...


FROM alpine:3.22

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/app /usr/local/bin/app
COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/processor /usr/local/bin/processor
COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/words /usr/local/bin/words
COPY --from=builder --chown=appuser:appgroup --chmod=755 /usr/local/bin/consumer /usr/local/bin/consumer

USER appuser

EXPOSE 8000
CMD ["app"]