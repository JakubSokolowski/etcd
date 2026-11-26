FROM golang:1.20-alpine AS builder
WORKDIR /src

COPY . .

WORKDIR /src/server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /out/etcd .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /out/savescum-driver ./cmd/savescum-driver

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/etcd /usr/local/bin/etcd
COPY --from=builder /out/savescum-driver /usr/local/bin/savescum-driver
ENTRYPOINT ["/usr/local/bin/etcd"]
