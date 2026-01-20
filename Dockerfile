FROM golang:1.24 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o helmetfs .

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends rclone ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/helmetfs /usr/local/bin/helmetfs

ENTRYPOINT ["helmetfs"]
