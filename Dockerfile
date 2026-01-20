FROM golang:1.24 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o helmetfs .

FROM ubuntu:24.04

ARG TARGETARCH=amd64

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl unzip && \
    curl -fsSL -o /tmp/rclone.zip "https://downloads.rclone.org/rclone-current-linux-${TARGETARCH}.zip" && \
    unzip -j /tmp/rclone.zip -d /tmp/rclone && \
    install /tmp/rclone/rclone /usr/local/bin/rclone && \
    rm -rf /tmp/rclone.zip /tmp/rclone && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/helmetfs /usr/local/bin/helmetfs

ENTRYPOINT ["helmetfs"]
