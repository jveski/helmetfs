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
    apt-get install -y --no-install-recommends ca-certificates curl && \
    curl -fsSL -o /tmp/rclone.deb "https://github.com/rclone/rclone/releases/latest/download/rclone-current-linux-${TARGETARCH}.deb" && \
    dpkg -i /tmp/rclone.deb && \
    rm /tmp/rclone.deb && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/helmetfs /usr/local/bin/helmetfs

ENTRYPOINT ["helmetfs"]
