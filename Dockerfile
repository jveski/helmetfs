FROM golang:1.24-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o helmetfs .

FROM alpine:3.21

RUN apk add --no-cache rclone ca-certificates

COPY --from=builder /app/helmetfs /usr/local/bin/helmetfs

ENTRYPOINT ["helmetfs"]
