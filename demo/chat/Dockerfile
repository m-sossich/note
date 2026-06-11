FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /chat ./demo/chat

FROM alpine:3.19
COPY --from=builder /chat /chat
ENTRYPOINT ["/chat"]
