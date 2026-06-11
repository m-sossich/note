FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /cas ./demo/cas

FROM alpine:3.19
RUN mkdir /data /out
COPY --from=builder /cas /cas
ENTRYPOINT ["/cas"]
