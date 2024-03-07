FROM golang:1.22-alpine AS BuilStage

ENV CGO_ENABLED 0

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY build/gojq_extentions/ ./../gojq_extentions/

RUN go mod download

COPY src/ ./src/

RUN go build -C src/main -o /app/streaming_monitors

FROM alpine:latest

WORKDIR /app

COPY --from=BuilStage /app/streaming_monitors ./streaming_monitors

ENTRYPOINT [ "./streaming_monitors" ]
