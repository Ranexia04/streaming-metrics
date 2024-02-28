FROM golang:1.21-alpine AS BuilStage

ENV CGO_ENABLED 0

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY build/gojq_extention/ ./../gojq_extention/

RUN go mod download

COPY src/ ./src/

RUN go build -C src/main -o /app/streaming_monitorsssss

FROM alpine:latest

WORKDIR /app

COPY --from=BuilStage /app/streaming_monitor ./streaming_monitors

ENTRYPOINT [ "./streaming_monitor" ]
