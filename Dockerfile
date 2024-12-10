FROM docker.io/golang:1.23-alpine AS BuildStage

ENV CGO_ENABLED 0

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY build/gojq_extentions/ ./../gojq_extentions/

RUN go mod download

COPY ./src/ ./src/

RUN go build -C src/main -o /app/streaming-metrics

FROM docker.io/alpine:latest

WORKDIR /app

COPY --from=BuildStage /app/streaming-metrics ./streaming-metrics

ENTRYPOINT [ "./streaming-metrics" ]
