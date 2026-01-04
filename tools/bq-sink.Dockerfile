FROM golang:1.22 AS build
WORKDIR /workspace
COPY go.mod go.sum ./
COPY main.go ./
COPY internal ./internal
COPY gen ./gen
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/tools .

FROM alpine:3.19
RUN adduser -D app && mkdir -p /workspace && chown app:app /workspace
USER app
WORKDIR /workspace
COPY --from=build /out/tools /usr/local/bin/tools
ENTRYPOINT ["tools"]
CMD ["bq-sink", "--config", "/workspace/tools/config.json"]
