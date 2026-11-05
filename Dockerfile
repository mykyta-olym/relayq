FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
    ./cmd/relayq ./cmd/relayq-bench ./examples/demo/producer ./examples/demo/worker \
    && mv /out/producer /out/demo-producer && mv /out/worker /out/demo-worker

FROM alpine:3.22
COPY --from=build /out/ /usr/local/bin/
USER 65534:65534
ENTRYPOINT []
CMD ["relayq", "serve", "-addr", ":8080"]
