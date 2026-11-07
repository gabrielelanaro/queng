FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOMAXPROCS=2 go build -p 1 -o /out/queng-coordinator ./cmd/queng-coordinator && \
    CGO_ENABLED=0 GOMAXPROCS=2 go build -p 1 -o /out/queng-worker ./cmd/queng-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
