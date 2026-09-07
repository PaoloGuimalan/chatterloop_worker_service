# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build

WORKDIR /src

# Manifests first, so the dependency layer caches independently of source edits
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 yields a static binary that runs on a distroless base.
# -s -w strips debug info and symbol tables to shrink it.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/worker ./cmd/worker

# static-debian12 ships ca-certificates, which the Supabase TLS connection needs
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/worker /app/worker

# The Astra secure-connect bundle is NOT baked in. It arrives at runtime as the
# external swarm secret `astra_bundle`, mounted at the path CASSANDRA_DB_BUNDLE
# points to. Copying it here would both break CI (the file is gitignored, so a
# fresh clone lacks it) and publish database credentials inside the image.

EXPOSE 8880
USER nonroot:nonroot

ENTRYPOINT ["/app/worker"]