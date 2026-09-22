# syntax=docker/dockerfile:1

# One Dockerfile for every Go service: the service is picked at build time.
#   docker build --build-arg SERVICE=gateway -t ticketwave/gateway .
#
# Two stages. The first has the whole Go toolchain (hundreds of MB) and exists only
# to produce one static binary. The second is what ships: nothing but that binary,
# so there is no shell or package manager for an attacker to use, and far less for a
# vulnerability scanner to flag.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first, on their own layer: they change rarely, so Docker reuses this
# layer (and the cache mount) instead of re-downloading on every code change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG SERVICE
RUN test -n "$SERVICE" || (echo "pass --build-arg SERVICE=<name>" && exit 1)
# CGO_ENABLED=0 gives a fully static binary, required because the runtime image has
# no libc. -trimpath removes build-machine paths; -s -w strip debug info (smaller).
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./services/${SERVICE}/cmd

FROM gcr.io/distroless/static-debian12:nonroot
# Services read relative paths (certs/...), so they run from /app. Keys are mounted
# there at runtime and are never baked into the image.
WORKDIR /app
COPY --from=build /out/app /app/app
USER nonroot:nonroot
ENTRYPOINT ["/app/app"]
