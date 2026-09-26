# Build the control plane as a single static binary.
#
# The whole cluster is goroutines in one process over the in-process transport,
# so this image is the entire system: no sidecars, no database, no orchestration.
# That is what lets it run on a free tier.

FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# cache on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

# CGO off keeps the binary static, which is what lets the runtime stage be
# scratch. There is no C dependency to begin with.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /out/cluster ./cmd/cluster

FROM scratch

# The binary makes no outbound TLS calls, but an empty /etc/passwd and CA bundle
# cost nothing and keep the image honest if that ever changes.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/cluster /cluster

# Render and Fly both inject PORT and expect the process to bind it. The default
# matters only for a bare `docker run`.
ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["/cluster"]
