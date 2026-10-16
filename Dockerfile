# syntax=docker/dockerfile:1
#
# qd container image.
#
#   docker build -t qd .                       # production image (distroless, nonroot)
#   docker build --target runtime-debug -t qd:debug .     # same binary + busybox shell (for compose healthchecks / debugging)
#
# Works with BuildKit, the classic builder, podman/buildah and kaniko (no BuildKit-only features required).

ARG RUST_VERSION=1
ARG DEBIAN_RELEASE=bookworm

###############################################################################
# Builder
###############################################################################
FROM docker.io/library/rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder
WORKDIR /src

# rust-toolchain.toml pins the "stable" channel; make sure the image's toolchain satisfies it
# without a network round-trip inside later layers.
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain || rustup toolchain install

# --- Dependency layer -------------------------------------------------------
# Copy only manifests + lockfile and build against stub sources, so the (slow) dependency
# compilation is cached until Cargo.toml / Cargo.lock change.
COPY Cargo.toml Cargo.lock ./
COPY crates/qd-engine/Cargo.toml     crates/qd-engine/Cargo.toml
COPY crates/qd-sqs/Cargo.toml        crates/qd-sqs/Cargo.toml
COPY crates/qd/Cargo.toml crates/qd/Cargo.toml
COPY crates/qd-e2e/Cargo.toml        crates/qd-e2e/Cargo.toml
RUN set -eux; \
    for c in qd-engine qd-sqs qd qd-e2e; do \
        mkdir -p crates/$c/src; echo '' > crates/$c/src/lib.rs; \
    done; \
    echo 'fn main() {}' > crates/qd/src/main.rs; \
    cargo build --release --locked -p qd; \
    rm -rf crates/*/src \
           target/release/qd* \
           target/release/deps/qd-* target/release/deps/libqd-* \
           target/release/deps/qd_engine-* target/release/deps/libqd_engine-* \
           target/release/deps/qd_sqs-* target/release/deps/libqd_sqs-* \
           target/release/.fingerprint/qd-* \
           target/release/.fingerprint/qd-engine-* \
           target/release/.fingerprint/qd-sqs-*

# --- Application layer ------------------------------------------------------
COPY crates ./crates
# Bump mtimes so cargo never mistakes the real sources for the (newer) stubs.
RUN set -eux; \
    find crates -type f \( -name '*.rs' -o -name '*.sql' \) -exec touch {} +; \
    cargo build --release --locked -p qd; \
    install -D -m 0755 target/release/qd /out/qd; \
    /out/qd --version

###############################################################################
# Debug runtime: distroless + busybox (sh, wget). Used by docker-compose for healthchecks.
###############################################################################
FROM gcr.io/distroless/cc-debian12:debug-nonroot AS runtime-debug
COPY --from=builder /out/qd /usr/local/bin/qd
ENV QD_LOG_FORMAT=json \
    QD_BIND=0.0.0.0:9324 \
    QD_ADMIN_BIND=0.0.0.0:9325
USER 65532:65532
EXPOSE 9324 9325
ENTRYPOINT ["/usr/local/bin/qd"]

###############################################################################
# Production runtime (default target): distroless, glibc + libgcc + CA certs, no shell, nonroot.
###############################################################################
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

ARG VERSION=0.1.0
ARG REVISION=unknown
ARG CREATED=unknown
LABEL org.opencontainers.image.title="qd" \
      org.opencontainers.image.description="Highly-available, Postgres-backed, AWS SQS-compatible message queue" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.source="https://gitlab.hexploits.cloud/hexploits/services/simply-queued" \
      org.opencontainers.image.url="https://gitlab.hexploits.cloud/hexploits/services/simply-queued" \
      org.opencontainers.image.vendor="Hexploits" \
      org.opencontainers.image.licenses="LicenseRef-Proprietary" \
      org.opencontainers.image.base.name="gcr.io/distroless/cc-debian12:nonroot"

COPY --from=builder /out/qd /usr/local/bin/qd

# JSON logs by default (container log pipelines); override with QD_LOG_FORMAT=text.
ENV QD_LOG_FORMAT=json \
    QD_BIND=0.0.0.0:9324 \
    QD_ADMIN_BIND=0.0.0.0:9325

# 9324: SQS API (server role)   9325: admin (/healthz liveness, /readyz readiness, /metrics)
EXPOSE 9324 9325
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/qd"]
