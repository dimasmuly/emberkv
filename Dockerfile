# syntax=docker/dockerfile:1

FROM rust:1.89-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p emberkv-server -p emberkv-cli

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --no-create-home emberkv \
 && mkdir /data /backups && chown emberkv /data /backups
COPY --from=build /src/target/release/emberkv-server /src/target/release/ekv /usr/local/bin/
USER emberkv
VOLUME ["/data", "/backups"]
EXPOSE 7878 8080 9464
# Listening on all interfaces requires an auth file (EMBERKV_AUTH_FILE);
# the server refuses to start publicly without one.
ENV EMBERKV_LISTEN=0.0.0.0:7878 \
    EMBERKV_HTTP_LISTEN=0.0.0.0:8080 \
    EMBERKV_DATA_DIR=/data \
    EMBERKV_METRICS_LISTEN=0.0.0.0:9464 \
    EMBERKV_LOG_FORMAT=json
HEALTHCHECK --interval=10s --timeout=2s CMD ["ekv", "ping"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["emberkv-server"]
