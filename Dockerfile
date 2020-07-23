FROM mangrove-builder as builder

# ------------------------------------------------------------------------------

FROM debian:buster-slim

COPY --from=builder /etc/apt/sources.list /etc/apt/sources.list
RUN apt-get update && apt-get install --no-install-recommends -y \
      librocksdb-dev
RUN apt-get clean autoclean && \
    apt-get autoremove --yes && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.cabal/bin/mangrove /usr/local/bin/mangrove
RUN mkdir -p /etc/mangrove/
COPY app/config.example.yaml /etc/mangrove/config.example.yaml

EXPOSE 6560
CMD ["/usr/local/bin/mangrove", "/etc/mangrove/config.example.yaml"]
