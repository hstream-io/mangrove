FROM hstreamio/haskell-rocksdb:8.10 as dependencies

ARG cabal_mirror_name="hackage.haskell.org"
ARG cabal_mirror_url="http://hackage.haskell.org/"

RUN cabal user-config init && echo "\
repository $cabal_mirror_name \n\
  url: $cabal_mirror_url \n\
" > /root/.cabal/config && cabal user-config update

RUN cabal update

RUN rm -rf /srv && mkdir /srv
COPY mangrove.cabal cabal.project /srv/
RUN cd /srv && cabal update && cabal v2-build --dependencies-only

# ------------------------------------------------------------------------------

FROM hstreamio/haskell-rocksdb:8.10 as builder

COPY --from=dependencies /srv/dist-newstyle /srv/dist-newstyle
COPY --from=dependencies /root/.cabal /root/.cabal
COPY . /srv
RUN cd /srv && cabal v2-install --flag benchmark-cli

# ------------------------------------------------------------------------------

FROM hstreamio/haskell-rocksdb:base-runtime

COPY --from=builder /root/.cabal/bin/mangrove /usr/local/bin/mangrove
RUN mkdir -p /etc/mangrove/
COPY app/config.example.yaml /etc/mangrove/config.example.yaml

EXPOSE 6560
CMD ["/usr/local/bin/mangrove", "/etc/mangrove/config.example.yaml"]
