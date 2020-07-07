FROM haskell:8.10-buster as dependencies

ARG deb_mirror="http://deb.debian.org"
ARG cabal_mirror_name="hackage.haskell.org"
ARG cabal_mirror_url="http://hackage.haskell.org/"

RUN echo "\
deb $deb_mirror/debian buster main \n\
deb http://security.debian.org/debian-security buster/updates main \n\
deb $deb_mirror/debian buster-updates main \n\
" > /etc/apt/sources.list

RUN apt-get update && apt-get install --no-install-recommends -y \
      librocksdb-dev

RUN cabal user-config init && echo "\
repository $cabal_mirror_name \n\
  url: $cabal_mirror_url \n\
" > /root/.cabal/config && cabal user-config update

RUN mkdir /src
WORKDIR /src

RUN cabal update

COPY mangrove.cabal cabal.project /src/
RUN cabal update && cabal v2-build --dependencies-only

# ------------------------------------------------------------------------------

FROM haskell:8.10-buster AS builder

WORKDIR /src

COPY --from=dependencies /etc/apt/sources.list /etc/apt/sources.list
RUN apt-get update && apt-get install --no-install-recommends -y \
      librocksdb-dev

COPY --from=dependencies /src/dist-newstyle /src/dist-newstyle
COPY --from=dependencies /root/.cabal /root/.cabal
COPY . /src

RUN cabal v2-install

# ------------------------------------------------------------------------------

FROM debian:buster-slim

COPY --from=dependencies /etc/apt/sources.list /etc/apt/sources.list
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
