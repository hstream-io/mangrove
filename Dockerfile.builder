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

FROM haskell:8.10-buster

WORKDIR /src

COPY --from=dependencies /etc/apt/sources.list /etc/apt/sources.list
RUN apt-get update && apt-get install --no-install-recommends -y \
      librocksdb-dev

COPY --from=dependencies /src/dist-newstyle /src/dist-newstyle
COPY --from=dependencies /root/.cabal /root/.cabal
COPY . /src

RUN cabal v2-install --flag benchmark-cli

# vim: set ft=dockerfile:
