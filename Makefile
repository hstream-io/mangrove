lint:
	-hlint src/ app/ test/ benchmark/

BUILD_TAG ?= $(shell git rev-parse HEAD)
CABAL_MIRROR_NAME ?= hackage.haskell.org
CABAL_MIRROR_URL ?= http://hackage.haskell.org/

docker-build:
	@docker build -f Dockerfile --build-arg cabal_mirror_name=$(CABAL_MIRROR_NAME) --build-arg cabal_mirror_url=$(CABAL_MIRROR_URL) -t mangrove:$(BUILD_TAG) .
