lint-show:
	-hlint src/ app/ test/ benchmark/

BUILD_TAG := $(shell git rev-parse HEAD)

docker-build:
	@docker build -f Dockerfile.builder . -t mangrove-builder:$(BUILD_TAG)
	@docker build -f Dockerfile . -t mangrove:$(BUILD_TAG)

docker-build-cn:
	@docker build --build-arg deb_mirror=http://mirrors.ustc.edu.cn --build-arg cabal_mirror_name=mirrors.tuna.tsinghua.edu.cn --build-arg cabal_mirror_url=http://mirrors.tuna.tsinghua.edu.cn/hackage -f Dockerfile.builder . -t mangrove-builder:$(BUILD_TAG)
	@docker build -f Dockerfile . -t mangrove:$(BUILD_TAG)
