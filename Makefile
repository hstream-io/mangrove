docker-build:
	@docker build -f Dockerfile.builder . -t mangrove-builder
	@docker build -f Dockerfile . -t mangrove

docker-build-cn:
	@docker build --build-arg deb_mirror=http://mirrors.ustc.edu.cn --build-arg cabal_mirror_name=mirrors.tuna.tsinghua.edu.cn --build-arg cabal_mirror_url=http://mirrors.tuna.tsinghua.edu.cn/hackage -f Dockerfile.builder . -t mangrove-builder
	@docker build -f Dockerfile . -t mangrove
