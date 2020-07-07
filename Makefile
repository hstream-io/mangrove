docker-build:
	@docker build . -t mangrove

docker-build-cn:
	@docker build --build-arg deb_mirror=http://mirrors.ustc.edu.cn --build-arg cabal_mirror_name=mirrors.tuna.tsinghua.edu.cn --build-arg cabal_mirror_url=http://mirrors.tuna.tsinghua.edu.cn/hackage . -t mangrove-cn
