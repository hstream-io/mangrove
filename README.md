# mangrove

**Moved to <https://github.com/hstreamdb/hstream>**

Mangrove is a database management server with some other facilities.

A mangrove has a huge number of aerial roots, enabling it to have high level of gas exchange, which is just like the I/O level of our databases. Also, a mangrove can survive many rough environments and provides habitats for many other species, as what the project does.


## Run Server

The configuration file is in `yaml` format. An example is at `app/config.example.yaml`.

```sh
$ cp app/config.example.yaml app/local_config.yaml
```

### With `stack`

```sh
$ stack run -- mangrove app/local_config.yaml
```

### With `cabal`

```sh
$ cabal new-run mangrove app/local_config.yaml
```


## Benchmark

```sh
$ stack build --flag mangrove:benchmark-cli && stack exec -- mangrove-benchmark-cli --help
```
