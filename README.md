# mangrove

Mangrove is a database management server with some other facilities.

A mangrove has a huge number of aerial roots, enabling it to have high level of gas exchange, which is just like the I/O level of our databases. Also, a mangrove can survive many rough environments and provides habitats for many other species, as what the project does.

## Instructions

### Server

```
$ stack run -- mangrove-exe -p 20202 -d ~/foo/bar
```

### Client
```
$ stack test -- test-arguments "-h localhost -p 20202"
```
