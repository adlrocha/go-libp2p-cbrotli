# go-libp2p-cbrotli

[![](https://img.shields.io/badge/made%20by-ETHBerlinZwei-blue.svg?style=flat-square)](https://ethberlinzwei.com)
[![](https://img.shields.io/badge/project-libp2p-yellow.svg?style=flat-square)](https://libp2p.io/)
[![](https://img.shields.io/badge/freenode-%23libp2p-yellow.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23libp2p)
[![Discourse posts](https://img.shields.io/discourse/https/discuss.libp2p.io/posts.svg)](https://discuss.libp2p.io)

> go-libp2p's brotli compressed transport from C implementation

`go-libp2p-cbrotli` is a component of the [libp2p project](https://libp2p.io), a
modular networking stack for developing peer-to-peer applications. It provides a
compressed transport channel for [`go-libp2p`][go-libp2p]. This transport is a wrapper
of [Google's C implementation of Brotli](https://github.com/google/brotli).

libp2p supports multiple [transport protocols][docs-transport]. `go-libp2p-cbrotli` is designed to work with
go-libp2p's ["transport upgrader"][transport-upgrader].

## Status

This implementation is currently a "work in progress" to evaluate the use of compression within libnp2p. Not ready for production yet. Use it at your own risk, many things could break while using it.

## Install

As `go-libp2p-cbrotli` is still in development, it is not included as a default dependency of `go-libp2p`.

`go-libp2p-cbrotli` is a standard Go module which can be installed with:

```sh
go get github.com/libp2p/go-libp2p-cbrotli
```

### Installing C implementation of Brotli
This transport needs the CGO binding to work. To run this transport you will need to have [Brotli's library in C installed](https://github.com/google/brotli). Be sure to have CMake and GCC installed to be able to compile the library code.

To compile the Brotli library run the following commands:
```
$ git clone https://github.com/google/brotli
$ cd brotli
$ mkdir out && cd out
$ ../configure-cmake
$ make
$ make test
$ sudo make install
```


This repo is [gomod](https://github.com/golang/go/wiki/Modules) compatible, and users of
go 1.12 and later with modules enabled will automatically pull the latest tagged release
by referencing this package. Upgrades to future releases can be managed using `go get`,
or by editing your `go.mod` file as [described by the gomod documentation](https://github.com/golang/go/wiki/Modules#how-to-upgrade-and-downgrade-dependencies).

## Usage

`go-libp2p-cbrotli` is not currently enabled by default when constructing a new libp2p
[Host][godoc-host], so you will need to explicitly enable it in order to use it.

To do so, you can pass `brotli.New` as an argument to a `libp2p.Compression` `Option` when
constructing a libp2p `Host` with `libp2p.New`:

```go
import (
  libp2p "github.com/libp2p/go-libp2p"
  brotli "github.com/libp2p/go-libp2p-cbrotli"
)

// wherever you create your libp2p instance:
host := libp2p.New(
  libp2p.Compression(brotli.ID, brotli.New)
)
```

## Contribute

Feel free to join in. All welcome. Open an [issue](https://github.com/libp2p/go-libp2p-cbrotli/issues)!

This repository falls under the libp2p [Code of Conduct](https://github.com/libp2p/community/blob/master/code-of-conduct.md).

### Want to hack on libp2p?

[![](https://cdn.rawgit.com/libp2p/community/master/img/contribute.gif)](https://github.com/libp2p/community/blob/master/CONTRIBUTE.md)

## License

MIT

---

[go-libp2p]: https://github.com/libp2p/go-libp2p
[conn-spec]: https://github.com/libp2p/specs/blob/master/connections/README.md
[docs-transport]: https://docs.libp2p.io/concepts/transport
[transport-upgrader]: https://github.com/libp2p/go-libp2p-transport-upgrader
[godoc-host]: https://godoc.org/github.com/libp2p/go-libp2p-core/host#Host
[godoc-option]: https://godoc.org/github.com/libp2p/go-libp2p#Option
[godoc-go-libp2p-pkg-vars]: https://godoc.org/github.com/libp2p/go-libp2p#pkg-variables 
[godoc-security-option]: https://godoc.org/github.com/libp2p/go-libp2p#Security
[godoc-securetransport]: https://godoc.org/github.com/libp2p/go-libp2p-core/sec#SecureTransport

