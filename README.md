[![codecov](https://codecov.io/gh/valkey-io/valkey/graph/badge.svg?token=KYYSJAYC5F)](https://codecov.io/gh/valkey-io/valkey)

This project was forked from the open source Redis project right before the transition to their new source available licenses.

This README is just a fast *quick start* document. More details can be found under [valkey.io](https://valkey.io/)

# What is Valkey?

Valkey is a high-performance data structure server that primarily serves key/value workloads.
It supports a wide range of native structures and an extensible plugin system for adding new data structures and access patterns.

# Building Valkey using `Makefile`

Valkey can be compiled and used on Linux, macOS, OpenBSD, NetBSD, FreeBSD.
We support big endian and little endian architectures, and both 32 bit
and 64 bit systems.

It may compile on Solaris derived systems (for instance SmartOS) but our
support for this platform is *best effort* and Valkey is not guaranteed to
work as well as in Linux, macOS, and \*BSD.

It is as simple as:

    % make

To build with TLS support, you'll need OpenSSL development libraries (e.g.
libssl-dev on Debian/Ubuntu).

To build TLS support as Valkey built-in:

    % make BUILD_TLS=yes

To build TLS as Valkey module:

    % make BUILD_TLS=module

Note that sentinel mode does not support TLS module.

To build with experimental RDMA support you'll need RDMA development libraries
(e.g. librdmacm-dev and libibverbs-dev on Debian/Ubuntu).

To build RDMA support as Valkey built-in:

    % make BUILD_RDMA=yes

To build RDMA as Valkey module:

    % make BUILD_RDMA=module

To build with systemd support, you'll need systemd development libraries (such
as libsystemd-dev on Debian/Ubuntu or systemd-devel on CentOS) and run:

    % make USE_SYSTEMD=yes

To append a suffix to Valkey program names, use:

    % make PROG_SUFFIX="-alt"

You can build a 32 bit Valkey binary using:

    % make 32bit

After building Valkey, it is a good idea to test it using:

    % make test

The above runs the main integration tests. Additional tests are started using:

    % make test-unit     # Unit tests
    % make test-modules  # Tests of the module API
    % make test-sentinel # Valkey Sentinel integration tests
    % make test-cluster  # Valkey Cluster integration tests

More about running the integration tests can be found in
[tests/README.md](tests/README.md) and for unit tests, see
[src/unit/README.md](src/unit/README.md).

## Fixing build problems with dependencies or cached build options

Valkey has some dependencies which are included in the `deps` directory.
`make` does not automatically rebuild dependencies even if something in
the source code of dependencies changes.

When you update the source code with `git pull` or when code inside the
dependencies tree is modified in any other way, make sure to use the following
command in order to really clean everything and rebuild from scratch:

    % make distclean

This will clean: jemalloc, lua, hiredis, linenoise and other dependencies.

Also if you force certain build options like 32bit target, no C compiler
optimizations (for debugging purposes), and other similar build time options,
those options are cached indefinitely until you issue a `make distclean`
command.

## Fixing problems building 32 bit binaries

If after building Valkey with a 32 bit target you need to rebuild it
with a 64 bit target, or the other way around, you need to perform a
`make distclean` in the root directory of the Valkey distribution.

In case of build errors when trying to build a 32 bit binary of Valkey, try
the following steps:

* Install the package libc6-dev-i386 (also try g++-multilib).
* Try using the following command line instead of `make 32bit`:
  `make CFLAGS="-m32 -march=native" LDFLAGS="-m32"`

## Allocator

Selecting a non-default memory allocator when building Valkey is done by setting
the `MALLOC` environment variable. Valkey is compiled and linked against libc
malloc by default, with the exception of jemalloc being the default on Linux
systems. This default was picked because jemalloc has proven to have fewer
fragmentation problems than libc malloc.

To force compiling against libc malloc, use:

    % make MALLOC=libc

To compile against jemalloc on Mac OS X systems, use:

    % make MALLOC=jemalloc

## Monotonic clock

By default, Valkey will build using the POSIX clock_gettime function as the
monotonic clock source.  On most modern systems, the internal processor clock
can be used to improve performance.  Cautions can be found here:
    http://oliveryang.net/2015/09/pitfalls-of-TSC-usage/

To build with support for the processor's internal instruction clock, use:

    % make CFLAGS="-DUSE_PROCESSOR_CLOCK"

## Verbose build

Valkey will build with a user-friendly colorized output by default.
If you want to see a more verbose output, use the following:

    % make V=1

# Running Valkey

To run Valkey with the default configuration, just type:

    % cd src
    % ./valkey-server

If you want to provide your valkey.conf, you have to run it using an additional
parameter (the path of the configuration file):

    % cd src
    % ./valkey-server /path/to/valkey.conf

It is possible to alter the Valkey configuration by passing parameters directly
as options using the command line. Examples:

    % ./valkey-server --port 9999 --replicaof 127.0.0.1 6379
    % ./valkey-server /etc/valkey/6379.conf --loglevel debug

All the options in valkey.conf are also supported as options using the command
line, with exactly the same name.

# Running Valkey with TLS:

## Running manually

To manually run a Valkey server with TLS mode (assuming `./gen-test-certs.sh` was invoked so sample certificates/keys are available):

* TLS built-in mode:
    ```
    ./src/valkey-server --tls-port 6379 --port 0 \
        --tls-cert-file ./tests/tls/valkey.crt \
        --tls-key-file ./tests/tls/valkey.key \
        --tls-ca-cert-file ./tests/tls/ca.crt
    ```

* TLS module mode:
    ```
    ./src/valkey-server --tls-port 6379 --port 0 \
        --tls-cert-file ./tests/tls/valkey.crt \
        --tls-key-file ./tests/tls/valkey.key \
        --tls-ca-cert-file ./tests/tls/ca.crt \
        --loadmodule src/valkey-tls.so
    ```

Note that you can disable TCP by specifying `--port 0` explicitly.
It's also possible to have both TCP and TLS available at the same time,
but you'll have to assign different ports.

Use `valkey-cli` to connect to the Valkey server:
```
./src/valkey-cli --tls \
    --cert ./tests/tls/valkey.crt \
    --key ./tests/tls/valkey.key \
    --cacert ./tests/tls/ca.crt
```

Specifying `--tls-replication yes` makes a replica connect to the primary.

Using `--tls-cluster yes` makes Valkey Cluster use TLS across nodes.

# Running Valkey with RDMA:

Note that Valkey Over RDMA is an experimental feature.
It may be changed or removed in any minor or major version.
Currently, it is only supported on Linux.

* RDMA built-in mode:
    ```
    ./src/valkey-server --protected-mode no \
         --rdma-bind 192.168.122.100 --rdma-port 6379
    ```

* RDMA module mode:
    ```
    ./src/valkey-server --protected-mode no \
         --loadmodule src/valkey-rdma.so --rdma-bind 192.168.122.100 --rdma-port 6379
    ```

It's possible to change bind address/port of RDMA by runtime command:

    192.168.122.100:6379> CONFIG SET rdma-port 6380

It's also possible to have both RDMA and TCP available, and there is no
conflict of TCP(6379) and RDMA(6379), Ex:

    % ./src/valkey-server --protected-mode no \
         --loadmodule src/valkey-rdma.so --rdma-bind 192.168.122.100 --rdma-port 6379 \
         --port 6379

Note that the network card (192.168.122.100 of this example) should support
RDMA. To test a server supports RDMA or not:

    % rdma res show (a new version iproute2 package)
Or:

    % ibv_devices


# Playing with Valkey

You can use valkey-cli to play with Valkey. Start a valkey-server instance,
then in another terminal try the following:

    % cd src
    % ./valkey-cli
    valkey> ping
    PONG
    valkey> set foo bar
    OK
    valkey> get foo
    "bar"
    valkey> incr mycounter
    (integer) 1
    valkey> incr mycounter
    (integer) 2
    valkey>

# Installing Valkey

In order to install Valkey binaries into /usr/local/bin, just use:

    % make install

You can use `make PREFIX=/some/other/directory install` if you wish to use a
different destination.

_Note_: For compatibility with Redis, we create symlinks from the Redis names (`redis-server`, `redis-cli`, etc.) to the Valkey binaries installed by `make install`.
The symlinks are created in same directory as the Valkey binaries.
The symlinks are removed when using `make uninstall`.
The creation of the symlinks can be skipped by setting the makefile variable `USE_REDIS_SYMLINKS=no`.

`make install` will just install binaries in your system, but will not configure
init scripts and configuration files in the appropriate place. This is not
needed if you just want to play a bit with Valkey, but if you are installing
it the proper way for a production system, we have a script that does this
for Ubuntu and Debian systems:

    % cd utils
    % ./install_server.sh

_Note_: `install_server.sh` will not work on macOS; it is built for Linux only.

The script will ask you a few questions and will setup everything you need
to run Valkey properly as a background daemon that will start again on
system reboots.

You'll be able to stop and start Valkey using the script named
`/etc/init.d/valkey_<portnumber>`, for instance `/etc/init.d/valkey_6379`.

# Building using `CMake`

In addition to the traditional `Makefile` build, Valkey supports an alternative, **experimental**, build system using `CMake`.

To build and install `Valkey`, in `Release` mode (an optimized build), type this into your terminal:

```bash
mkdir build-release
cd $_
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/valkey
sudo make install
# Valkey is now installed under /opt/valkey
```

Other options supported by Valkey's `CMake` build system:

## Special build flags

- `-DBUILD_TLS=<yes|no>` enable TLS build for Valkey. Default: `no`
- `-DBUILD_RDMA=<no|module>` enable RDMA module build (only module mode supported). Default: `no`
- `-DBUILD_MALLOC=<libc|jemalloc|tcmalloc|tcmalloc_minimal>` choose the allocator to use. Default on Linux: `jemalloc`, for other OS: `libc`
- `-DBUILD_SANITIZER=<address|thread|undefined>` build with address sanitizer enabled. Default: disabled (no sanitizer)
- `-DBUILD_UNIT_TESTS=[yes|no]`  when set, the build will produce the executable `valkey-unit-tests`. Default: `no`
- `-DBUILD_TEST_MODULES=[yes|no]`  when set, the build will include the modules located under the `tests/modules` folder. Default: `no`
- `-DBUILD_EXAMPLE_MODULES=[yes|no]`  when set, the build will include the example modules located under the `src/modules` folder. Default: `no`

## Common flags

- `-DCMAKE_BUILD_TYPE=<Debug|Release...>` define the build type, see CMake manual for more details
- `-DCMAKE_INSTALL_PREFIX=/installation/path` override this value to define a custom install prefix. Default: `/usr/local`
- `-G"<Generator Name>"` generate build files for "Generator Name". By default, CMake will generate `Makefile`s.

## Verbose build

`CMake` generates a user-friendly colorized output by default.
If you want to see a more verbose output, use the following:

```bash
make VERBOSE=1
```

## Troubleshooting

During the `CMake` stage, `CMake` caches variables in a local file named `CMakeCache.txt`. All variables generated by Valkey
are removed from the cache once consumed (this is done by calling to `unset(VAR-NAME CACHE)`). However, some variables,
like the compiler path, are kept in cache. To start a fresh build either remove the cache file `CMakeCache.txt` from the
build folder, or delete the build folder completely.

**It is important to re-run `CMake` when adding new source files.**

## Integration with IDE

During the `CMake` stage of the build, `CMake` generates a JSON file named `compile_commands.json` and places it under the
build folder. This file is used by many IDEs and text editors for providing code completion (via `clangd`).

A small caveat is that these tools will look for `compile_commands.json` under the Valkey's top folder.
A common workaround is to create a symbolic link to it:

```bash
cd /path/to/valkey/
# We assume here that your build folder is `build-release`
ln -sf $(pwd)/build-release/compile_commands.json $(pwd)/compile_commands.json
```

Restart your IDE and voila

# Code contributions

Please see the [CONTRIBUTING.md][2]. For security bugs and vulnerabilities, please see [SECURITY.md][3].

# Valkey is an open community project under LF Projects

Valkey a Series of LF Projects, LLC
2810 N Church St, PMB 57274
Wilmington, Delaware 19802-4447

[1]: https://github.com/valkey-io/valkey/blob/unstable/COPYING
[2]: https://github.com/valkey-io/valkey/blob/unstable/CONTRIBUTING.md
[3]: https://github.com/valkey-io/valkey/blob/unstable/SECURITY.md
