Valkey Test Suite
=================

Overview
--------

Integration tests are written in Tcl, a high-level, general-purpose, interpreted, dynamic programming language [[source](https://wiki.tcl-lang.org/page/What+is+Tcl)].
`runtest` is the main entrance point for running integration tests.
For example, to run a single test;

```shell
./runtest --single unit/your_test_name
# For additional arguments, you may refer to the `runtest` script itself.
```

The normal execution mode of the test suite involves starting and manipulating
local `valkey-server` instances, inspecting process state, log files, etc.

The test suite also supports execution against an external server, which is
enabled using the `--host` and `--port` parameters. When executing against an
external server, tests tagged `external:skip` are skipped.

There are additional runtime options that can further adjust the test suite to
match different external server configurations. All options are listed by
`./runtest --help`. The following table is just a subset of the options:

| Option                     | Impact                                                   |
| -------------------------- | -------------------------------------------------------- |
| `--singledb`               | Only use database 0, don't assume others are supported. |
| `--ignore-encoding`        | Skip all checks for specific encoding.  |
| `--ignore-digest`          | Skip key value digest validations. |
| `--cluster-mode`           | Run in strict Valkey Cluster compatibility mode. |
| `--large-memory`           | Enables tests that consume more than 100MB |
| `--tls`                    | Run tests with TLS. See below. |
| `--tls-module`             | Run tests with TLS, when TLS support is built as a module. |
| `--other-server-path PATH` | Run compatibility tests with an other server executable. |
| `--help`                   | Displays the full set of options. |

Running with TLS requires the following preparations:

* Build Valkey is TLS support, e.g. using `make BUILD_TLS=yes`, or `make BUILD_TLS=module`.
* Run `./utils/gen-test-certs.sh` to generate a root CA and a server certificate.
* Install TLS support for TCL, e.g. the `tcl-tls` package on Debian/Ubuntu.

Additional tests
----------------

Not all tests are included in the `./runtest` scripts. Some additional entry points are provided by the following scripts, which support a subset of the options listed above:

* `./runtest-cluster` runs more extensive tests for Valkey Cluster.
  Some cluster tests are included in `./runtest`, but not all.
* `./runtest-sentinel` runs tests of Valkey Sentinel.
* `./runtests-module` runs tests of the module API.

Debugging
---------

You can set a breakpoint and invoke a minimal debugger using the `bp` function.

```
... your test code before break-point
bp 1
... your test code after break-point
```

The `bp 1` will give back the tcl interpreter to the developer, and allow you to interactively print local variables (through `puts`), run functions and so forth [[source](https://wiki.tcl-lang.org/page/A+minimal+debugger)]. 
`bp` takes a single argument, which is `1` for the case above, and is used to label a breakpoint with a string.
Labels are printed out when breakpoints are hit, so you can identify which breakpoint was triggered.
Breakpoints can be skipped by setting the global variable `::bp_skip`, and by providing the labels you want to skip.

The minimal debugger comes with the following predefined functions.
* Press `c` to continue past the breakpoint.
* Press `i` to print local variables.

Tags
----

Tags are applied to tests to classify them according to the subsystem they test,
but also to indicate compatibility with different run modes and required
capabilities.

Tags can be applied in different context levels:
* `start_server` context
* `tags` context that bundles several tests together
* A single test context.

The following compatibility and capability tags are currently used:

| Tag                       | Indicates |
| ------------------------- | --------- |
| `external:skip`           | Not compatible with external servers. |
| `cluster`                 | Uses cluster with multiple nodes. |
| `cluster:skip`            | Not compatible with `--cluster-mode`. |
| `large-memory`            | Test that requires more than 100MB |
| `tls`                     | Uses TLS. |
| `tls:skip`                | Not compatible with `--tls`. |
| `ipv6`                    | Uses IPv6. |
| `needs:repl`, `repl`      | Uses replication and needs to be able to `SYNC` from server. |
| `needs:debug`             | Uses the `DEBUG` command or other debugging focused commands (like `OBJECT REFCOUNT`). |
| `needs:pfdebug`           | Uses the `PFDEBUG` command. |
| `needs:config-maxmemory`  | Uses `CONFIG SET` to manipulate memory limit, eviction policies, etc. |
| `needs:config-resetstat`  | Uses `CONFIG RESETSTAT` to reset statistics. |
| `needs:reset`             | Uses `RESET` to reset client connections. |
| `needs:save`              | Uses `SAVE` or `BGSAVE` to create an RDB file. |
| `needs:other-server`      | Requires `--other-server-path`. |

When using an external server (`--host` and `--port`), filtering using the
`external:skip` tags is done automatically.

When using `--cluster-mode`, filtering using the `cluster:skip` tag is done
automatically.

When not using `--large-memory`, filtering using the `largemem:skip` tag is done
automatically.

In addition, it is possible to specify additional configuration. For example, to
run tests on a server that does not permit `SYNC` use:

    ./runtest --host <host> --port <port> --tags -needs:repl

