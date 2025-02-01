# Valkey Makefile
# Copyright (C) 2009 Redis Ltd.
# This file is released under the BSD license, see the COPYING file
#
# The Makefile composes the final FINAL_CFLAGS and FINAL_LDFLAGS using
# what is needed for Valkey plus the standard CFLAGS and LDFLAGS passed.
# However when building the dependencies (Jemalloc, Lua, Hiredis, ...)
# CFLAGS and LDFLAGS are propagated to the dependencies, so to pass
# flags only to be used when compiling / linking Valkey itself SERVER_CFLAGS
# and SERVER_LDFLAGS are used instead (this is the case of 'make gcov').
#
# Dependencies are stored in the Makefile.dep file. To rebuild this file
# Just use 'make dep', but this is only needed by developers.

release_hdr := $(shell sh -c './mkreleasehdr.sh')
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
uname_M := $(shell sh -c 'uname -m 2>/dev/null || echo not')
CLANG := $(findstring clang,$(shell sh -c '$(CC) --version | head -1'))

# Optimization flags. To override, the OPTIMIZATION variable can be passed, but
# some automatic defaults are added to it. To specify optimization flags
# explicitly without any defaults added, pass the OPT variable instead.
OPTIMIZATION?=-O3
ifeq ($(OPTIMIZATION),-O3)
	ifeq (clang,$(CLANG))
		OPTIMIZATION+=-flto
	else
		OPTIMIZATION+=-flto=auto -ffat-lto-objects
	endif
endif
ifneq ($(OPTIMIZATION),-O0)
	OPTIMIZATION+=-fno-omit-frame-pointer
endif
DEPENDENCY_TARGETS=hiredis linenoise lua hdr_histogram fpconv
NODEPS:=clean distclean

# Default settings
STD=-pedantic

# Use -Wno-c11-extensions on clang, either where explicitly used or on
# platforms we can assume it's being used.
ifeq (clang,$(CLANG))
  STD+=-Wno-c11-extensions
else
ifneq (,$(findstring FreeBSD,$(uname_S)))
  STD+=-Wno-c11-extensions
endif
endif
WARN=-Wall -W -Wno-missing-field-initializers -Werror=deprecated-declarations -Wstrict-prototypes
OPT=$(OPTIMIZATION)

# Detect if the compiler supports C11 _Atomic.
# NUMBER_SIGN_CHAR is a workaround to support both GNU Make 4.3 and older versions.
NUMBER_SIGN_CHAR := \#
C11_ATOMIC := $(shell sh -c 'echo "$(NUMBER_SIGN_CHAR)include <stdatomic.h>" > foo.c; \
	$(CC) -std=gnu11 -c foo.c -o foo.o > /dev/null 2>&1; \
	if [ -f foo.o ]; then echo "yes"; rm foo.o; fi; rm foo.c')
ifeq ($(C11_ATOMIC),yes)
	STD+=-std=gnu11
else
	STD+=-std=c99
endif

PREFIX?=/usr/local
INSTALL_BIN=$(PREFIX)/bin
INSTALL=install
PKG_CONFIG?=pkg-config

ifndef PYTHON
PYTHON := $(shell which python3 || which python)
endif

# Default allocator defaults to Jemalloc on Linux and libc otherwise
MALLOC=libc
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
endif

# To get ARM stack traces if Valkey crashes we need a special C flag.
ifneq (,$(filter aarch64 armv%,$(uname_M)))
        CFLAGS+=-funwind-tables
endif

# Backwards compatibility for selecting an allocator
ifeq ($(USE_TCMALLOC),yes)
	MALLOC=tcmalloc
endif

ifeq ($(USE_TCMALLOC_MINIMAL),yes)
	MALLOC=tcmalloc_minimal
endif

ifeq ($(USE_JEMALLOC),yes)
	MALLOC=jemalloc
endif

ifeq ($(USE_JEMALLOC),no)
	MALLOC=libc
endif

ifdef SANITIZER
ifeq ($(SANITIZER),address)
	MALLOC=libc
	CFLAGS+=-fsanitize=address -fno-sanitize-recover=all -fno-omit-frame-pointer
	LDFLAGS+=-fsanitize=address
else
ifeq ($(SANITIZER),undefined)
	MALLOC=libc
	CFLAGS+=-fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer
	LDFLAGS+=-fsanitize=undefined
else
ifeq ($(SANITIZER),thread)
	CFLAGS+=-fsanitize=thread -fno-sanitize-recover=all -fno-omit-frame-pointer
	LDFLAGS+=-fsanitize=thread
else
    $(error "unknown sanitizer=${SANITIZER}")
endif
endif
endif
endif

# Override default settings if possible
-include .make-settings

# For added compatibility REDIS_CFLAGS and REDIS_LDFLAGS are also supported.
ifdef REDIS_CFLAGS
    SERVER_CFLAGS := $(REDIS_CFLAGS)
endif
ifdef REDIS_LDFLAGS
    SERVER_LDFLAGS := $(REDIS_LDFLAGS)
endif

# Special case of forcing defrag to run even though we have no Jemlloc support
ifeq ($(DEBUG_FORCE_DEFRAG), yes)
	SERVER_CFLAGS +=-DHAVE_DEFRAG -DDEBUG_FORCE_DEFRAG
endif

FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS) $(SERVER_CFLAGS)
FINAL_LDFLAGS=$(LDFLAGS) $(OPT) $(SERVER_LDFLAGS) $(DEBUG)
FINAL_LIBS=-lm
DEBUG=-g -ggdb

# Linux ARM32 needs -latomic at linking time
ifneq (,$(findstring armv,$(uname_M)))
        FINAL_LIBS+=-latomic
else
# Linux POWER needs -latomic at linking time
ifneq (,$(findstring ppc,$(uname_M)))
        FINAL_LIBS+=-latomic
endif
endif

ifeq ($(uname_S),SunOS)
	# SunOS
	ifeq ($(findstring -m32,$(FINAL_CFLAGS)),)
		CFLAGS+=-m64
	endif
	ifeq ($(findstring -m32,$(FINAL_LDFLAGS)),)
		LDFLAGS+=-m64
	endif
	DEBUG=-g
	DEBUG_FLAGS=-g
	export CFLAGS LDFLAGS DEBUG DEBUG_FLAGS
	INSTALL=cp -pf
	FINAL_CFLAGS+= -D__EXTENSIONS__ -D_XPG6
	FINAL_LIBS+= -ldl -lnsl -lsocket -lresolv -lpthread -lrt
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE
	endif
else
ifeq ($(uname_S),Darwin)
	# Darwin
	FINAL_LIBS+= -ldl
	# Homebrew's OpenSSL is not linked to /usr/local to avoid
	# conflicts with the system's LibreSSL installation so it
	# must be referenced explicitly during build.
ifeq ($(uname_M),arm64)
	# Homebrew arm64 uses /opt/homebrew as HOMEBREW_PREFIX
	OPENSSL_PREFIX?=/opt/homebrew/opt/openssl
else
	# Homebrew x86/ppc uses /usr/local as HOMEBREW_PREFIX
	OPENSSL_PREFIX?=/usr/local/opt/openssl
endif
else
ifeq ($(uname_S),AIX)
        # AIX
        FINAL_LDFLAGS+= -Wl,-bexpall
        FINAL_LIBS+=-ldl -pthread -lcrypt -lbsd
else
ifeq ($(uname_S),OpenBSD)
	# OpenBSD
	FINAL_LIBS+= -lpthread
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE -I/usr/local/include
	    FINAL_LDFLAGS+= -L/usr/local/lib
	    FINAL_LIBS+= -lexecinfo
    	endif

else
ifeq ($(uname_S),NetBSD)
	# NetBSD
	FINAL_LIBS+= -lpthread
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE -I/usr/pkg/include
	    FINAL_LDFLAGS+= -L/usr/pkg/lib
	    FINAL_LIBS+= -lexecinfo
    	endif
else
ifeq ($(uname_S),FreeBSD)
	# FreeBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),DragonFly)
	# DragonFly
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),OpenBSD)
	# OpenBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),NetBSD)
	# NetBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),Haiku)
	# Haiku
	FINAL_CFLAGS+= -DBSD_SOURCE
	FINAL_LDFLAGS+= -lbsd -lnetwork
	FINAL_LIBS+= -lpthread
else
	# All the other OSes (notably Linux)
	FINAL_LDFLAGS+= -rdynamic
	FINAL_LIBS+=-ldl -pthread -lrt
endif
endif
endif
endif
endif
endif
endif
endif
endif
endif

ifdef OPENSSL_PREFIX
	OPENSSL_CFLAGS=-I$(OPENSSL_PREFIX)/include
	OPENSSL_LDFLAGS=-L$(OPENSSL_PREFIX)/lib
	# Also export OPENSSL_PREFIX so it ends up in deps sub-Makefiles
	export OPENSSL_PREFIX
endif

# Include paths to dependencies
FINAL_CFLAGS+= -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src -I../deps/hdr_histogram -I../deps/fpconv

# Determine systemd support and/or build preference (defaulting to auto-detection)
BUILD_WITH_SYSTEMD=no
LIBSYSTEMD_LIBS=-lsystemd

# If 'USE_SYSTEMD' in the environment is neither "no" nor "yes", try to
# auto-detect libsystemd's presence and link accordingly.
ifneq ($(USE_SYSTEMD),no)
	LIBSYSTEMD_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libsystemd && echo $$?)
# If libsystemd cannot be detected, continue building without support for it
# (unless a later check tells us otherwise)
ifeq ($(LIBSYSTEMD_PKGCONFIG),0)
	BUILD_WITH_SYSTEMD=yes
	LIBSYSTEMD_LIBS=$(shell $(PKG_CONFIG) --libs libsystemd)
endif
endif

# If 'USE_SYSTEMD' is set to "yes" use pkg-config if available or fall back to
# default -lsystemd.
ifeq ($(USE_SYSTEMD),yes)
	BUILD_WITH_SYSTEMD=yes
endif

ifeq ($(BUILD_WITH_SYSTEMD),yes)
	FINAL_LIBS+=$(LIBSYSTEMD_LIBS)
	FINAL_CFLAGS+= -DHAVE_LIBSYSTEMD
endif

ifeq ($(MALLOC),tcmalloc)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc
endif

ifeq ($(MALLOC),tcmalloc_minimal)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc_minimal
endif

ifeq ($(MALLOC),jemalloc)
	DEPENDENCY_TARGETS+= jemalloc
	FINAL_CFLAGS+= -DUSE_JEMALLOC -I../deps/jemalloc/include
	FINAL_LIBS := ../deps/jemalloc/lib/libjemalloc.a $(FINAL_LIBS)
endif

# LIBSSL & LIBCRYPTO
LIBSSL_LIBS=
LIBSSL_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libssl && echo $$?)
ifeq ($(LIBSSL_PKGCONFIG),0)
	LIBSSL_LIBS=$(shell $(PKG_CONFIG) --libs libssl)
else
	LIBSSL_LIBS=-lssl
endif
LIBCRYPTO_LIBS=
LIBCRYPTO_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libcrypto && echo $$?)
ifeq ($(LIBCRYPTO_PKGCONFIG),0)
	LIBCRYPTO_LIBS=$(shell $(PKG_CONFIG) --libs libcrypto)
else
	LIBCRYPTO_LIBS=-lcrypto
endif

BUILD_NO:=0
BUILD_YES:=1
BUILD_MODULE:=2
ifeq ($(BUILD_TLS),yes)
	FINAL_CFLAGS+=-DUSE_OPENSSL=$(BUILD_YES) $(OPENSSL_CFLAGS) -DBUILD_TLS_MODULE=$(BUILD_NO)
	FINAL_LDFLAGS+=$(OPENSSL_LDFLAGS)
	FINAL_LIBS += ../deps/hiredis/libhiredis_ssl.a $(LIBSSL_LIBS) $(LIBCRYPTO_LIBS)
endif

TLS_MODULE=
TLS_MODULE_NAME:=valkey-tls$(PROG_SUFFIX).so
TLS_MODULE_CFLAGS:=$(FINAL_CFLAGS)
ifeq ($(BUILD_TLS),module)
	FINAL_CFLAGS+=-DUSE_OPENSSL=$(BUILD_MODULE) $(OPENSSL_CFLAGS)
	TLS_CLIENT_LIBS = ../deps/hiredis/libhiredis_ssl.a $(LIBSSL_LIBS) $(LIBCRYPTO_LIBS)
	TLS_MODULE=$(TLS_MODULE_NAME)
	TLS_MODULE_CFLAGS+=-DUSE_OPENSSL=$(BUILD_MODULE) $(OPENSSL_CFLAGS) -DBUILD_TLS_MODULE=$(BUILD_MODULE)
endif

RDMA_LIBS=
RDMA_PKGCONFIG := $(shell $(PKG_CONFIG) --exists librdmacm libibverbs && echo $$?)
ifeq ($(RDMA_PKGCONFIG),0)
	RDMA_LIBS=$(shell $(PKG_CONFIG) --libs librdmacm libibverbs)
else
	RDMA_LIBS=-lrdmacm -libverbs
endif

ifeq ($(BUILD_RDMA),yes)
	FINAL_CFLAGS+=-DUSE_RDMA=$(BUILD_YES) -DBUILD_RDMA_MODULE=$(BUILD_NO)
	FINAL_LIBS += $(RDMA_LIBS)
endif

RDMA_MODULE=
RDMA_MODULE_NAME:=valkey-rdma$(PROG_SUFFIX).so
RDMA_MODULE_CFLAGS:=$(FINAL_CFLAGS)
ifeq ($(BUILD_RDMA),module)
	FINAL_CFLAGS+=-DUSE_RDMA=$(BUILD_MODULE)
	RDMA_MODULE=$(RDMA_MODULE_NAME)
	RDMA_MODULE_CFLAGS+=-DUSE_RDMA=$(BUILD_MODULE) -DBUILD_RDMA_MODULE=$(BUILD_MODULE) $(RDMA_LIBS)
endif

ifndef V
    define MAKE_INSTALL
        @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$(1)$(ENDCOLOR) 1>&2
        @$(INSTALL) $(1) $(2)
    endef

    define INSTALL_REDIS_SYMLINK
        @printf '    %b %b %b %b %b %b %b\n' \
        $(LINKCOLOR)INSTALL SYMLINK$(ENDCOLOR) \
        $(BINCOLOR)$(subst $(ENGINE_NAME),redis,$(1))$(ENDCOLOR) -\> $(BINCOLOR)$(1)$(ENDCOLOR) 1>&2
        @ln -sf $(1) $(2)/$(subst $(ENGINE_NAME),redis,$(1))
    endef
else
    define MAKE_INSTALL
        $(INSTALL) $(1) $(2)
    endef

    define INSTALL_REDIS_SYMLINK
        ln -sf $(1) $(2)/$(subst $(ENGINE_NAME),redis,$(1))
    endef
endif

# Determine install/uninstall Redis symlinks for compatibility when
# installing/uninstalling Valkey binaries (defaulting to `yes`)
USE_REDIS_SYMLINKS?=yes
ifeq ($(USE_REDIS_SYMLINKS),yes)
	MAYBE_INSTALL_REDIS_SYMLINK=$(INSTALL_REDIS_SYMLINK)
	MAYBE_UNINSTALL_REDIS_SYMLINK=@rm -f $(1)/$(subst $(ENGINE_NAME),redis,$(2))
else
	MAYBE_INSTALL_REDIS_SYMLINK=
	MAYBE_UNINSTALL_REDIS_SYMLINK=
endif

SERVER_CC=$(QUIET_CC)$(CC) $(FINAL_CFLAGS)
SERVER_AR=$(QUIET_AR)$(AR)
SERVER_LD=$(QUIET_LINK)$(CC) $(FINAL_LDFLAGS)
ENGINE_INSTALL=$(QUIET_INSTALL)$(INSTALL)

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

ifndef V
QUIET_CC = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_GEN = @printf '    %b %b\n' $(CCCOLOR)GEN$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_INSTALL = @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_AR = @printf '    %b %b\n' $(CCCOLOR)ARCHIVE$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
endif

ifneq (, $(findstring LOG_REQ_RES, $(SERVER_CFLAGS)))
	COMMANDS_DEF_FILENAME=commands_with_reply_schema
	GEN_COMMANDS_FLAGS=--with-reply-schema
else
	COMMANDS_DEF_FILENAME=commands
	GEN_COMMANDS_FLAGS=
endif

ENGINE_NAME=valkey
SERVER_NAME=$(ENGINE_NAME)-server$(PROG_SUFFIX)
ENGINE_SENTINEL_NAME=$(ENGINE_NAME)-sentinel$(PROG_SUFFIX)
ENGINE_SERVER_OBJ=threads_mngr.o adlist.o quicklist.o ae.o anet.o dict.o hashtable.o kvstore.o server.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o ziplist.o release.o memory_prefetch.o io_threads.o networking.o util.o object.o db.o replication.o rdb.o t_string.o t_list.o t_set.o t_zset.o t_hash.o config.o aof.o pubsub.o multi.o debug.o sort.o intset.o syncio.o cluster.o cluster_legacy.o cluster_slot_stats.o crc16.o endianconv.o commandlog.o eval.o bio.o rio.o rand.o memtest.o syscheck.o crcspeed.o crccombine.o crc64.o bitops.o sentinel.o notify.o setproctitle.o blocked.o hyperloglog.o latency.o sparkline.o valkey-check-rdb.o valkey-check-aof.o geo.o lazyfree.o module.o evict.o expire.o geohash.o geohash_helper.o childinfo.o allocator_defrag.o defrag.o siphash.o rax.o t_stream.o listpack.o localtime.o lolwut.o lolwut5.o lolwut6.o acl.o tracking.o socket.o tls.o sha256.o timeout.o setcpuaffinity.o monotonic.o mt19937-64.o resp_parser.o call_reply.o script_lua.o script.o functions.o function_lua.o commands.o strl.o connection.o unix.o logreqres.o rdma.o scripting_engine.o
ENGINE_CLI_NAME=$(ENGINE_NAME)-cli$(PROG_SUFFIX)
ENGINE_CLI_OBJ=anet.o adlist.o dict.o valkey-cli.o zmalloc.o release.o ae.o serverassert.o crcspeed.o crccombine.o crc64.o siphash.o crc16.o monotonic.o cli_common.o mt19937-64.o strl.o cli_commands.o
ENGINE_BENCHMARK_NAME=$(ENGINE_NAME)-benchmark$(PROG_SUFFIX)
ENGINE_BENCHMARK_OBJ=ae.o anet.o valkey-benchmark.o adlist.o dict.o zmalloc.o serverassert.o release.o crcspeed.o crccombine.o crc64.o siphash.o crc16.o monotonic.o cli_common.o mt19937-64.o strl.o
ENGINE_CHECK_RDB_NAME=$(ENGINE_NAME)-check-rdb$(PROG_SUFFIX)
ENGINE_CHECK_AOF_NAME=$(ENGINE_NAME)-check-aof$(PROG_SUFFIX)
ENGINE_LIB_NAME=lib$(ENGINE_NAME).a
ENGINE_TEST_FILES:=$(wildcard unit/*.c)
ENGINE_TEST_OBJ:=$(sort $(patsubst unit/%.c,unit/%.o,$(ENGINE_TEST_FILES)))
ENGINE_UNIT_TESTS:=$(ENGINE_NAME)-unit-tests$(PROG_SUFFIX)
ALL_SOURCES=$(sort $(patsubst %.o,%.c,$(ENGINE_SERVER_OBJ) $(ENGINE_CLI_OBJ) $(ENGINE_BENCHMARK_OBJ)))

USE_FAST_FLOAT?=no
ifeq ($(USE_FAST_FLOAT),yes)
	# valkey_strtod.h uses this flag to switch valkey_strtod function to fast_float_strtod,
	# therefore let's pass it to compiler for preprocessing.
	FINAL_CFLAGS += -D USE_FAST_FLOAT
	# next, let's build and add actual library containing fast_float_strtod function for linking.
	DEPENDENCY_TARGETS += fast_float_c_interface
	FAST_FLOAT_STRTOD_OBJECT := ../deps/fast_float_c_interface/fast_float_strtod.o
	FINAL_LIBS += $(FAST_FLOAT_STRTOD_OBJECT)
endif

all: $(SERVER_NAME) $(ENGINE_SENTINEL_NAME) $(ENGINE_CLI_NAME) $(ENGINE_BENCHMARK_NAME) $(ENGINE_CHECK_RDB_NAME) $(ENGINE_CHECK_AOF_NAME) $(TLS_MODULE) $(RDMA_MODULE)
	@echo ""
	@echo "Hint: It's a good idea to run 'make test' ;)"
	@echo ""

Makefile.dep:
	-$(SERVER_CC) -MM $(ALL_SOURCES) > Makefile.dep 2> /dev/null || true

ifeq (0, $(words $(findstring $(MAKECMDGOALS), $(NODEPS))))
-include Makefile.dep
endif

.PHONY: all

all-with-unit-tests: all $(ENGINE_UNIT_TESTS)
.PHONY: all

persist-settings: distclean
	echo STD=$(STD) >> .make-settings
	echo WARN=$(WARN) >> .make-settings
	echo OPT=$(OPT) >> .make-settings
	echo MALLOC=$(MALLOC) >> .make-settings
	echo BUILD_TLS=$(BUILD_TLS) >> .make-settings
	echo BUILD_RDMA=$(BUILD_RDMA) >> .make-settings
	echo USE_SYSTEMD=$(USE_SYSTEMD) >> .make-settings
	echo CFLAGS=$(CFLAGS) >> .make-settings
	echo LDFLAGS=$(LDFLAGS) >> .make-settings
	echo SERVER_CFLAGS=$(SERVER_CFLAGS) >> .make-settings
	echo SERVER_LDFLAGS=$(SERVER_LDFLAGS) >> .make-settings
	echo PREV_FINAL_CFLAGS=$(FINAL_CFLAGS) >> .make-settings
	echo PREV_FINAL_LDFLAGS=$(FINAL_LDFLAGS) >> .make-settings
	-(cd ../deps && $(MAKE) $(DEPENDENCY_TARGETS))

.PHONY: persist-settings

# Prerequisites target
.make-prerequisites:
	@touch $@

# Clean everything, persist settings and build dependencies if anything changed
ifneq ($(strip $(PREV_FINAL_CFLAGS)), $(strip $(FINAL_CFLAGS)))
.make-prerequisites: persist-settings
endif

ifneq ($(strip $(PREV_FINAL_LDFLAGS)), $(strip $(FINAL_LDFLAGS)))
.make-prerequisites: persist-settings
endif

# valkey-server
$(SERVER_NAME): $(ENGINE_SERVER_OBJ)
	$(SERVER_LD) -o $@ $^ ../deps/hiredis/libhiredis.a ../deps/lua/src/liblua.a ../deps/hdr_histogram/libhdrhistogram.a ../deps/fpconv/libfpconv.a $(FINAL_LIBS)

# Valkey static library, used to compile against for unit testing
$(ENGINE_LIB_NAME): $(ENGINE_SERVER_OBJ)
	$(SERVER_AR) rcs $@ $^

# valkey-unit-tests
$(ENGINE_UNIT_TESTS): $(ENGINE_TEST_OBJ) $(ENGINE_LIB_NAME)
	$(SERVER_LD) -o $@ $^ ../deps/hiredis/libhiredis.a ../deps/lua/src/liblua.a ../deps/hdr_histogram/libhdrhistogram.a ../deps/fpconv/libfpconv.a $(FINAL_LIBS)

# valkey-sentinel
$(ENGINE_SENTINEL_NAME): $(SERVER_NAME)
	$(ENGINE_INSTALL) $(SERVER_NAME) $(ENGINE_SENTINEL_NAME)

# valkey-check-rdb
$(ENGINE_CHECK_RDB_NAME): $(SERVER_NAME)
	$(ENGINE_INSTALL) $(SERVER_NAME) $(ENGINE_CHECK_RDB_NAME)

# valkey-check-aof
$(ENGINE_CHECK_AOF_NAME): $(SERVER_NAME)
	$(ENGINE_INSTALL) $(SERVER_NAME) $(ENGINE_CHECK_AOF_NAME)

# valkey-tls.so
$(TLS_MODULE_NAME): $(SERVER_NAME)
	$(QUIET_CC)$(CC) -o $@ tls.c -shared -fPIC $(TLS_MODULE_CFLAGS) $(TLS_CLIENT_LIBS)

# valkey-rdma.so
$(RDMA_MODULE_NAME): $(SERVER_NAME)
	$(QUIET_CC)$(CC) -o $@ rdma.c -shared -fPIC $(RDMA_MODULE_CFLAGS)

# valkey-cli
$(ENGINE_CLI_NAME): $(ENGINE_CLI_OBJ)
	$(SERVER_LD) -o $@ $^ ../deps/hiredis/libhiredis.a ../deps/linenoise/linenoise.o $(FINAL_LIBS) $(TLS_CLIENT_LIBS)

# valkey-benchmark
$(ENGINE_BENCHMARK_NAME): $(ENGINE_BENCHMARK_OBJ)
	$(SERVER_LD) -o $@ $^ ../deps/hiredis/libhiredis.a ../deps/hdr_histogram/libhdrhistogram.a $(FINAL_LIBS) $(TLS_CLIENT_LIBS)

DEP = $(ENGINE_SERVER_OBJ:%.o=%.d) $(ENGINE_CLI_OBJ:%.o=%.d) $(ENGINE_BENCHMARK_OBJ:%.o=%.d)
-include $(DEP)

# Because the jemalloc.h header is generated as a part of the jemalloc build,
# building it should complete before building any other object. Instead of
# depending on a single artifact, build all dependencies first.
%.o: %.c .make-prerequisites
	$(SERVER_CC) -MMD -o $@ -c $<

unit/%.o: unit/%.c .make-prerequisites
	$(SERVER_CC) -MMD -o $@ -c $<

# The following files are checked in and don't normally need to be rebuilt. They
# are built only if python is available and their prereqs are modified.
ifneq (,$(PYTHON))
$(COMMANDS_DEF_FILENAME).def: commands/*.json ../utils/generate-command-code.py
	$(QUIET_GEN)$(PYTHON) ../utils/generate-command-code.py $(GEN_COMMANDS_FLAGS)

fmtargs.h: ../utils/generate-fmtargs.py
	$(QUITE_GEN)sed '/Everything below this line/,$$d' $@ > $@.tmp
	$(QUITE_GEN)$(PYTHON) ../utils/generate-fmtargs.py >> $@.tmp
	$(QUITE_GEN)mv $@.tmp $@

unit/test_files.h: unit/*.c ../utils/generate-unit-test-header.py
	$(QUIET_GEN)$(PYTHON) ../utils/generate-unit-test-header.py

unit/test_main.o: unit/test_files.h
endif

commands.c: $(COMMANDS_DEF_FILENAME).def

clean:
	rm -rf $(SERVER_NAME) $(ENGINE_SENTINEL_NAME) $(ENGINE_CLI_NAME) $(ENGINE_BENCHMARK_NAME) $(ENGINE_CHECK_RDB_NAME) $(ENGINE_CHECK_AOF_NAME) $(ENGINE_UNIT_TESTS) $(ENGINE_LIB_NAME) unit/*.o unit/*.d *.o *.gcda *.gcno *.gcov valkey.info lcov-html Makefile.dep *.so
	rm -f $(DEP)

.PHONY: clean

distclean: clean
	-(cd ../deps && $(MAKE) distclean)
	-(cd modules && $(MAKE) clean)
	-(cd ../tests/modules && $(MAKE) clean)
	-(rm -f .make-*)

.PHONY: distclean

test: $(SERVER_NAME) $(ENGINE_CHECK_AOF_NAME) $(ENGINE_CLI_NAME) $(ENGINE_BENCHMARK_NAME)
	@(cd ..; ./runtest)

test-unit: $(ENGINE_UNIT_TESTS)
	./$(ENGINE_UNIT_TESTS)

test-modules: $(SERVER_NAME)
	@(cd ..; ./runtest-moduleapi)

test-sentinel: $(ENGINE_SENTINEL_NAME) $(ENGINE_CLI_NAME)
	@(cd ..; ./runtest-sentinel)

test-cluster: $(SERVER_NAME) $(ENGINE_CLI_NAME)
	@(cd ..; ./runtest-cluster)

check: test

lcov:
	@lcov --version
	$(MAKE) gcov
	@(set -e; cd ..; ./runtest)
	@geninfo -o valkey.info .
	@genhtml --legend -o lcov-html valkey.info

.PHONY: lcov

bench: $(ENGINE_BENCHMARK_NAME)
	./$(ENGINE_BENCHMARK_NAME)

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386 and libstdc++-11-dev-i386-cross"
	@echo ""
	$(MAKE) all-with-unit-tests CFLAGS="-m32" LDFLAGS="-m32"

gcov:
	$(MAKE) SERVER_CFLAGS="-fprofile-arcs -ftest-coverage -DCOVERAGE_TEST" SERVER_LDFLAGS="-fprofile-arcs -ftest-coverage"

noopt:
	$(MAKE) OPTIMIZATION="-O0"

valgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc"

helgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc" CFLAGS="-D__ATOMIC_VAR_FORCE_SYNC_MACROS" SERVER_CFLAGS="-I/usr/local/include" SERVER_LDFLAGS="-L/usr/local/lib"

install: all
	@mkdir -p $(INSTALL_BIN)
	$(call MAKE_INSTALL,$(SERVER_NAME),$(INSTALL_BIN))
	$(call MAKE_INSTALL,$(ENGINE_BENCHMARK_NAME),$(INSTALL_BIN))
	$(call MAKE_INSTALL,$(ENGINE_CLI_NAME),$(INSTALL_BIN))
	@ln -sf $(SERVER_NAME) $(INSTALL_BIN)/$(ENGINE_CHECK_RDB_NAME)
	@ln -sf $(SERVER_NAME) $(INSTALL_BIN)/$(ENGINE_CHECK_AOF_NAME)
	@ln -sf $(SERVER_NAME) $(INSTALL_BIN)/$(ENGINE_SENTINEL_NAME)
	$(call MAYBE_INSTALL_REDIS_SYMLINK,$(SERVER_NAME),$(INSTALL_BIN))
	$(call MAYBE_INSTALL_REDIS_SYMLINK,$(ENGINE_CLI_NAME),$(INSTALL_BIN))
	$(call MAYBE_INSTALL_REDIS_SYMLINK,$(ENGINE_BENCHMARK_NAME),$(INSTALL_BIN))
	$(call MAYBE_INSTALL_REDIS_SYMLINK,$(ENGINE_CHECK_RDB_NAME),$(INSTALL_BIN))
	$(call MAYBE_INSTALL_REDIS_SYMLINK,$(ENGINE_CHECK_AOF_NAME),$(INSTALL_BIN))
	$(call MAYBE_INSTALL_REDIS_SYMLINK,$(ENGINE_SENTINEL_NAME),$(INSTALL_BIN))

uninstall:
	@rm -f $(INSTALL_BIN)/{$(SERVER_NAME),$(ENGINE_BENCHMARK_NAME),$(ENGINE_CLI_NAME),$(ENGINE_CHECK_RDB_NAME),$(ENGINE_CHECK_AOF_NAME),$(ENGINE_SENTINEL_NAME)}
	$(call MAYBE_UNINSTALL_REDIS_SYMLINK,$(INSTALL_BIN),$(SERVER_NAME))
	$(call MAYBE_UNINSTALL_REDIS_SYMLINK,$(INSTALL_BIN),$(ENGINE_CLI_NAME))
	$(call MAYBE_UNINSTALL_REDIS_SYMLINK,$(INSTALL_BIN),$(ENGINE_BENCHMARK_NAME))
	$(call MAYBE_UNINSTALL_REDIS_SYMLINK,$(INSTALL_BIN),$(ENGINE_CHECK_RDB_NAME))
	$(call MAYBE_UNINSTALL_REDIS_SYMLINK,$(INSTALL_BIN),$(ENGINE_CHECK_AOF_NAME))
	$(call MAYBE_UNINSTALL_REDIS_SYMLINK,$(INSTALL_BIN),$(ENGINE_SENTINEL_NAME))
