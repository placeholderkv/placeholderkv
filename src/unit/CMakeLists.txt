project(valkey-unit-tests)

file(GLOB UNIT_TEST_SRCS "${CMAKE_CURRENT_LIST_DIR}/*.c")
set(UNIT_TEST_SRCS "${UNIT_TEST_SRCS}")

get_valkey_server_linker_option(VALKEY_SERVER_LDFLAGS)

# Build unit tests only
message(STATUS "Building unit tests")
if (USE_TLS)
    if (BUILD_TLS_MODULE)
        # TLS as a module
        list(APPEND COMPILE_DEFINITIONS "USE_OPENSSL=2")
    else (BUILD_TLS_MODULE)
        # Built-in TLS support
        list(APPEND COMPILE_DEFINITIONS "USE_OPENSSL=1")
        list(APPEND COMPILE_DEFINITIONS "BUILD_TLS_MODULE=0")
    endif ()
endif ()

# Build Valkey sources as a static library for the test
add_library(valkeylib STATIC ${VALKEY_SERVER_SRCS})
target_compile_options(valkeylib PRIVATE "${COMPILE_FLAGS}")
target_compile_definitions(valkeylib PRIVATE "${COMPILE_DEFINITIONS}")

add_executable(valkey-unit-tests ${UNIT_TEST_SRCS})
target_compile_options(valkey-unit-tests PRIVATE "${COMPILE_FLAGS}")
target_compile_definitions(valkey-unit-tests PRIVATE "${COMPILE_DEFINITIONS}")
add_dependencies(valkey-unit-tests generate_test_files_h)

if (UNIX AND NOT APPLE)
    # Avoid duplicate symbols on non macOS
    target_link_options(valkey-unit-tests PRIVATE "-Wl,--allow-multiple-definition")
endif ()

if (USE_JEMALLOC)
    # Using jemalloc
    target_link_libraries(valkey-unit-tests jemalloc)
endif ()

if (IS_FREEBSD)
    target_link_libraries(valkey-unit-tests execinfo)
endif ()

target_link_libraries(
    valkey-unit-tests
    valkeylib
    fpconv
    lualib
    hdr_histogram
    hiredis
    ${VALKEY_SERVER_LDFLAGS})

if (USE_TLS)
    # Add required libraries needed for TLS
    target_link_libraries(valkey-unit-tests OpenSSL::SSL hiredis_ssl)
endif ()
