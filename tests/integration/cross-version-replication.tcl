# Test replication from an older version primary.
#
# Use minimal.conf to make sure we don't use any configs not supported on the old version.

proc server_name_and_version {} {
    set server_name [s server_name]
    if {$server_name eq {}} {
        set server_name redis
    }
    set server_version [s "${server_name}_version"]
    return "$server_name $server_version"
}

start_server {tags {"repl needs:other-server external:skip"} start-other-server 1 config "minimal.conf"} {
    set primary_name_and_version [server_name_and_version]
    r set foo bar

    start_server {} {
        test "Start replication from $primary_name_and_version" {
            r replicaof [srv -1 host] [srv -1 port]
            wait_for_sync r 500 100
            # The key has been transferred.
            assert_equal bar [r get foo]
            assert_equal up [s master_link_status]
        }

        test "Replicate a SET command from $primary_name_and_version" {
            r -1 set baz quux
            wait_for_ofs_sync [srv 0 client] [srv -1 client]
            set reply [r get baz]
            assert_equal $reply quux
        }
    }
}
