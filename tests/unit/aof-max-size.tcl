start_server {tags {"aof-max-size" "external:skip"}} {
    r config set auto-aof-rewrite-percentage 0 ; # disable auto-rewrite
    r config set appendonly yes ; # enable AOF

    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    test "Low aof-max-size stops writing AOF with ENOSPC" {
        r set k v
        r config set aof-max-size 1

        r set k2 v2
        wait_for_log_messages 0 {"*Error writing to the AOF file: No space left on device*"} 0 100 10
    }

    test "New write attempts fail and doesn't insrease AOF buffer anymore" {
        set info1 [r info]
        set buf1 [getInfoProperty $info1 mem_aof_buffer]
        set len1 [getInfoProperty $info1 aof_buffer_length]

        catch {r set somelongerkey somelongervalue} err
        assert {$err eq "MISCONF Errors writing to the AOF file: No space left on device"}
        assert_equal [r get somelongerkey] ""

        set info2 [r info]
        set buf2 [getInfoProperty $info2 mem_aof_buffer]
        set len2 [getInfoProperty $info2 aof_buffer_length]
        assert_equal $buf1 $buf2
        assert_equal $len1 $len2
    }

    test "Increasing aof-max-size fixes AOF write error" {
        r config set aof-max-size 1000
        wait_for_log_messages 0 {"*AOF write error looks solved. The server can write again.*"} 0 100 10

        assert_equal [r set k3 v3] "OK"
        assert_equal [r get k3] "v3"
    }

    test "Meeting aof-max-size does not prevent AOF rewrite" {
        set loglines [count_log_lines 0] ; # want to check new line, not from previous test
        
        # start write load
        set load_handle0 [start_write_load $master_host $master_port 10]
        wait_for_condition 50 100 {
            [r dbsize] > 0
        } else {
            fail "No write load detected."
        }

        waitForBgrewriteaof r
        r bgrewriteaof
        wait_for_log_messages 0 {"*Background AOF rewrite finished successfully*"} $loglines 100 10
        wait_for_log_messages 0 {"*AOF write error looks solved. The server can write again.*"} $loglines 100 10

        # stop write load
        stop_write_load $load_handle0
        wait_load_handlers_disconnected
    }
}