proc setup {{size 1}} {
    r set k v
    r config set aof-max-size $size
    r set k2 v2
}

proc cleanup {} {
    r config set aof-max-size 0
    r flushall
}

start_server {tags {"external:skip"}} {
    r config set auto-aof-rewrite-percentage 0 ; # disable auto-rewrite
    r config set appendonly yes ; # enable AOF

    set master_host [srv 0 host]
    set master_port [srv 0 port]

    test "Low aof-max-size stops writing AOF with ENOSPC" {
        setup
        wait_for_log_messages 0 {"*Error writing to the AOF file: No space left on device*"} 0 100 10
        cleanup
    }

    test "New write attempts when limited with aof-max-size fail and doesn't insrease AOF buffer anymore" {
        setup
        set info1 [r info]
        set buf1 [getInfoProperty $info1 mem_aof_buffer]
        set len1 [getInfoProperty $info1 aof_buffer_length]

        catch {r set somelongerkey somelongrvalue} err
        assert {$err eq "MISCONF Errors writing to the AOF file: No space left on device"}
        assert_equal [r get somelongerkey] ""

        set info2 [r info]
        set buf2 [getInfoProperty $info2 mem_aof_buffer]
        set len2 [getInfoProperty $info2 aof_buffer_length]
        assert_equal $buf1 $buf2
        assert_equal $len1 $len2
        cleanup
    }

    test "Increasing aof-max-size fixes AOF write error" {
        setup
        set loglines [count_log_lines 0] ; # want to check new line, not from previous test
        r config set aof-max-size 1000
        wait_for_log_messages 0 {"*AOF write error looks solved. The server can write again.*"} $loglines 100 10

        assert_equal [r set k3 v3] "OK"
        assert_equal [r get k3] "v3"
        cleanup
    }

    test "Meeting aof-max-size does not prevent AOF rewrite" {
        setup 200
        set loglines [count_log_lines 0] ; # want to check new line, not from previous test

        waitForBgrewriteaof r
        r bgrewriteaof
        wait_for_log_messages 0 {"*Background AOF rewrite finished successfully*"} $loglines 100 10
        wait_for_log_messages 0 {"*AOF write error looks solved. The server can write again.*"} $loglines 100 10
        cleanup
    }
}