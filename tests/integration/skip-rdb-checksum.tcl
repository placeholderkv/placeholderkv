proc test_skip_rdb_checksum {primary primary_host primary_port primary_skipped_rdb_checksum_counter primary_diskless_sync replica_diskless_load} {
    upvar primary_skipped_rdb_checksum_counter counter
    $primary config set repl-diskless-sync $primary_diskless_sync
    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica config set repl-diskless-load $replica_diskless_load
        $replica replicaof $primary_host $primary_port
        
        wait_for_condition 50 100 {
            [s 0 master_link_status] eq {up}
        } else {
            fail "Replication not started"
        }
        
        set replica_skipping_rdb_checksum_count [count_log_message 0 "RDB file was saved with checksum disabled: skipped checksum for this transfer"]
        set primary_skipping_rdb_checksum_count [count_log_message -1 "while skipping RDB checksum for this transfer"]
        
        if {$replica_diskless_load eq "disabled" || $primary_diskless_sync eq "no" || !$::tls} {
            assert_equal $counter $primary_skipping_rdb_checksum_count "Primary should not skip RDB checksum in this scenario"
            assert_equal 0 $replica_skipping_rdb_checksum_count "Replica should not skip RDB checksum in this scenario"
        } else {
            incr counter
            assert_equal $counter $primary_skipping_rdb_checksum_count "Primary should skip RDB checksum in this scenario"
            assert_equal 1 $replica_skipping_rdb_checksum_count "Replica should skip RDB checksum in this scenario"
        }
    }
}

start_server {tags {"repl tls cluster:skip external:skip"} overrides {save {}}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set primary_skipped_rdb_checksum_counter 0
    if {$::tls} {
        foreach primary_diskless_sync {no yes} {
            foreach replica_diskless_load {disabled on-empty-db swapdb flush-before-load} {
                test "Skip RDB checksum sync - tls:$::tls, repl_diskless_sync:$primary_diskless_sync, repl_diskless_load:$replica_diskless_load" {
                    test_skip_rdb_checksum $primary $primary_host $primary_port $primary_skipped_rdb_checksum_counter $primary_diskless_sync $replica_diskless_load
                }
            }
        }
    } else {
        set primary_diskless_sync yes
        set replica_diskless_load on-empty-db
        test "Skip RDB checksum sync - tls:$::tls, repl_diskless_sync:$primary_diskless_sync, repl_diskless_load:$replica_diskless_load" {
            test_skip_rdb_checksum $primary $primary_host $primary_port $primary_skipped_rdb_checksum_counter $primary_diskless_sync $replica_diskless_load
        }
    }
}
