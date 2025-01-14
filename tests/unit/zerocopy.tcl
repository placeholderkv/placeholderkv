start_server {tags {"repl zerocopy external:skip"}} {
start_server {} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    set replica_pid [s -1 process_id]
    set replica [srv -1 client]
    set replica_host [srv -1 host]
    set replica_port [srv -1 port]

    # Only test if zerocopy is supported.
    if {[lindex [$primary config get tcp-tx-zerocopy] 1] == "yes"} {
        $primary debug zerocopy-for-loopback 1
        $primary config set repl-timeout 1200 ;# 20 minutes (for valgrind and slow machines)
        $replica config set repl-timeout 1200 ;# 20 minutes (for valgrind and slow machines)
        $primary config set client-output-buffer-limit "replica 0 0 0"
        $primary config set repl-backlog-size [expr 64*1024]
        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica

        test {First zerocopy write allocates tracker} {
            assert_equal [s 0 used_memory_zero_copy_tracking] 0

            # Note that we have no control over the actual write size to replica,
            # so we set this to zero to force zero copy to be used.
            $primary config set tcp-zerocopy-min-write-size 0

            populate 1 "with_zcp:" 1024 0
            wait_for_ofs_sync $primary $replica

            assert {[s 0 used_memory_zero_copy_tracking] > 0}
        }

        test {tcp-zerocopy-min-write-size enforcement} {
            set initial_zerocopy_writes [s 0 zero_copy_writes_processed]

            $primary config set tcp-zerocopy-min-write-size 10240

            populate 1 "no_zcp:" 1024 0
            wait_for_ofs_sync $primary $replica

            assert_equal [s 0 zero_copy_writes_processed] $initial_zerocopy_writes
            assert_equal [s 0 zero_copy_writes_in_flight] 0

            $primary config set tcp-zerocopy-min-write-size 0

            populate 1 "with_zcp:" 1024 0
            wait_for_ofs_sync $primary $replica

            # In-flight zero copy writes should get their ACKs
            wait_for_condition 100 100 {
                [s 0 zero_copy_writes_in_flight] == 0
            } else {
                fail "In flight zero copy writes never completed"
            }
            assert {[s 0 zero_copy_writes_processed] > $initial_zerocopy_writes}
        }

        test {Zero copy writes trim backlog once received} {
            $primary config set tcp-zerocopy-min-write-size 0

            assert {[s 0 repl_backlog_histlen] < [expr 64 * 1024 + 16*1024]}

            populate 100 "big_key:" 10240 0
            wait_for_ofs_sync $primary $replica

            # In-flight zero copy writes should get their ACKs
            wait_for_condition 100 100 {
                [s 0 zero_copy_writes_in_flight] == 0
            } else {
                fail "In flight zero copy writes never completed"
            }

            # Backlog should be trimmed to repl-backlog-size (plus up to PROTO_REPLY_CHUNK_BYTES/16KiB)
            wait_for_condition 100 100 {
                [s 0 repl_backlog_histlen] < [expr 64*1024 + 16*1024]
            } else {
                fail "Backlog should eventually be trimmed back to repl-backlog-size"
            }
        }

        test {Zero copy handles late ACKs gracefully} {
            $primary config set tcp-zerocopy-min-write-size 0

            # Pause handling of error queue events to simulate slow client
            $primary debug pause-errqueue-events 1

            # Write 100 KiB, which should grow the repl backlog beyond the max
            populate 1 "zerocopy_key:big:" [expr 100 * 1024] 0
            wait_for_condition 100 100 {
                [s 0 zero_copy_writes_in_flight] > 0
            } else {
                fail "Expect in flight zero copy writes to be accumulated"
            }
            wait_for_condition 100 100 {
                [s 0 repl_backlog_histlen] > [expr 64*1024 + 16*1024]
            } else {
                fail "Expect replication backlog to grow beyond configuration"
            }

            # Resume the error queue events
            $primary debug pause-errqueue-events 0
            wait_for_ofs_sync $primary $replica

            # In-flight zero copy writes should get their ACKs
            wait_for_condition 100 100 {
                [s 0 zero_copy_writes_in_flight] == 0
            } else {
                fail "In flight zero copy writes never completed"
            }

            # Backlog should be trimmed to repl-backlog-size (plus up to PROTO_REPLY_CHUNK_BYTES/16KiB)
            wait_for_condition 100 100 {
                [s 0 repl_backlog_histlen] < [expr 64*1024 + 16*1024]
            } else {
                fail "Backlog should eventually be trimmed back to repl-backlog-size"
            }
        }

        test {In-flight zerocopy writes are gracefully flushed when replica is killed} {
            $primary config set tcp-zerocopy-min-write-size 0

            # Pause handling of error queue events to simulate slow client
            $primary debug pause-errqueue-events 1

            # Pause the replica to ensure it doesn't attempt reconnect
            pause_process $replica_pid

            # Write 100 KiB, which should grow the repl backlog beyond the max
            populate 1 "zerocopy_key:extra:" [expr 100 * 1024] 0
            wait_for_condition 100 100 {
                [s 0 zero_copy_writes_in_flight] > 0
            } else {
                fail "Expect in flight zero copy writes to be accumulated"
            }
            wait_for_condition 100 100 {
                [s 0 repl_backlog_histlen] > [expr 64*1024 + 16*1024]
            } else {
                fail "Expect replication backlog to grow beyond configuration"
            }

            # Kill the replica client
            assert {[$primary client kill type replica] > 0}

            # Should now be draining
            assert_equal [s 0 draining_clients] 1

            # Unpause the error queue and the draining should end gracefully
            $primary debug pause-errqueue-events 0
            wait_for_condition 100 100 {
                [s 0 draining_clients] eq 0
            } else {
                fail "Client never finished draining"
            }

            # Backlog should be trimmed to repl-backlog-size (plus up to PROTO_REPLY_CHUNK_BYTES/16KiB)
            wait_for_condition 100 100 {
                [s 0 repl_backlog_histlen] < [expr 64*1024 + 16*1024]
            } else {
                fail "Backlog should eventually be trimmed back to repl-backlog-size"
            }

            # Replica should be able to resync
            resume_process $replica_pid
            wait_for_ofs_sync $primary $replica
        }

        test {Zero copy tracker grows and shrinks as needed} {
            $primary config set tcp-zerocopy-min-write-size 0

            # Initialize the zero copy tracker
            populate 1 "zerocopy_key:init:" 1024 0
            wait_for_ofs_sync $primary $replica
            set initial_zerocopy_mem [s 0 used_memory_zero_copy_tracking]
            assert {$initial_zerocopy_mem > 0}

            # Accumulate a lot of in flight writes
            $primary debug pause-errqueue-events 1
            set success 0
            for {set i 0} {$i < 1000000} {incr i} {
                if {[status $primary zero_copy_writes_in_flight] <= 1024} {
                    populate 1 "zerocopy_key:small-$i:" 1 0
                } else {
                    set success 1
                    break
                }
            }
            if {$success == 0} {
                fail "After one million writes, still don't have 1025 in flight zero copy writes"
            }

            # At 1025 in flight writes, our tracking buffer should have grown
            assert {[s 0 used_memory_zero_copy_tracking] > $initial_zerocopy_mem}

            # Flush the writes
            $primary debug pause-errqueue-events 0
            wait_for_condition 100 100 {
                [s 0 zero_copy_writes_in_flight] == 0
            } else {
                fail "In flight zero copy writes never completed"
            }

            # Buffer should shrink back to original size
            assert_equal [s 0 used_memory_zero_copy_tracking] $initial_zerocopy_mem
        }
    } else {
        if {$::verbose} { puts "Skipping zero copy tests." }
    }
}
}
