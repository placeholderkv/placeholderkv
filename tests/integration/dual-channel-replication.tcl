proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

proc get_client_id_by_last_cmd {r cmd} {
    set client_list [$r client list]
    set client_id ""
    set lines [split $client_list "\n"]
    foreach line $lines {
        if {[string match *cmd=$cmd* $line]} {
            set parts [split $line " "]
            foreach part $parts {
                if {[string match id=* $part]} {
                    set client_id [lindex [split $part "="] 1]
                    return $client_id
                }
            }
        }
    }
    return $client_id
}

# Wait until the process enters a paused state.
proc wait_process_paused idx {
    set pid [srv $idx pid]
    wait_for_condition 50 1000 {
        [string match "T*" [exec ps -o state= -p $pid]]
    } else {
        fail "Process $pid didn't stop, current state is [exec ps -o state= -p $pid]"
    }
}

# Wait until the process enters a paused state, then resume the process.
proc wait_and_resume_process idx {
    set pid [srv $idx pid]
    wait_process_paused $idx
    resume_process $pid
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set replica_log [srv 0 stdout]
    $replica config set loglevel debug    
    start_server {} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]

        # Configure the primary in order to hang waiting for the BGSAVE
        # operation, so that the replica remains in the handshake state.
        $primary config set repl-diskless-sync yes
        $primary config set repl-diskless-sync-delay 1000
        $primary config set dual-channel-replication-enabled yes
        $primary config set loglevel debug

        # Start the replication process...
        $replica config set dual-channel-replication-enabled yes
        $replica replicaof $primary_host $primary_port

        test "Test dual-channel-replication-enabled replica enters handshake" {
            wait_for_condition 50 1000 {
                [string match *handshake* [$replica role]]
            } else {
                fail "Replica does not enter handshake state"
            }
        }

        test "Test dual-channel-replication-enabled enters wait_bgsave" {
            wait_for_condition 50 1000 {
                [string match *state=wait_bgsave* [$primary info replication]]
            } else {
                fail "Replica does not enter wait_bgsave state"
            }
        }

        $primary config set repl-diskless-sync-delay 0

        test "Test dual-channel-replication-enabled replica is able to sync" {
            verify_replica_online $primary 0 500
            wait_for_condition 50 1000 {
                [string match *connected_slaves:1* [$primary info]]
            } else {
                fail "Replica rdb connection is still open"
            }
            set offset [status $primary master_repl_offset]
            wait_for_condition 500 100 {
                [string match "*slave0:*,offset=$offset,*" [$primary info replication]] &&
                $offset == [status $replica master_repl_offset]
            } else {
                fail "Replicas and primary offsets were unable to match."
            }
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set replica_log [srv 0 stdout]
    start_server {} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]

        $primary config set rdb-key-save-delay 200
        $primary config set dual-channel-replication-enabled yes
        $primary config set repl-diskless-sync-delay 0
        $replica config set dual-channel-replication-enabled yes
        $replica config set repl-diskless-sync no

        populate 1000 primary 10000
        set load_handle1 [start_one_key_write_load $primary_host $primary_port 100 "mykey1"]
        set load_handle2 [start_one_key_write_load $primary_host $primary_port 100 "mykey2"]
        set load_handle3 [start_one_key_write_load $primary_host $primary_port 100 "mykey3"]
        
        # wait for load handlers to start
        wait_for_condition 50 1000 {
            ([$primary get "mykey1"] != "") &&
            ([$primary get "mykey2"] != "") &&
            ([$primary get "mykey3"] != "")
        } else {
            fail "Can't set new keys"
        }

        set before_used [s 0 used_memory]

        test "Primary memory usage does not increase during dual-channel-replication sync" {        
            $replica replicaof $primary_host $primary_port

            # Verify used_memory stays low through all the sync
            set max_retry 500
            while {$max_retry} {
                # Verify memory
                set used_memory [s 0 used_memory]
                assert {$used_memory-$before_used <= 1.5*10^6}; # ~1/3 of the space
                # Check replica state
                set primary_info [$primary info]
                set replica_info [$replica info]
                if {[string match *slave0:*state=online* $primary_info] &&
                    [string match *master_link_status:up* $replica_info]} {
                    break
                } else {
                    incr max_retry -1
                    after 10
                }
            }
            if {$max_retry == 0} {
                error "assertion:Replica not in sync after 5 seconds"
            }
        }
        stop_write_load $load_handle1
        stop_write_load $load_handle2
        stop_write_load $load_handle3

        test "Steady state after dual channel sync" {
            wait_for_condition 50 1000 {
                ([$replica get "mykey1"] eq [$primary get mykey1]) &&
                ([$replica get "mykey2"] eq [$primary get mykey2]) &&
                ([$replica get "mykey3"] eq [$primary get mykey3])
            } else {
                fail "Can't set new keys"
            }
        }

        test "Dual channel replication sync doesn't impair subsequent normal syncs" {
            $replica replicaof no one
            $replica config set dual-channel-replication-enabled no
            $primary set newkey newval

            set sync_full [s 0 sync_full]
            set sync_partial [s 0 sync_partial_ok]

            $replica replicaof $primary_host $primary_port
            verify_replica_online $primary 0 500
            # Verify replica used  normal sync this time
            assert_equal [expr $sync_full + 1] [s 0 sync_full]
            assert_equal [expr $sync_partial] [s 0 sync_partial_ok]
            assert [string match *connected_slaves:1* [$primary info]]
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set replica_log [srv 0 stdout]
    start_server {} {
        foreach enable {yes no} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            $primary config set repl-diskless-sync yes
            # Set primary shared replication buffer size to a bit more then the size of 
            # a replication buffer block.
            $primary config set client-output-buffer-limit "replica 1100k 0 0"
            $primary config set dual-channel-replication-enabled $enable
            $primary config set repl-diskless-sync-delay 0
            $replica config set dual-channel-replication-enabled $enable

            test "Toggle dual-channel-replication-enabled: $enable start" {    
                populate 1000 primary 10000
                $primary config set rdb-key-save-delay 0
                set prev_sync_full [s 0 sync_full]
                set prev_sync_partial [s 0 sync_partial_ok]

                $replica replicaof $primary_host $primary_port
                verify_replica_online $primary 0 500
                wait_for_sync $replica


                set cur_sync_full [s 0 sync_full]
                set cur_sync_partial [s 0 sync_partial_ok]
                if {$enable == "yes"} {
                    # Verify that dual channel replication sync was used
                    assert {$cur_sync_full == [expr $prev_sync_full + 1]}
                    assert {$cur_sync_partial == [expr $prev_sync_partial + 1]}
                } else {
                    # Verify that normal sync was used
                    assert {[s 0 sync_full] == [expr $prev_sync_full + 1]}
                    assert {[s 0 sync_partial_ok] == $prev_sync_partial}
                }

                $replica replicaof no one
                if {$enable == "yes"} {
                    # disable dual channel sync
                    $replica config set dual-channel-replication-enabled no
                    $primary config set dual-channel-replication-enabled no
                } else {
                    $replica config set dual-channel-replication-enabled yes
                    $primary config set dual-channel-replication-enabled yes
                }

                # Force replica to full sync next time
                populate 1000 primary 10000
                set prev_sync_full [s 0 sync_full]
                set prev_sync_partial [s 0 sync_partial_ok]

                $replica replicaof $primary_host $primary_port
                verify_replica_online $primary 0 500
                wait_for_sync $replica

                set cur_sync_full [s 0 sync_full]
                set cur_sync_partial [s 0 sync_partial_ok]
                if {$enable == "yes"} {
                    # Verify that normal sync was used
                    assert {$cur_sync_full == [expr $prev_sync_full + 1]}
                    assert {$cur_sync_partial == $prev_sync_partial}
                } else {
                    # Verify that dual channel replication sync was used
                    assert {$cur_sync_full == [expr $prev_sync_full + 1]}
                    assert {$cur_sync_partial == [expr $prev_sync_partial + 1]}
                }
                $replica replicaof no one
            }

            foreach test_instance {primary replica} {
                $primary config set dual-channel-replication-enabled $enable
                $replica config set dual-channel-replication-enabled $enable
                test "Online toggle dual-channel-replication-enabled on $test_instance, starting with '$enable'" {    
                    populate 1000 primary 10000
                    $primary config set rdb-key-save-delay 100000   

                    $replica replicaof $primary_host $primary_port
                    # wait for sync to start
                    if {$enable == "yes"} {
                        wait_for_condition 500 1000 {
                            [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                            [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                            [s 0 rdb_bgsave_in_progress] eq 1
                        } else {
                            fail "replica didn't start a dual-channel sync session in time"
                        }
                    } else {
                        wait_for_condition 500 1000 {
                            [string match "*slave*,state=wait_bgsave*,type=replica*" [$primary info replication]] &&
                            [s 0 rdb_bgsave_in_progress] eq 1
                        } else {
                            fail "replica didn't start a normal full sync session in time"
                        }
                    }
                    # Toggle config
                    set new_value "yes"
                    if {$enable == "yes"} {
                        set new_value "no"
                    }
                    set instance $primary
                    if {$test_instance == "replica"} {
                        set instance $replica
                    }
                    $instance config set dual-channel-replication-enabled $new_value
                    # Wait for at least one server cron
                    after 1000

                    if {$enable == "yes"} {
                        # Verify that dual channel replication sync is still in progress
                        assert [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]]
                        assert [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]]
                        assert {[s 0 rdb_bgsave_in_progress] eq 1}
                    } else {
                        # Verify that normal sync is still in progress
                        assert [string match "*slave*,state=wait_bgsave*,type=replica*" [$primary info replication]]
                        assert {[s 0 rdb_bgsave_in_progress] eq 1}
                    }
                    $replica replicaof no one
                    wait_for_condition 500 1000 {
                        [s -1 rdb_bgsave_in_progress] eq 0
                    } else {
                        fail "Primary should abort sync"
                    }
                }
            }
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica1 [srv 0 client]
    set replica1_host [srv 0 host]
    set replica1_port [srv 0 port]
    set replica1_log [srv 0 stdout]
    start_server {} {
        set replica2 [srv 0 client]
        set replica2_host [srv 0 host]
        set replica2_port [srv 0 port]
        set replica2_log [srv 0 stdout]
        start_server {} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]
            set loglines [count_log_lines -1]

            populate 10000 primary 10
            $primary set key1 val1

            $primary config set repl-diskless-sync yes
            $primary config set repl-diskless-sync-delay 5; # allow both replicas to ask for sync
            $primary config set dual-channel-replication-enabled yes

            $replica1 config set dual-channel-replication-enabled yes
            $replica2 config set dual-channel-replication-enabled yes
            $replica1 config set repl-diskless-sync no
            $replica2 config set repl-diskless-sync no
            $replica1 config set loglevel debug
            $replica2 config set loglevel debug

            test "dual-channel-replication with multiple replicas" {
                $replica1 replicaof $primary_host $primary_port
                $replica2 replicaof $primary_host $primary_port
                verify_replica_online $primary 0 500
                verify_replica_online $primary 1 500

                wait_for_value_to_propagate_to_replica $primary $replica1 "key1"
                wait_for_value_to_propagate_to_replica $primary $replica2 "key1"

                assert {[s 0 total_forks] eq "1" }       
            }

            $replica1 replicaof no one
            $replica2 replicaof no one

            $replica1 config set dual-channel-replication-enabled yes
            $replica2 config set dual-channel-replication-enabled no

            $primary set key2 val2

            test "Test diverse replica sync: dual-channel on/off" {
                $replica1 replicaof $primary_host $primary_port
                $replica2 replicaof $primary_host $primary_port
                verify_replica_online $primary 0 500
                verify_replica_online $primary 1 500
                wait_for_value_to_propagate_to_replica $primary $replica1 "key2"
                wait_for_value_to_propagate_to_replica $primary $replica2 "key2"
                wait_for_condition 50 1000 {
                    [status $replica1 master_link_status] == "up"
                } else {
                    fail "Replica is not synced"
                }
            }

            $replica1 replicaof no one

            test "Test replica's buffer limit reached" {
                $primary config set repl-diskless-sync-delay 0
                $primary config set rdb-key-save-delay 10000
                # At this point we have about 10k keys in the db, 
                # We expect that the next full sync will take 100 seconds (10k*10000)ms
                # It will give us enough time to fill the replica buffer.
                $replica1 config set dual-channel-replication-enabled yes
                $replica1 config set client-output-buffer-limit "replica 16383 16383 0"

                $replica1 replicaof $primary_host $primary_port
                # Wait for replica to establish psync using main channel
                wait_for_condition 500 1000 {
                    [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]]
                } else {
                    fail "replica didn't start sync session in time"
                }  

                populate 10000 primary 10; # set ~ 100kb
                # Wait for replica's buffer limit reached
                wait_for_condition 50 1000 {
                    [log_file_matches $replica1_log "*Replication buffer limit reached, stopping buffering*"]
                } else {
                    fail "Replica buffer should fill"
                }
                assert {[s -2 replicas_replication_buffer_size] <= 16385*2}

                # Primary replication buffer should grow
                wait_for_condition 50 1000 {
                    [status $primary mem_total_replication_buffers] >= 81915
                } else {
                    fail "Primary should take the load"
                }
            }

            $replica1 replicaof no one
            $replica1 config set client-output-buffer-limit "replica 256mb 256mb 0"; # remove repl buffer limitation
            $primary config set rdb-key-save-delay 0

            wait_for_condition 500 1000 {
                [s 0 rdb_bgsave_in_progress] eq 0
            } else {
                fail "can't kill rdb child"
            }

            $primary set key3 val3
            
            test "dual-channel-replication fails when primary diskless disabled" {
                set cur_psync [status $primary sync_partial_ok]
                $primary config set repl-diskless-sync no

                $replica1 config set dual-channel-replication-enabled yes
                $replica1 replicaof $primary_host $primary_port

                # Wait for mitigation and resync
                wait_for_condition 50 1000 {
                    [status $replica1 master_link_status] == "up"
                } else {
                    fail "Replica is not synced"
                }
                wait_for_value_to_propagate_to_replica $primary $replica1 "key3"

                # Verify that we did not use dual-channel-replication sync
                assert {[status $primary sync_partial_ok] == $cur_psync}
            }
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set replica_log [srv 0 stdout]
    start_server {} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]
        # Create small enough db to be loaded before replica establish psync connection
        $primary set key1 val1

        $primary config set repl-diskless-sync yes
        $primary debug pause-after-fork 1
        $primary config set dual-channel-replication-enabled yes

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug

        test "Test dual-channel-replication sync- psync established after rdb load" {
            $replica replicaof $primary_host $primary_port
            wait_for_log_messages -1 {"*Done loading RDB*"} 0 2000 1
            wait_and_resume_process 0

            verify_replica_online $primary 0 500
            wait_for_condition 50 1000 {
                [status $replica master_link_status] == "up"
            } else {
                fail "Replica is not synced"
            }
            wait_for_value_to_propagate_to_replica $primary $replica "key1"
            # Confirm the occurrence of a race condition.
            wait_for_log_messages -1 {"*Dual channel replication: Psync established after rdb load*"} 0 2000 1
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set replica_log [srv 0 stdout]
    start_server {} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]
        set backlog_size [expr {10 ** 5}]
        set loglines [count_log_lines -1]

        $primary config set repl-diskless-sync yes
        $primary config set dual-channel-replication-enabled yes
        $primary config set repl-backlog-size $backlog_size
        $primary config set loglevel debug
        $primary config set repl-diskless-sync-delay 0
        if {$::valgrind} {
            $primary config set repl-timeout 100
            $replica config set repl-timeout 100
        } else {
            $primary config set repl-timeout 10            
            $replica config set repl-timeout 10
        }
        $primary config set rdb-key-save-delay 200
        populate 10000 primary 10000
        
        set load_handle1 [start_one_key_write_load $primary_host $primary_port 100 "mykey1"]
        set load_handle2 [start_one_key_write_load $primary_host $primary_port 100 "mykey2"]
        set load_handle3 [start_one_key_write_load $primary_host $primary_port 100 "mykey3"]

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        
        # Pause replica after primary fork
        $replica debug pause-after-fork 1

        test "dual-channel-replication: Primary COB growth with inactive replica" {
            $replica replicaof $primary_host $primary_port
            # Verify repl backlog can grow
            wait_for_condition 1000 10 {
                [s 0 mem_total_replication_buffers] > [expr {2 * $backlog_size}]
            } else {
                fail "Primary should allow backlog to grow beyond its limits during dual-channel-replication sync handshake"
            }
            wait_and_resume_process -1

            verify_replica_online $primary 0 500
            wait_for_condition 50 1000 {
                [status $replica master_link_status] == "up"
            } else {
                fail "Replica is not synced"
            }
        }

        stop_write_load $load_handle1
        stop_write_load $load_handle2
        stop_write_load $load_handle3

    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica1 [srv 0 client]
    set replica1_host [srv 0 host]
    set replica1_port [srv 0 port]
    set replica1_log [srv 0 stdout]
    start_server {} {
        set replica2 [srv 0 client]
        set replica2_host [srv 0 host]
        set replica2_port [srv 0 port]
        set replica2_log [srv 0 stdout]
        start_server {} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]
            set backlog_size [expr {10 ** 6}]
            set loglines [count_log_lines -1]

            $primary config set repl-diskless-sync yes
            $primary config set dual-channel-replication-enabled yes
            $primary config set repl-backlog-size $backlog_size
            $primary config set loglevel debug
            $primary config set repl-timeout 10
            $primary config set rdb-key-save-delay 10
            populate 1024 primary 16
            
            set load_handle0 [start_write_load $primary_host $primary_port 20]

            $replica1 config set dual-channel-replication-enabled yes
            $replica2 config set dual-channel-replication-enabled yes
            $replica1 config set loglevel debug
            $replica2 config set loglevel debug
            $replica1 config set repl-timeout 10
            $replica2 config set repl-timeout 10

            # Pause replicas after primary forks for
            $replica1 debug pause-after-fork 1
            $replica2 debug pause-after-fork 1
            test "Test dual-channel: primary tracking replica backlog refcount - start with empty backlog" {
                $replica1 replicaof $primary_host $primary_port
                set res [wait_for_log_messages 0 {"*Add rdb replica * no repl-backlog to track*"} $loglines 2000 1]
                set res [wait_for_log_messages 0 {"*Attach replica rdb client*"} $loglines 2000 1]
                set loglines [lindex $res 1]
                incr $loglines
                wait_and_resume_process -2
                verify_replica_online $primary 0 700
                wait_for_condition 50 1000 {
                    [status $replica1 master_link_status] == "up"
                } else {
                    fail "Replica is not synced"
                }
                $replica1 replicaof no one
                assert [string match *replicas_waiting_psync:0* [$primary info replication]]
            }

            test "Test dual-channel: primary tracking replica backlog refcount - start with backlog" {
                $replica2 replicaof $primary_host $primary_port
                set res [wait_for_log_messages 0 {"*Add rdb replica * tracking repl-backlog tail*"} $loglines 2000 1]
                set loglines [lindex $res 1]
                incr $loglines
                wait_and_resume_process -1
                verify_replica_online $primary 0 700
                wait_for_condition 50 1000 {
                    [status $replica2 master_link_status] == "up"
                } else {
                    fail "Replica is not synced"
                }
                assert [string match *replicas_waiting_psync:0* [$primary info replication]]
            }

            stop_write_load $load_handle0
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    
    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set repl-backlog-size [expr {10 ** 6}]
    $primary config set loglevel debug
    $primary config set repl-timeout 10
    # generate small db
    populate 10 primary 10
    # Pause primary main process after fork
    $primary debug pause-after-fork 1
    # Give replica two second grace period before disconnection
    $primary debug delay-rdb-client-free-seconds 2

    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]
        set replica_pid  [srv 0 pid]
        set loglines [count_log_lines 0]
        
        set load_handle0 [start_write_load $primary_host $primary_port 20]

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10 

        test "Psync established after rdb load - within grace period" {
            # Test Sequence:
            # 1. Replica initiates synchronization via RDB channel.
            # 2. Primary's main process is suspended.
            # 3. Replica completes RDB loading and pauses before establishing PSYNC connection.
            # 4. Primary resumes operation and detects closed RDB channel.
            # 5. Replica resumes operation.
            # Expected outcome: Primary maintains RDB channel until replica establishes PSYNC connection.
            $replica replicaof $primary_host $primary_port
            wait_for_log_messages 0 {"*Done loading RDB*"} $loglines 2000 1
            pause_process $replica_pid
            wait_and_resume_process -1
            wait_for_condition 50 100 {
                [string match {*replicas_waiting_psync:1*} [$primary info replication]]
            } else {
                fail "Primary freed RDB client before psync was established"
            }
            resume_process $replica_pid

            verify_replica_online $primary 0 500
            wait_for_condition 50 100 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after psync establishment"
            }
            $replica replicaof no one
        }
        stop_write_load $load_handle0
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set repl-backlog-size [expr {10 ** 6}]
    $primary config set loglevel debug
    $primary config set repl-timeout 10
    # generate small db
    populate 10 primary 10
    # Pause primary main process after fork
    $primary debug pause-after-fork 1
    # Give replica two second grace period before disconnection
    $primary debug delay-rdb-client-free-seconds 2

    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]
        set replica_pid  [srv 0 pid]
        set loglines [count_log_lines 0]
        
        set load_handle0 [start_write_load $primary_host $primary_port 20]

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10

        test "Psync established after RDB load - beyond grace period" {
            # Test Sequence:
            # 1. Replica initiates synchronization via RDB channel.
            # 2. Primary's main process is suspended.
            # 3. Replica completes RDB loading and pauses before establishing PSYNC connection.
            # 4. Primary resumes operation and detects closed RDB channel.
            # Expected outcome: Primary drops the RDB channel after grace period is done.
            $replica replicaof $primary_host $primary_port
            wait_for_log_messages 0 {"*Done loading RDB*"} $loglines 2000 1
            pause_process $replica_pid
            wait_and_resume_process -1
            wait_for_condition 50 100 {
                [string match {*replicas_waiting_psync:1*} [$primary info replication]]
            } else {
                fail "Primary should wait before freeing repl block"
            }

            # Sync should fail once the replica ask for PSYNC using main channel
            set res [wait_for_log_messages -1 {"*Replica main channel failed to establish PSYNC within the grace period*"} 0 4000 1]
            wait_for_condition 50 100 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free waiting psync replica after grace period"
            }
            resume_process $replica_pid
        }
        stop_write_load $load_handle0
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set client-output-buffer-limit "replica 1100k 0 0"
    $primary config set loglevel debug
    # generate small db
    populate 10 primary 10
    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]
        set replica_pid  [srv 0 pid]
        
        set load_handle0 [start_write_load $primary_host $primary_port 60]
        set load_handle1 [start_write_load $primary_host $primary_port 60]
        set load_handle2 [start_write_load $primary_host $primary_port 60]

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 60
        $primary config set repl-backlog-size 1mb

        test "Test dual-channel-replication primary gets cob overrun before established psync" {
            # Pause primary main process after fork
            $primary debug pause-after-fork 1
            $replica replicaof $primary_host $primary_port
            wait_for_log_messages 0 {"*Done loading RDB*"} 0 1000 10

            # At this point rdb is loaded but psync hasn't been established yet. 
            # Pause the replica so the primary main process will wake up while the
            # replica is unresponsive. We expect the main process to fill the COB and disconnect the replica.
            pause_process $replica_pid
            wait_and_resume_process -1
            $primary debug pause-after-fork 0
            wait_for_log_messages -1 {"*Client * closed * for overcoming of output buffer limits.*"} $loglines 1000 10
            wait_for_condition 50 100 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            # Full sync will be triggered after the replica is reconnected, pause primary main process after fork.
            # In this way, in the subsequent replicaof no one, we won't get the LOADING error if the replica reconnects
            # too quickly and enters the loading state.
            $primary debug pause-after-fork 1
            resume_process $replica_pid
            set res [wait_for_log_messages -1 {"*Unable to partial resync with replica * for lack of backlog*"} $loglines 2000 10]
            set loglines [lindex $res 1]
        }
        # Waiting for the primary to enter the paused state, that is, make sure that bgsave is triggered.
        wait_process_paused -1
        $replica replicaof no one
        # Resume the primary and make sure the sync is dropped.
        resume_process [srv -1 pid]
        $primary debug pause-after-fork 0
        wait_for_condition 500 1000 {
            [s -1 rdb_bgsave_in_progress] eq 0
        } else {
            fail "Primary should abort sync"
        }
        stop_write_load $load_handle0
        stop_write_load $load_handle1
        stop_write_load $load_handle2
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set client-output-buffer-limit "replica 1100k 0 0"
    $primary config set loglevel debug
    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]
        set replica_pid  [srv 0 pid]
        
        set load_handle0 [start_write_load $primary_host $primary_port 60]
        set load_handle1 [start_write_load $primary_host $primary_port 60]
        set load_handle2 [start_write_load $primary_host $primary_port 60]

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 60
        $primary config set repl-backlog-size 1mb
        
        $primary debug populate 1000 primary 100000
        # Set primary with a slow rdb generation, so that we can easily intercept loading
        # 10ms per key, with 1000 keys is 10 seconds
        $primary config set rdb-key-save-delay 10000

        test "Test dual-channel-replication primary gets cob overrun during replica rdb load" {
            set cur_client_closed_count [s -1 client_output_buffer_limit_disconnections]
            $replica debug pause-after-fork 1
            $replica replicaof $primary_host $primary_port
            wait_for_condition 500 1000 {
                [s -1 client_output_buffer_limit_disconnections] > $cur_client_closed_count
            } else {
                fail "Primary should disconnect replica due to COB overrun"
            }

            wait_for_condition 50 100 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }

            # Increase the delay to make sure the replica doesn't start another sync
            # after it resumes after the first one.
            $primary config set repl-diskless-sync-delay 100
            wait_and_resume_process 0
            $replica debug pause-after-fork 0
            set res [wait_for_log_messages -1 {"*Unable to partial resync with replica * for lack of backlog*"} $loglines 20000 1]
            set loglines [lindex $res 0]
        }
        stop_write_load $load_handle0
        stop_write_load $load_handle1
        stop_write_load $load_handle2
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set loglevel debug
    $primary config set repl-diskless-sync-delay 5
    $primary config set client-output-buffer-limit "replica 0 0 0"

    # Generating RDB will cost 5s(10000 * 0.0005s)
    $primary debug populate 10000 primary 1
    $primary config set rdb-key-save-delay 500
    $primary config set dual-channel-replication-enabled yes

    start_server {} {
        set replica1 [srv 0 client]
        $replica1 config set dual-channel-replication-enabled yes
        $replica1 config set loglevel debug
        start_server {} {
            set replica2 [srv 0 client]
            $replica2 config set dual-channel-replication-enabled yes
            $replica2 config set loglevel debug
            $replica2 config set repl-timeout 60

            set load_handle [start_one_key_write_load $primary_host $primary_port 100 "mykey1"]
            test "Sync should continue if not all slaves dropped" {
                $replica1 replicaof $primary_host $primary_port
                $replica2 replicaof $primary_host $primary_port

                wait_for_condition 50 1000 {
                    [status $primary rdb_bgsave_in_progress] == 1
                } else {
                    fail "Sync did not start"
                }
                # Wait for both replicas main conns to establish psync
                wait_for_condition 50 1000 {
                    [status $primary sync_partial_ok] == 2
                } else {
                    fail "Replicas main conns didn't establish psync [status $primary sync_partial_ok]"
                }
                catch {$replica1 shutdown nosave}
                wait_for_condition 50 2000 {
                    [status $replica2 master_link_status] == "up" &&
                    [status $primary sync_full] == 2 &&
                    ([status $primary sync_partial_ok] == 2)
                } else {
                    fail "Sync session interapted\n
                        sync_full:[status $primary sync_full]\n
                        sync_partial_ok:[status $primary sync_partial_ok]"
                }
            }
            
            $replica2 replicaof no one

            # Generating RDB will cost 500s(1000000 * 0.0001s)
            $primary debug populate 1000000 primary 1
            $primary config set rdb-key-save-delay 100
    
            test "Primary abort sync if all slaves dropped dual-channel-replication" {
                set cur_psync [status $primary sync_partial_ok]
                $replica2 replicaof $primary_host $primary_port

                wait_for_condition 50 1000 {
                    [status $primary rdb_bgsave_in_progress] == 1
                } else {
                    fail "Sync did not start"
                }
                # Wait for both replicas main conns to establish psync
                wait_for_condition 50 1000 {
                    [status $primary sync_partial_ok] == $cur_psync + 1
                } else {
                    fail "Replicas main conns didn't establish psync [status $primary sync_partial_ok]"
                }

                catch {$replica2 shutdown nosave}
                wait_for_condition 50 1000 {
                    [status $primary rdb_bgsave_in_progress] == 0
                } else {
                    fail "Primary should abort the sync"
                }
            }
            stop_write_load $load_handle
        }
    }
}


start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set loglevel debug
    $primary config set repl-diskless-sync-delay 0
    # Generating RDB will cost 500s(1000000 * 0.0001s)
    $primary debug populate 1000000 primary 1
    $primary config set rdb-key-save-delay 100
    
    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]

        set load_handle [start_write_load $primary_host $primary_port 20]

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10
        test "Test dual-channel-replication replica main channel disconnected" {
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }            

            $primary debug log "killing replica main connection"
            set replica_main_conn_id [get_client_id_by_last_cmd $primary "psync"]
            assert {$replica_main_conn_id != ""}
            set loglines [count_log_lines -1]
            $primary config set repl-diskless-sync-delay 5; # allow catch failed sync before retry
            $primary client kill id $replica_main_conn_id
            # Wait for primary to abort the sync
            wait_for_condition 50 1000 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10
        }

        test "Test dual channel replication slave of no one after main conn kill" {
            $replica replicaof no one
            wait_for_condition 500 1000 {
                [s -1 rdb_bgsave_in_progress] eq 0
            } else {
                fail "Primary should abort sync"
            }
        }

        test "Test dual-channel-replication replica rdb connection disconnected" {
            $primary config set repl-diskless-sync-delay 0
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }            

            set replica_rdb_channel_id [get_client_id_by_last_cmd $primary "sync"]
            $primary debug log "killing replica rdb connection $replica_rdb_channel_id"
            assert {$replica_rdb_channel_id != ""}
            set loglines [count_log_lines -1]
            $primary config set repl-diskless-sync-delay 5; # allow catch failed sync before retry
            $primary client kill id $replica_rdb_channel_id
            # Wait for primary to abort the sync
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10
        }

        test "Test dual channel replication slave of no one after rdb conn kill" {
            $replica replicaof no one
            wait_for_condition 500 1000 {
                [s -1 rdb_bgsave_in_progress] eq 0
            } else {
                fail "Primary should abort sync"
            }
        }

        test "Test dual-channel-replication primary reject set-rdb-client after client killed" {
            $primary config set repl-diskless-sync-delay 0
            # Ensure replica main channel will not handshake before rdb client is killed
            $replica debug pause-after-fork 1
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }

            set replica_rdb_channel_id [get_client_id_by_last_cmd $primary "sync"]
            assert {$replica_rdb_channel_id != ""}
            $primary debug log "killing replica rdb connection $replica_rdb_channel_id"
            $primary config set repl-diskless-sync-delay 5; # allow catch failed sync before retry
            $primary client kill id $replica_rdb_channel_id
            # Wait for primary to abort the sync
            wait_and_resume_process 0
            wait_for_condition 10000000 10 {
                [s -1 rdb_bgsave_in_progress] eq 0 &&
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary should abort sync"
            }
            # Verify primary reject replconf set-rdb-client-id
            set res [catch {$primary replconf set-rdb-client-id $replica_rdb_channel_id} err]
            assert [string match *ERR* $err]
        }
        stop_write_load $load_handle
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set loglevel debug
    $primary config set repl-diskless-sync-delay 0; # don't wait for other replicas

    # Generating RDB will cost 100s
    $primary debug populate 10000 primary 1
    $primary config set rdb-key-save-delay 10000
    
    start_server {} {
        set replica_1 [srv 0 client]
        set replica_host_1 [srv 0 host]
        set replica_port_1 [srv 0 port]
        set replica_log_1 [srv 0 stdout]
        
        $replica_1 config set dual-channel-replication-enabled yes
        $replica_1 config set loglevel debug
        $replica_1 config set repl-timeout 10
        start_server {} {
            set replica_2 [srv 0 client]
            set replica_host_2 [srv 0 host]
            set replica_port_2 [srv 0 port]
            set replica_log_2 [srv 0 stdout]
            
            set load_handle [start_write_load $primary_host $primary_port 20]

            $replica_2 config set dual-channel-replication-enabled yes
            $replica_2 config set loglevel debug
            $replica_2 config set repl-timeout 10
            test "Test replica unable to join dual channel replication sync after started" {
                $replica_1 replicaof $primary_host $primary_port
                # Wait for sync session to start
                wait_for_condition 50 100 {
                    [s -2 rdb_bgsave_in_progress] eq 1
                } else {
                    fail "replica didn't start sync session in time1"
                }
                $replica_2 replicaof $primary_host $primary_port
                wait_for_log_messages -2 {"*Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC*"} $loglines 100 1000
            }
            stop_write_load $load_handle
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set loglevel debug
    $primary config set repl-diskless-sync-delay 0

    # Generating RDB will cost 100 sec to generate
    $primary debug populate 100000 primary 1
    $primary config set rdb-key-save-delay 1000
    
    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]
        
        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10
        set load_handle [start_one_key_write_load $primary_host $primary_port 100 "mykey"]
        test "Replica recover rdb-connection killed" {
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }            

            $primary debug log "killing replica rdb connection"
            set replica_rdb_channel_id [get_client_id_by_last_cmd $primary "sync"]
            assert {$replica_rdb_channel_id != ""}
            set loglines [count_log_lines -1]
            $primary config set repl-diskless-sync-delay 5; # allow catch failed sync before retry
            $primary client kill id $replica_rdb_channel_id
            # Wait for primary to abort the sync
            wait_for_condition 50 1000 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            $primary config set repl-diskless-sync-delay 0
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10
            # Replica should retry
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't retry after connection close"
            }
        }
        $replica replicaof no one
        wait_for_condition 500 1000 {
            [s -1 rdb_bgsave_in_progress] eq 0
        } else {
            fail "Primary should abort sync"
        }
        test "Replica recover main-connection killed" {
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }            
            $primary debug log "killing replica main connection"
            set replica_main_conn_id [get_client_id_by_last_cmd $primary "psync"]
            assert {$replica_main_conn_id != ""}
            set loglines [count_log_lines -1]
            $primary config set repl-diskless-sync-delay 5; # allow catch failed sync before retry
            $primary client kill id $replica_main_conn_id
            # Wait for primary to abort the sync
            wait_for_condition 50 1000 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            $primary config set repl-diskless-sync-delay 0
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10
            # Replica should retry
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't retry after connection close"
            }    
        }
        stop_write_load $load_handle
    }
}


start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set repl-diskless-sync-delay 0

    # Generating RDB will take 100 sec to generate
    $primary debug populate 1000000 primary 1
    $primary config set rdb-key-save-delay -10
    
    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]
        
        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-diskless-load flush-before-load

        if {$::valgrind} {
            $primary config set repl-timeout 100
            $replica config set repl-timeout 100
            set max_tries 5000
        } else {
            $primary config set repl-timeout 10
            $replica config set repl-timeout 10
            set max_tries 500
        }

        test "Replica notice main-connection killed during rdb load callback" {; # https://github.com/valkey-io/valkey/issues/1152
            set loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }
            wait_for_log_messages 0 {"*Loading RDB produced by Valkey version*"} $loglines 1000 10
            $primary set key val
            set replica_main_conn_id [get_client_id_by_last_cmd $primary "psync"]
            $primary config set repl-diskless-sync-delay 5; # allow catch failed sync before retry
            $primary debug log "killing replica main connection $replica_main_conn_id"
            assert {$replica_main_conn_id != ""}
            set loglines [count_log_lines 0]
            $primary config set rdb-key-save-delay 0; # disable delay to allow next sync to succeed
            $primary client kill id $replica_main_conn_id
            # Wait for primary to abort the sync
            wait_for_condition 50 1000 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            wait_for_log_messages 0 {"*Failed trying to load the PRIMARY synchronization DB from socket*"} $loglines $max_tries 10
            verify_replica_online $primary 0 $max_tries
        }
    }
}
