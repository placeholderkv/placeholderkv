proc shutdown_how {srv_id how} {
    if {$how == "shutdown"} {
        catch {R $srv_id shutdown nosave}
    } elseif {$how == "sigterm"} {
        exec kill -SIGTERM [s -$srv_id process_id]
    }
}

# We will start a cluster with 3 primary nodes and 4 replicas, the primary 1 will have 2 replicas.
# We will pause the replica 1, and then shutdown the primary 1, and making replica 2 to become
# the new primary.
proc test_auto_failover {how shutdown_timeout} {
    test "auto-failover-on-shutdown will always pick a best replica and send CLUSTER FAILOVER - $how - shutdown-timeout: $shutdown_timeout" {
        set primary [srv 0 client]
        set replica1 [srv -3 client]
        set replica1_pid [s -3 process_id]
        set replica2 [srv -6 client]
        set replica2_ip [srv -6 host]
        set replica2_port [srv -6 port]

        $primary config set auto-failover-on-shutdown yes
        $primary config set shutdown-timeout $shutdown_timeout
        $primary config set repl-ping-replica-period 3600

        # To avoid failover kick in.
        $replica2 config set cluster-replica-no-failover yes

        # Pause a replica so it has no chance to catch up with the offset.
        pause_process $replica1_pid

        # Primary write some data to increase the offset.
        for {set i 0} {$i < 10} {incr i} {
            $primary incr key_991803
        }

        if {$shutdown_timeout == 0} {
            # Wait the replica2 catch up with the offset
            wait_for_ofs_sync $primary $replica2
            wait_replica_acked_ofs $primary $replica2 $replica2_ip $replica2_port
        } else {
            # If shutdown-timeout is enable, we expect the primary to pause writing
            # and wait for the replica to catch up with the offset.
        }

        # Shutdown the primary.
        shutdown_how 0 $how

        # Wait for the replica2 to become a primary.
        wait_for_condition 1000 50 {
            [s -6 role] eq {master}
        } else {
            puts "s -6 role: [s -6 role]"
            fail "Failover does not happened"
        }

        # Make sure that the expected logs are printed.
        verify_log_message -6 "*Forced failover primary request accepted*" 0

        resume_process $replica1_pid
    }

    test "Unable to find a replica to perform an auto failover - $how" {
        set primary [srv -6 client]
        set replica1 [srv -3 client]
        set replica1_pid [s -3 process_id]

        pause_process $replica1_pid

        $primary config set auto-failover-on-shutdown yes
        $primary client kill type replica
        shutdown_how 6 $how
        wait_for_log_messages -6 {"*Unable to find a replica to perform the auto failover on shutdown*"} 0 1000 10

        resume_process $replica1_pid
    }
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_auto_failover "shutdown" 0
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_auto_failover "sigterm" 0
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_auto_failover "shutdown" 10
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_auto_failover "sigterm" 10
}
