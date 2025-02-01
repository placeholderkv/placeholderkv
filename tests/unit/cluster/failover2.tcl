# Check the basic monitoring and failover capabilities.

start_cluster 3 4 {tags {external:skip cluster} overrides {cluster-ping-interval 1000 cluster-node-timeout 5000}} {

    test "Cluster is up" {
        wait_for_cluster_state ok
    }

    test "Cluster is writable" {
        cluster_write_test [srv 0 port]
    }

    set paused_pid [srv 0 pid]
    test "Killing one primary node" {
        pause_process $paused_pid
    }

    test "Wait for failover" {
        wait_for_condition 1000 50 {
            [s -3 role] == "master" || [s -6 role] == "master"
        } else {
            fail "No failover detected"
        }
    }

    test "Killing the new primary node" {
        if {[s -3 role] == "master"} {
            set replica_to_be_primary -6
            set paused_pid2 [srv -3 pid]
        } else {
            set replica_to_be_primary -3
            set paused_pid2 [srv -6 pid]
        }
        pause_process $paused_pid2
    }

    test "Cluster should eventually be up again" {
        for {set j 0} {$j < [llength $::servers]} {incr j} {
            if {[process_is_paused [srv -$j pid]]} continue
            wait_for_condition 1000 50 {
                [CI $j cluster_state] eq "ok"
            } else {
                fail "Cluster node $j cluster_state:[CI $j cluster_state]"
            }
        }
    }

    test "wait for new failover" {
        wait_for_condition 1000 50 {
            [s $replica_to_be_primary role] == "master"
        } else {
            fail "No failover detected"
        }
    }

    test "Restarting the previously killed primary nodes" {
        resume_process $paused_pid
        resume_process $paused_pid2
    }

    test "Make sure there is no failover timeout" {
        verify_no_log_message -3 "*Failover attempt expired*" 0
        verify_no_log_message -6 "*Failover attempt expired*" 0
    }
} ;# start_cluster

start_cluster 7 3 {tags {external:skip cluster} overrides {cluster-ping-interval 1000 cluster-node-timeout 5000}} {
    test "Primaries will not time out then they are elected in the same epoch" {
        # Since we have the delay time, so these node may not initiate the
        # election at the same time (same epoch). But if they do, we make
        # sure there is no failover timeout.

        # Killing there primary nodes.
        pause_process [srv 0 pid]
        pause_process [srv -1 pid]
        pause_process [srv -2 pid]

        # Wait for the failover
        wait_for_condition 1000 50 {
            [s -7 role] == "master" &&
            [s -8 role] == "master" &&
            [s -9 role] == "master"
        } else {
            fail "No failover detected"
        }

        # Make sure there is no false epoch 0.
        verify_no_log_message -7 "*Failover election in progress for epoch 0*" 0
        verify_no_log_message -8 "*Failover election in progress for epoch 0*" 0
        verify_no_log_message -9 "*Failover election in progress for epoch 0*" 0

        # Make sure there is no failover timeout.
        verify_no_log_message -7 "*Failover attempt expired*" 0
        verify_no_log_message -8 "*Failover attempt expired*" 0
        verify_no_log_message -9 "*Failover attempt expired*" 0

        # Resuming these primary nodes, speed up the shutdown.
        resume_process [srv 0 pid]
        resume_process [srv -1 pid]
        resume_process [srv -2 pid]
    }
} ;# start_cluster

run_solo {cluster} {
    start_cluster 32 15 {tags {external:skip cluster} overrides {cluster-ping-interval 1000 cluster-node-timeout 15000}} {
        test "Multiple primary nodes are down, rank them based on the failed primary" {
            # Killing these primary nodes.
            for {set j 0} {$j < 15} {incr j} {
                pause_process [srv -$j pid]
            }

            # Make sure that a node starts failover.
            wait_for_condition 1000 100 {
                [s -40 role] == "master"
            } else {
                fail "No failover detected"
            }

            # Wait for the cluster state to become ok.
            for {set j 0} {$j < [llength $::servers]} {incr j} {
                if {[process_is_paused [srv -$j pid]]} continue
                wait_for_condition 1000 100 {
                    [CI $j cluster_state] eq "ok"
                } else {
                    fail "Cluster node $j cluster_state:[CI $j cluster_state]"
                }
            }

            # Resuming these primary nodes, speed up the shutdown.
            for {set j 0} {$j < 15} {incr j} {
                resume_process [srv -$j pid]
            }
        }
    } ;# start_cluster
} ;# run_solo
