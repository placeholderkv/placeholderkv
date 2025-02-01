start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-replica-no-failover yes}} {
    test "NOADDR nodes will be marked as FAIL" {
        set primary0_id [R 0 CLUSTER MYID]

        # R 0 is a primary, after doing a CLUSTER RESET, the node name will be modified,
        # and other nodes will set it to NOADDR and FAIL.
        R 0 cluster reset hard
        wait_for_log_messages -1 {"*PONG contains mismatching sender ID*"} 0 1000 10
        wait_for_log_messages -2 {"*PONG contains mismatching sender ID*"} 0 1000 10
        wait_for_condition 1000 10 {
            [cluster_has_flag [cluster_get_node_by_id 1 $primary0_id] noaddr] eq 1 &&
            [cluster_has_flag [cluster_get_node_by_id 1 $primary0_id] fail] eq 1 &&
            [cluster_has_flag [cluster_get_node_by_id 2 $primary0_id] noaddr] eq 1 &&
            [cluster_has_flag [cluster_get_node_by_id 2 $primary0_id] fail] eq 1
        } else {
            fail "The node is not marked with the correct flag"
        }

        # Also we will set the node to FAIL, so the cluster will eventually be down.
        wait_for_condition 1000 50 {
            [CI 1 cluster_state] eq {fail} &&
            [CI 2 cluster_state] eq {fail} &&
            [CI 3 cluster_state] eq {fail} &&
            [CI 4 cluster_state] eq {fail} &&
            [CI 5 cluster_state] eq {fail}
        } else {
            fail "Cluster doesn't fail"
        }

        # key_977613 belong to slot 0 and belong to R 0.
        # Make sure we get a CLUSTER DOWN instead of an invalid MOVED.
        assert_error {CLUSTERDOWN*} {R 1 set key_977613 bar}

        # Let the replica 3 do the failover.
        R 3 config set cluster-replica-validity-factor 0
        R 3 config set cluster-replica-no-failover no
        wait_for_condition 1000 50 {
            [CI 1 cluster_state] eq {ok} &&
            [CI 2 cluster_state] eq {ok} &&
            [CI 3 cluster_state] eq {ok} &&
            [CI 4 cluster_state] eq {ok} &&
            [CI 5 cluster_state] eq {ok}
        } else {
            fail "Cluster doesn't stabilize"
        }
    }
} ;# start_cluster
