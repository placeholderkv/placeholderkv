# Test PUBLISH propagation across the cluster.

start_cluster 5 5 {tags {external:skip cluster}} {

proc test_cluster_publish {instance instances} {
    # Subscribe all the instances but the one we use to send.
    for {set j 0} {$j < $instances} {incr j} {
        if {$j != $instance} {
            R $j deferred 1
            R $j subscribe testchannel
            R $j read; # Read the subscribe reply
        }
    }

    set data [randomValue]
    R $instance PUBLISH testchannel $data

    # Read the message back from all the nodes.
    for {set j 0} {$j < $instances} {incr j} {
        if {$j != $instance} {
            set msg [R $j read]
            assert {$data eq [lindex $msg 2]}
            R $j unsubscribe testchannel
            R $j read; # Read the unsubscribe reply
            R $j deferred 0
        }
    }
}

test "Test publishing to master" {
    test_cluster_publish 0 10
}

test "Test publishing to slave" {
    test_cluster_publish 5 10
}
} ;# start_cluster

start_cluster 3 0 {tags {external:skip cluster}} {
    test "Test cluster info stats for publish" {
        R 0 PUBLISH hello world
        assert_equal 2 [CI 0 cluster_stats_messages_publish_sent]
        wait_for_condition 50 100 {
            [CI 1 cluster_stats_messages_publish_received] eq 1 &&
            [CI 2 cluster_stats_messages_publish_received] eq 1
        } else {
            fail "node 2 or node 3 didn't receive clusterbus publish packet"
        }
    }
}
