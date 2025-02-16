set old_singledb $::singledb
set ::singledb 1

tags {tls:skip external:skip cluster} {
    set base_conf [list cluster-enabled yes save ""] 
    start_multiple_servers 2 [list overrides $base_conf] {
        test "Cluster nodes are reachable" {
            for {set id 0} {$id < [llength $::servers]} {incr id} {
                # Every node should be reachable.
                wait_for_condition 1000 50 {
                    ([catch {R $id ping} ping_reply] == 0) &&
                    ($ping_reply eq {PONG})
                } else {
                    catch {R $id ping} err
                    fail "Node #$id keeps replying '$err' to PING."
                }
            }
        }

        test "Before slots allocation, all nodes report cluster failure" {
            wait_for_cluster_state fail
        }

        test "Cluster nodes haven't met each other" {
            assert {[llength [get_cluster_nodes 1]] == 1}
            assert {[llength [get_cluster_nodes 0]] == 1}
        }

        test "Allocate slots" {
            cluster_allocate_slots 1 1
        }

        test "Restart of node in cluster mode doesn't cause nodes.conf corruption due to shard id mismatch" {
            set primary_id [R 0 CLUSTER MYID]
            R 1 CLUSTER MEET 127.0.0.1 [srv 0 port]
            set pattern *$primary_id*
            wait_for_condition 100 50 {
                [string match $pattern [R 1 CLUSTER NODES]]
            } else {
                fail "Meet took longer"
            }
            R 1 CLUSTER REPLICATE $primary_id
            wait_replica_online [srv 0 client]
            set result [catch {R 0 DEBUG RESTART 0} err]

            if {$result != 0 && $err ne "I/O error reading reply"} {
                fail "Unexpected error restarting server: $err"
            }

             wait_for_condition 100 100 {
                [check_server_response 0] eq 1
            } else {
                fail "Server didn't come back online in time"
            }
        }
    }
}

set ::singledb $old_singledb
