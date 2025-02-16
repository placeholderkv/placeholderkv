proc get_open_slots {srv_idx} {
    set slots [dict get [cluster_get_myself $srv_idx] slots]
    if {[regexp {\[.*} $slots slots]} {
        set slots [regsub -all {[{}]} $slots ""]
        return $slots
    } else {
        return {}
    }
}

proc get_cluster_role {srv_idx} {
    set flags [dict get [cluster_get_myself $srv_idx] flags]
    set role [lindex $flags 1]
    return $role
}

proc get_myself_primary_flags {srv_idx} {
    set flags [dict get [cluster_get_myself_primary $srv_idx] flags]
    return $flags
}

proc get_myself_primary_linkstate {srv_idx} {
    set linkstate [dict get [cluster_get_myself_primary $srv_idx] linkstate]
    return $linkstate
}

proc wait_for_role {srv_idx role} {
    # Wait for the role, make sure the replication role matches.
    wait_for_condition 100 100 {
        [lindex [split [R $srv_idx ROLE] " "] 0] eq $role
    } else {
        puts "R $srv_idx ROLE: [R $srv_idx ROLE]"
        fail "R $srv_idx didn't assume the replication $role in time"
    }

    if {$role eq "slave"} {
        # Wait for the replication link, make sure the replication link is normal.
        wait_for_condition 100 100 {
            [s -$srv_idx master_link_status] eq "up"
        } else {
            puts "R $srv_idx INFO REPLICATION: [R $srv_idx INFO REPLICATION]"
            fail "R $srv_idx didn't assume the replication link in time"
        }
    }

    # Wait for the cluster role, make sure the cluster role matches.
    wait_for_condition 100 100 {
        [get_cluster_role $srv_idx] eq $role
    } else {
        puts "R $srv_idx CLUSTER NODES: [R $srv_idx CLUSTER NODES]"
        fail "R $srv_idx didn't assume the cluster $role in time"
    }

    if {$role eq "slave"} {
        # Wait for the flags, make sure the primary node is not failed.
        wait_for_condition 100 100 {
            [get_myself_primary_flags $srv_idx] eq "master"
        } else {
            puts "R $srv_idx CLUSTER NODES: [R $srv_idx CLUSTER NODES]"
            fail "R $srv_idx didn't assume the primary state in time"
        }

        # Wait for the cluster link, make sure that the cluster connection is normal.
        wait_for_condition 100 100 {
            [get_myself_primary_linkstate $srv_idx] eq "connected"
        } else {
            puts "R $srv_idx CLUSTER NODES: [R $srv_idx CLUSTER NODES]"
            fail "R $srv_idx didn't assume the cluster primary link in time"
        }
    }

    wait_for_cluster_propagation
}

proc wait_for_slot_state {srv_idx pattern} {
    wait_for_condition 100 100 {
        [get_open_slots $srv_idx] eq $pattern
    } else {
        fail "incorrect slot state on R $srv_idx: expected $pattern; got [get_open_slots $srv_idx]"
    }
}

# restart a server and wait for it to come back online
proc fail_server {server_id} {
    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    pause_process [srv [expr -1*$server_id] pid]
    after [expr 3*$node_timeout]
    resume_process [srv [expr -1*$server_id] pid]
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {

    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set R0_id [R 0 CLUSTER MYID]
    set R1_id [R 1 CLUSTER MYID]
    set R2_id [R 2 CLUSTER MYID]
    set R3_id [R 3 CLUSTER MYID]
    set R4_id [R 4 CLUSTER MYID]
    set R5_id [R 5 CLUSTER MYID]
    R 0 SET "{aga}2" banana

    test "Slot migration states are replicated" {
        # Validate initial states
        assert_not_equal [get_open_slots 0] "\[609->-$R1_id\]"
        assert_not_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        assert_not_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_not_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        # Kick off the migration of slot 609 from R0 to R1
        assert_equal {OK} [R 0 CLUSTER SETSLOT 609 MIGRATING $R1_id]
        assert_equal {OK} [R 1 CLUSTER SETSLOT 609 IMPORTING $R0_id]
        # Validate that R0 is migrating slot 609 to R1
        assert_equal [get_open_slots 0] "\[609->-$R1_id\]"
        # Validate that R1 is importing slot 609 from R0 
        assert_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
    }

    test "Migration target is auto-updated after failover in target shard" {
        # Trigger an auto-failover from R1 to R4
        fail_server 1
        # Wait for R1 to become a replica
        wait_for_role 1 slave
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R4_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R4_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
        # Restore R1's primaryship
        assert_equal {OK} [R 1 cluster failover]
        wait_for_role 1 master
        # Validate initial states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
    }

    test "Migration source is auto-updated after failover in source shard" {
        # Trigger an auto-failover from R0 to R3
        fail_server 0
        # Wait for R0 to become a replica
        wait_for_role 0 slave
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R3_id\]"
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 4 "\[609-<-$R3_id\]"
        # Restore R0's primaryship
        assert_equal {OK} [R 0 cluster failover]
        wait_for_role 0 master
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
    }

    test "Replica redirects key access in migrating slots" {
        # Validate initial states
        assert_equal [get_open_slots 0] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        assert_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        catch {[R 3 get aga]} e
        set port0 [srv 0 port]
        assert_equal "MOVED 609 127.0.0.1:$port0" $e
    }

    test "Replica of migrating node returns ASK redirect after READONLY" {
        # Validate initial states
        assert_equal [get_open_slots 0] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        assert_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        # Read missing key in readonly replica in migrating state.
        assert_equal OK [R 3 READONLY]
        set port1 [srv -1 port]
        catch {[R 3 get aga]} e
        assert_equal "ASK 609 127.0.0.1:$port1" $e
        assert_equal OK [R 3 READWRITE]
    }

    test "Replica of migrating node returns TRYAGAIN after READONLY" {
        # Validate initial states
        assert_equal [get_open_slots 0] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        assert_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        # Read some existing and some missing keys in readonly replica in
        # migrating state results in TRYAGAIN, just like its primary would do.
        assert_equal OK [R 3 READONLY]
        catch {[R 3 mget "{aga}1" "{aga}2"]} e
        assert_match "TRYAGAIN *" $e
        assert_equal OK [R 3 READWRITE]
    }

    test "Replica of importing node returns TRYAGAIN after READONLY and ASKING" {
        # Validate initial states
        assert_equal [get_open_slots 0] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        assert_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        # A client follows an ASK redirect to a primary, but wants to read from a replica.
        # The replica returns TRYAGAIN just like a primary would do for two missing keys.
        assert_equal OK [R 4 READONLY]
        assert_equal OK [R 4 ASKING]
        catch {R 4 MGET "{aga}1" "{aga}2"} e
        assert_match "TRYAGAIN *" $e
        assert_equal OK [R 4 READWRITE]
    }

    test "New replica inherits migrating slot" {
        # Reset R3 to turn it into an empty node
        assert_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_equal {OK} [R 3 CLUSTER RESET]
        assert_not_equal [get_open_slots 3] "\[609->-$R1_id\]"
        # Add R3 back as a replica of R0
        assert_equal {OK} [R 3 CLUSTER MEET [srv 0 "host"] [srv 0 "port"]]
        wait_for_role 0 master
        assert_equal {OK} [R 3 CLUSTER REPLICATE $R0_id]
        wait_for_role 3 slave
        # Validate that R3 now sees slot 609 open
        wait_for_slot_state 3 "\[609->-$R1_id\]"
    }

    test "New replica inherits importing slot" {
        # Reset R4 to turn it into an empty node
        assert_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        assert_equal {OK} [R 4 CLUSTER RESET]
        assert_not_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        # Add R4 back as a replica of R1
        assert_equal {OK} [R 4 CLUSTER MEET [srv -1 "host"] [srv -1 "port"]]
        wait_for_role 1 master
        assert_equal {OK} [R 4 CLUSTER REPLICATE $R1_id]
        wait_for_role 4 slave
        # Validate that R4 now sees slot 609 open
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
    }
}

proc create_empty_shard {p r} {
    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    assert_equal {OK} [R $p CLUSTER RESET]
    assert_equal {OK} [R $r CLUSTER RESET]
    assert_equal {OK} [R $p CLUSTER MEET [srv 0 "host"] [srv 0 "port"]]
    assert_equal {OK} [R $r CLUSTER MEET [srv 0 "host"] [srv 0 "port"]]
    wait_for_role $p master
    assert_equal {OK} [R $r CLUSTER REPLICATE [R $p CLUSTER MYID]]
    wait_for_role $r slave
    wait_for_role $p master
}

# Temporarily disable empty shard migration tests while we
# work to reduce their flakiness. See https://github.com/valkey-io/valkey/issues/858.
if {0} {
start_cluster 3 5 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {

    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set R0_id [R 0 CLUSTER MYID]
    set R1_id [R 1 CLUSTER MYID]
    set R2_id [R 2 CLUSTER MYID]
    set R3_id [R 3 CLUSTER MYID]
    set R4_id [R 4 CLUSTER MYID]
    set R5_id [R 5 CLUSTER MYID]

    create_empty_shard 6 7
    set R6_id [R 6 CLUSTER MYID]
    set R7_id [R 7 CLUSTER MYID]

    test "Empty-shard migration replicates slot importing states" {
        # Validate initial states
        assert_not_equal [get_open_slots 0] "\[609->-$R6_id\]"
        assert_not_equal [get_open_slots 6] "\[609-<-$R0_id\]"
        assert_not_equal [get_open_slots 3] "\[609->-$R6_id\]"
        assert_not_equal [get_open_slots 7] "\[609-<-$R0_id\]"
        # Kick off the migration of slot 609 from R0 to R6
        assert_equal {OK} [R 0 CLUSTER SETSLOT 609 MIGRATING $R6_id]
        assert_equal {OK} [R 6 CLUSTER SETSLOT 609 IMPORTING $R0_id]
        # Validate that R0 is migrating slot 609 to R6
        assert_equal [get_open_slots 0] "\[609->-$R6_id\]"
        # Validate that R6 is importing slot 609 from R0 
        assert_equal [get_open_slots 6] "\[609-<-$R0_id\]"
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R6_id\]"
        wait_for_slot_state 6 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R6_id\]"
        wait_for_slot_state 7 "\[609-<-$R0_id\]"
    }

    test "Empty-shard migration target is auto-updated after failover in target shard" {
        wait_for_role 6 master
        # Trigger an auto-failover from R6 to R7
        fail_server 6
        # Wait for R6 to become a replica
        wait_for_role 6 slave
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R7_id\]"
        wait_for_slot_state 6 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R7_id\]"
        wait_for_slot_state 7 "\[609-<-$R0_id\]"
        # Restore R6's primaryship
        assert_equal {OK} [R 6 cluster failover]
        wait_for_role 6 master
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R6_id\]"
        wait_for_slot_state 6 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R6_id\]"
        wait_for_slot_state 7 "\[609-<-$R0_id\]"
    }

    test "Empty-shard migration source is auto-updated after failover in source shard" {
        wait_for_role 0 master
        # Trigger an auto-failover from R0 to R3
        fail_server 0
        # Wait for R0 to become a replica
        wait_for_role 0 slave
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R6_id\]"
        wait_for_slot_state 6 "\[609-<-$R3_id\]"
        wait_for_slot_state 3 "\[609->-$R6_id\]"
        wait_for_slot_state 7 "\[609-<-$R3_id\]"
        # Restore R0's primaryship
        assert_equal {OK} [R 0 cluster failover]
        wait_for_role 0 master
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R6_id\]"
        wait_for_slot_state 6 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R6_id\]"
        wait_for_slot_state 7 "\[609-<-$R0_id\]"
    }
}
}

proc migrate_slot {from to slot} {
    set from_id [R $from CLUSTER MYID]
    set to_id [R $to CLUSTER MYID]
    assert_equal {OK} [R $from CLUSTER SETSLOT $slot MIGRATING $to_id]
    assert_equal {OK} [R $to CLUSTER SETSLOT $slot IMPORTING $from_id]
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {

    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set R0_id [R 0 CLUSTER MYID]
    set R1_id [R 1 CLUSTER MYID]
    set R2_id [R 2 CLUSTER MYID]
    set R3_id [R 3 CLUSTER MYID]
    set R4_id [R 4 CLUSTER MYID]
    set R5_id [R 5 CLUSTER MYID]

    test "Multiple slot migration states are replicated" {
        migrate_slot 0 1 13
        migrate_slot 0 1 7
        migrate_slot 0 1 17
        # Validate final states
        wait_for_slot_state 0 "\[7->-$R1_id\] \[13->-$R1_id\] \[17->-$R1_id\]"
        wait_for_slot_state 1 "\[7-<-$R0_id\] \[13-<-$R0_id\] \[17-<-$R0_id\]"
        wait_for_slot_state 3 "\[7->-$R1_id\] \[13->-$R1_id\] \[17->-$R1_id\]"
        wait_for_slot_state 4 "\[7-<-$R0_id\] \[13-<-$R0_id\] \[17-<-$R0_id\]"
    }

    test "New replica inherits multiple migrating slots" {
        # Reset R3 to turn it into an empty node
        assert_equal {OK} [R 3 CLUSTER RESET]
        # Add R3 back as a replica of R0
        assert_equal {OK} [R 3 CLUSTER MEET [srv 0 "host"] [srv 0 "port"]]
        wait_for_role 0 master
        assert_equal {OK} [R 3 CLUSTER REPLICATE $R0_id]
        wait_for_role 3 slave
        # Validate final states
        wait_for_slot_state 3 "\[7->-$R1_id\] \[13->-$R1_id\] \[17->-$R1_id\]"
    }

    test "Slot finalization succeeds on both primary and replicas" {
        assert_equal {OK} [R 1 CLUSTER SETSLOT 7 NODE $R1_id]
        wait_for_slot_state 1 "\[13-<-$R0_id\] \[17-<-$R0_id\]"
        wait_for_slot_state 4 "\[13-<-$R0_id\] \[17-<-$R0_id\]"
        assert_equal {OK} [R 1 CLUSTER SETSLOT 13 NODE $R1_id]
        wait_for_slot_state 1 "\[17-<-$R0_id\]"
        wait_for_slot_state 4 "\[17-<-$R0_id\]"
        assert_equal {OK} [R 1 CLUSTER SETSLOT 17 NODE $R1_id]
        wait_for_slot_state 1 ""
        wait_for_slot_state 4 ""
    }

}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {

    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set R0_id [R 0 CLUSTER MYID]
    set R1_id [R 1 CLUSTER MYID]

    test "Slot is auto-claimed by target after source relinquishes ownership" {
        migrate_slot 0 1 609
        #Validate that R1 doesn't own slot 609
        catch {[R 1 get aga]} e
        assert_equal {MOVED} [lindex [split $e] 0]
        #Finalize the slot on the source first
        assert_equal {OK} [R 0 CLUSTER SETSLOT 609 NODE $R1_id]
        after $node_timeout
        #R1 should claim slot 609 since it is still importing slot 609
        #from R0 but R0 no longer owns this slot
        assert_equal {OK} [R 1 set aga foo]
    }
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {
    set R1_id [R 1 CLUSTER MYID]

    test "CLUSTER SETSLOT with invalid timeouts" {
        catch {R 0 CLUSTER SETSLOT 609 MIGRATING $R1_id TIMEOUT} e
        assert_equal $e "ERR Missing timeout value"

        catch {R 0 CLUSTER SETSLOT 609 MIGRATING $R1_id TIMEOUT -1} e
        assert_equal $e "ERR timeout is negative"

        catch {R 0 CLUSTER SETSLOT 609 MIGRATING $R1_id TIMEOUT 99999999999999999999} e
        assert_equal $e "ERR timeout is not an integer or out of range"

        catch {R 0 CLUSTER SETSLOT 609 MIGRATING $R1_id TIMEOUT abc} e
        assert_equal $e "ERR timeout is not an integer or out of range"

        catch {R 0 CLUSTER SETSLOT 609 TIMEOUT 100 MIGRATING $R1_id} e
        assert_equal $e "ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP"
    }
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {
    set R1_id [R 1 CLUSTER MYID]

    test "CLUSTER SETSLOT with an explicit timeout" {
        # Pause the replica to simulate a failure
        pause_process [srv -3 pid]

        # Setslot with an explicit 1ms timeoout
        set start_time [clock milliseconds]
        catch {R 0 CLUSTER SETSLOT 609 MIGRATING $R1_id TIMEOUT 3000} e
        set end_time [clock milliseconds]
        set duration [expr {$end_time - $start_time}]

        # Assert that the execution time is greater than the default 2s timeout
        assert {$duration > 2000}

        # Setslot should fail with not enough good replicas to write after the timeout
        assert_equal {NOREPLICAS Not enough good replicas to write.} $e

        resume_process [srv -3 pid]
    }
}

start_cluster 2 0 {tags {tls:skip external:skip cluster regression} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {
    # Issue #563 regression test
    test "Client blocked on XREADGROUP while stream's slot is migrated" {
        set stream_name aga
        set slot 609

        # Start a deferring client to simulate a blocked client on XREADGROUP
        R 0 XGROUP CREATE $stream_name mygroup $ MKSTREAM
        set rd [valkey_deferring_client]
        $rd xreadgroup GROUP mygroup consumer BLOCK 0 streams $stream_name >
        wait_for_blocked_client

        # Migrate the slot to the target node
        R 0 CLUSTER SETSLOT $slot MIGRATING [dict get [cluster_get_myself 1] id]
        R 1 CLUSTER SETSLOT $slot IMPORTING [dict get [cluster_get_myself 0] id]

        # This line should cause the crash
        R 0 MIGRATE 127.0.0.1 [lindex [R 1 CONFIG GET port] 1] $stream_name 0 5000
    }
}

start_cluster 3 6 {tags {external:skip cluster} overrides {cluster-node-timeout 1000} } {
    test "Slot migration is ok when the replicas are down" {
        # Killing all replicas in primary 0.
        assert_equal 2 [s 0 connected_slaves]
        catch {R 3 shutdown nosave}
        catch {R 6 shutdown nosave}
        wait_for_condition 50 100 {
            [s 0 connected_slaves] == 0
        } else {
            fail "The replicas in primary 0 are still connecting"
        }

        # Killing one replica in primary 1.
        assert_equal 2 [s -1 connected_slaves]
        catch {R 4 shutdown nosave}
        wait_for_condition 50 100 {
            [s -1 connected_slaves] == 1
        } else {
            fail "The replica in primary 1 is still connecting"
        }

        # Check slot migration is ok when the replicas are down.
        migrate_slot 0 1 0
        migrate_slot 0 2 1
        assert_equal {OK} [R 0 CLUSTER SETSLOT 0 NODE [R 1 CLUSTER MYID]]
        assert_equal {OK} [R 0 CLUSTER SETSLOT 1 NODE [R 2 CLUSTER MYID]]
        wait_for_slot_state 0 ""
        wait_for_slot_state 1 ""
        wait_for_slot_state 2 ""
    }
}
