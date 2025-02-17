# Test manual coordinated failover

source "../tests/includes/init-tests.tcl"

foreach_sentinel_id id {
    S $id sentinel debug info-period 2000
    S $id sentinel debug publish-period 1000
}

test "Sentinel is able to reconfigure a node that is stuck in failover state" {
    # Pick a replica
    foreach_valkey_id id {
        if {$id != $master_id} {
            set failover_to_port [RPort $id]
            set failover_to_pid [get_instance_attrib valkey $id pid]
        }
    }

    wait_for_condition 1000 50 {
        [string match "*flags master link-pending-commands*" [S 0 SENTINEL PRIMARY mymaster] ]
    } else {
        fail "Test"
    }

    pause_process $failover_to_pid

    R $master_id failover to 127.0.0.1 $failover_to_port TIMEOUT 10 FORCE

    wait_for_condition 1000 50 {
        [string match "no-failover" [RI $master_id master_failover_state]]
    } else {
        fail "Failover was not aborted by Sentinel"
    }
    resume_process $failover_to_pid
}

test "All the replicas now point to the new primary" {
    set old_master_id $master_id
    set addr [S 0 SENTINEL GET-PRIMARY-ADDR-BY-NAME mymaster]
    set master_id [get_instance_id_by_port valkey [lindex $addr 1]]

    assert {$old_master_id ne $master_id}

    foreach_valkey_id id {
        if {$id != $master_id} {
            wait_for_condition 1000 50 {
                [RI $id master_port] == [lindex $addr 1]
            } else {
                fail "Valkey ID $id not configured to replicate with new master"
            }
        }
    }
}
