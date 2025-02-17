# Test manual coordinated failover

source "../tests/includes/init-tests.tcl"

foreach_sentinel_id id {
    S $id sentinel debug info-period 2000
    S $id sentinel debug publish-period 1000
}

test "Manual coordinated failover works" {
    set old_port [RPort $master_id]
    set addr [S 0 SENTINEL GET-PRIMARY-ADDR-BY-NAME mymaster]
    assert {[lindex $addr 1] == $old_port}

    set rd [valkey_client valkey $master_id]
    $rd reconnect 0
    $rd DEL FOO

    # Since we reduced the info-period (default 10000) above immediately,
    # sentinel - replica may not have enough time to exchange INFO and update
    # the replica's info-period, so the test may get a NOGOODSLAVE.
    wait_for_condition 300 50 {
        [catch {S 0 SENTINEL FAILOVER mymaster COORDINATED}] == 0
    } else {
        catch {S 0 SENTINEL FAILOVER mymaster COORDINATED} reply
        puts [S 0 SENTINEL REPLICAS mymaster]
        fail "Sentinel manual failover did not work, got: $reply"
    }

    # After sending SENTINEL FAILOVER, continue writing to the primary
    # to observe the final data consistency. We must get disconnected
    # when the failover finishes (and we must see no error).
    for {set j 1} {$j < 1000000} {incr j} {
        catch {$rd INCR FOO} reply
        if {[string match "*I/O error*" $reply]} {
            break
        }
        assert_equal $j $reply
        set val $reply
    }

    foreach_sentinel_id id {
        wait_for_condition 1000 50 {
            [lindex [S $id SENTINEL GET-PRIMARY-ADDR-BY-NAME mymaster] 1] != $old_port
        } else {
            fail "At least one Sentinel did not receive failover info"
        }
    }
    set addr [S 0 SENTINEL GET-PRIMARY-ADDR-BY-NAME mymaster]
    set master_id [get_instance_id_by_port valkey [lindex $addr 1]]
}

test "New primary [join $addr {:}] role matches" {
    assert {[RI $master_id role] eq {master}}
}

test "The old primary is already reconfigured as a replica" {
    assert {[RI 0 master_port] == [lindex $addr 1]}
}

test "No blocked clients (by sentinel hello PUBLISH commands) on the old primary" {
    assert {[RI 0 blocked_clients] eq "0"}
}

test "All the other replicas now point to the new primary" {
    foreach_valkey_id id {
        if {$id != $master_id && $id != 0} {
            wait_for_condition 1000 50 {
                [RI $id master_port] == [lindex $addr 1]
            } else {
                fail "Valkey ID $id not configured to replicate with new master"
            }
        }
    }
}

test "Check data consistency" {
    # New primary must be synced already
    assert_equal $val [R $master_id GET FOO]
    # Replicas will get the value eventually
    foreach_valkey_id id {
        wait_for_condition 100 50 {
            [R $id GET FOO] == $val
        } else {
            fail "Data is not consistent"
        }
    }
}

test "Failover fails and times out" {
    set addr [S 0 SENTINEL GET-PRIMARY-ADDR-BY-NAME mymaster]
    set master_id [get_instance_id_by_port valkey [lindex $addr 1]]
    foreach_valkey_id id {
        # Do not accept PSYNC (used during FAILOVER) on any replica
        if {$id != $master_id} {
            R $id ACL SETUSER default -psync
        }
    }
    wait_for_condition 300 50 {
        [catch {S 0 SENTINEL FAILOVER mymaster COORDINATED}] == 0
    } else {
        catch {S 0 SENTINEL FAILOVER mymaster COORDINATED} reply
        fail "Sentinel manual failover did not work, got: $reply"
    }

    catch {S 0 SENTINEL FAILOVER mymaster COORDINATED} reply
    assert_match {*INPROG*} $reply ;# Failover already in progress

    wait_for_condition 300 50 {
        [string match "*failover_in_progress*" [dict get [S 0 SENTINEL PRIMARY mymaster] flags]] == 0
    } else {
        fail "Failover did not timeout"
    }
    foreach_valkey_id id {
        # Re-enable psync
        R $id ACL SETUSER default +psync
    }
}

test "No change after failed failover: primary [join $addr {:}] role matches" {
    assert {[RI $master_id role] eq {master}}
}

test "No change after failed failover: All the other replicas still point to the primary" {
    foreach_valkey_id id {
        if {$id != $master_id} {
            assert {[RI $id master_port] == [lindex $addr 1]}
        }
    }
}

test "No change after failed failover: All sentinels agree on primary" {
    foreach_sentinel_id id {
        assert {[lindex [S $id SENTINEL GET-PRIMARY-ADDR-BY-NAME mymaster] 1] == [lindex $addr 1]}
    }
}

foreach flag {crash-after-election crash-after-promotion} {
    # Before each SIMULATE-FAILURE test, re-source init-tests to get a clean environment
    source "../tests/includes/init-tests.tcl"

    test "SENTINEL SIMULATE-FAILURE $flag works" {
        assert_equal {OK} [S 0 SENTINEL SIMULATE-FAILURE $flag]

        # Trigger a failover, failover will trigger leader election, replica promotion
        # Sentinel may enter failover and exit before the command, catch it and allow it
        wait_for_condition 300 50 {
            [catch {S 0 SENTINEL FAILOVER mymaster COORDINATED}] == 0
            ||
            ([catch {S 0 SENTINEL FAILOVER mymaster COORDINATED} reply] == 1 &&
            [string match {*couldn't open socket: connection refused*} $reply])
        } else {
            catch {S 0 SENTINEL FAILOVER mymaster COORDINATED} reply
            fail "Sentinel manual failover did not work, got: $reply"
        }

        # Wait for sentinel to exit (due to simulate-failure flags)
        wait_for_condition 1000 50 {
            [catch {S 0 PING}] == 1
        } else {
            fail "Sentinel set $flag but did not exit"
        }
        assert_error {*couldn't open socket: connection refused*} {S 0 PING}

        restart_instance sentinel 0
    }
}
