# Tests multi-databases in cluster mode

proc pause {{message "Hit Enter to continue ==> "}} {
    puts -nonewline $message
    flush stdout
    gets stdin
}

source "../tests/includes/init-tests.tcl"


proc get_my_replica {cluster_nodes} {
    set my_node_id ""
    
    # Split the string into lines
    set lines [split $cluster_nodes "\n"]
    
    # Find "myself" node ID
    foreach line $lines {
        if {[string match {*myself,master*} $line]} {
            set my_node_id [lindex $line 0]
            break
        }
    }

    # Find the slave of "myself"
    if {$my_node_id != ""} {
        foreach line $lines {
            if {[string match {*slave*} $line]} {
                if {[lindex $line 3] == $my_node_id} {
                    set slave_info [split [lindex $line 1] ":@"]
                    set ip [lindex $slave_info 0]
                    set port [lindex $slave_info 1]
                    return [valkey_client_by_addr $ip $port]
                    
                }
            }
        }
    }
    assert_failed "No replica found!"
}


test "Create a 5 nodes cluster with replicas, all slots allocated to one node" {        
    cluster_allocate_slaves 5 5

    # Allocate all slots in one shard to allow easier testing with key-based commands
    cluster_allocate_with_continuous_slots 1
    # pause "\nPress any key: ";
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Key-based commands can be used on multiple databases" {
    set primary_id 0
    # Switching database from 0 to 9
    R $primary_id select 9    
    # Writing a key to database 9    
    set key "{1}key1"
    set val "hello"     
    R $primary_id set $key $val    
    assert_equal [R $primary_id get $key] $val    
    R $primary_id flushdb
}

test "Key-based commands across multiple databases" {
    set primary_id 0
    set keys_per_db 100

    # Set keys in all DBs
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "{$db}key$i"
            set val "hello_db${db}_key$i"
            R $primary_id set $key $val
            assert_equal [R $primary_id get $key] $val
        }
        assert_equal [R $primary_id dbsize] $keys_per_db
    }

    # Verify all values
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        assert_equal [R $primary_id dbsize] $keys_per_db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "{$db}key$i"
            set expected_val "hello_db${db}_key$i"
            assert_equal [R $primary_id get $key] $expected_val
        }
    }

    # Delete all keys and verify deletion
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "{$db}key$i"
            R $primary_id del $key
        }
        assert_equal [R $primary_id dbsize] 0
    }
    
}

test "Validate slot statistics using cluster countkeysinslot and cluster getkeysinslot" {
    set primary_id 0
    set keys_per_db 100

    # Set keys in all DBs with same hash slot
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "{x}key$i"
            set val "hello_db${db}_{x}key$i"
            R $primary_id set $key $val
            assert_equal [R $primary_id get $key] $val
        }
    }

    # Get a random key to determine the slot, which will be identical to all keys
    set random_key [R $primary_id randomkey]
    assert_not_equal $random_key ""
    
    set slot [R $primary_id cluster keyslot $random_key]
    assert_not_equal $slot ""

    # Validate slot key distribution in each database
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        set keys_in_slot [R $primary_id cluster countkeysinslot $slot]
        # Since all keys are mapped to a single slot, the number of keys in the currently selected db should match $keys_per_db
        assert_equal $keys_in_slot $keys_per_db 
    }

    # Verify key retrieval by slot for each database
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        set slot_keys [R $primary_id cluster getkeysinslot $slot $keys_per_db]
        assert_equal [llength $slot_keys] $keys_per_db
        foreach key $slot_keys {
            if {![regexp {^\{x\}key\d+$} $key]} {
                error "Key format mismatch: $key"
            }
            set expected_val "hello_db${db}_$key"            
            assert_equal [R $primary_id get $key] $expected_val
        }
    }

    # Delete all keys and verify slot emptiness
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "{x}key$i"
            R $primary_id del $key
        }
        assert_equal [R $primary_id dbsize] 0
    }

    # Ensure the slot is empty
    set remaining_keys [R $primary_id cluster countkeysinslot $slot]
    assert_equal $remaining_keys 0
}



test "Replication: Write to multiple databases and verify replica" {
    set primary_id 0    

    set replica [get_my_replica [R $primary_id cluster nodes]]    

    $replica READONLY
    

    set keys_per_db 50

    # Set keys in all DBs on the primary node
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "key$i"
            set val "primary_db${db}_key$i"
            R $primary_id set $key $val            
            assert_equal [R $primary_id get $key] $val
        }
        assert_equal [R $primary_id dbsize] $keys_per_db
    }


    # Wait for replication to catch up
    after 500

    # Verify data exists in the replica
    for {set db 0} {$db < 16} {incr db} {
        $replica select $db
        assert_equal [$replica dbsize] $keys_per_db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "key$i"
            set expected_val "primary_db${db}_key$i"
            assert_equal [$replica get $key] $expected_val
        }
    }

    # Delete all keys on the primary node
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "key$i"
            R $primary_id del $key
        }
        assert_equal [R $primary_id dbsize] 0
    }

    # Wait for deletion to replicate
    after 500

    # Ensure replica is also empty
    for {set db 0} {$db < 16} {incr db} {
        $replica select $db
        assert_equal [$replica dbsize] 0
    }
}


test "Replication: Swap and Flush Databases" {
    set primary_id 0    
    set replica [get_my_replica [R $primary_id cluster nodes]]    

    $replica READONLY

    # Create two databases and add keys
    R $primary_id select 1
    R $primary_id set key1 "value1_db1"
    R $primary_id set key2 "value2_db1"
    
    R $primary_id select 2
    R $primary_id set key1 "value1_db2"
    R $primary_id set key2 "value2_db2"
    
    # Wait for replication to catch up
    after 500
    
    # Verify keys exist in replica
    $replica select 1
    assert_equal [$replica get key1] "value1_db1"
    assert_equal [$replica get key2] "value2_db1"
    
    $replica select 2
    assert_equal [$replica get key1] "value1_db2"
    assert_equal [$replica get key2] "value2_db2"
    
    # Swap databases on primary
    R $primary_id swapdb 1 2
    
    # Wait for replication to catch up
    after 500
    
    # Verify swap is reflected in replica
    $replica select 1
    assert_equal [$replica get key1] "value1_db2"
    assert_equal [$replica get key2] "value2_db2"
    
    $replica select 2
    assert_equal [$replica get key1] "value1_db1"
    assert_equal [$replica get key2] "value2_db1"
    
    # Flush database on primary
    R $primary_id select 1
    R $primary_id flushdb
    
    R $primary_id select 2
    R $primary_id flushdb
    
    # Wait for replication to catch up
    after 500
    
    # Ensure databases are empty in replica
    $replica select 1
    assert_equal [$replica dbsize] 0
    
    $replica select 2
    assert_equal [$replica dbsize] 0
}

test "Cross-DB Expiry Handling" {
    set primary_id 0   
    set replica [get_my_replica [R $primary_id cluster nodes]]    
    $replica READONLY
    
    set key "key1"
    $replica select 1
    R $primary_id select 1
    R $primary_id set $key "value1"
    after 500 
    
    assert_equal [$replica exists $key] 1

    R $primary_id expire $key 1
    
    after 1500
    assert_equal [R $primary_id exists $key] 0
    assert_equal [$replica exists $key] 0
        
    
    R $primary_id flushall
}


test "Slot Migration With Multiple Databases" {
    set primary_id_src 0
    set primary_id_src_nodeid [R $primary_id_src CLUSTER MYID]
    set primary_id_target 1
    set primary_id_target_port [RPort $primary_id_target]
    set primary_id_target_nodeid [R $primary_id_target CLUSTER MYID]
    
    R $primary_id_src select 1
    R $primary_id_src set "{x}key1" "value1_db1"
    assert_equal [R $primary_id_src get "{x}key1"] "value1_db1"

    R $primary_id_src select 2
    R $primary_id_src set "{x}key2" "value2_db2"
    assert_equal [R $primary_id_src get "{x}key2"] "value2_db2"
    
    set slot [R $primary_id_src cluster keyslot "{x}key1"]

    
    R $primary_id_target cluster setslot $slot importing $primary_id_src_nodeid
    R $primary_id_src cluster setslot $slot migrating $primary_id_target_nodeid

    R $primary_id_src select 1
    R $primary_id_src migrate 127.0.0.1 $primary_id_target_port "{x}key1" 1 5000

    # If not all keys were migrated, the slot can not be migrated    
    set result [catch {assert_error [R $primary_id_src cluster setslot $slot node $primary_id_target_nodeid]} err]    
    assert_match "ERR*" $err        

    R $primary_id_src select 2    
    R $primary_id_src migrate 127.0.0.1 $primary_id_target_port "{x}key2" 2 5000
        
    R $primary_id_target cluster setslot $slot node $primary_id_target_nodeid
    R $primary_id_src cluster setslot $slot node $primary_id_target_nodeid
    
    R $primary_id_target select 1
    assert_equal [R $primary_id_target get "{x}key1"] "value1_db1"
    R $primary_id_target select 2
    assert_equal [R $primary_id_target get "{x}key2"] "value2_db2"
    
    R $primary_id_src flushall
    R $primary_id_target flushall
}


test "Persistence across restart with multiple databases" {
    set primary_id 0
    set keys_per_db 100

    # Set keys in all DBs
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "${db}key$i"
            set val "value_db${db}_key$i"
            R $primary_id set $key $val
            assert_equal [R $primary_id get $key] $val
        }
        assert_equal [R $primary_id dbsize] $keys_per_db
    }

    # Run BGSAVE to save the RDB
    R $primary_id save
    
    # Restart instance
    kill_instance valkey $primary_id
    restart_instance valkey $primary_id

    assert_cluster_state ok

    # Verify keys after restart
    for {set db 0} {$db < 16} {incr db} {
        R $primary_id select $db
        assert_equal [R $primary_id dbsize] $keys_per_db
        for {set i 0} {$i < $keys_per_db} {incr i} {
            set key "${db}key$i"
            set expected_val "value_db${db}_key$i"
            assert_equal [R $primary_id get $key] $expected_val
        }
    }
    R $primary_id_src flushall
    R $primary_id_target flushall
}

test "Copy key to other database" {
    set primary_id 0

    set key "{xyz}key"
    set key_copy "{xyz}key_copy"

    # Set key "xyz" in database 0 with a test value
    R $primary_id select 0
    R $primary_id set $key "test_value"
        
    # Use the COPY command to copy key "xyz" to a new key "xyz_copy" in database 15
    R $primary_id copy $key $key_copy DB 15

    # Verify that the copied key exists in database 15 with the correct value
    R $primary_id select 15
    assert_equal [R $primary_id get $key_copy] "test_value"

    # Optionally, verify that the original key still exists in database 0
    R $primary_id select 0
    assert_equal [R $primary_id get $key] "test_value"

    R $primary_id flushall
}

test "CLUSTER RESET should fail if databases contain keys" {
    set primary_id 0

    R $primary_id select 0
    R $primary_id flushall
    R $primary_id select 9
    R $primary_id set "key1" "test_value"

    # CLUSTER RESET should fail as the db isn't empty
    assert_error "ERR CLUSTER RESET can't be called with master nodes containing keys" {R $primary_id CLUSTER RESET}

    R $primary_id flushall
}

test "Move key to other database" {
    set primary_id 0

    set key "{xyz}key1"
    

    # Set key "xyz" in database 0 with a test value
    R $primary_id select 0
    R $primary_id set $key "test_value"
      
    R $primary_id move $key 15  

    # Verify that the copied key exists in database 15 with the correct value
    R $primary_id select 15
    assert_equal [R $primary_id get $key] "test_value"

    R $primary_id flushall
}

test "Flushslot with multiple databases" {
    set primary_id 0
    # Add a key in each of the first 4 databases (db0 to db3)
    for {set db 0} {$db < 4} {incr db} {
        R $primary_id select $db
        R $primary_id set "key${db}" "value${db}"
    }

    # Attempt to run CLUSTER FLUSHSLOTS and expect it to fail
    assert_error "ERR DB must be empty to perform CLUSTER FLUSHSLOTS." {R $primary_id CLUSTER FLUSHSLOTS}

    # Flush database 0 and try again; it should still fail since keys remain in db1, db2, and db3
    R $primary_id select 0
    R $primary_id flushdb
    
    # Attempt to run CLUSTER FLUSHSLOTS and expect it to fail
    assert_error "ERR DB must be empty to perform CLUSTER FLUSHSLOTS." {R $primary_id CLUSTER FLUSHSLOTS}
    
    for {set db 0} {$db < 4} {incr db} {
        R $primary_id select $db
        R $primary_id flushdb
    }
    
    # FLUSHSLOTS should not fail now 
    R $primary_id CLUSTER FLUSHSLOTS
 
}