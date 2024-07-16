# Integration tests for CLUSTER SLOT-STATS command.

# -----------------------------------------------------------------------------
# Helper functions for CLUSTER SLOT-STATS test cases.
# -----------------------------------------------------------------------------

# Converts array RESP response into a dict.
# This is useful for many test cases, where unnecessary nesting is removed.
proc convert_array_into_dict {slot_stats} {
    set res [dict create]
    foreach slot_stat $slot_stats {
        # slot_stat is an array of size 2, where 0th index represents (int) slot, 
        # and 1st index represents (map) usage statistics.
        dict set res [lindex $slot_stat 0] [lindex $slot_stat 1]
    }
    return $res
}

proc initialize_expected_slots_dict {} {
    set expected_slots [dict create]
    for {set i 0} {$i < 16384} {incr i 1} {
        dict set expected_slots $i 0
    }
    return $expected_slots
}

proc initialize_expected_slots_dict_with_range {start_slot end_slot} {
    assert {$start_slot <= $end_slot}
    set expected_slots [dict create]
    for {set i $start_slot} {$i <= $end_slot} {incr i 1} {
        dict set expected_slots $i 0
    }
    return $expected_slots
}

proc assert_empty_slot_stats {slot_stats} {
    set slot_stats [convert_array_into_dict $slot_stats]
    dict for {slot stats} $slot_stats {
        assert {[dict get $stats key-count] == 0}
    }
}

proc assert_empty_slot_stats_with_exception {slot_stats exception_slots metrics_to_assert} {
    set slot_stats [convert_array_into_dict $slot_stats]
    dict for {slot stats} $slot_stats {
        if {[dict exists $exception_slots $slot]} {
            foreach metric_name $metrics_to_assert {
                set metric_value [dict get $exception_slots $slot $metric_name]
                assert {[dict get $stats $metric_name] == $metric_value}
            }
        } else {
            dict for {metric value} $stats {
                assert {$value == 0}
            }
        }
    }
}

proc assert_equal_slot_stats {slot_stats_1 slot_stats_2 metrics_to_assert} {
    set slot_stats_1 [convert_array_into_dict $slot_stats_1]
    set slot_stats_2 [convert_array_into_dict $slot_stats_2]
    assert {[dict size $slot_stats_1] == [dict size $slot_stats_2]}

    dict for {slot stats_1} $slot_stats_1 {
        assert {[dict exists $slot_stats_2 $slot]}
        set stats_2 [dict get $slot_stats_2 $slot]
        foreach metric_name $metrics_to_assert {
            assert {[dict get $stats_1 $metric_name] == [dict get $stats_2 $metric_name]}
        }
    }
}

proc assert_all_slots_have_been_seen {expected_slots} {
    dict for {k v} $expected_slots {
        assert {$v == 1}
    }
}

proc assert_slot_visibility {slot_stats expected_slots} {
    set slot_stats [convert_array_into_dict $slot_stats]
    dict for {slot _} $slot_stats {
        assert {[dict exists $expected_slots $slot]}
        dict set expected_slots $slot 1
    }

    assert_all_slots_have_been_seen $expected_slots
}

proc assert_slot_stats_key_count {slot_stats expected_slots_key_count} {
    set slot_stats [convert_array_into_dict $slot_stats]
    dict for {slot stats} $slot_stats {
        if {[dict exists $expected_slots_key_count $slot]} {
            set key_count [dict get $stats key-count]
            set key_count_expected [dict get $expected_slots_key_count $slot]
            assert {$key_count == $key_count_expected}
        }
    }
}

proc assert_slot_stats_monotonic_order {slot_stats orderby is_desc} {
    # For Tcl dict, the order of iteration is the order in which the keys were inserted into the dictionary
    # Thus, the response ordering is preserved upon calling 'convert_array_into_dict()'.
    # Source: https://www.tcl.tk/man/tcl8.6.11/TclCmd/dict.htm
    set slot_stats [convert_array_into_dict $slot_stats]
    set prev_metric -1
    dict for {_ stats} $slot_stats {
        set curr_metric [dict get $stats $orderby]
        if {$prev_metric != -1} {
            if {$is_desc == 1} {
                assert {$prev_metric >= $curr_metric}
            } else {
                assert {$prev_metric <= $curr_metric}
            }
        }
        set prev_metric $curr_metric
    }
}

proc assert_slot_stats_monotonic_descent {slot_stats orderby} {
    assert_slot_stats_monotonic_order $slot_stats $orderby 1
}

proc assert_slot_stats_monotonic_ascent {slot_stats orderby} {
    assert_slot_stats_monotonic_order $slot_stats $orderby 0
}

proc wait_for_replica_key_exists {key key_count} {
    wait_for_condition 1000 50 {
        [R 1 exists $key] eq "$key_count"
    } else {
        fail "Test key was not replicated"
    }
}

# -----------------------------------------------------------------------------
# Test cases for CLUSTER SLOT-STATS network-bytes-out correctness.
# -----------------------------------------------------------------------------

start_cluster 1 0 {tags {external:skip cluster}} {

    # Define shared variables.
    set key "FOO"
    set key_slot [R 0 cluster keyslot $key]
    set expected_slots_to_key_count [dict create $key_slot 1]
    set metrics_to_assert [list network-bytes-out]
    R 0 CONFIG SET cluster-slot-stats-enabled yes

    test "CLUSTER SLOT-STATS network-bytes-out, for non-slot specific commands." {
        R 0 INFO
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_empty_slot_stats $slot_stats
    }
    R 0 CONFIG RESETSTAT
    R 0 FLUSHALL

    test "CLUSTER SLOT-STATS network-bytes-out, for slot specific commands." {
        R 0 SET $key value
        # +OK\r\n --> 5 bytes

        set expected_slot_stats [
            dict create $key_slot [
                dict create network-bytes-out 5
            ]
        ]
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_empty_slot_stats_with_exception $slot_stats $expected_slot_stats $metrics_to_assert
    }
    R 0 CONFIG RESETSTAT
    R 0 FLUSHALL

    test "CLUSTER SLOT-STATS network-bytes-out, blocking commands." {
        set rd [valkey_deferring_client]
        $rd BLPOP $key 0
        wait_for_blocked_clients_count 1

        # Assert empty slot stats here, since COB is yet to be flushed due to the block.
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_empty_slot_stats $slot_stats

        # Unblock the command.
        # LPUSH client) :1\r\n --> 4 bytes.
        # BLPOP client) *2\r\n$3\r\nkey\r\n$5\r\nvalue\r\n --> 24 bytes, upon unblocking.
        R 0 LPUSH $key value
        wait_for_blocked_clients_count 0

        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        set expected_slot_stats [
            dict create $key_slot [
                dict create network-bytes-out 28 ;# 4 + 24 bytes.
            ]
        ]
        assert_empty_slot_stats_with_exception $slot_stats $expected_slot_stats $metrics_to_assert
    }
    R 0 CONFIG RESETSTAT
    R 0 FLUSHALL
}

start_cluster 1 1 {tags {external:skip cluster}} {

    # Define shared variables.
    set key "FOO"
    set key_slot [R 0 CLUSTER KEYSLOT $key]
    set metrics_to_assert [list network-bytes-out]
    R 0 CONFIG SET cluster-slot-stats-enabled yes

    # Setup replication.
    assert {[s -1 role] eq {slave}}
    wait_for_condition 1000 50 {
        [s -1 master_link_status] eq {up}
    } else {
        fail "Instance #1 master link status is not up"
    }
    R 1 readonly

    test "CLUSTER SLOT-STATS network-bytes-out, replication stream egress." {
        assert_equal [R 0 SET $key VALUE] {OK}
        # Local client) +OK\r\n --> 5 bytes.
        # Replication stream) *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n --> 33 bytes.
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        set expected_slot_stats [
            dict create $key_slot [
                dict create network-bytes-out 38 ;# 5 + 33 bytes.
            ]
        ]
        assert_empty_slot_stats_with_exception $slot_stats $expected_slot_stats $metrics_to_assert
    }
}

start_cluster 1 1 {tags {external:skip cluster}} {

    # Define shared variables.
    set channel "channel"
    set key_slot [R 0 cluster keyslot $channel]
    set channel_secondary "channel2"
    set key_slot_secondary [R 0 cluster keyslot $channel_secondary]
    set metrics_to_assert [list network-bytes-out]
    R 0 CONFIG SET cluster-slot-stats-enabled yes

    test "CLUSTER SLOT-STATS network-bytes-out, sharded pub/sub, single channel." {
        set slot [R 0 cluster keyslot $channel]
        set publisher [Rn 0]
        set subscriber [valkey_client]
        set replica [valkey_deferring_client -1]

        # Subscriber client) *3\r\n$10\r\nssubscribe\r\n$7\r\nchannel\r\n:1\r\n --> 38 bytes
        $subscriber SSUBSCRIBE $channel 
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        set expected_slot_stats [
            dict create $key_slot [
                dict create network-bytes-out 38
            ]
        ]
        R 0 CONFIG RESETSTAT

        # Publisher client) :1\r\n --> 4 bytes.
        # Subscriber client) *3\r\n$8\r\nsmessage\r\n$7\r\nchannel\r\n$5\r\nhello\r\n --> 42 bytes.
        # Cluster propagation) sdslen(channel) + sdslen(hello) --> 12 bytes.
        assert_equal 1 [$publisher SPUBLISH $channel hello]
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        set expected_slot_stats [
            dict create $key_slot [
                dict create network-bytes-out 58 ;# 4 + 42 + 12 bytes.
            ]
        ]
        assert_empty_slot_stats_with_exception $slot_stats $expected_slot_stats $metrics_to_assert
    }
    $subscriber QUIT
    R 0 FLUSHALL
    R 0 CONFIG RESETSTAT

    test "CLUSTER SLOT-STATS network-bytes-out, sharded pub/sub, cross-slot channels." {
        set slot [R 0 cluster keyslot $channel]
        set publisher [Rn 0]
        set subscriber [valkey_client]
        set replica [valkey_deferring_client -1]

        # Stack multi-slot subscriptions against a single client.
        # For primary channel;
        # Subscriber client) *3\r\n$10\r\nssubscribe\r\n$7\r\nchannel\r\n:1\r\n --> 38 bytes
        # For secondary channel;
        # Subscriber client) *3\r\n$10\r\nssubscribe\r\n$8\r\nchannel2\r\n:1\r\n --> 39 bytes
        $subscriber SSUBSCRIBE $channel
        $subscriber SSUBSCRIBE $channel_secondary
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        set expected_slot_stats [
            dict create \
                $key_slot [ \
                    dict create network-bytes-out 38
                ] \
                $key_slot_secondary [ \
                    dict create network-bytes-out 39
                ]
        ]
        R 0 CONFIG RESETSTAT

        # For primary channel;
        # Publisher client) :1\r\n --> 4 bytes.
        # Subscriber client) *3\r\n$8\r\nsmessage\r\n$7\r\nchannel\r\n$5\r\nhello\r\n --> 42 bytes.
        # Cluster propagation) sdslen(channel) + sdslen(hello) --> 12 bytes.
        # For secondary channel;
        # Publisher client) :1\r\n --> 4 bytes.
        # Subscriber client) *3\r\n$8\r\nsmessage\r\n$8\r\nchannel2\r\n$5\r\nhello\r\n --> 43 bytes.
        # Cluster propagation) sdslen(channel2) + sdslen(hello) --> 13 bytes.
        assert_equal 1 [$publisher SPUBLISH $channel hello]
        assert_equal 1 [$publisher SPUBLISH $channel_secondary hello]
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        set expected_slot_stats [
            dict create \
                $key_slot [ \
                    dict create network-bytes-out 58 ;# 4 + 42 + 12 bytes.
                ] \
                $key_slot_secondary [ \
                    dict create network-bytes-out 60 ;# 4 + 43 + 13 bytes.
                ]
        ]
        assert_empty_slot_stats_with_exception $slot_stats $expected_slot_stats $metrics_to_assert
    }
}

# -----------------------------------------------------------------------------
# Test cases for CLUSTER SLOT-STATS key-count correctness.
# -----------------------------------------------------------------------------

start_cluster 1 0 {tags {external:skip cluster}} {

    # Define shared variables.
    set key "FOO"
    set key_slot [R 0 cluster keyslot $key]
    set metrics_to_assert [list key-count]
    set expected_slot_stats [
        dict create $key_slot [
            dict create key-count 1
        ]
    ]
    R 0 CONFIG SET cluster-slot-stats-enabled yes

    test "CLUSTER SLOT-STATS contains default value upon valkey-server startup" {
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_empty_slot_stats $slot_stats
    }

    test "CLUSTER SLOT-STATS contains correct metrics upon key introduction" {
        R 0 SET $key TEST
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_empty_slot_stats_with_exception $slot_stats $expected_slot_stats $metrics_to_assert
    }

    test "CLUSTER SLOT-STATS contains correct metrics upon key mutation" {
        R 0 SET $key NEW_VALUE
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_empty_slot_stats_with_exception $slot_stats $expected_slot_stats $metrics_to_assert
    }

    test "CLUSTER SLOT-STATS contains correct metrics upon key deletion" {
        R 0 DEL $key
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_empty_slot_stats $slot_stats
    }

    test "CLUSTER SLOT-STATS slot visibility based on slot ownership changes" {
        R 0 CONFIG SET cluster-require-full-coverage no
        
        R 0 CLUSTER DELSLOTS $key_slot
        set expected_slots [initialize_expected_slots_dict]
        dict unset expected_slots $key_slot
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert {[dict size $expected_slots] == 16383}
        assert_slot_visibility $slot_stats $expected_slots

        R 0 CLUSTER ADDSLOTS $key_slot
        set expected_slots [initialize_expected_slots_dict]
        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert {[dict size $expected_slots] == 16384}
        assert_slot_visibility $slot_stats $expected_slots
    }
}

# -----------------------------------------------------------------------------
# Test cases for CLUSTER SLOT-STATS SLOTSRANGE sub-argument.
# -----------------------------------------------------------------------------

start_cluster 1 0 {tags {external:skip cluster}} {

    test "CLUSTER SLOT-STATS SLOTSRANGE all slots present" {
        set start_slot 100
        set end_slot 102
        set expected_slots [initialize_expected_slots_dict_with_range $start_slot $end_slot]

        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE $start_slot $end_slot]
        assert_slot_visibility $slot_stats $expected_slots
    }

    test "CLUSTER SLOT-STATS SLOTSRANGE some slots missing" {
        set start_slot 100
        set end_slot 102
        set expected_slots [initialize_expected_slots_dict_with_range $start_slot $end_slot]

        R 0 CLUSTER DELSLOTS $start_slot
        dict unset expected_slots $start_slot

        set slot_stats [R 0 CLUSTER SLOT-STATS SLOTSRANGE $start_slot $end_slot]
        assert_slot_visibility $slot_stats $expected_slots
    }
}

# -----------------------------------------------------------------------------
# Test cases for CLUSTER SLOT-STATS ORDERBY sub-argument.
# -----------------------------------------------------------------------------

start_cluster 1 0 {tags {external:skip cluster}} {

    # SET keys for target hashslots, to encourage ordering.
    set hash_tags [list 0 1 2 3 4]
    set num_keys 1
    foreach hash_tag $hash_tags {
        for {set i 0} {$i < $num_keys} {incr i 1} {
            R 0 SET "$i{$hash_tag}" VALUE
        }
        incr num_keys 1
    }

    # SET keys for random hashslots, for random noise.
    set num_keys 0
    while {$num_keys < 1000} {
        set random_key [randomInt 16384]
        R 0 SET $random_key VALUE
        incr num_keys 1
    }

    test "CLUSTER SLOT-STATS ORDERBY DESC correct ordering" {
        set orderby "key-count"
        assert_error "ERR*" {R 0 CLUSTER SLOT-STATS ORDERBY $orderby DESC LIMIT -1}
        set slot_stats [R 0 CLUSTER SLOT-STATS ORDERBY $orderby DESC]
        assert_slot_stats_monotonic_descent $slot_stats $orderby
    }

    test "CLUSTER SLOT-STATS ORDERBY ASC correct ordering" {
        set orderby "key-count"
        set slot_stats [R 0 CLUSTER SLOT-STATS ORDERBY $orderby ASC]
        assert_slot_stats_monotonic_ascent $slot_stats $orderby
    }

    test "CLUSTER SLOT-STATS ORDERBY LIMIT correct response pagination, where limit is less than number of assigned slots" {
        R 0 FLUSHALL SYNC

        set limit 5
        set slot_stats_desc [R 0 CLUSTER SLOT-STATS ORDERBY key-count LIMIT $limit DESC]
        set slot_stats_asc [R 0 CLUSTER SLOT-STATS ORDERBY key-count LIMIT $limit ASC]
        set slot_stats_desc_length [llength $slot_stats_desc]
        set slot_stats_asc_length [llength $slot_stats_asc]
        assert {$limit == $slot_stats_desc_length && $limit == $slot_stats_asc_length}

        # The key count of all slots is 0, so we will order by slot in ascending order.
        set expected_slots [dict create 0 0 1 0 2 0 3 0 4 0]
        assert_slot_visibility $slot_stats_desc $expected_slots
        assert_slot_visibility $slot_stats_asc $expected_slots
    }

    test "CLUSTER SLOT-STATS ORDERBY LIMIT correct response pagination, where limit is greater than number of assigned slots" {
        R 0 CONFIG SET cluster-require-full-coverage no
        R 0 FLUSHALL SYNC
        R 0 CLUSTER FLUSHSLOTS
        R 0 CLUSTER ADDSLOTS 100 101

        set num_assigned_slots 2
        set limit 5
        set slot_stats_desc [R 0 CLUSTER SLOT-STATS ORDERBY key-count LIMIT $limit DESC]
        set slot_stats_asc [R 0 CLUSTER SLOT-STATS ORDERBY key-count LIMIT $limit ASC]
        set slot_stats_desc_length [llength $slot_stats_desc]
        set slot_stats_asc_length [llength $slot_stats_asc]
        set expected_response_length [expr min($num_assigned_slots, $limit)]
        assert {$expected_response_length == $slot_stats_desc_length && $expected_response_length == $slot_stats_asc_length}

        set expected_slots [dict create 100 0 101 0]
        assert_slot_visibility $slot_stats_desc $expected_slots
        assert_slot_visibility $slot_stats_asc $expected_slots
    }
}

# -----------------------------------------------------------------------------
# Test cases for CLUSTER SLOT-STATS replication.
# -----------------------------------------------------------------------------

start_cluster 1 1 {tags {external:skip cluster}} {

    # Define shared variables.
    set key "FOO"
    set key_slot [R 0 CLUSTER KEYSLOT $key]
    R 0 CONFIG SET cluster-slot-stats-enabled yes

    # For replication, only those metrics that are deterministic upon replication are asserted.
    # * key-count is asserted, as both the primary and its replica must hold the same number of keys.
    # * network-bytes-out is not asserted, as the replication egress bytes is only accumulated 
    #   within the primary, and not its replica.
    set metrics_to_assert [list key-count]

    # Setup replication.
    assert {[s -1 role] eq {slave}}
    wait_for_condition 1000 50 {
        [s -1 master_link_status] eq {up}
    } else {
        fail "Instance #1 master link status is not up"
    }
    R 1 readonly

    test "CLUSTER SLOT-STATS key-count replication for new keys" {
        R 0 SET $key VALUE
        set slot_stats_master [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]

        set expected_slots_key_count [dict create $key_slot 1]
        assert_slot_stats_key_count $slot_stats_master $expected_slots_key_count
        wait_for_replica_key_exists $key 1

        set slot_stats_replica [R 1 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_equal_slot_stats $slot_stats_master $slot_stats_replica $metrics_to_assert
    }

    test "CLUSTER SLOT-STATS key-count replication for existing keys" {
        R 0 SET $key VALUE_UPDATED
        set slot_stats_master [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]

        set expected_slots_key_count [dict create $key_slot 1]
        assert_slot_stats_key_count $slot_stats_master $expected_slots_key_count
        wait_for_replica_key_exists $key 1

        set slot_stats_replica [R 1 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_equal_slot_stats $slot_stats_master $slot_stats_replica $metrics_to_assert
    }

    test "CLUSTER SLOT-STATS key-count replication for deleting keys" {
        R 0 DEL $key
        set slot_stats_master [R 0 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]

        set expected_slots_key_count [dict create $key_slot 0]
        assert_slot_stats_key_count $slot_stats_master $expected_slots_key_count
        wait_for_replica_key_exists $key 0

        set slot_stats_replica [R 1 CLUSTER SLOT-STATS SLOTSRANGE 0 16383]
        assert_equal_slot_stats $slot_stats_master $slot_stats_replica $metrics_to_assert
    }
}