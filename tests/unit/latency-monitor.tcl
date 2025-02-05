start_server {tags {"latency-monitor needs:latency"}} {
    # Set a threshold high enough to avoid spurious latency events.
    r config set latency-monitor-threshold 200
    r latency reset

    test {LATENCY HISTOGRAM with empty histogram} {
        r config resetstat
        set histo [dict create {*}[r latency histogram]]
        # Config resetstat is recorded
        assert_equal [dict size $histo] 1
        assert_match {*config|resetstat*} $histo
    }

    test {LATENCY HISTOGRAM all commands} {
        r config resetstat
        r set a b
        r set c d
        set histo [dict create {*}[r latency histogram]]
        assert_match {calls 2 histogram_usec *} [dict get $histo set]
        assert_match {calls 1 histogram_usec *} [dict get $histo "config|resetstat"]
    }

    test {LATENCY HISTOGRAM sub commands} {
        r config resetstat
        r client id
        r client list
        # parent command reply with its sub commands
        set histo [dict create {*}[r latency histogram client]]
        assert {[dict size $histo] == 2}
        assert_match {calls 1 histogram_usec *} [dict get $histo "client|id"]
        assert_match {calls 1 histogram_usec *} [dict get $histo "client|list"]

        # explicitly ask for one sub-command
        set histo [dict create {*}[r latency histogram "client|id"]]
        assert {[dict size $histo] == 1}
        assert_match {calls 1 histogram_usec *} [dict get $histo "client|id"]
    }

    test {LATENCY HISTOGRAM with a subset of commands} {
        r config resetstat
        r set a b
        r set c d
        r get a
        r hset f k v
        r hgetall f
        set histo [dict create {*}[r latency histogram set hset]]
        assert_match {calls 2 histogram_usec *} [dict get $histo set]
        assert_match {calls 1 histogram_usec *} [dict get $histo hset]
        assert_equal [dict size $histo] 2
        set histo [dict create {*}[r latency histogram hgetall get zadd]]
        assert_match {calls 1 histogram_usec *} [dict get $histo hgetall]
        assert_match {calls 1 histogram_usec *} [dict get $histo get]
        assert_equal [dict size $histo] 2
    }

    test {LATENCY HISTOGRAM command} {
        r config resetstat
        r set a b
        r get a
        assert {[llength [r latency histogram set get]] == 4}
    }

    test {LATENCY HISTOGRAM with wrong command name skips the invalid one} {
        r config resetstat
        assert {[llength [r latency histogram blabla]] == 0}
        assert {[llength [r latency histogram blabla blabla2 set get]] == 0}
        r set a b
        r get a
        assert_match {calls 1 histogram_usec *} [lindex [r latency histogram blabla blabla2 set get] 1]
        assert_match {calls 1 histogram_usec *} [lindex [r latency histogram blabla blabla2 set get] 3]
        assert {[string length [r latency histogram blabla set get]] > 0}
    }

tags {"needs:debug"} {
    set old_threshold_value [lindex [r config get latency-monitor-threshold] 1]

    test {Test latency events logging} {
        r config set latency-monitor-threshold 200
        r latency reset
        r debug sleep 0.3
        after 1100
        r debug sleep 0.4
        after 1100
        r debug sleep 0.5
        r config set latency-monitor-threshold 0
        assert {[r latency history command] >= 3}
    }

    test {LATENCY HISTORY output is ok} {
        set res [r latency history command]
        if {$::verbose} {
            puts "LATENCY HISTORY data:"
            puts $res
        }

        set min 250
        set max 450
        foreach event $res {
            lassign $event time latency
            if {!$::no_latency} {
                assert {$latency >= $min && $latency <= $max}
            }
            incr min 100
            incr max 100
            set last_time $time ; # Used in the next test
        }
    }

    test {LATENCY LATEST output is ok} {
        set res [r latency latest]
        if {$::verbose} {
            puts "LATENCY LATEST data:"
            puts $res
        }

        # See the previous "Test latency events logging" test for each call.
        foreach event $res {
            lassign $event eventname time latency max sum cnt
            assert {$eventname eq "command"}
            if {!$::no_latency} {
                # To avoid timing issues, each event decreases by 50 and
                # increases by 150 to increase the range.
                assert_equal $time $last_time
                assert_range $max 450 650 ;# debug sleep 0.5
                assert_range $sum 1050 1650 ;# debug sleep 0.3 + 0.4 + 0.5
                assert_equal $cnt 3
            }
            break
        }
    }

    test {LATENCY GRAPH can output the event graph} {
        set res [r latency graph command]
        if {$::verbose} {
            puts "LATENCY GRAPH data:"
            puts $res
        }
        assert_match {*command*high*low*} $res

        # These numbers are taken from the "Test latency events logging" test.
        # (debug sleep 0.3) and (debug sleep 0.5), using range to prevent timing issue.
        regexp "command - high (.*?) ms, low (.*?) ms" $res -> high low
        assert_morethan_equal $high 500
        assert_morethan_equal $low 300
    }

    r config set latency-monitor-threshold $old_threshold_value
} ;# tag

    test {LATENCY of expire events are correctly collected} {
        r config set latency-monitor-threshold 1
        r config set lazyfree-lazy-expire no
        r flushdb
        if {$::valgrind} {set count 100000} else {set count 1000000}
        r eval {
            local i = 0
            while (i < tonumber(ARGV[1])) do
                redis.call('sadd',KEYS[1],i)
                i = i+1
             end
        } 1 mybigkey $count
        r pexpire mybigkey 50
        wait_for_condition 5 100 {
            [r dbsize] == 0
        } else {
            fail "key wasn't expired"
        }
        assert_match {*expire-cycle*} [r latency latest]

        test {LATENCY GRAPH can output the expire event graph} {
             assert_match {*expire-cycle*high*low*} [r latency graph expire-cycle]
        }

        r config set latency-monitor-threshold 200
        r config set lazyfree-lazy-expire yes
    }

    test {LATENCY HISTORY / RESET with wrong event name is fine} {
        assert {[llength [r latency history blabla]] == 0}
        assert {[r latency reset blabla] == 0}
    }

    test {LATENCY DOCTOR produces some output} {
        assert {[string length [r latency doctor]] > 0}
    }

    test {LATENCY RESET is able to reset events} {
        assert {[r latency reset] > 0}
        assert {[r latency latest] eq {}}
    }

    test {LATENCY HELP should not have unexpected options} {
        catch {r LATENCY help xxx} e
        assert_match "*wrong number of arguments for 'latency|help' command" $e
    }
}

start_cluster 1 1 {tags {"latency-monitor cluster external:skip needs:latency"} overrides {latency-monitor-threshold 1}} {
    test "Cluster config file latency" {
        # This test just a sanity test so that we can make sure the code path is cover.
        # We don't assert anything since we can't be sure whether it will be counted.
        R 0 cluster saveconfig
        R 1 cluster saveconfig
        R 1 cluster failover force
        R 0 latency latest
        R 1 latency latest
    }
}
