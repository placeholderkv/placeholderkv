start_server {tags {"repl external:skip" "throttling"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set replica_log [srv 0 stdout]

    start_server {} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]

        test {Establish replica and primary relationship} {
            $replica replicaof $primary_host $primary_port
            wait_for_condition 50 1000 {
                [status $replica master_link_status] == "up"
            } else {
                fail "Replica not replicating from primary"
            }
        }

        $primary config set repl-backlog-size 25000
        $primary config set client-output-buffer-limit "replica 512 256 0"
        $primary config set write-throttling yes

        test {Do not throttle on command below soft limit} {
            wait_for_condition 50 1000 {
                [status $replica master_link_status] == "up"
            } else {
                fail "Replica not replicating from primary"
            }

            set smallval [string repeat x 50]
            catch {$primary set foo $smallval} result

            assert_match {OK} $result
        }

        test {Do not throttle on command size above hard limit but below repl-backlog-size} {
            wait_for_condition 50 1000 {
                [status $replica master_link_status] == "up"
            } else {
                fail "Replica not replicating from primary"
            }

            set bigval [string repeat x 600]
            catch {$primary set foo $bigval} result

            assert_match {OK} $result
        }

        test {Throttle on command size above hard limit and repl-backlog-size} {
            wait_for_condition 50 1000 {
                [status $replica master_link_status] == "up"
            } else {
                fail "Replica not replicating from primary"
            }

            set bigval [string repeat x 26000]
            catch {$primary set foo $bigval} err

            assert_match {THROTTLED*} $err
        }
    }
}