start_server {tags {needs:repl external:skip}} {
    start_server {} {
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set primary_pid [srv -1 pid]

        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_pid [srv 0 pid]

        r replicaof $primary_host $primary_port
        wait_for_condition 50 100 {
            [s 0 master_link_status] eq {up}
        } else {
            fail "Replicas not replicating from primary"
        }

        test {replica allow read command by default} {
            r get foo
        } {}

        test {replica reply READONLY error for write command by default} {
            assert_error {READONLY*} {r set foo bar}
        }

        test {replica redirect read and write command after CLIENT CAPA REDIRECT} {
            r client capa redirect
            assert_error "REDIRECT $primary_host:$primary_port" {r set foo bar}
            assert_error "REDIRECT $primary_host:$primary_port" {r get foo}
        }

        test {non-data access commands are not redirected} {
            r ping
        } {PONG}

        test {replica allow read command in READONLY mode} {
            r readonly
            r get foo
        } {}

        test {client paused during failover-in-progress} {
            pause_process $replica_pid
            # replica will never acknowledge this write
            r -1 set foo bar
            r -1 failover to $replica_host $replica_port TIMEOUT 100 FORCE

            # Wait for primary to give up on sync attempt and start failover
            wait_for_condition 50 100 {
                [s -1 master_failover_state] == "failover-in-progress"
            } else {
                fail "Failover from primary to replica did not timeout"
            }

            set rd [valkey_deferring_client -1]
            $rd client capa redirect
            assert_match "OK" [$rd read]
            $rd set foo bar

            # Client paused during failover-in-progress, see more details in PR #871
            wait_for_blocked_clients_count 1 100 10 -1

            resume_process $replica_pid

            # Wait for failover to end
            wait_for_condition 50 100 {
                [s -1 master_failover_state] == "no-failover"
            } else {
                fail "Failover from primary to replica did not finish"
            }

            assert_match *master* [r role]
            assert_match *slave* [r -1 role]

            assert_error "REDIRECT $replica_host:$replica_port" {$rd read}
            $rd close
        }
    }
}
