tags {external:skip tls:skip} {
    test {admin-port: CONFIG SET port number should fail} {
        start_server {} {
            set avail_port [find_available_port $::baseport $::portcount]
            set rd [valkey [srv 0 host] [srv 0 admin-port]]
            assert_error {*can't set immutable config*} {$rd CONFIG SET admin-port $avail_port}
            $rd PING
            $rd close
        }
    }

    test {admin-port: connection should fail on non-loopback interface} {
        start_server {} {
            catch {valkey [get_nonloopback_addr] [srv 0 admin-port]} e
            assert_match {*connection refused*} $e
        }
    }

    test {admin-port: setting admin-port to server port should fail} {
        start_server {} {
            catch {r CONFIG SET port [srv 0 admin-port]} e
            assert_match {*Unable to listen on this port*} $e
        }
    }

    test {admin-port: client could connect on admin-port after maxclients reached} {
        start_server {} {
            set original_maxclients [lindex [r config get maxclients] 1]
            r config set maxclients 2
            set rd [valkey [srv 0 host] [srv 0 port]]
            assert_match "PONG" [$rd PING]
            set rd1 [valkey [srv 0 host] [srv 0 port]]
            catch {$rd1 PING} e
            assert_match "*ERR max*reached*" $e
            set rd2 [valkey [srv 0 host] [srv 0 admin-port]]
            assert_match "PONG" [$rd2 PING]
            r config set maxclients $original_maxclients
            $rd close
            $rd1 close
            $rd2 close
        }
    }
}
