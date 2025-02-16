start_cluster 1 1 {tags {external:skip cluster}} {
    set primary_id 0
    set replica1_id 1

    set primary [Rn $primary_id]
    set replica [Rn $replica1_id]

    test "Sharded pubsub publish behavior within multi/exec" {
        foreach {node} {primary replica} {
            set node [set $node]
            $node MULTI
            $node SPUBLISH ch1 "hello"
            $node EXEC
        }
    }

    test "Sharded pubsub within multi/exec with cross slot operation" {
        $primary MULTI
        $primary SPUBLISH ch1 "hello"
        $primary GET foo
        catch {[$primary EXEC]} err
        assert_match {CROSSSLOT*} $err
    }

    test "Sharded pubsub publish behavior within multi/exec with read operation on primary" {
        $primary MULTI
        $primary SPUBLISH foo "hello"
        $primary GET foo
        $primary EXEC
    } {0 {}}

    test "Sharded pubsub publish behavior within multi/exec with read operation on replica" {
        $replica MULTI
        $replica SPUBLISH foo "hello"
        catch {[$replica GET foo]} err
        assert_match {MOVED*} $err
        catch {[$replica EXEC]} err
        assert_match {EXECABORT*} $err
    }

    test "Sharded pubsub publish behavior within multi/exec with write operation on primary" {
        $primary MULTI
        $primary SPUBLISH foo "hello"
        $primary SET foo bar
        $primary EXEC
    } {0 OK}

    test "Sharded pubsub publish behavior within multi/exec with write operation on replica" {
        $replica MULTI
        $replica SPUBLISH foo "hello"
        catch {[$replica SET foo bar]} err
        assert_match {MOVED*} $err
        catch {[$replica EXEC]} err
        assert_match {EXECABORT*} $err
    }

    test "SHARDNUMSUB for CROSSSLOT Error in cluster mode" {
        set rd1 [valkey_deferring_client]
        set rd2 [valkey_deferring_client]
        set rd3 [valkey_deferring_client]

        assert_equal {1} [ssubscribe $rd1 chan1]
        assert_equal {1} [ssubscribe $rd2 chan2]
        assert_error "CROSSSLOT Keys in request don't hash to the same slot" {r pubsub shardnumsub chan1 chan2}

        assert_equal {1} [ssubscribe $rd3 {"{chan1}2"}]
        assert_equal {chan1 1 {{chan1}2} 1} [r pubsub shardnumsub "chan1" "{chan1}2"]

        # clean up clients
        $rd1 close
        $rd2 close
        $rd3 close
    }

}