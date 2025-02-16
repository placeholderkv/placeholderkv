start_cluster 2 2 {tags {external:skip cluster}} {
    test {Key lazy expires during key migration} {
        R 0 DEBUG SET-ACTIVE-EXPIRE 0

        set key_slot [R 0 CLUSTER KEYSLOT FOO]
        R 0 set FOO BAR PX 10
        set src_id [R 0 CLUSTER MYID]
        set trg_id [R 1 CLUSTER MYID]
        R 0 CLUSTER SETSLOT $key_slot MIGRATING $trg_id
        R 1 CLUSTER SETSLOT $key_slot IMPORTING $src_id
        after 11
        assert_error {ASK*} {R 0 GET FOO}
        R 0 ping
    } {PONG}

    test "Coverage: Basic cluster commands" {
        assert_equal {OK} [R 0 CLUSTER saveconfig]

        set id [R 0 CLUSTER MYID]
        assert_equal {0} [R 0 CLUSTER count-failure-reports $id]

        R 0 flushall
        assert_equal {OK} [R 0 CLUSTER flushslots]
    }
}

# Create a folder called "nodes.conf" to trigger temp nodes.conf rename
# failure and it will cause cluster config file save to fail at the rename.
proc create_nodes_conf_folder {srv_idx} {
    set dir [lindex [R $srv_idx config get dir] 1]
    set cluster_conf [lindex [R $srv_idx config get cluster-config-file] 1]
    set cluster_conf_path [file join $dir $cluster_conf]
    if {[file exists $cluster_conf_path]} { exec rm -f $cluster_conf_path }
    exec mkdir -p $cluster_conf_path
}

start_cluster 1 1 {tags {external:skip cluster}} {
    test {Cluster cluster-ignore-disk-write-errors is work as expected} {
        # If the save fails, R 0 will exit and R 1 will not exit.
        R 0 config set cluster-ignore-disk-write-errors no
        R 1 config set cluster-ignore-disk-write-errors yes

        # Create folder that can cause the rename fail.
        create_nodes_conf_folder 0
        create_nodes_conf_folder 1

        # Trigger a takeover so that cluster will need to update the config file.
        R 1 cluster failover takeover

        # Make sure R 0 is exit and R 1 isn't exit.
        assert_error "*I/O error*" {R 0 ping}
        assert_equal {PONG} [R 1 ping]
        assert_equal 0 [process_is_alive [srv 0 pid]]
        assert_equal 1 [process_is_alive [srv -1 pid]]

        # Make sure relevant logs are printed.
        verify_log_message 0 "*Could not rename tmp cluster config file*" 0
        verify_log_message -1 "*Could not rename tmp cluster config file*" 0
        verify_log_message 0 "*Fatal: can't update cluster config file*" 0
        verify_log_message -1 "*Cluster config file is applying a change even though it is unable to write to disk*" 0
    }
}
