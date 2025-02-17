proc restart_killed_instances {} {
    foreach type {valkey sentinel} {
        foreach_${type}_id id {
            if {[get_instance_attrib $type $id pid] == -1} {
                puts -nonewline "$type/$id "
                flush stdout
                restart_instance $type $id
            }
        }
    }
}

proc verify_sentinel_connect_sentinels {id} {
    foreach sentinel [S $id SENTINEL SENTINELS mymaster] {
        if {[string match "*disconnected*" [dict get $sentinel flags]]} {
            return 0
        }
    }
    return 1
}

proc verify_sentinel_auto_discovery {} {
    set sentinels [llength $::sentinel_instances]
    foreach_sentinel_id id {
        wait_for_condition 1000 50 {
            [dict get [S $id SENTINEL PRIMARY mymaster] num-other-sentinels] == ($sentinels-1)
        } else {
            fail "At least some sentinel can't detect some other sentinel"
        }
        wait_for_condition 1000 50 {
            [verify_sentinel_connect_sentinels $id] == 1
        } else {
            fail "At least some sentinel can't connect to other sentinel"
        }
    }
}
