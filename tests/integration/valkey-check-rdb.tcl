tags {"check-rdb network external:skip logreqres:skip"} {
    start_server {} {
        r select 0
        for {set i 10} {$i < 20} {incr i} {
            set key [string repeat "$i" 10]
            set value [string repeat "$i" 100]
            r set $key $value
        }
        r select 1
        for {set i 20} {$i < 30} {incr i} {
            set key [string repeat "$i" 10]
            for {set j 0} {$j < 5} {incr j} {
                r lpush $key [string repeat "$i" 100]
            }
        }
        r select 2
        for {set i 30} {$i < 40} {incr i} {
            set key [string repeat "$i" 10]
            for {set j 10} {$j < 20} {incr j} {
                r sadd $key [string repeat "$j" 100]
            }
        }
        r select 3
        for {set i 40} {$i < 50} {incr i} {
            set key [string repeat "$i" 10]
            for {set j 10} {$j < 20} {incr j} {
                set score $j
                set member [string repeat "$j" 100]
                r zadd $key $score $member
            }
        }
        r select 4
        for {set i 50} {$i < 60} {incr i} {
            set key [string repeat "$i" 10]
            for {set j 10} {$j < 20} {incr j} {
                set field [string repeat "$j" 10]
                set field_value [string repeat "$j" 10]
                r hset $key $field $field_value
            }
        }
        r select 5
        for {set i 60} {$i < 70} {incr i} {
            set key [string repeat "$i" 10]
            for {set j 10} {$j < 20} {incr j} {
                set field [string repeat "$j" 10]
                set field_value [string repeat "$j" 10]
                r xadd $key * $field $field_value
            }
        }
        r save
        
        set dump_rdb [file join [lindex [r config get dir] 1] dump.rdb]
        catch {
            exec src/valkey-check-rdb $dump_rdb --profiler
        } result

        assert_match "*db.0.type.string.keys.total:10*" $result
        assert_match "*db.1.type.list.keys.total:10*" $result
        assert_match "*db.2.type.set.keys.total:10*" $result
        assert_match "*db.3.type.zset.keys.total:10*" $result
        assert_match "*db.4.type.hash.keys.total:10*" $result
        assert_match "*db.5.type.stream.keys.total:10*" $result
    }
}