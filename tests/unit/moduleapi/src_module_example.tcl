source tests/support/cli.tcl

set helloblockmodule [file normalize src/modules/helloblock.so]
set hellodictmodule [file normalize src/modules/hellodict.so]
set hellotimermodule [file normalize src/modules/hellotimer.so]
set hellotypemodule [file normalize src/modules/hellotype.so]
set helloworldmodule [file normalize src/modules/helloworld.so]

start_server {tags {"modules"}} {
    r module load $hellodictmodule
    r module load $helloblockmodule
    r module load $hellotypemodule
    r module load $hellotimermodule
    r module load $helloworldmodule
    r select 0

    test {test module command specs} {
        check_commands_specs r
    }
    test {test hellodict} {
        foreach i {a b c d e f g h i j k l m n o p q r s t u v w x y z} {
            r hellodict.set $i $i
            assert_match [r hellodict.get $i] $i
        }
        assert_equal [llength [r hellodict.keyrange a b 2]] 2
        assert_equal [llength [r hellodict.keyrange a z 2]] 2
    }

    test {test helloblock} {
        assert_match [r hello.block 1 2] "Request timedout"
        assert {[r hello.block 1 2000] > 0}
        r set foo bar
        assert {[llength [r hello.keys]] > 0}
        r flushall
        assert_equal [llength [r hello.keys]] 0
    }

    test {test hellotype} {
        set cnt 1
        while {$cnt <= 10000} {
            r hellotype.insert hello $cnt
            incr cnt
        }
        assert_equal [llength [r hellotype.range hello 1 100]] 100
        assert_equal [llength [r hellotype.range hello 1 1000]] 1000
        assert_equal [llength [r hellotype.brange hello 1 20000 1]] 10000
    }

    test {test helloworld} {
        assert_equal [r hello.simple] 0
        assert_equal [r hello.push.native lkey 1] 1
        assert_equal [r hello.push.call lkey 2] 2
        assert_equal [r hello.push.call2 lkey 3] 3
        assert_equal [r hello.list.sum.len lkey] 3
        assert_equal [r hello.list.splice lkey lkey2 1] 2
        assert_equal [r hello.list.sum.len lkey] 2
        assert_equal [r hello.list.sum.len lkey2] 1
        assert_equal [r hello.list.splice.auto lkey lkey2 1] 1
        assert_equal [r hello.list.sum.len lkey] 1
        assert_equal [r hello.list.sum.len lkey2] 2
        assert_equal [llength [r hello.rand.array 10]] 10

        r hello.repl1
        r hello.repl1
        assert_equal [r get foo] 2
        assert_equal [r get bar] 2

        assert_equal [r hello.push.native lsum 1] 1
        assert_equal [r hello.push.call lsum 2] 2
        assert_equal [r hello.push.call2 lsum 3] 3
        assert_equal [r hello.repl2 lsum] 9

        r set a b
        r hello.toggle.case a
        assert_equal [r get a] B
        r hello.toggle.case a
        assert_equal [r get a] b

        r set tk bb EX 100
        assert {[r ttl tk] < 101}
        r hello.more.expire tk 1000000000
        assert {[r ttl tk] > 100}

        r zadd myzset 1 a 2 b 3 c 4 d
        set res [r hello.zsumrange myzset 1 3]
        assert_equal [lindex $res 0] 6
        assert_equal [lindex $res 0] [lindex $res 1]

        r zadd myzset 0 e 0 f 0 g 0 h
        set lexres [r hello.lexrange myzset "\[b" "\[f" 0 0]
        assert_equal [llength $lexres] 2

        r hset hkey a a
        r hello.hcopy hkey a b
        assert_match [r hget hkey a] [r hget hkey b]

        assert_match [r hello.leftpad td 4 0] "00td"
    }

    test {unload modules} {
        r FLUSHALL
        assert_equal {OK} [r module unload hellodict]
        assert_equal {OK} [r module unload helloblock]
        assert_equal {OK} [r module unload hellotimer]
        assert_equal {OK} [r module unload helloworld]
    }
}