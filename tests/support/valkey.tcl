# Tcl client library - used by the server test
# Copyright (c) 2009-2014 Redis Ltd.
# Released under the BSD license like Redis itself
#
# Example usage:
#
# set r [valkey 127.0.0.1 6379]
# $r lpush mylist foo
# $r lpush mylist bar
# $r lrange mylist 0 -1
# $r close
#
# Non blocking usage example:
#
# proc handlePong {r type reply} {
#     puts "PONG $type '$reply'"
#     if {$reply ne "PONG"} {
#         $r ping [list handlePong]
#     }
# }
#
# set r [valkey]
# $r blocking 0
# $r get fo [list handlePong]
#
# vwait forever

package provide valkey 0.1

source [file join [file dirname [info script]] "response_transformers.tcl"]

namespace eval valkey {}
set ::valkey::id 0
array set ::valkey::fd {}
array set ::valkey::addr {}
array set ::valkey::blocking {}
array set ::valkey::deferred {}
array set ::valkey::readraw {}
array set ::valkey::attributes {} ;# Holds the RESP3 attributes from the last call
array set ::valkey::reconnect {}
array set ::valkey::tls {}
array set ::valkey::callback {}
array set ::valkey::state {} ;# State in non-blocking reply reading
array set ::valkey::statestack {} ;# Stack of states, for nested mbulks
array set ::valkey::curr_argv {} ;# Remember the current argv, to be used in response_transformers.tcl
array set ::valkey::testing_resp3 {} ;# Indicating if the current client is using RESP3 (only if the test is trying to test RESP3 specific behavior. It won't be on in case of force_resp3)

set ::force_resp3 0
set ::log_req_res 0

proc valkey {{server 127.0.0.1} {port 6379} {defer 0} {tls 0} {tlsoptions {}} {readraw 0}} {
    if {$tls} {
        package require tls
        ::tls::init \
            -cafile "$::tlsdir/ca.crt" \
            -certfile "$::tlsdir/client.crt" \
            -keyfile "$::tlsdir/client.key" \
            {*}$tlsoptions
        set fd [::tls::socket $server $port]
    } else {
        set fd [socket $server $port]
    }
    fconfigure $fd -translation binary
    set id [incr ::valkey::id]
    set ::valkey::fd($id) $fd
    set ::valkey::addr($id) [list $server $port]
    set ::valkey::blocking($id) 1
    set ::valkey::deferred($id) $defer
    set ::valkey::readraw($id) $readraw
    set ::valkey::reconnect($id) 0
    set ::valkey::curr_argv($id) 0
    set ::valkey::testing_resp3($id) 0
    set ::valkey::tls($id) $tls
    ::valkey::valkey_reset_state $id
    interp alias {} ::valkey::valkeyHandle$id {} ::valkey::__dispatch__ $id
}

# On recent versions of tcl-tls/OpenSSL, reading from a dropped connection
# results with an error we need to catch and mimic the old behavior.
proc ::valkey::valkey_safe_read {fd len} {
    if {$len == -1} {
        set err [catch {set val [read $fd]} msg]
    } else {
        set err [catch {set val [read $fd $len]} msg]
    }
    if {!$err} {
        return $val
    }
    if {[string match "*connection abort*" $msg]} {
        return {}
    }
    error $msg
}

proc ::valkey::valkey_safe_gets {fd} {
    if {[catch {set val [gets $fd]} msg]} {
        if {[string match "*connection abort*" $msg]} {
            return {}
        }
        error $msg
    }
    return $val
}

# This is a wrapper to the actual dispatching procedure that handles
# reconnection if needed.
proc ::valkey::__dispatch__ {id method args} {
    set errorcode [catch {::valkey::__dispatch__raw__ $id $method $args} retval]
    if {$errorcode && $::valkey::reconnect($id) && $::valkey::fd($id) eq {}} {
        # Try again if the connection was lost.
        # FIXME: we don't re-select the previously selected DB, nor we check
        # if we are inside a transaction that needs to be re-issued from
        # scratch.
        set errorcode [catch {::valkey::__dispatch__raw__ $id $method $args} retval]
    }
    return -code $errorcode $retval
}

proc ::valkey::__dispatch__raw__ {id method argv} {
    set fd $::valkey::fd($id)

    # Reconnect the link if needed.
    if {$fd eq {} && $method ne {close}} {
        lassign $::valkey::addr($id) host port
        if {$::valkey::tls($id)} {
            set ::valkey::fd($id) [::tls::socket $host $port]
        } else {
            set ::valkey::fd($id) [socket $host $port]
        }
        fconfigure $::valkey::fd($id) -translation binary
        set fd $::valkey::fd($id)
    }

    # Transform HELLO 2 to HELLO 3 if force_resp3
    # All set the connection var testing_resp3 in case of HELLO 3
    if {[llength $argv] > 0 && [string compare -nocase $method "HELLO"] == 0} {
        if {[lindex $argv 0] == 3} {
            set ::valkey::testing_resp3($id) 1
        } else {
            set ::valkey::testing_resp3($id) 0
            if {$::force_resp3} {
                # If we are in force_resp3 we run HELLO 3 instead of HELLO 2
                lset argv 0 3
            }
        }
    }

    set blocking $::valkey::blocking($id)
    set deferred $::valkey::deferred($id)
    if {$blocking == 0} {
        if {[llength $argv] == 0} {
            error "Please provide a callback in non-blocking mode"
        }
        set callback [lindex $argv end]
        set argv [lrange $argv 0 end-1]
    }
    if {[info command ::valkey::__method__$method] eq {}} {
        catch {unset ::valkey::attributes($id)}
        set cmd "*[expr {[llength $argv]+1}]\r\n"
        append cmd "$[string length $method]\r\n$method\r\n"
        foreach a $argv {
            append cmd "$[string length $a]\r\n$a\r\n"
        }
        ::valkey::valkey_write $fd $cmd
        if {[catch {flush $fd}]} {
            catch {close $fd}
            set ::valkey::fd($id) {}
            return -code error "I/O error reading reply"
        }

        set ::valkey::curr_argv($id) [concat $method $argv]
        if {!$deferred} {
            if {$blocking} {
                ::valkey::valkey_read_reply $id $fd
            } else {
                # Every well formed reply read will pop an element from this
                # list and use it as a callback. So pipelining is supported
                # in non blocking mode.
                lappend ::valkey::callback($id) $callback
                fileevent $fd readable [list ::valkey::valkey_readable $fd $id]
            }
        }
    } else {
        uplevel 1 [list ::valkey::__method__$method $id $fd] $argv
    }
}

proc ::valkey::__method__blocking {id fd val} {
    set ::valkey::blocking($id) $val
    fconfigure $fd -blocking $val
}

proc ::valkey::__method__reconnect {id fd val} {
    set ::valkey::reconnect($id) $val
}

proc ::valkey::__method__read {id fd} {
    ::valkey::valkey_read_reply $id $fd
}

proc ::valkey::__method__rawread {id fd {len -1}} {
    return [valkey_safe_read $fd $len]
}

proc ::valkey::__method__write {id fd buf} {
    ::valkey::valkey_write $fd $buf
}

proc ::valkey::__method__flush {id fd} {
    flush $fd
}

proc ::valkey::__method__close {id fd} {
    catch {close $fd}
    catch {unset ::valkey::fd($id)}
    catch {unset ::valkey::addr($id)}
    catch {unset ::valkey::blocking($id)}
    catch {unset ::valkey::deferred($id)}
    catch {unset ::valkey::readraw($id)}
    catch {unset ::valkey::attributes($id)}
    catch {unset ::valkey::reconnect($id)}
    catch {unset ::valkey::tls($id)}
    catch {unset ::valkey::state($id)}
    catch {unset ::valkey::statestack($id)}
    catch {unset ::valkey::callback($id)}
    catch {unset ::valkey::curr_argv($id)}
    catch {unset ::valkey::testing_resp3($id)}
    catch {interp alias {} ::valkey::valkeyHandle$id {}}
}

proc ::valkey::__method__channel {id fd} {
    return $fd
}

proc ::valkey::__method__deferred {id fd val} {
    set ::valkey::deferred($id) $val
}

proc ::valkey::__method__readraw {id fd val} {
    set ::valkey::readraw($id) $val
}

proc ::valkey::__method__readingraw {id fd} {
    return $::valkey::readraw($id)
}

proc ::valkey::__method__attributes {id fd} {
    set _ $::valkey::attributes($id)
}

proc ::valkey::valkey_write {fd buf} {
    puts -nonewline $fd $buf
}

proc ::valkey::valkey_writenl {fd buf} {
    valkey_write $fd $buf
    valkey_write $fd "\r\n"
    flush $fd
}

proc ::valkey::valkey_readnl {fd len} {
    set buf [valkey_safe_read $fd $len]
    valkey_safe_read $fd 2 ; # discard CR LF
    return $buf
}

proc ::valkey::valkey_bulk_read {fd} {
    set count [valkey_read_line $fd]
    if {$count == -1} return {}
    set buf [valkey_readnl $fd $count]
    return $buf
}

proc ::valkey::redis_multi_bulk_read {id fd} {
    set count [valkey_read_line $fd]
    if {$count == -1} return {}
    set l {}
    set err {}
    for {set i 0} {$i < $count} {incr i} {
        if {[catch {
            lappend l [valkey_read_reply_logic $id $fd]
        } e] && $err eq {}} {
            set err $e
        }
    }
    if {$err ne {}} {return -code error $err}
    return $l
}

proc ::valkey::valkey_read_map {id fd} {
    set count [valkey_read_line $fd]
    if {$count == -1} return {}
    set d {}
    set err {}
    for {set i 0} {$i < $count} {incr i} {
        if {[catch {
            set k [valkey_read_reply_logic $id $fd] ; # key
            set v [valkey_read_reply_logic $id $fd] ; # value
            dict set d $k $v
        } e] && $err eq {}} {
            set err $e
        }
    }
    if {$err ne {}} {return -code error $err}
    return $d
}

proc ::valkey::valkey_read_line fd {
    string trim [valkey_safe_gets $fd]
}

proc ::valkey::valkey_read_null fd {
    valkey_safe_gets $fd
    return {}
}

proc ::valkey::valkey_read_bool fd {
    set v [valkey_read_line $fd]
    if {$v == "t"} {return 1}
    if {$v == "f"} {return 0}
    return -code error "Bad protocol, '$v' as bool type"
}

proc ::valkey::valkey_read_double {id fd} {
    set v [valkey_read_line $fd]
    # unlike many other DTs, there is a textual difference between double and a string with the same value,
    # so we need to transform to double if we are testing RESP3 (i.e. some tests check that a
    # double reply is "1.0" and not "1")
    if {[should_transform_to_resp2 $id]} {
        return $v
    } else {
        return [expr {double($v)}]
    }
}

proc ::valkey::valkey_read_verbatim_str fd {
    set v [valkey_bulk_read $fd]
    # strip the first 4 chars ("txt:")
    return [string range $v 4 end]
}

proc ::valkey::valkey_read_reply_logic {id fd} {
    if {$::valkey::readraw($id)} {
        return [valkey_read_line $fd]
    }

    while {1} {
        set type [valkey_safe_read $fd 1]
        switch -exact -- $type {
            _ {return [valkey_read_null $fd]}
            : -
            ( -
            + {return [valkey_read_line $fd]}
            , {return [valkey_read_double $id $fd]}
            # {return [valkey_read_bool $fd]}
            = {return [valkey_read_verbatim_str $fd]}
            - {return -code error [valkey_read_line $fd]}
            $ {return [valkey_bulk_read $fd]}
            > -
            ~ -
            * {return [redis_multi_bulk_read $id $fd]}
            % {return [valkey_read_map $id $fd]}
            | {
                set attrib [valkey_read_map $id $fd]
                set ::valkey::attributes($id) $attrib
                continue
            }
            default {
                if {$type eq {}} {
                    catch {close $fd}
                    set ::valkey::fd($id) {}
                    return -code error "I/O error reading reply"
                }
                return -code error "Bad protocol, '$type' as reply type byte"
            }
        }
    }
}

proc ::valkey::valkey_read_reply {id fd} {
    set response [valkey_read_reply_logic $id $fd]
    ::response_transformers::transform_response_if_needed $id $::valkey::curr_argv($id) $response
}

proc ::valkey::valkey_reset_state id {
    set ::valkey::state($id) [dict create buf {} mbulk -1 bulk -1 reply {}]
    set ::valkey::statestack($id) {}
}

proc ::valkey::valkey_call_callback {id type reply} {
    set cb [lindex $::valkey::callback($id) 0]
    set ::valkey::callback($id) [lrange $::valkey::callback($id) 1 end]
    uplevel #0 $cb [list ::valkey::valkeyHandle$id $type $reply]
    ::valkey::valkey_reset_state $id
}

# Read a reply in non-blocking mode.
proc ::valkey::valkey_readable {fd id} {
    if {[eof $fd]} {
        valkey_call_callback $id eof {}
        ::valkey::__method__close $id $fd
        return
    }
    if {[dict get $::valkey::state($id) bulk] == -1} {
        set line [gets $fd]
        if {$line eq {}} return ;# No complete line available, return
        switch -exact -- [string index $line 0] {
            : -
            + {valkey_call_callback $id reply [string range $line 1 end-1]}
            - {valkey_call_callback $id err [string range $line 1 end-1]}
            ( {valkey_call_callback $id reply [string range $line 1 end-1]}
            $ {
                dict set ::valkey::state($id) bulk \
                    [expr [string range $line 1 end-1]+2]
                if {[dict get $::valkey::state($id) bulk] == 1} {
                    # We got a $-1, hack the state to play well with this.
                    dict set ::valkey::state($id) bulk 2
                    dict set ::valkey::state($id) buf "\r\n"
                    ::valkey::valkey_readable $fd $id
                }
            }
            * {
                dict set ::valkey::state($id) mbulk [string range $line 1 end-1]
                # Handle *-1
                if {[dict get $::valkey::state($id) mbulk] == -1} {
                    valkey_call_callback $id reply {}
                }
            }
            default {
                valkey_call_callback $id err \
                    "Bad protocol, $type as reply type byte"
            }
        }
    } else {
        set totlen [dict get $::valkey::state($id) bulk]
        set buflen [string length [dict get $::valkey::state($id) buf]]
        set toread [expr {$totlen-$buflen}]
        set data [read $fd $toread]
        set nread [string length $data]
        dict append ::valkey::state($id) buf $data
        # Check if we read a complete bulk reply
        if {[string length [dict get $::valkey::state($id) buf]] ==
            [dict get $::valkey::state($id) bulk]} {
            if {[dict get $::valkey::state($id) mbulk] == -1} {
                valkey_call_callback $id reply \
                    [string range [dict get $::valkey::state($id) buf] 0 end-2]
            } else {
                dict with ::valkey::state($id) {
                    lappend reply [string range $buf 0 end-2]
                    incr mbulk -1
                    set bulk -1
                }
                if {[dict get $::valkey::state($id) mbulk] == 0} {
                    valkey_call_callback $id reply \
                        [dict get $::valkey::state($id) reply]
                }
            }
        }
    }
}

# when forcing resp3 some tests that rely on resp2 can fail, so we have to translate the resp3 response to resp2
proc ::valkey::should_transform_to_resp2 {id} {
    return [expr {$::force_resp3 && !$::valkey::testing_resp3($id)}]
}
