proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

proc get_client_id_by_last_cmd {r cmd} {
    set client_list [$r client list]
    set client_id ""
    set lines [split $client_list "\n"]
    foreach line $lines {
        if {[string match *cmd=$cmd* $line]} {
            set parts [split $line " "]
            foreach part $parts {
                if {[string match id=* $part]} {
                    set client_id [lindex [split $part "="] 1]
                    return $client_id
                }
            }
        }
    }
    return $client_id
}

# Wait until the process enters a paused state, then resume the process.
proc wait_and_resume_process idx {
    set pid [srv $idx pid]
    wait_for_condition 50 1000 {
        [string match "T*" [exec ps -o state= -p $pid]]
    } else {
        fail "Process $pid didn't stop, current state is [exec ps -o state= -p $pid]"
    }
    resume_process $pid
}


start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set client-output-buffer-limit "replica 1100k 0 0"
    $primary config set loglevel debug
    # generate small db
    populate 10 primary 10
    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]
        set replica_pid  [srv 0 pid]
        
        set load_handle0 [start_write_load $primary_host $primary_port 20]
        set load_handle1 [start_write_load $primary_host $primary_port 20]
        set load_handle2 [start_write_load $primary_host $primary_port 20]

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10

        test "Test dual-channel-replication primary gets cob overrun before established psync" {
            # Pause primary main process after fork
            $primary debug pause-after-fork 1
            $replica replicaof $primary_host $primary_port
            wait_for_log_messages 0 {"*Done loading RDB*"} 0 2000 1

            # At this point rdb is loaded but psync hasn't been established yet. 
            # Pause the replica so the primary main process will wake up while the
            # replica is unresponsive. We expect the main process to fill the COB and disconnect the replica.
            pause_process $replica_pid
            wait_and_resume_process -1
            $primary debug pause-after-fork 0
            wait_for_log_messages -1 {"*Client * closed * for overcoming of output buffer limits.*"} $loglines 2000 1
            wait_for_condition 50 100 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            resume_process $replica_pid
            set res [wait_for_log_messages -1 {"*Unable to partial resync with replica * for lack of backlog*"} $loglines 20000 1]
            set loglines [lindex $res 1]
        }
        $replica replicaof no one
        wait_for_condition 500 1000 {
            [s -1 rdb_bgsave_in_progress] eq 0
        } else {
            fail "Primary should abort sync"
        }
        
        $replica debug pause-after-fork 1
        $primary debug populate 1000 primary 100000
        # Set primary with a slow rdb generation, so that we can easily intercept loading
        # 10ms per key, with 1000 keys is 10 seconds
        $primary config set rdb-key-save-delay 10000
        $primary config set client-output-buffer-limit "replica 10kb 0 0"

        test "Test dual-channel-replication primary gets cob overrun during replica rdb load" {
            set cur_client_closed_count [s -1 client_output_buffer_limit_disconnections]
            $replica replicaof $primary_host $primary_port
            wait_for_condition 500 100 {
                [s -1 client_output_buffer_limit_disconnections] > $cur_client_closed_count
            } else {
                fail "Primary should disconnect replica due to COB overrun"
            }

            wait_for_condition 50 100 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            wait_and_resume_process 0
            set res [wait_for_log_messages -1 {"*Unable to partial resync with replica * for lack of backlog*"} $loglines 20000 1]
            set loglines [lindex $res 0]
        }
        stop_write_load $load_handle0
        stop_write_load $load_handle1
        stop_write_load $load_handle2
    }
}

foreach dualchannel {yes no} {
start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set loglevel debug
    $primary config set repl-diskless-sync-delay 5
    $primary config set client-output-buffer-limit "replica 1100kb 0 0"
    
    # Generating RDB will cost 5s(10000 * 0.0005s)
    $primary debug populate 10000 primary 1
    $primary config set rdb-key-save-delay 500

    $primary config set dual-channel-replication-enabled $dualchannel

    start_server {} {
        set replica1 [srv 0 client]
        $replica1 config set dual-channel-replication-enabled $dualchannel
        $replica1 config set loglevel debug
        start_server {} {
            set replica2 [srv 0 client]
            $replica2 config set dual-channel-replication-enabled $dualchannel
            $replica2 config set loglevel debug
            $replica2 config set repl-timeout 60

            set load_handle [start_one_key_write_load $primary_host $primary_port 100 "mykey1"]
            test "Sync should continue if not all slaves dropped dual-channel-replication $dualchannel" {
                $replica1 replicaof $primary_host $primary_port
                $replica2 replicaof $primary_host $primary_port

                wait_for_condition 50 1000 {
                    [status $primary rdb_bgsave_in_progress] == 1
                } else {
                    fail "Sync did not start"
                }
                if {$dualchannel == "yes"} {
                    # Wait for both replicas main conns to establish psync
                    wait_for_condition 50 1000 {
                        [status $primary sync_partial_ok] == 2
                    } else {
                        fail "Replicas main conns didn't establish psync [status $primary sync_partial_ok]"
                    }
                }

                catch {$replica1 shutdown nosave}
                wait_for_condition 50 2000 {
                    [status $replica2 master_link_status] == "up" &&
                    [status $primary sync_full] == 2 &&
                    (($dualchannel == "yes" && [status $primary sync_partial_ok] == 2) || $dualchannel == "no")
                } else {
                    fail "Sync session interapted\n
                        sync_full:[status $primary sync_full]\n
                        sync_partial_ok:[status $primary sync_partial_ok]"
                }
            }
            
            $replica2 replicaof no one

            # Generating RDB will cost 500s(1000000 * 0.0001s)
            $primary debug populate 1000000 primary 1
            $primary config set rdb-key-save-delay 100
    
            test "Primary abort sync if all slaves dropped dual-channel-replication $dualchannel" {
                set cur_psync [status $primary sync_partial_ok]
                $replica2 replicaof $primary_host $primary_port

                wait_for_condition 50 1000 {
                    [status $primary rdb_bgsave_in_progress] == 1
                } else {
                    fail "Sync did not start"
                }
                if {$dualchannel == "yes"} {
                    # Wait for both replicas main conns to establish psync
                    wait_for_condition 50 1000 {
                        [status $primary sync_partial_ok] == $cur_psync + 1
                    } else {
                        fail "Replicas main conns didn't establish psync [status $primary sync_partial_ok]"
                    }
                }

                catch {$replica2 shutdown nosave}
                wait_for_condition 50 1000 {
                    [status $primary rdb_bgsave_in_progress] == 0
                } else {
                    fail "Primary should abort the sync"
                }
            }
            stop_write_load $load_handle
        }
    }
}
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set loglevel debug
    $primary config set repl-diskless-sync-delay 5; # allow catch failed sync before retry

    # Generating RDB will cost 500s(1000000 * 0.0001s)
    $primary debug populate 1000000 primary 1
    $primary config set rdb-key-save-delay 100
    
    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]

        set load_handle [start_write_load $primary_host $primary_port 20]

        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10
        test "Test dual-channel-replication replica main channel disconnected" {
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }            

            $primary debug log "killing replica main connection"
            set replica_main_conn_id [get_client_id_by_last_cmd $primary "psync"]
            assert {$replica_main_conn_id != ""}
            set loglines [count_log_lines -1]
            $primary client kill id $replica_main_conn_id
            # Wait for primary to abort the sync
            wait_for_condition 50 1000 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10
        }

        test "Test dual channel replication slave of no one after main conn kill" {
            $replica replicaof no one
            wait_for_condition 500 1000 {
                [s -1 rdb_bgsave_in_progress] eq 0
            } else {
                fail "Primary should abort sync"
            }
        }

        test "Test dual-channel-replication replica rdb connection disconnected" {
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }            

            set replica_rdb_channel_id [get_client_id_by_last_cmd $primary "sync"]
            $primary debug log "killing replica rdb connection $replica_rdb_channel_id"
            assert {$replica_rdb_channel_id != ""}
            set loglines [count_log_lines -1]
            $primary client kill id $replica_rdb_channel_id
            # Wait for primary to abort the sync
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10
        }

        test "Test dual channel replication slave of no one after rdb conn kill" {
            $replica replicaof no one
            wait_for_condition 500 1000 {
                [s -1 rdb_bgsave_in_progress] eq 0
            } else {
                fail "Primary should abort sync"
            }
        }

        test "Test dual-channel-replication primary reject set-rdb-client after client killed" {
            # Ensure replica main channel will not handshake before rdb client is killed
            $replica debug pause-after-fork 1
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }

            set replica_rdb_channel_id [get_client_id_by_last_cmd $primary "sync"]
            assert {$replica_rdb_channel_id != ""}
            $primary debug log "killing replica rdb connection $replica_rdb_channel_id"
            $primary client kill id $replica_rdb_channel_id
            # Wait for primary to abort the sync
            wait_and_resume_process 0
            wait_for_condition 10000000 10 {
                [s -1 rdb_bgsave_in_progress] eq 0 &&
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary should abort sync"
            }
            # Verify primary reject replconf set-rdb-client-id
            set res [catch {$primary replconf set-rdb-client-id $replica_rdb_channel_id} err]
            assert [string match *ERR* $err]
        }
        stop_write_load $load_handle
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set loglevel debug
    $primary config set repl-diskless-sync-delay 0; # don't wait for other replicas

    # Generating RDB will cost 100s
    $primary debug populate 10000 primary 1
    $primary config set rdb-key-save-delay 10000
    
    start_server {} {
        set replica_1 [srv 0 client]
        set replica_host_1 [srv 0 host]
        set replica_port_1 [srv 0 port]
        set replica_log_1 [srv 0 stdout]
        
        $replica_1 config set dual-channel-replication-enabled yes
        $replica_1 config set loglevel debug
        $replica_1 config set repl-timeout 10
        start_server {} {
            set replica_2 [srv 0 client]
            set replica_host_2 [srv 0 host]
            set replica_port_2 [srv 0 port]
            set replica_log_2 [srv 0 stdout]
            
            set load_handle [start_write_load $primary_host $primary_port 20]

            $replica_2 config set dual-channel-replication-enabled yes
            $replica_2 config set loglevel debug
            $replica_2 config set repl-timeout 10
            test "Test replica unable to join dual channel replication sync after started" {
                $replica_1 replicaof $primary_host $primary_port
                # Wait for sync session to start
                wait_for_condition 50 100 {
                    [s -2 rdb_bgsave_in_progress] eq 1
                } else {
                    fail "replica didn't start sync session in time1"
                }
                $replica_2 replicaof $primary_host $primary_port
                wait_for_log_messages -2 {"*Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC*"} $loglines 100 1000
            }
            stop_write_load $load_handle
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set loglines [count_log_lines 0]

    $primary config set repl-diskless-sync yes
    $primary config set dual-channel-replication-enabled yes
    $primary config set loglevel debug
    $primary config set repl-diskless-sync-delay 5; # allow catch failed sync before retry

    # Generating RDB will cost 100 sec to generate
    $primary debug populate 10000 primary 1
    $primary config set rdb-key-save-delay 10000
    
    start_server {} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        set replica_log [srv 0 stdout]
        
        $replica config set dual-channel-replication-enabled yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10
        set load_handle [start_one_key_write_load $primary_host $primary_port 100 "mykey"]
        test "Replica recover rdb-connection killed" {
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }            

            $primary debug log "killing replica rdb connection"
            set replica_rdb_channel_id [get_client_id_by_last_cmd $primary "sync"]
            assert {$replica_rdb_channel_id != ""}
            set loglines [count_log_lines -1]
            $primary client kill id $replica_rdb_channel_id
            # Wait for primary to abort the sync
            wait_for_condition 50 1000 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10
            # Replica should retry
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't retry after connection close"
            }            
        }
        $replica replicaof no one
        wait_for_condition 500 1000 {
            [s -1 rdb_bgsave_in_progress] eq 0
        } else {
            fail "Primary should abort sync"
        }
        test "Replica recover main-connection killed" {
            $replica replicaof $primary_host $primary_port
            # Wait for sync session to start
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }            

            $primary debug log "killing replica main connection"
            set replica_main_conn_id [get_client_id_by_last_cmd $primary "sync"]
            assert {$replica_main_conn_id != ""}
            set loglines [count_log_lines -1]
            $primary client kill id $replica_main_conn_id
            # Wait for primary to abort the sync
            wait_for_condition 50 1000 {
                [string match {*replicas_waiting_psync:0*} [$primary info replication]]
            } else {
                fail "Primary did not free repl buf block after sync failure"
            }
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10
            # Replica should retry
            wait_for_condition 500 1000 {
                [string match "*slave*,state=wait_bgsave*,type=rdb-channel*" [$primary info replication]] &&
                [string match "*slave*,state=bg_transfer*,type=main-channel*" [$primary info replication]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't retry after connection close"
            }    
        }
        stop_write_load $load_handle
    }
}
