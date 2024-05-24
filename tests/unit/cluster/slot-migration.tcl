proc get_open_slots {srv_idx} {
    set slots [dict get [cluster_get_myself $srv_idx] slots]
    if {[regexp {\[.*} $slots slots]} {
        set slots [regsub -all {[{}]} $slots ""]
        return $slots
    } else {
        return {}
    }
}

proc get_cluster_role {srv_idx} {
    set flags [dict get [cluster_get_myself $srv_idx] flags]
    set role [lindex $flags 1]
    return $role
}

proc wait_for_role {srv_idx role} {
    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    # wait for a gossip cycle for states to be propagated throughout the cluster
    after $node_timeout
    wait_for_condition 100 100 {
        [lindex [split [R $srv_idx ROLE] " "] 0] eq $role
    } else {
        fail "R $srv_idx didn't assume the replication $role in time"
    }
    wait_for_condition 100 100 {
        [get_cluster_role $srv_idx] eq $role
    } else {
        fail "R $srv_idx didn't assume the cluster $role in time"
    }
    wait_for_cluster_propagation
}

proc wait_for_slot_state {srv_idx pattern} {
    wait_for_condition 100 100 {
        [get_open_slots $srv_idx] eq $pattern
    } else {
        fail "incorrect slot state on R $srv_idx: expected $pattern; got [get_open_slots $srv_idx]"
    }
}

# Check if the server responds with "PONG"
proc check_server_response {server_id} {
    # Send a PING command and check if the response is "PONG"
    return [expr {[catch {R $server_id PING} result] == 0 && $result eq "PONG"}]
}

# restart a server and wait for it to come back online
proc restart_server_and_wait {server_id} {
    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set result [catch {R $server_id DEBUG RESTART [expr 3*$node_timeout]} err]

    # Check if the error is the expected "I/O error reading reply"
    if {$result != 0 && $err ne "I/O error reading reply"} {
        fail "Unexpected error restarting server $server_id: $err"
    }

    wait_for_condition 100 100 {
        [check_server_response $server_id] eq 1
    } else {
        fail "Server $server_id didn't come back online in time"
    }
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {

    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set R0_id [R 0 CLUSTER MYID]
    set R1_id [R 1 CLUSTER MYID]
    set R2_id [R 2 CLUSTER MYID]
    set R3_id [R 3 CLUSTER MYID]
    set R4_id [R 4 CLUSTER MYID]
    set R5_id [R 5 CLUSTER MYID]

    test "Slot migration states are replicated" {
        # Validate initial states
        assert_not_equal [get_open_slots 0] "\[609->-$R1_id\]"
        assert_not_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        assert_not_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_not_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        # Kick off the migration of slot 609 from R0 to R1
        assert_equal {OK} [R 0 CLUSTER SETSLOT 609 MIGRATING $R1_id]
        assert_equal {OK} [R 1 CLUSTER SETSLOT 609 IMPORTING $R0_id]
        # Validate that R0 is migrating slot 609 to R1
        assert_equal [get_open_slots 0] "\[609->-$R1_id\]"
        # Validate that R1 is importing slot 609 from R0 
        assert_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
    }

    test "Migration target is auto-updated after failover in target shard" {
        # Restart R1 to trigger an auto-failover to R4
        restart_server_and_wait 1
        # Wait for R1 to become a replica
        wait_for_role 1 slave
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R4_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R4_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
        # Restore R1's primaryship
        assert_equal {OK} [R 1 cluster failover]
        wait_for_role 1 master
        # Validate initial states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
    }

    test "Migration source is auto-updated after failover in source shard" {
        # Restart R0 to trigger an auto-failover to R3
        restart_server_and_wait 0
        # Wait for R0 to become a replica
        wait_for_role 0 slave
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R3_id\]"
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 4 "\[609-<-$R3_id\]"
        # Restore R0's primaryship
        assert_equal {OK} [R 0 cluster failover]
        wait_for_role 0 master
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"
    }

    test "Replica redirects key access in migrating slots" {
        # Validate initial states
        assert_equal [get_open_slots 0] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 1] "\[609-<-$R0_id\]"
        assert_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        catch {[R 3 get aga]} e
        assert_equal {MOVED} [lindex [split $e] 0]
        assert_equal {609} [lindex [split $e] 1]
    }

    test "New replica inherits migrating slot" {
        # Reset R3 to turn it into an empty node
        assert_equal [get_open_slots 3] "\[609->-$R1_id\]"
        assert_equal {OK} [R 3 CLUSTER RESET]
        assert_not_equal [get_open_slots 3] "\[609->-$R1_id\]"
        # Add R3 back as a replica of R0
        assert_equal {OK} [R 3 CLUSTER MEET [srv 0 "host"] [srv 0 "port"]]
        wait_for_role 0 master
        assert_equal {OK} [R 3 CLUSTER REPLICATE $R0_id]
        wait_for_role 3 slave
        # Validate that R3 now sees slot 609 open
        assert_equal [get_open_slots 3] "\[609->-$R1_id\]"
    }

    test "New replica inherits importing slot" {
        # Reset R4 to turn it into an empty node
        assert_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        assert_equal {OK} [R 4 CLUSTER RESET]
        assert_not_equal [get_open_slots 4] "\[609-<-$R0_id\]"
        # Add R4 back as a replica of R1
        assert_equal {OK} [R 4 CLUSTER MEET [srv -1 "host"] [srv -1 "port"]]
        wait_for_role 1 master
        assert_equal {OK} [R 4 CLUSTER REPLICATE $R1_id]
        wait_for_role 4 slave
        # Validate that R4 now sees slot 609 open
        assert_equal [get_open_slots 4] "\[609-<-$R0_id\]"
    }
}

proc create_empty_shard {p r} {
    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    assert_equal {OK} [R $p CLUSTER RESET]
    assert_equal {OK} [R $r CLUSTER RESET]
    assert_equal {OK} [R $p CLUSTER MEET [srv 0 "host"] [srv 0 "port"]]
    assert_equal {OK} [R $r CLUSTER MEET [srv 0 "host"] [srv 0 "port"]]
    wait_for_role $p master
    assert_equal {OK} [R $r CLUSTER REPLICATE [R $p CLUSTER MYID]]
    wait_for_role $r slave
    wait_for_role $p master
}

start_cluster 3 5 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {

    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set R0_id [R 0 CLUSTER MYID]
    set R1_id [R 1 CLUSTER MYID]
    set R2_id [R 2 CLUSTER MYID]
    set R3_id [R 3 CLUSTER MYID]
    set R4_id [R 4 CLUSTER MYID]
    set R5_id [R 5 CLUSTER MYID]

    create_empty_shard 6 7
    set R6_id [R 6 CLUSTER MYID]
    set R7_id [R 7 CLUSTER MYID]

    test "Empty-shard migration replicates slot importing states" {
        # Validate initial states
        assert_not_equal [get_open_slots 0] "\[609->-$R6_id\]"
        assert_not_equal [get_open_slots 6] "\[609-<-$R0_id\]"
        assert_not_equal [get_open_slots 3] "\[609->-$R6_id\]"
        assert_not_equal [get_open_slots 7] "\[609-<-$R0_id\]"
        # Kick off the migration of slot 609 from R0 to R6
        assert_equal {OK} [R 0 CLUSTER SETSLOT 609 MIGRATING $R6_id]
        assert_equal {OK} [R 6 CLUSTER SETSLOT 609 IMPORTING $R0_id]
        # Validate that R0 is migrating slot 609 to R6
        assert_equal [get_open_slots 0] "\[609->-$R6_id\]"
        # Validate that R6 is importing slot 609 from R0 
        assert_equal [get_open_slots 6] "\[609-<-$R0_id\]"
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R6_id\]"
        wait_for_slot_state 6 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R6_id\]"
        wait_for_slot_state 7 "\[609-<-$R0_id\]"
    }

    test "Empty-shard migration target is auto-updated after faiover in target shard" {
        wait_for_role 6 master
        # Restart R6 to trigger an auto-failover to R7
        restart_server_and_wait 6
        # Wait for R6 to become a replica
        wait_for_role 6 slave
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R7_id\]"
        wait_for_slot_state 6 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R7_id\]"
        wait_for_slot_state 7 "\[609-<-$R0_id\]"
        # Restore R6's primaryship
        assert_equal {OK} [R 6 cluster failover]
        wait_for_role 6 master
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R6_id\]"
        wait_for_slot_state 6 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R6_id\]"
        wait_for_slot_state 7 "\[609-<-$R0_id\]"
    }

    test "Empty-shard migration source is auto-updated after faiover in source shard" {
        wait_for_role 0 master
        # Restart R0 to trigger an auto-failover to R3
        restart_server_and_wait 0
        # Wait for R0 to become a replica
        wait_for_role 0 slave
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R6_id\]"
        wait_for_slot_state 6 "\[609-<-$R3_id\]"
        wait_for_slot_state 3 "\[609->-$R6_id\]"
        wait_for_slot_state 7 "\[609-<-$R3_id\]"
        # Restore R0's primaryship
        assert_equal {OK} [R 0 cluster failover]
        wait_for_role 0 master
        # Validate final states
        wait_for_slot_state 0 "\[609->-$R6_id\]"
        wait_for_slot_state 6 "\[609-<-$R0_id\]"
        wait_for_slot_state 3 "\[609->-$R6_id\]"
        wait_for_slot_state 7 "\[609-<-$R0_id\]"
    }
}

proc migrate_slot {from to slot} {
    set from_id [R $from CLUSTER MYID]
    set to_id [R $to CLUSTER MYID]
    assert_equal {OK} [R $from CLUSTER SETSLOT $slot MIGRATING $to_id]
    assert_equal {OK} [R $to CLUSTER SETSLOT $slot IMPORTING $from_id]
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {

    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set R0_id [R 0 CLUSTER MYID]
    set R1_id [R 1 CLUSTER MYID]
    set R2_id [R 2 CLUSTER MYID]
    set R3_id [R 3 CLUSTER MYID]
    set R4_id [R 4 CLUSTER MYID]
    set R5_id [R 5 CLUSTER MYID]

    test "Multiple slot migration states are replicated" {
        migrate_slot 0 1 13
        migrate_slot 0 1 7
        migrate_slot 0 1 17
        # Validate final states
        wait_for_slot_state 0 "\[7->-$R1_id\] \[13->-$R1_id\] \[17->-$R1_id\]"
        wait_for_slot_state 1 "\[7-<-$R0_id\] \[13-<-$R0_id\] \[17-<-$R0_id\]"
        wait_for_slot_state 3 "\[7->-$R1_id\] \[13->-$R1_id\] \[17->-$R1_id\]"
        wait_for_slot_state 4 "\[7-<-$R0_id\] \[13-<-$R0_id\] \[17-<-$R0_id\]"
    }

    test "New replica inherits multiple migrating slots" {
        # Reset R3 to turn it into an empty node
        assert_equal {OK} [R 3 CLUSTER RESET]
        # Add R3 back as a replica of R0
        assert_equal {OK} [R 3 CLUSTER MEET [srv 0 "host"] [srv 0 "port"]]
        wait_for_role 0 master
        assert_equal {OK} [R 3 CLUSTER REPLICATE $R0_id]
        wait_for_role 3 slave
        # Validate final states
        wait_for_slot_state 3 "\[7->-$R1_id\] \[13->-$R1_id\] \[17->-$R1_id\]"
    }

    test "Slot finalization succeeds on both primary and replicas" {
        assert_equal {OK} [R 1 CLUSTER SETSLOT 7 NODE $R1_id]
        wait_for_slot_state 1 "\[13-<-$R0_id\] \[17-<-$R0_id\]"
        wait_for_slot_state 4 "\[13-<-$R0_id\] \[17-<-$R0_id\]"
        assert_equal {OK} [R 1 CLUSTER SETSLOT 13 NODE $R1_id]
        wait_for_slot_state 1 "\[17-<-$R0_id\]"
        wait_for_slot_state 4 "\[17-<-$R0_id\]"
        assert_equal {OK} [R 1 CLUSTER SETSLOT 17 NODE $R1_id]
        wait_for_slot_state 1 ""
        wait_for_slot_state 4 ""
    }

}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {

    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    set R0_id [R 0 CLUSTER MYID]
    set R1_id [R 1 CLUSTER MYID]

    test "Slot is auto-claimed by target after source relinquishes ownership" {
        migrate_slot 0 1 609
        #Validate that R1 doesn't own slot 609
        catch {[R 1 get aga]} e
        assert_equal {MOVED} [lindex [split $e] 0]
        #Finalize the slot on the source first
        assert_equal {OK} [R 0 CLUSTER SETSLOT 609 NODE $R1_id]
        after $node_timeout
        #R1 should claim slot 609 since it is still importing slot 609
        #from R0 but R0 no longer owns this slot
        assert_equal {OK} [R 1 set aga foo]
    }
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000} } {
    set R1_id [R 1 CLUSTER MYID]

    test "CLUSTER SETSLOT with an explicit timeout" {
        # Pause the replica to simulate a failure
        pause_process [srv -3 pid]

        # Setslot with an explicit 1ms timeoout
        set start_time [clock milliseconds]
        catch {R 0 CLUSTER SETSLOT 609 MIGRATING $R1_id TIMEOUT 3000} e
        set end_time [clock milliseconds]
        set duration [expr {$end_time - $start_time}]

        # Assert that the execution time is greater than the default 2s timeout
        assert {$duration > 2000}

        # Setslot should fail with not enough good replicas to write after the timeout
        assert_equal {NOREPLICAS Not enough good replicas to write.} $e

        resume_process [srv -3 pid]
    }
}
