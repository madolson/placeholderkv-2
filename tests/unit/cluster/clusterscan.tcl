source tests/support/cluster.tcl

# Start a cluster
start_cluster 2 0 {tags {external:skip cluster} overrides {cluster-replica-no-failover yes}} {
    test "Test keys distributed to mutiple nodes are all hit during cluster scan" {
        # Cluster client handles redirection, so fill the cluster with 10000 keys
        set cluster [valkey_cluster [srv 0 host]:[srv 0 port]]
        set total_keys 10000
        for {set j 0} {$j < $total_keys} {incr j} {
            $cluster set $j foo
        }

        set key_count 0
        set result [$cluster clusterscan "0-0"]
        set cursor [lindex $result 0]
        while {$cursor != "0-0"} {
            set result [$cluster clusterscan $cursor]
            set cursor [lindex $result 0]
            set key_count [expr $key_count + [llength [lindex $result 1]]]
        }
        assert_equal $total_keys $key_count
    }
}

# Start a cluster with 1 primary
start_cluster 2 0 {tags {external:skip cluster} overrides {cluster-replica-no-failover yes}} {
    test "Test keys distributed to mutiple nodes are all hit during cluster scan" {
        # Cluster client handles redirection, so fill the cluster with 10000 keys
        set cluster [valkey_cluster [srv 0 host]:[srv 0 port]]
        set total_keys 10000
        for {set j 0} {$j < $total_keys} {incr j} {
            $cluster set $j foo
        }

        # Only do it for primary 0 because I'm lazy
        for {set j 0} {$j < 8192} {incr j} {
            puts "hi"
            set expected_count [r 0 cluster countkeysinslot $j]
            set result [r 0 scan "0" SLOT $j]
            set cursor [lindex $result 0]
            set key_count [llength [lindex $result 1]]
            while {$cursor != "0"} {
                set result [r 0 scan $cursor SLOT $j]
                set cursor [lindex $result 0]
                set key_count [expr $key_count + [llength [lindex $result 1]]]
            }
            assert_equal $expected_count $key_count
        }
    }
}
