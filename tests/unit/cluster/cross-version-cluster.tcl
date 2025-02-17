# Test cross version compatibility of cluster.
#
# Use minimal.conf to make sure we don't use any configs not supported on the old version.

# make sure the test infra won't use SELECT
set old_singledb $::singledb
set ::singledb 1

tags {external:skip needs:other-server cluster} {
    # To run this test use the `--other-server-path` parameter and pass in a compatible server path supporting
    # SendClusterMessage module API.
    #
    # ./runtest --single unit/cluster/cross-version-cluster --other-server-path tests/tmp/valkey-7-2/valkey-server

    # Test cross version compatibility of cluster module send message API.
    start_cluster 3 1 {tags {external:skip cluster} overrides {auto-failover-on-shutdown yes cluster-ping-interval 1000}} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]
        set primary_id [$primary cluster myid]

        start_server {config "minimal-cluster.conf" start-other-server 1 overrides {cluster-ping-interval 1000}} {
            # Add a replica of the old version to the cluster
            r cluster meet $primary_host $primary_port
            wait_for_cluster_propagation
            r cluster replicate $primary_id
            wait_for_cluster_state "ok"

            # Make sure the primary won't do the auto-failover.
            catch {$primary shutdown nosave}
            verify_log_message -1 "*Unable to perform auto failover on shutdown since there are legacy replicas*" 0
        }
    }
}

set ::singledb $old_singledb
