#include "server.h"
#include "cluster.h"
#include "script.h"
#include "cluster_legacy.h"

/* General use-cases. */
void clusterSlotStatReset(int slot);
void clusterSlotStatResetAll(void);
int clusterSlotStatsEnabled(void);

/* cpu-usec metric. */
void clusterSlotStatsAddCpuDuration(client *c, ustime_t duration);
void clusterSlotStatsInvalidateSlotIfApplicable(scriptRunCtx *ctx);

/* network-bytes-in metric. */
void clusterSlotStatsAddNetworkBytesInForUserClient(client *c);
void clusterSlotStatsSetClusterMsgLength(uint32_t len);
void clusterSlotStatsResetClusterMsgLength(void);

/* network-bytes-out metric. */
void clusterSlotStatsAddNetworkBytesOutForSlot(int slot, unsigned long long net_bytes_out);
void clusterSlotStatsAddNetworkBytesOutForUserClient(client *c);
void clusterSlotStatsIncrNetworkBytesOutForReplication(long long len);
void clusterSlotStatsDecrNetworkBytesOutForReplication(long long len);
void clusterSlotStatsAddNetworkBytesOutForShardedPubSubInternalPropagation(client *c, int slot);
