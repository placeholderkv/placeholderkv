#ifndef CLUSTER_LEGACY_H
#define CLUSTER_LEGACY_H

#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). */
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2  /* Fail report validity. */
#define CLUSTER_FAIL_UNDO_TIME_MULT 2        /* Undo fail if primary is back. */
#define CLUSTER_MF_PAUSE_MULT 2              /* Primary pause manual failover mult. */
#define CLUSTER_REPLICA_MIGRATION_DELAY 5000 /* Delay for replica migration. */

/* Reasons why a replica is not able to failover. */
#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD 1 /* seconds. */

/* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_HANDLE_FAILOVER (1 << 0)
#define CLUSTER_TODO_UPDATE_STATE (1 << 1)
#define CLUSTER_TODO_SAVE_CONFIG (1 << 2)
#define CLUSTER_TODO_FSYNC_CONFIG (1 << 3)
#define CLUSTER_TODO_HANDLE_MANUALFAILOVER (1 << 4)
#define CLUSTER_TODO_BROADCAST_ALL (1 << 5)

/* clusterLink encapsulates everything needed to talk with a remote node. */
typedef struct clusterLink {
    mstime_t ctime;                        /* Link creation time */
    connection *conn;                      /* Connection to remote node */
    list *send_msg_queue;                  /* List of messages to be sent */
    size_t head_msg_send_offset;           /* Number of bytes already sent of message at head of queue */
    unsigned long long send_msg_queue_mem; /* Memory in bytes used by message queue */
    char *rcvbuf;                          /* Packet reception buffer */
    size_t rcvbuf_len;                     /* Used size of rcvbuf */
    size_t rcvbuf_alloc;                   /* Allocated size of rcvbuf */
    clusterNode *node;                     /* Node related to this link. Initialized to NULL when unknown */
    int inbound;                           /* 1 if this link is an inbound link accepted from the related node */
} clusterLink;

/* Cluster node flags and macros. */
#define CLUSTER_NODE_PRIMARY (1 << 0)                      /* The node is a primary */
#define CLUSTER_NODE_REPLICA (1 << 1)                      /* The node is a replica */
#define CLUSTER_NODE_PFAIL (1 << 2)                        /* Failure? Need acknowledge */
#define CLUSTER_NODE_FAIL (1 << 3)                         /* The node is believed to be malfunctioning */
#define CLUSTER_NODE_MYSELF (1 << 4)                       /* This node is myself */
#define CLUSTER_NODE_HANDSHAKE (1 << 5)                    /* We have still to exchange the first ping */
#define CLUSTER_NODE_NOADDR (1 << 6)                       /* We don't know the address of this node */
#define CLUSTER_NODE_MEET (1 << 7)                         /* Send a MEET message to this node */
#define CLUSTER_NODE_MIGRATE_TO (1 << 8)                   /* Primary eligible for replica migration. */
#define CLUSTER_NODE_NOFAILOVER (1 << 9)                   /* Replica will not try to failover. */
#define CLUSTER_NODE_EXTENSIONS_SUPPORTED (1 << 10)        /* This node supports extensions. */
#define CLUSTER_NODE_LIGHT_HDR_PUBLISH_SUPPORTED (1 << 11) /* This node supports light message header for publish type. */
#define CLUSTER_NODE_LIGHT_HDR_MODULE_SUPPORTED (1 << 12)  /* This node supports light message header for module type. */
#define CLUSTER_NODE_NULL_NAME                                                                                         \
    "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000" \
    "\000\000\000\000\000\000\000\000\000\000\000\000"

#define nodeIsPrimary(n) ((n)->flags & CLUSTER_NODE_PRIMARY)
#define nodeIsReplica(n) ((n)->flags & CLUSTER_NODE_REPLICA)
#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeInMeetState(n) ((n)->flags & CLUSTER_NODE_MEET)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n) ((n)->flags & CLUSTER_NODE_NOFAILOVER)
#define nodeSupportsExtensions(n) ((n)->flags & CLUSTER_NODE_EXTENSIONS_SUPPORTED)
#define nodeSupportsLightMsgHdrForPubSub(n) ((n)->flags & CLUSTER_NODE_LIGHT_HDR_PUBLISH_SUPPORTED)
#define nodeSupportsLightMsgHdrForModule(n) ((n)->flags & CLUSTER_NODE_LIGHT_HDR_MODULE_SUPPORTED)
#define nodeInNormalState(n) (!((n)->flags & (CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_MEET | CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL)))

/* This structure represent elements of node->fail_reports. */
typedef struct clusterNodeFailReport {
    clusterNode *node; /* Node reporting the failure condition. */
    mstime_t time;     /* Time of the last report from this node. */
} clusterNodeFailReport;

/* Cluster messages header */

/* Message types.
 *
 * Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
#define CLUSTERMSG_TYPE_PING 0                  /* Ping */
#define CLUSTERMSG_TYPE_PONG 1                  /* Pong (reply to Ping) */
#define CLUSTERMSG_TYPE_MEET 2                  /* Meet "let's join" message */
#define CLUSTERMSG_TYPE_FAIL 3                  /* Mark node xxx as failing */
#define CLUSTERMSG_TYPE_PUBLISH 4               /* Pub/Sub Publish propagation */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */
#define CLUSTERMSG_TYPE_UPDATE 7                /* Another node slots configuration */
#define CLUSTERMSG_TYPE_MFSTART 8               /* Pause clients for manual failover */
#define CLUSTERMSG_TYPE_MODULE 9                /* Module cluster API message. */
#define CLUSTERMSG_TYPE_PUBLISHSHARD 10         /* Pub/Sub Publish shard propagation */
#define CLUSTERMSG_TYPE_COUNT 11                /* Total number of message types. */

#define CLUSTERMSG_LIGHT 0x8000 /* Modifier bit for message types that support light header */

#define CLUSTERMSG_MODIFIER_MASK (CLUSTERMSG_LIGHT) /* Modifier mask for header types. (if we add more in the future) */

/* We check for the modifier bit to determine if the message is sent using light header.*/
#define IS_LIGHT_MESSAGE(type) ((type) & CLUSTERMSG_LIGHT)

/* Types of header supported over the cluster bus. */
typedef enum {
    CLUSTERMSG_HDR_NORMAL = 0, /* This corresponds to `clusterMsg` struct. */
    CLUSTERMSG_HDR_LIGHT,      /* This corresponds to `clusterMsgLight` struct. */
    CLUSTERMSG_HDR_NUM,        /* Overall count of header type supported. */
} clusterMsgHdrType;

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */
typedef struct {
    char nodename[CLUSTER_NAMELEN];
    uint32_t ping_sent;
    uint32_t pong_received;
    char ip[NET_IP_STR_LEN]; /* IP address last time it was seen */
    uint16_t port;           /* primary port last time it was seen */
    uint16_t cport;          /* cluster port last time it was seen */
    uint16_t flags;          /* node->flags copy */
    uint16_t pport;          /* secondary port last time it was seen */
    uint16_t notused1;
} clusterMsgDataGossip;

typedef struct {
    char nodename[CLUSTER_NAMELEN];
} clusterMsgDataFail;

typedef struct {
    uint32_t channel_len;
    uint32_t message_len;
    unsigned char bulk_data[8]; /* 8 bytes just as placeholder. */
} clusterMsgDataPublish;

typedef struct {
    uint64_t configEpoch;                   /* Config epoch of the specified instance. */
    char nodename[CLUSTER_NAMELEN];         /* Name of the slots owner. */
    unsigned char slots[CLUSTER_SLOTS / 8]; /* Slots bitmap. */
} clusterMsgDataUpdate;

typedef struct {
    uint64_t module_id;         /* ID of the sender module. */
    uint32_t len;               /* ID of the sender module. */
    uint8_t type;               /* Type from 0 to 255. */
    unsigned char bulk_data[3]; /* 3 bytes just as placeholder. */
} clusterMsgModule;

/* The cluster supports optional extension messages that can be sent
 * along with ping/pong/meet messages to give additional info in a
 * consistent manner. */
typedef enum {
    CLUSTERMSG_EXT_TYPE_HOSTNAME,
    CLUSTERMSG_EXT_TYPE_HUMAN_NODENAME,
    CLUSTERMSG_EXT_TYPE_FORGOTTEN_NODE,
    CLUSTERMSG_EXT_TYPE_SHARDID,
    CLUSTERMSG_EXT_TYPE_CLIENT_IPV4,
    CLUSTERMSG_EXT_TYPE_CLIENT_IPV6,
} clusterMsgPingtypes;

/* Helper function for making sure extensions are eight byte aligned. */
#define EIGHT_BYTE_ALIGN(size) ((((size) + 7) / 8) * 8)

typedef struct {
    char hostname[1]; /* The announced hostname, ends with \0. */
} clusterMsgPingExtHostname;

typedef struct {
    char human_nodename[1]; /* The announced nodename, ends with \0. */
} clusterMsgPingExtHumanNodename;

typedef struct {
    char name[CLUSTER_NAMELEN]; /* Node name. */
    uint64_t ttl;               /* Remaining time to blacklist the node, in seconds. */
} clusterMsgPingExtForgottenNode;

static_assert(sizeof(clusterMsgPingExtForgottenNode) % 8 == 0, "");

typedef struct {
    char shard_id[CLUSTER_NAMELEN]; /* The shard_id, 40 bytes fixed. */
} clusterMsgPingExtShardId;

typedef struct {
    char announce_client_ipv4[1]; /* Announced client IPv4, ends with \0. */
} clusterMsgPingExtClientIpV4;

typedef struct {
    char announce_client_ipv6[1]; /* Announced client IPv6, ends with \0. */
} clusterMsgPingExtClientIpV6;

typedef struct {
    uint32_t length; /* Total length of this extension message (including this header) */
    uint16_t type;   /* Type of this extension message (see clusterMsgPingtypes) */
    uint16_t unused; /* 16 bits of padding to make this structure 8 byte aligned. */
    union {
        clusterMsgPingExtHostname hostname;
        clusterMsgPingExtHumanNodename human_nodename;
        clusterMsgPingExtForgottenNode forgotten_node;
        clusterMsgPingExtShardId shard_id;
        clusterMsgPingExtClientIpV4 announce_client_ipv4;
        clusterMsgPingExtClientIpV6 announce_client_ipv6;
    } ext[]; /* Actual extension information, formatted so that the data is 8
              * byte aligned, regardless of its content. */
} clusterMsgPingExt;

union clusterMsgData {
    /* PING, MEET and PONG */
    struct {
        /* Array of N clusterMsgDataGossip structures */
        clusterMsgDataGossip gossip[1];
        /* Extension data that can optionally be sent for ping/meet/pong
         * messages. We can't explicitly define them here though, since
         * the gossip array isn't the real length of the gossip data. */
    } ping;

    /* FAIL */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* PUBLISH */
    struct {
        clusterMsgDataPublish msg;
    } publish;

    /* UPDATE */
    struct {
        clusterMsgDataUpdate nodecfg;
    } update;

    /* MODULE */
    struct {
        clusterMsgModule msg;
    } module;
};

#define CLUSTER_PROTO_VER 1 /* Cluster bus protocol version. */

typedef struct {
    char sig[4];                  /* Signature "RCmb" (Cluster message bus). */
    uint32_t totlen;              /* Total length of this message */
    uint16_t ver;                 /* Protocol version, currently set to CLUSTER_PROTO_VER. */
    uint16_t port;                /* Primary port number (TCP or TLS). */
    uint16_t type;                /* Message type */
    uint16_t count;               /* Number of gossip sections. */
    uint64_t currentEpoch;        /* The epoch accordingly to the sending node. */
    uint64_t configEpoch;         /* The config epoch if it's a primary, or the last
                                     epoch advertised by its primary if it is a
                                     replica. */
    uint64_t offset;              /* Primary replication offset if node is a primary or
                                     processed replication offset if node is a replica. */
    char sender[CLUSTER_NAMELEN]; /* Name of the sender node */
    unsigned char myslots[CLUSTER_SLOTS / 8];
    char replicaof[CLUSTER_NAMELEN];
    char myip[NET_IP_STR_LEN]; /* Sender IP, if not all zeroed. */
    uint16_t extensions;       /* Number of extensions sent along with this packet. */
    char notused1[30];         /* 30 bytes reserved for future usage. */
    uint16_t pport;            /* Secondary port number: if primary port is TCP port, this is
                                  TLS port, and if primary port is TLS port, this is TCP port.*/
    uint16_t cport;            /* Sender TCP cluster bus port */
    uint16_t flags;            /* Sender node flags */
    unsigned char state;       /* Cluster state from the POV of the sender */
    unsigned char mflags[3];   /* Message flags: CLUSTERMSG_FLAG[012]_... */
    union clusterMsgData data;
} clusterMsg;

/* clusterMsg defines the gossip wire protocol exchanged among cluster
 * members, which can be running different versions of server bits,
 * especially during cluster rolling upgrades.
 *
 * Therefore, fields in this struct should remain at the same offset from
 * release to release. The static asserts below ensures that incompatible
 * changes in clusterMsg be caught at compile time.
 */

static_assert(offsetof(clusterMsg, sig) == 0, "unexpected field offset");
static_assert(offsetof(clusterMsg, totlen) == 4, "unexpected field offset");
static_assert(offsetof(clusterMsg, ver) == 8, "unexpected field offset");
static_assert(offsetof(clusterMsg, port) == 10, "unexpected field offset");
static_assert(offsetof(clusterMsg, type) == 12, "unexpected field offset");
static_assert(offsetof(clusterMsg, count) == 14, "unexpected field offset");
static_assert(offsetof(clusterMsg, currentEpoch) == 16, "unexpected field offset");
static_assert(offsetof(clusterMsg, configEpoch) == 24, "unexpected field offset");
static_assert(offsetof(clusterMsg, offset) == 32, "unexpected field offset");
static_assert(offsetof(clusterMsg, sender) == 40, "unexpected field offset");
static_assert(offsetof(clusterMsg, myslots) == 80, "unexpected field offset");
static_assert(offsetof(clusterMsg, replicaof) == 2128, "unexpected field offset");
static_assert(offsetof(clusterMsg, myip) == 2168, "unexpected field offset");
static_assert(offsetof(clusterMsg, extensions) == 2214, "unexpected field offset");
static_assert(offsetof(clusterMsg, notused1) == 2216, "unexpected field offset");
static_assert(offsetof(clusterMsg, pport) == 2246, "unexpected field offset");
static_assert(offsetof(clusterMsg, cport) == 2248, "unexpected field offset");
static_assert(offsetof(clusterMsg, flags) == 2250, "unexpected field offset");
static_assert(offsetof(clusterMsg, state) == 2252, "unexpected field offset");
static_assert(offsetof(clusterMsg, mflags) == 2253, "unexpected field offset");
static_assert(offsetof(clusterMsg, data) == 2256, "unexpected field offset");

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg) - sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
#define CLUSTERMSG_FLAG0_PAUSED (1 << 0)   /* Primary paused for manual failover. */
#define CLUSTERMSG_FLAG0_FORCEACK (1 << 1) /* Give ACK to AUTH_REQUEST even if \
                                              primary is up. */
#define CLUSTERMSG_FLAG0_EXT_DATA (1 << 2) /* Message contains extension data */

typedef struct {
    char sig[4];     /* Signature "RCmb" (Cluster message bus). */
    uint32_t totlen; /* Total length of this message */
    uint16_t ver;    /* Protocol version, currently set to CLUSTER_PROTO_VER. */
    uint16_t notused1;
    uint16_t type; /* Message type */
    uint16_t notused2;
    union clusterMsgData data;
} clusterMsgLight;

static_assert(offsetof(clusterMsgLight, sig) == offsetof(clusterMsg, sig), "unexpected field offset");
static_assert(offsetof(clusterMsgLight, totlen) == offsetof(clusterMsg, totlen), "unexpected field offset");
static_assert(offsetof(clusterMsgLight, ver) == offsetof(clusterMsg, ver), "unexpected field offset");
static_assert(offsetof(clusterMsgLight, notused1) == offsetof(clusterMsg, port), "unexpected field offset");
static_assert(offsetof(clusterMsgLight, type) == offsetof(clusterMsg, type), "unexpected field offset");
static_assert(offsetof(clusterMsgLight, notused2) == offsetof(clusterMsg, count), "unexpected field offset");
static_assert(offsetof(clusterMsgLight, data) == 16, "unexpected field offset");

#define CLUSTERMSG_LIGHT_MIN_LEN (sizeof(clusterMsgLight) - sizeof(union clusterMsgData))

struct _clusterNode {
    mstime_t ctime;                         /* Node object creation time. */
    char name[CLUSTER_NAMELEN];             /* Node name, hex string, sha1-size */
    char shard_id[CLUSTER_NAMELEN];         /* shard id, hex string, sha1-size */
    int flags;                              /* CLUSTER_NODE_... */
    uint64_t configEpoch;                   /* Last configEpoch observed for this node */
    unsigned char slots[CLUSTER_SLOTS / 8]; /* slots handled by this node */
    uint16_t *slot_info_pairs;              /* Slots info represented as (start/end) pair (consecutive index). */
    int slot_info_pairs_count;              /* Used number of slots in slot_info_pairs */
    int numslots;                           /* Number of slots handled by this node */
    int num_replicas;                       /* Number of replica nodes, if this is a primary */
    clusterNode **replicas;                 /* pointers to replica nodes */
    clusterNode *replicaof;                 /* pointer to the primary node. Note that it
                                             may be NULL even if the node is a replica
                                             if we don't have the primary node in our
                                             tables. */
    unsigned long long last_in_ping_gossip; /* The number of the last carried in the ping gossip section */
    mstime_t ping_sent;                     /* Unix time we sent latest ping */
    mstime_t pong_received;                 /* Unix time we received the pong */
    mstime_t data_received;                 /* Unix time we received any data */
    mstime_t meet_sent;                     /* Unix time we sent latest meet packet */
    mstime_t fail_time;                     /* Unix time when FAIL flag was set */
    mstime_t repl_offset_time;              /* Unix time we received offset for this node */
    mstime_t orphaned_time;                 /* Starting time of orphaned primary condition */
    mstime_t inbound_link_freed_time;       /* Last time we freed the inbound link for this node.
                                               If it was never freed, it is the same as ctime */
    long long repl_offset;                  /* Last known repl offset for this node. */
    char ip[NET_IP_STR_LEN];                /* Latest known IP address of this node */
    sds announce_client_ipv4;               /* IPv4 for clients only. */
    sds announce_client_ipv6;               /* IPv6 for clients only. */
    sds hostname;                           /* The known hostname for this node */
    sds human_nodename;                     /* The known human readable nodename for this node */
    int tcp_port;                           /* Latest known clients TCP port. */
    int tls_port;                           /* Latest known clients TLS port */
    int cport;                              /* Latest known cluster port of this node. */
    clusterLink *link;                      /* TCP/IP link established toward this node */
    clusterLink *inbound_link;              /* TCP/IP link accepted from this node */
    list *fail_reports;                     /* List of nodes signaling this as failing */
    int is_node_healthy;                    /* Boolean indicating the cached node health.
                                               Update with updateAndCountChangedNodeHealth(). */
};

/* Struct used for storing slot statistics. */
typedef struct slotStat {
    uint64_t cpu_usec;
    uint64_t network_bytes_in;
    uint64_t network_bytes_out;
} slotStat;

struct clusterState {
    clusterNode *myself; /* This node */
    uint64_t currentEpoch;
    int state;              /* CLUSTER_OK, CLUSTER_FAIL, ... */
    int fail_reason;        /* Why the cluster state changes to fail. */
    int size;               /* Num of primary nodes with at least one slot */
    dict *nodes;            /* Hash table of name -> clusterNode structures */
    dict *shards;           /* Hash table of shard_id -> list (of nodes) structures */
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];
    clusterNode *importing_slots_from[CLUSTER_SLOTS];
    clusterNode *slots[CLUSTER_SLOTS];
    /* The following fields are used to take the replica state on elections. */
    mstime_t failover_auth_time;      /* Time of previous or next election. */
    int failover_auth_count;          /* Number of votes received so far. */
    int failover_auth_sent;           /* True if we already asked for votes. */
    int failover_auth_rank;           /* This replica rank for current auth request. */
    int failover_failed_primary_rank; /* The rank of this instance in the context of all failed primary list. */
    uint64_t failover_auth_epoch;     /* Epoch of the current election. */
    int cant_failover_reason;         /* Why a replica is currently not able to
                                       * failover. See the CANT_FAILOVER_* macros. */
    /* Manual failover state in common. */
    mstime_t mf_end; /* Manual failover time limit (ms unixtime).
                        It is zero if there is no MF in progress. */
    /* Manual failover state of primary. */
    clusterNode *mf_replica; /* replica performing the manual failover. */
    /* Manual failover state of replica. */
    long long mf_primary_offset; /* Primary offset the replica needs to start MF
                                   or -1 if still not received. */
    int mf_can_start;            /* If non-zero signal that the manual failover
                                    can start requesting primary vote. */
    /* The following fields are used by primaries to take state on elections. */
    uint64_t lastVoteEpoch; /* Epoch of the last vote granted. */
    int todo_before_sleep;  /* Things to do in clusterBeforeSleep(). */
    /* Stats */
    /* Messages received and sent by type. */
    long long stats_bus_messages_sent[CLUSTERMSG_TYPE_COUNT];
    long long stats_bus_messages_received[CLUSTERMSG_TYPE_COUNT];
    long long stats_pfail_nodes;                                 /* Number of nodes in PFAIL status,
                                                                    excluding nodes without address. */
    unsigned long long stat_cluster_links_buffer_limit_exceeded; /* Total number of cluster links freed due to exceeding
                                                                    buffer limit */

    /* Bit map for slots that are no longer claimed by the owner in cluster PING
     * messages. During slot migration, the owner will stop claiming the slot after
     * the ownership transfer. Set the bit corresponding to the slot when a node
     * stops claiming the slot. This prevents spreading incorrect information (that
     * source still owns the slot) using UPDATE messages. */
    unsigned char owner_not_claiming_slot[CLUSTER_SLOTS / 8];
    /* Struct used for storing slot statistics, for all slots owned by the current shard. */
    slotStat slot_stats[CLUSTER_SLOTS];
};

#endif // CLUSTER_LEGACY_H
