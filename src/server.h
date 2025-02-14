/*
 * Copyright (c) 2009-2012, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef VALKEY_H
#define VALKEY_H

#include "fmacros.h"
#include "config.h"
#include "solarisfixes.h"
#include "rio.h"
#include "commands.h"
#include "allocator_defrag.h"

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <signal.h>

#ifdef HAVE_LIBSYSTEMD
#include <systemd/sd-daemon.h>
#endif

#ifndef static_assert
#define static_assert(expr, lit) extern char __static_assert_failure[(expr) ? 1 : -1]
#endif

typedef long long mstime_t; /* millisecond time type. */
typedef long long ustime_t; /* microsecond time type. */

#include "ae.h"         /* Event driven programming library */
#include "sds.h"        /* Dynamic safe strings */
#include "dict.h"       /* Hash tables (old implementation) */
#include "hashtable.h"  /* Hash tables (new implementation) */
#include "kvstore.h"    /* Slot-based hash table */
#include "adlist.h"     /* Linked lists */
#include "zmalloc.h"    /* total memory usage aware version of malloc/free */
#include "anet.h"       /* Networking the easy way */
#include "version.h"    /* Version macro */
#include "util.h"       /* Misc functions useful in many places */
#include "latency.h"    /* Latency monitor API */
#include "sparkline.h"  /* ASCII graphs API */
#include "quicklist.h"  /* Lists are encoded as linked lists of
                           N-elements flat arrays */
#include "rax.h"        /* Radix tree */
#include "connection.h" /* Connection abstraction */
#include "memory_prefetch.h"

#define dismissMemory zmadvise_dontneed

#define VALKEYMODULE_CORE 1
typedef struct serverObject robj;
#include "valkeymodule.h" /* Modules API defines. */

/* Following includes allow test functions to be called from main() */
#include "zipmap.h"
#include "ziplist.h" /* Compact list data structure */
#include "sha1.h"
#include "endianconv.h"
#include "crc64.h"

struct hdr_histogram;

/* helpers */
#define numElements(x) (sizeof(x) / sizeof((x)[0]))

/* min/max */
#undef min
#undef max
#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))

/* Get the pointer of the outer struct from a member address */
#define server_member2struct(struct_name, member_name, member_addr) \
    ((struct_name *)((char *)member_addr - offsetof(struct_name, member_name)))

/* Error codes */
#define C_OK 0
#define C_ERR -1
#define C_RETRY -2

/* Static server configuration */
#define CONFIG_DEFAULT_HZ 10 /* Time interrupt calls/sec. */
#define CONFIG_MIN_HZ 1
#define CONFIG_MAX_HZ 500
#define CRON_DBS_PER_CALL 16
#define CRON_DICTS_PER_DB 16
#define NET_MAX_WRITES_PER_EVENT (1024 * 64)
#define PROTO_SHARED_SELECT_CMDS 10
#define OBJ_SHARED_INTEGERS 10000
#define OBJ_SHARED_BULKHDR_LEN 32
#define OBJ_SHARED_HDR_STRLEN(_len_) (((_len_) < 10) ? 4 : 5) /* see shared.mbulkhdr etc. */
#define LOG_MAX_LEN 1024                                      /* Default maximum length of syslog messages.*/
#define AOF_REWRITE_ITEMS_PER_CMD 64
#define AOF_ANNOTATION_LINE_MAX_LEN 1024
#define CONFIG_RUN_ID_SIZE 40
#define RDB_EOF_MARK_SIZE 40
#define CONFIG_REPL_BACKLOG_MIN_SIZE (1024 * 16) /* 16k */
#define CONFIG_BGSAVE_RETRY_DELAY 5              /* Wait a few secs before trying again. */
#define CONFIG_DEFAULT_PID_FILE "/var/run/valkey.pid"
#define CONFIG_DEFAULT_BINDADDR_COUNT 2
#define CONFIG_DEFAULT_BINDADDR {"*", "-::*"}
#define NET_HOST_STR_LEN 256                          /* Longest valid hostname */
#define NET_IP_STR_LEN 46                             /* INET6_ADDRSTRLEN is 46, but we need to be sure */
#define NET_ADDR_STR_LEN (NET_IP_STR_LEN + 32)        /* Must be enough for ip:port */
#define NET_HOST_PORT_STR_LEN (NET_HOST_STR_LEN + 32) /* Must be enough for hostname:port */
#define CONFIG_BINDADDR_MAX 16
#define CONFIG_MIN_RESERVED_FDS 32
#define CONFIG_DEFAULT_PROC_TITLE_TEMPLATE "{title} {listen-addr} {server-mode}"
#define DEFAULT_WAIT_BEFORE_RDB_CLIENT_FREE 60      /* Grace period in seconds for replica main \
                                                     * channel to establish psync. */
#define LOADING_PROCESS_EVENTS_INTERVAL_DEFAULT 100 /* Default: 0.1 seconds */
#if !defined(DEBUG_FORCE_DEFRAG)
#define CONFIG_ACTIVE_DEFRAG_DEFAULT 0
#else
#define CONFIG_ACTIVE_DEFRAG_DEFAULT 1
#endif

/* Bucket sizes for client eviction pools. Each bucket stores clients with
 * memory usage of up to twice the size of the bucket below it. */
#define CLIENT_MEM_USAGE_BUCKET_MIN_LOG 15 /* Bucket sizes start at up to 32KB (2^15) */
#define CLIENT_MEM_USAGE_BUCKET_MAX_LOG 33 /* Bucket for largest clients: sizes above 4GB (2^32) */
#define CLIENT_MEM_USAGE_BUCKETS (1 + CLIENT_MEM_USAGE_BUCKET_MAX_LOG - CLIENT_MEM_USAGE_BUCKET_MIN_LOG)

#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

/* Children process will exit with this status code to signal that the
 * process terminated without an error: this is useful in order to kill
 * a saving child (RDB or AOF one), without triggering in the parent the
 * write protection that is normally turned on on write errors.
 * Usually children that are terminated with SIGUSR1 will exit with this
 * special code. */
#define SERVER_CHILD_NOERROR_RETVAL 255

/* Reading copy-on-write info is sometimes expensive and may slow down child
 * processes that report it continuously. We measure the cost of obtaining it
 * and hold back additional reading based on this factor. */
#define CHILD_COW_DUTY_CYCLE 100

/* When child process is performing write to connset it iterates on the set
 * writing a chunk of the available data to send on each connection.
 * This constant defines the maximal size of the chunk to use. */
#define RIO_CONNSET_WRITE_MAX_CHUNK_SIZE 16384

/* Instantaneous metrics tracking. */
#define STATS_METRIC_SAMPLES 16               /* Number of samples per metric. */
#define STATS_METRIC_COMMAND 0                /* Number of commands executed. */
#define STATS_METRIC_NET_INPUT 1              /* Bytes read to network. */
#define STATS_METRIC_NET_OUTPUT 2             /* Bytes written to network. */
#define STATS_METRIC_NET_INPUT_REPLICATION 3  /* Bytes read to network during replication. */
#define STATS_METRIC_NET_OUTPUT_REPLICATION 4 /* Bytes written to network during replication. */
#define STATS_METRIC_EL_CYCLE 5               /* Number of eventloop cycled. */
#define STATS_METRIC_EL_DURATION 6            /* Eventloop duration. */
#define STATS_METRIC_COUNT 7

/* Protocol and I/O related defines */
#define PROTO_IOBUF_LEN (1024 * 16)         /* Generic I/O buffer size */
#define PROTO_REPLY_CHUNK_BYTES (16 * 1024) /* 16k output buffer */
#define PROTO_INLINE_MAX_SIZE (1024 * 64)   /* Max size of inline reads */
#define PROTO_MBULK_BIG_ARG (1024 * 32)
#define PROTO_RESIZE_THRESHOLD (1024 * 32)     /* Threshold for determining whether to resize query buffer */
#define PROTO_REPLY_MIN_BYTES (1024)           /* the lower limit on reply buffer size */
#define REDIS_AUTOSYNC_BYTES (1024 * 1024 * 4) /* Sync file every 4MB. */

#define REPLY_BUFFER_DEFAULT_PEAK_RESET_TIME 5000 /* 5 seconds */

/* When configuring the server eventloop, we setup it so that the total number
 * of file descriptors we can handle are server.maxclients + RESERVED_FDS +
 * a few more to stay safe. Since RESERVED_FDS defaults to 32, we add 96
 * in order to make sure of not over provisioning more than 128 fds. */
#define CONFIG_FDSET_INCR (CONFIG_MIN_RESERVED_FDS + 96)

/* OOM Score Adjustment classes. */
#define CONFIG_OOM_PRIMARY 0
#define CONFIG_OOM_REPLICA 1
#define CONFIG_OOM_BGCHILD 2
#define CONFIG_OOM_COUNT 3

extern int configOOMScoreAdjValuesDefaults[CONFIG_OOM_COUNT];

/* Command flags. Please check the definition of struct serverCommand in this file
 * for more information about the meaning of every flag. */
#define CMD_WRITE (1ULL << 0)
#define CMD_READONLY (1ULL << 1)
#define CMD_DENYOOM (1ULL << 2)
#define CMD_MODULE (1ULL << 3) /* Command exported by module. */
#define CMD_ADMIN (1ULL << 4)
#define CMD_PUBSUB (1ULL << 5)
#define CMD_NOSCRIPT (1ULL << 6)
#define CMD_BLOCKING (1ULL << 8) /* Has potential to block. */
#define CMD_LOADING (1ULL << 9)
#define CMD_STALE (1ULL << 10)
#define CMD_SKIP_MONITOR (1ULL << 11)
#define CMD_SKIP_COMMANDLOG (1ULL << 12)
#define CMD_ASKING (1ULL << 13)
#define CMD_FAST (1ULL << 14)
#define CMD_NO_AUTH (1ULL << 15)
#define CMD_MAY_REPLICATE (1ULL << 16)
#define CMD_SENTINEL (1ULL << 17)
#define CMD_ONLY_SENTINEL (1ULL << 18)
#define CMD_NO_MANDATORY_KEYS (1ULL << 19)
#define CMD_PROTECTED (1ULL << 20)
#define CMD_MODULE_GETKEYS (1ULL << 21)    /* Use the modules getkeys interface. */
#define CMD_MODULE_NO_CLUSTER (1ULL << 22) /* Deny on Cluster. */
#define CMD_NO_ASYNC_LOADING (1ULL << 23)
#define CMD_NO_MULTI (1ULL << 24)
#define CMD_MOVABLE_KEYS (1ULL << 25) /* The legacy range spec doesn't cover all keys. \
                                       * Populated by populateCommandLegacyRangeSpec. */
#define CMD_ALLOW_BUSY ((1ULL << 26))
#define CMD_MODULE_GETCHANNELS (1ULL << 27) /* Use the modules getchannels interface. */
#define CMD_TOUCHES_ARBITRARY_KEYS (1ULL << 28)
/* Command flags. Please don't forget to add command flag documentation in struct
 * serverCommand in this file. */

/* Command flags that describe ACLs categories. */
#define ACL_CATEGORY_KEYSPACE (1ULL << 0)
#define ACL_CATEGORY_READ (1ULL << 1)
#define ACL_CATEGORY_WRITE (1ULL << 2)
#define ACL_CATEGORY_SET (1ULL << 3)
#define ACL_CATEGORY_SORTEDSET (1ULL << 4)
#define ACL_CATEGORY_LIST (1ULL << 5)
#define ACL_CATEGORY_HASH (1ULL << 6)
#define ACL_CATEGORY_STRING (1ULL << 7)
#define ACL_CATEGORY_BITMAP (1ULL << 8)
#define ACL_CATEGORY_HYPERLOGLOG (1ULL << 9)
#define ACL_CATEGORY_GEO (1ULL << 10)
#define ACL_CATEGORY_STREAM (1ULL << 11)
#define ACL_CATEGORY_PUBSUB (1ULL << 12)
#define ACL_CATEGORY_ADMIN (1ULL << 13)
#define ACL_CATEGORY_FAST (1ULL << 14)
#define ACL_CATEGORY_SLOW (1ULL << 15)
#define ACL_CATEGORY_BLOCKING (1ULL << 16)
#define ACL_CATEGORY_DANGEROUS (1ULL << 17)
#define ACL_CATEGORY_CONNECTION (1ULL << 18)
#define ACL_CATEGORY_TRANSACTION (1ULL << 19)
#define ACL_CATEGORY_SCRIPTING (1ULL << 20)

/* Key-spec flags *
 * -------------- */
/* The following refer what the command actually does with the value or metadata
 * of the key, and not necessarily the user data or how it affects it.
 * Each key-spec may must have exactly one of these. Any operation that's not
 * distinctly deletion, overwrite or read-only would be marked as RW. */
#define CMD_KEY_RO (1ULL << 0) /* Read-Only - Reads the value of the key, but \
                                * doesn't necessarily returns it. */
#define CMD_KEY_RW (1ULL << 1) /* Read-Write - Modifies the data stored in the \
                                * value of the key or its metadata. */
#define CMD_KEY_OW (1ULL << 2) /* Overwrite - Overwrites the data stored in \
                                * the value of the key. */
#define CMD_KEY_RM (1ULL << 3) /* Deletes the key. */
/* The following refer to user data inside the value of the key, not the metadata
 * like LRU, type, cardinality. It refers to the logical operation on the user's
 * data (actual input strings / TTL), being used / returned / copied / changed,
 * It doesn't refer to modification or returning of metadata (like type, count,
 * presence of data). Any write that's not INSERT or DELETE, would be an UPDATE.
 * Each key-spec may have one of the writes with or without access, or none: */
#define CMD_KEY_ACCESS (1ULL << 4) /* Returns, copies or uses the user data from \
                                    * the value of the key. */
#define CMD_KEY_UPDATE (1ULL << 5) /* Updates data to the value, new value may \
                                    * depend on the old value. */
#define CMD_KEY_INSERT (1ULL << 6) /* Adds data to the value with no chance of \
                                    * modification or deletion of existing data. */
#define CMD_KEY_DELETE (1ULL << 7) /* Explicitly deletes some content \
                                    * from the value of the key. */
/* Other flags: */
#define CMD_KEY_NOT_KEY (1ULL << 8)         /* A 'fake' key that should be routed \
                                             * like a key in cluster mode but is  \
                                             * excluded from other key checks. */
#define CMD_KEY_INCOMPLETE (1ULL << 9)      /* Means that the keyspec might not point \
                                             * out to all keys it should cover */
#define CMD_KEY_VARIABLE_FLAGS (1ULL << 10) /* Means that some keys might have \
                                             * different flags depending on arguments */

/* Key flags for when access type is unknown */
#define CMD_KEY_FULL_ACCESS (CMD_KEY_RW | CMD_KEY_ACCESS | CMD_KEY_UPDATE)

/* Key flags for how key is removed */
#define DB_FLAG_KEY_NONE 0
#define DB_FLAG_KEY_DELETED (1ULL << 0)
#define DB_FLAG_KEY_EXPIRED (1ULL << 1)
#define DB_FLAG_KEY_EVICTED (1ULL << 2)
#define DB_FLAG_KEY_OVERWRITE (1ULL << 3)

/* Channel flags share the same flag space as the key flags */
#define CMD_CHANNEL_PATTERN (1ULL << 11)     /* The argument is a channel pattern */
#define CMD_CHANNEL_SUBSCRIBE (1ULL << 12)   /* The command subscribes to channels */
#define CMD_CHANNEL_UNSUBSCRIBE (1ULL << 13) /* The command unsubscribes to channels */
#define CMD_CHANNEL_PUBLISH (1ULL << 14)     /* The command publishes to channels. */

/* AOF states */
#define AOF_OFF 0          /* AOF is off */
#define AOF_ON 1           /* AOF is on */
#define AOF_WAIT_REWRITE 2 /* AOF waits rewrite to start appending */

/* AOF return values for loadAppendOnlyFiles() and loadSingleAppendOnlyFile() */
#define AOF_OK 0
#define AOF_NOT_EXIST 1
#define AOF_EMPTY 2
#define AOF_OPEN_ERR 3
#define AOF_FAILED 4
#define AOF_TRUNCATED 5

/* RDB return values for rdbLoad. */
#define RDB_OK 0
#define RDB_NOT_EXIST 1 /* RDB file doesn't exist. */
#define RDB_FAILED 2    /* Failed to load the RDB file. */

/* Command doc flags */
#define CMD_DOC_NONE 0
#define CMD_DOC_DEPRECATED (1 << 0) /* Command is deprecated */
#define CMD_DOC_SYSCMD (1 << 1)     /* System (internal) command */

/* Client capabilities */
#define CLIENT_CAPA_REDIRECT (1 << 0) /* Indicate that the client can handle redirection */

/* Client block type (btype field in client structure)
 * if CLIENT_BLOCKED flag is set. */
typedef enum blocking_type {
    BLOCKED_NONE,     /* Not blocked, no CLIENT_BLOCKED flag set. */
    BLOCKED_LIST,     /* BLPOP & co. */
    BLOCKED_WAIT,     /* WAIT for synchronous replication. */
    BLOCKED_MODULE,   /* Blocked by a loadable module. */
    BLOCKED_STREAM,   /* XREAD. */
    BLOCKED_ZSET,     /* BZPOP et al. */
    BLOCKED_POSTPONE, /* Blocked by processCommand, re-try processing later. */
    BLOCKED_SHUTDOWN, /* SHUTDOWN. */
    BLOCKED_NUM,      /* Number of blocked states. */
    BLOCKED_END       /* End of enumeration */
} blocking_type;

/* Client request types */
#define PROTO_REQ_INLINE 1
#define PROTO_REQ_MULTIBULK 2

/* Client classes for client limits, currently used only for
 * the max-client-output-buffer limit implementation. */
#define CLIENT_TYPE_NORMAL 0     /* Normal req-reply clients + MONITORs */
#define CLIENT_TYPE_REPLICA 1    /* Replicas. */
#define CLIENT_TYPE_PUBSUB 2     /* Clients subscribed to PubSub channels. */
#define CLIENT_TYPE_PRIMARY 3    /* Primary. */
#define CLIENT_TYPE_COUNT 4      /* Total number of client types. */
#define CLIENT_TYPE_OBUF_COUNT 3 /* Number of clients to expose to output \
                                    buffer configuration. Just the first  \
                                    three: normal, replica, pubsub. */

/* Type of commandlog */
typedef enum {
    COMMANDLOG_TYPE_SLOW = 0,
    COMMANDLOG_TYPE_LARGE_REQUEST,
    COMMANDLOG_TYPE_LARGE_REPLY,
    COMMANDLOG_TYPE_NUM
} commandlog_type;

/* Configuration and entry list of different types of command logs */
typedef struct commandlog {
    list *entries;
    long long entry_id;
    long long threshold;
    unsigned long max_len;
} commandlog;

/* Replica replication state. Used in server.repl_state for replicas to remember
 * what to do next. */
typedef enum {
    REPL_STATE_NONE = 0,   /* No active replication */
    REPL_STATE_CONNECT,    /* Must connect to primary */
    REPL_STATE_CONNECTING, /* Connecting to primary */
    /* --- Handshake states, must be ordered --- */
    REPL_STATE_RECEIVE_PING_REPLY,    /* Wait for PING reply */
    REPL_STATE_SEND_HANDSHAKE,        /* Send handshake sequence to primary */
    REPL_STATE_RECEIVE_AUTH_REPLY,    /* Wait for AUTH reply */
    REPL_STATE_RECEIVE_PORT_REPLY,    /* Wait for REPLCONF reply */
    REPL_STATE_RECEIVE_IP_REPLY,      /* Wait for REPLCONF reply */
    REPL_STATE_RECEIVE_CAPA_REPLY,    /* Wait for REPLCONF reply */
    REPL_STATE_RECEIVE_VERSION_REPLY, /* Wait for REPLCONF reply */
    REPL_STATE_SEND_PSYNC,            /* Send PSYNC */
    REPL_STATE_RECEIVE_PSYNC_REPLY,   /* Wait for PSYNC reply */
    /* --- End of handshake states --- */
    REPL_STATE_TRANSFER,  /* Receiving .rdb from primary */
    REPL_STATE_CONNECTED, /* Connected to primary */
} repl_state;

/* Replica rdb-channel replication state. Used in server.repl_rdb_channel_state for
 * replicas to remember what to do next. */
typedef enum {
    REPL_DUAL_CHANNEL_STATE_NONE = 0,         /* No active rdb channel sync */
    REPL_DUAL_CHANNEL_SEND_HANDSHAKE,         /* Send handshake sequence to primary */
    REPL_DUAL_CHANNEL_RECEIVE_AUTH_REPLY,     /* Wait for AUTH reply */
    REPL_DUAL_CHANNEL_RECEIVE_REPLCONF_REPLY, /* Wait for REPLCONF reply */
    REPL_DUAL_CHANNEL_RECEIVE_ENDOFF,         /* Wait for $ENDOFF reply */
    REPL_DUAL_CHANNEL_RDB_LOAD,               /* Loading rdb using rdb channel */
    REPL_DUAL_CHANNEL_RDB_LOADED,
} repl_rdb_channel_state;

/* The state of an in progress coordinated failover */
typedef enum {
    NO_FAILOVER = 0,        /* No failover in progress */
    FAILOVER_WAIT_FOR_SYNC, /* Waiting for target replica to catch up */
    FAILOVER_IN_PROGRESS    /* Waiting for target replica to accept
                             * PSYNC FAILOVER request. */
} failover_state;

/* State of replicas from the POV of the primary. Used in client->replstate.
 * In SEND_BULK and ONLINE state the replica receives new updates
 * in its output queue. In the WAIT_BGSAVE states instead the server is waiting
 * to start the next background saving in order to send updates to it. */
#define REPLICA_STATE_WAIT_BGSAVE_START 6 /* We need to produce a new RDB file. */
#define REPLICA_STATE_WAIT_BGSAVE_END 7   /* Waiting RDB file creation to finish. */
#define REPLICA_STATE_SEND_BULK 8         /* Sending RDB file to replica. */
#define REPLICA_STATE_ONLINE 9            /* RDB file transmitted, sending just updates. */
#define REPLICA_STATE_RDB_TRANSMITTED 10  /* RDB file transmitted - This state is used only for \
                                           * a replica that only wants RDB without replication buffer  */
#define REPLICA_STATE_BG_RDB_LOAD 11      /* Main channel of a replica which uses dual channel replication. */

/* Replica capability flags */
#define REPLICA_CAPA_NONE 0
#define REPLICA_CAPA_EOF (1 << 0)               /* Can parse the RDB EOF streaming format. */
#define REPLICA_CAPA_PSYNC2 (1 << 1)            /* Supports PSYNC2 protocol. */
#define REPLICA_CAPA_DUAL_CHANNEL (1 << 2)      /* Supports dual channel replication sync */
#define REPLICA_CAPA_SKIP_RDB_CHECKSUM (1 << 3) /* Supports skipping RDB checksum for sync requests. */

/* Replica capability strings */
#define REPLICA_CAPA_SKIP_RDB_CHECKSUM_STR "skip-rdb-checksum" /* Supports skipping RDB checksum for sync requests. */

/* Replica requirements */
#define REPLICA_REQ_NONE 0
#define REPLICA_REQ_RDB_EXCLUDE_DATA (1 << 0)      /* Exclude data from RDB */
#define REPLICA_REQ_RDB_EXCLUDE_FUNCTIONS (1 << 1) /* Exclude functions from RDB */
#define REPLICA_REQ_RDB_CHANNEL (1 << 2)           /* Use dual-channel-replication */
/* Mask of all bits in the replica requirements bitfield that represent non-standard (filtered) RDB requirements */
#define REPLICA_REQ_RDB_MASK (REPLICA_REQ_RDB_EXCLUDE_DATA | REPLICA_REQ_RDB_EXCLUDE_FUNCTIONS)

/* Synchronous read timeout - replica side */
#define CONFIG_REPL_SYNCIO_TIMEOUT 5

/* The default number of replication backlog blocks to trim per call. */
#define REPL_BACKLOG_TRIM_BLOCKS_PER_CALL 64

/* In order to quickly find the requested offset for PSYNC requests,
 * we index some nodes in the replication buffer linked list into a rax. */
#define REPL_BACKLOG_INDEX_PER_BLOCKS 64

/* List related stuff */
#define LIST_HEAD 0
#define LIST_TAIL 1
#define ZSET_MIN 0
#define ZSET_MAX 1

/* Sort operations */
#define SORT_OP_GET 0

/* Log levels */
#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_NOTHING 4
#define LL_RAW (1 << 10) /* Modifier to log without timestamp */

/* Supervision options */
#define SUPERVISED_NONE 0
#define SUPERVISED_AUTODETECT 1
#define SUPERVISED_SYSTEMD 2
#define SUPERVISED_UPSTART 3

/* Anti-warning macro... */
#define UNUSED(V) ((void)V)

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */
#define ZSKIPLIST_MAX_SEARCH 10

/* Append only defines */
#define AOF_FSYNC_NO 0
#define AOF_FSYNC_ALWAYS 1
#define AOF_FSYNC_EVERYSEC 2

/* Replication diskless load defines */
#define REPL_DISKLESS_LOAD_DISABLED 0
#define REPL_DISKLESS_LOAD_WHEN_DB_EMPTY 1
#define REPL_DISKLESS_LOAD_SWAPDB 2
#define REPL_DISKLESS_LOAD_FLUSH_BEFORE_LOAD 3

/* TLS Client Authentication */
#define TLS_CLIENT_AUTH_NO 0
#define TLS_CLIENT_AUTH_YES 1
#define TLS_CLIENT_AUTH_OPTIONAL 2

/* Sanitize dump payload */
#define SANITIZE_DUMP_NO 0
#define SANITIZE_DUMP_YES 1
#define SANITIZE_DUMP_CLIENTS 2

/* Enable protected config/command */
#define PROTECTED_ACTION_ALLOWED_NO 0
#define PROTECTED_ACTION_ALLOWED_YES 1
#define PROTECTED_ACTION_ALLOWED_LOCAL 2

/* Sets operations codes */
#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

/* oom-score-adj defines */
#define OOM_SCORE_ADJ_NO 0
#define OOM_SCORE_RELATIVE 1
#define OOM_SCORE_ADJ_ABSOLUTE 2

/* Server maxmemory strategies. Instead of using just incremental number
 * for this defines, we use a set of flags so that testing for certain
 * properties common to multiple policies is faster. */
#define MAXMEMORY_FLAG_LRU (1 << 0)
#define MAXMEMORY_FLAG_LFU (1 << 1)
#define MAXMEMORY_FLAG_ALLKEYS (1 << 2)
#define MAXMEMORY_FLAG_NO_SHARED_INTEGERS (MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU)

#define MAXMEMORY_VOLATILE_LRU ((0 << 8) | MAXMEMORY_FLAG_LRU)
#define MAXMEMORY_VOLATILE_LFU ((1 << 8) | MAXMEMORY_FLAG_LFU)
#define MAXMEMORY_VOLATILE_TTL (2 << 8)
#define MAXMEMORY_VOLATILE_RANDOM (3 << 8)
#define MAXMEMORY_ALLKEYS_LRU ((4 << 8) | MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_LFU ((5 << 8) | MAXMEMORY_FLAG_LFU | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_RANDOM ((6 << 8) | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_NO_EVICTION (7 << 8)

/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

/* SHUTDOWN flags */
#define SHUTDOWN_NOFLAGS 0 /* No flags. */
#define SHUTDOWN_SAVE 1    /* Force SAVE on SHUTDOWN even if no save \
                              points are configured. */
#define SHUTDOWN_NOSAVE 2  /* Don't SAVE on SHUTDOWN. */
#define SHUTDOWN_NOW 4     /* Don't wait for replicas to catch up. */
#define SHUTDOWN_FORCE 8   /* Don't let errors prevent shutdown. */

/* Command call flags, see call() function */
#define CMD_CALL_NONE 0
#define CMD_CALL_PROPAGATE_AOF (1 << 0)
#define CMD_CALL_PROPAGATE_REPL (1 << 1)
#define CMD_CALL_REPROCESSING (1 << 2)
#define CMD_CALL_FROM_MODULE (1 << 3) /* From RM_Call */
#define CMD_CALL_PROPAGATE (CMD_CALL_PROPAGATE_AOF | CMD_CALL_PROPAGATE_REPL)
#define CMD_CALL_FULL (CMD_CALL_PROPAGATE)

/* Command propagation flags, see propagateNow() function */
#define PROPAGATE_NONE 0
#define PROPAGATE_AOF 1
#define PROPAGATE_REPL 2

/* Actions pause types */
#define PAUSE_ACTION_CLIENT_WRITE (1 << 0)
#define PAUSE_ACTION_CLIENT_ALL (1 << 1) /* must be bigger than PAUSE_ACTION_CLIENT_WRITE */
#define PAUSE_ACTION_EXPIRE (1 << 2)
#define PAUSE_ACTION_EVICT (1 << 3)
#define PAUSE_ACTION_REPLICA (1 << 4) /* pause replica traffic */

/* Sets log format */
typedef enum { LOG_FORMAT_LEGACY = 0,
               LOG_FORMAT_LOGFMT } log_format_type;

/* Sets log timestamp format */
typedef enum { LOG_TIMESTAMP_LEGACY = 0,
               LOG_TIMESTAMP_ISO8601,
               LOG_TIMESTAMP_MILLISECONDS } log_timestamp_type;

typedef enum { RDB_VERSION_CHECK_STRICT = 0,
               RDB_VERSION_CHECK_RELAXED } rdb_version_check_type;

/* common sets of actions to pause/unpause */
#define PAUSE_ACTIONS_CLIENT_WRITE_SET \
    (PAUSE_ACTION_CLIENT_WRITE | PAUSE_ACTION_EXPIRE | PAUSE_ACTION_EVICT | PAUSE_ACTION_REPLICA)
#define PAUSE_ACTIONS_CLIENT_ALL_SET \
    (PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_EXPIRE | PAUSE_ACTION_EVICT | PAUSE_ACTION_REPLICA)

/* Client pause purposes. Each purpose has its own end time and pause type. */
typedef enum {
    PAUSE_BY_CLIENT_COMMAND = 0,
    PAUSE_DURING_SHUTDOWN,
    PAUSE_DURING_FAILOVER,
    NUM_PAUSE_PURPOSES /* This value is the number of purposes above. */
} pause_purpose;

typedef struct {
    uint32_t paused_actions; /* Bitmask of actions */
    mstime_t end;
} pause_event;

/* Ways that a clusters endpoint can be described */
typedef enum {
    CLUSTER_ENDPOINT_TYPE_IP = 0,          /* Show IP address */
    CLUSTER_ENDPOINT_TYPE_HOSTNAME,        /* Show hostname */
    CLUSTER_ENDPOINT_TYPE_UNKNOWN_ENDPOINT /* Show NULL or empty */
} cluster_endpoint_type;

/* RDB active child save type. */
#define RDB_CHILD_TYPE_NONE 0
#define RDB_CHILD_TYPE_DISK 1   /* RDB is written to disk. */
#define RDB_CHILD_TYPE_SOCKET 2 /* RDB is written to replica socket. */

/* Keyspace changes notification classes. Every class is associated with a
 * character for configuration purposes. */
#define NOTIFY_KEYSPACE (1 << 0)  /* K */
#define NOTIFY_KEYEVENT (1 << 1)  /* E */
#define NOTIFY_GENERIC (1 << 2)   /* g */
#define NOTIFY_STRING (1 << 3)    /* $ */
#define NOTIFY_LIST (1 << 4)      /* l */
#define NOTIFY_SET (1 << 5)       /* s */
#define NOTIFY_HASH (1 << 6)      /* h */
#define NOTIFY_ZSET (1 << 7)      /* z */
#define NOTIFY_EXPIRED (1 << 8)   /* x */
#define NOTIFY_EVICTED (1 << 9)   /* e */
#define NOTIFY_STREAM (1 << 10)   /* t */
#define NOTIFY_KEY_MISS (1 << 11) /* m (Note: This one is excluded from NOTIFY_ALL on purpose) */
#define NOTIFY_LOADED (1 << 12)   /* module only key space notification, indicate a key loaded from rdb */
#define NOTIFY_MODULE (1 << 13)   /* d, module key space notification */
#define NOTIFY_NEW (1 << 14)      /* n, new key notification */
#define NOTIFY_ALL                                                                                            \
    (NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST | NOTIFY_SET | NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED | \
     NOTIFY_EVICTED | NOTIFY_STREAM | NOTIFY_MODULE) /* A flag */

/* Using the following macro you can run code inside serverCron() with the
 * specified period, specified in milliseconds.
 * The actual resolution depends on server.hz. */
#define run_with_period(_ms_) if (((_ms_) <= 1000 / server.hz) || !(server.cronloops % ((_ms_) / (1000 / server.hz))))

/* We can print the stacktrace, so our assert is defined this way: */
#define serverAssertWithInfo(_c, _o, _e) \
    (likely(_e) ? (void)0 : (_serverAssertWithInfo(_c, _o, #_e, __FILE__, __LINE__), valkey_unreachable()))
#define serverAssert(_e) (likely(_e) ? (void)0 : (_serverAssert(#_e, __FILE__, __LINE__), valkey_unreachable()))
#define serverPanic(...) _serverPanic(__FILE__, __LINE__, __VA_ARGS__), valkey_unreachable()

/* The following macros provide a conditional assertion that is only executed
 * when the server config 'enable-debug-assert' is true. This is useful for adding
 * assertions that are too computationally expensive or risky to run in normal
 * operation, but are valuable for debugging or testing. */
#define debugServerAssert(...) (server.enable_debug_assert ? serverAssert(__VA_ARGS__) : (void)0)
#define debugServerAssertWithInfo(...) (server.enable_debug_assert ? serverAssertWithInfo(__VA_ARGS__) : (void)0)

/* latency histogram per command init settings */
#define LATENCY_HISTOGRAM_MIN_VALUE 1L          /* >= 1 nanosec */
#define LATENCY_HISTOGRAM_MAX_VALUE 1000000000L /* <= 1 secs */
#define LATENCY_HISTOGRAM_PRECISION 2           /* Maintain a value precision of 2 significant digits across LATENCY_HISTOGRAM_MIN_VALUE and                  \
                                                 * LATENCY_HISTOGRAM_MAX_VALUE range. Value quantization within the range will thus be no larger than 1/100th \
                                                 * (or 1%) of any value. The total size per histogram should sit around 40 KiB Bytes. */

/* Busy module flags, see busy_module_yield_flags */
#define BUSY_MODULE_YIELD_NONE (0)
#define BUSY_MODULE_YIELD_EVENTS (1 << 0)
#define BUSY_MODULE_YIELD_CLIENTS (1 << 1)

/* IO poll */
typedef enum {
    AE_IO_STATE_NONE,
    AE_IO_STATE_POLL,
    AE_IO_STATE_DONE
} AeIoState;

/*-----------------------------------------------------------------------------
 * Data types
 *----------------------------------------------------------------------------*/

/* An Object, that is a type able to hold a string / list / set */

/* The actual Object */
#define OBJ_STRING 0 /* String object. */
#define OBJ_LIST 1   /* List object. */
#define OBJ_SET 2    /* Set object. */
#define OBJ_ZSET 3   /* Sorted set object. */
#define OBJ_HASH 4   /* Hash object. */

/* The "module" object type is a special one that signals that the object
 * is one directly managed by a module. In this case the value points
 * to a moduleValue struct, which contains the object value (which is only
 * handled by the module itself) and the ValkeyModuleType struct which lists
 * function pointers in order to serialize, deserialize, AOF-rewrite and
 * free the object.
 *
 * Inside the RDB file, module types are encoded as OBJ_MODULE followed
 * by a 64 bit module type ID, which has a 54 bits module-specific signature
 * in order to dispatch the loading to the right module, plus a 10 bits
 * encoding version. */
#define OBJ_MODULE 5   /* Module object. */
#define OBJ_STREAM 6   /* Stream object. */
#define OBJ_TYPE_MAX 7 /* Maximum number of object types */

typedef struct ValkeyModuleType moduleType;

/* Macro to check if the client is in the middle of module based authentication. */
#define clientHasModuleAuthInProgress(c) (((c)->module_data && (c)->module_data->module_auth_ctx != NULL))

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define OBJ_ENCODING_RAW 0        /* Raw representation */
#define OBJ_ENCODING_INT 1        /* Encoded as integer */
#define OBJ_ENCODING_HASHTABLE 2  /* Encoded as a hashtable */
#define OBJ_ENCODING_ZIPMAP 3     /* No longer used: old hash encoding. */
#define OBJ_ENCODING_LINKEDLIST 4 /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5    /* No longer used: old list/hash/zset encoding. */
#define OBJ_ENCODING_INTSET 6     /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7   /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8     /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9  /* Encoded as linked list of listpacks */
#define OBJ_ENCODING_STREAM 10    /* Encoded as a radix tree of listpacks */
#define OBJ_ENCODING_LISTPACK 11  /* Encoded as a listpack */

#define LRU_BITS 24
#define LRU_CLOCK_MAX ((1 << LRU_BITS) - 1) /* Max value of obj->lru */
#define LRU_CLOCK_RESOLUTION 1000           /* LRU clock resolution in ms */

#define OBJ_REFCOUNT_BITS 30
#define OBJ_SHARED_REFCOUNT ((1 << OBJ_REFCOUNT_BITS) - 1) /* Global object never destroyed. */
#define OBJ_STATIC_REFCOUNT ((1 << OBJ_REFCOUNT_BITS) - 2) /* Object allocated in the stack. */
#define OBJ_FIRST_SPECIAL_REFCOUNT OBJ_STATIC_REFCOUNT
struct serverObject {
    unsigned type : 4;
    unsigned encoding : 4;
    unsigned lru : LRU_BITS; /* LRU time (relative to global lru_clock) or
                              * LFU data (least significant 8 bits frequency
                              * and most significant 16 bits access time). */
    unsigned hasexpire : 1;
    unsigned hasembkey : 1;
    unsigned refcount : OBJ_REFCOUNT_BITS;
    void *ptr;
};

/* The string name for an object's type as listed above
 * Native types are checked against the OBJ_STRING, OBJ_LIST, OBJ_* defines,
 * and Module types have their registered name returned. */
char *getObjectTypeName(robj *);

/* Macro used to initialize an Object allocated on the stack.
 * Note that this macro is taken near the structure definition to make sure
 * we'll update it when the structure is changed, to avoid bugs like
 * bug #85 introduced exactly in this way. */
#define initStaticStringObject(_var, _ptr)   \
    do {                                     \
        _var.refcount = OBJ_STATIC_REFCOUNT; \
        _var.type = OBJ_STRING;              \
        _var.encoding = OBJ_ENCODING_RAW;    \
        _var.hasexpire = 0;                  \
        _var.hasembkey = 0;                  \
        _var.ptr = _ptr;                     \
    } while (0)

struct evictionPoolEntry; /* Defined in evict.c */

/* This structure is used in order to represent the output buffer of a client,
 * which is actually a linked list of blocks like that, that is: client->reply. */
typedef struct clientReplyBlock {
    size_t size, used;
    char buf[];
} clientReplyBlock;

/* Replication buffer blocks is the list of replBufBlock.
 *
 * +--------------+       +--------------+       +--------------+
 * | refcount = 1 |  ...  | refcount = 0 |  ...  | refcount = 2 |
 * +--------------+       +--------------+       +--------------+
 *      |                                            /       \
 *      |                                           /         \
 *      |                                          /           \
 *  Repl Backlog                               Replica_A    Replica_B
 *
 * Each replica or replication backlog increments only the refcount of the
 * 'ref_repl_buf_node' which it points to. So when replica walks to the next
 * node, it should first increase the next node's refcount, and when we trim
 * the replication buffer nodes, we remove node always from the head node which
 * refcount is 0. If the refcount of the head node is not 0, we must stop
 * trimming and never iterate the next node. */

/* Similar with 'clientReplyBlock', it is used for shared buffers between
 * all replica clients and replication backlog. */
typedef struct replBufBlock {
    int refcount;          /* Number of replicas or repl backlog using. */
    long long id;          /* The unique incremental number. */
    long long repl_offset; /* Start replication offset of the block. */
    size_t size, used;
    char buf[];
} replBufBlock;

/* Database representation. There are multiple databases identified
 * by integers from 0 (the default database) up to the max configured
 * database. The database number is the 'id' field in the structure. */
typedef struct serverDb {
    kvstore *keys;                        /* The keyspace for this DB */
    kvstore *expires;                     /* Timeout of keys with a timeout set */
    dict *blocking_keys;                  /* Keys with clients waiting for data (BLPOP)*/
    dict *blocking_keys_unblock_on_nokey; /* Keys with clients waiting for
                                           * data, and should be unblocked if key is deleted (XREADEDGROUP).
                                           * This is a subset of blocking_keys*/
    dict *ready_keys;                     /* Blocked keys that received a PUSH */
    dict *watched_keys;                   /* WATCHED keys for MULTI/EXEC CAS */
    int id;                               /* Database ID */
    long long avg_ttl;                    /* Average TTL, just for stats */
    unsigned long expires_cursor;         /* Cursor of the active expire cycle. */
} serverDb;

/* forward declaration for functions ctx */
typedef struct functionsLibCtx functionsLibCtx;

/* Holding object that need to be populated during
 * rdb loading. On loading end it is possible to decide
 * whether not to set those objects on their rightful place.
 * For example: dbarray need to be set as main database on
 *              successful loading and dropped on failure. */
typedef struct rdbLoadingCtx {
    serverDb *dbarray;
    functionsLibCtx *functions_lib_ctx;
} rdbLoadingCtx;

typedef sds (*rdbAuxFieldEncoder)(int flags);
typedef int (*rdbAuxFieldDecoder)(int flags, sds s);

/* Client MULTI/EXEC state */
typedef struct multiCmd {
    robj **argv;
    int argv_len;
    int argc;
    struct serverCommand *cmd;
} multiCmd;

typedef struct multiState {
    multiCmd *commands;   /* Array of MULTI commands */
    int count;            /* Total number of MULTI commands */
    int cmd_flags;        /* The accumulated command flags OR-ed together.
                             So if at least a command has a given flag, it
                             will be set in this field. */
    int cmd_inv_flags;    /* Same as cmd_flags, OR-ing the ~flags. so that it
                             is possible to know if all the commands have a
                             certain flag. */
    size_t argv_len_sums; /* mem used by all commands arguments */
    int alloc_count;      /* total number of multiCmd struct memory reserved. */
    list watched_keys;
} multiState;

/* This structure holds the blocking operation state for a client.
 * The fields used depend on client->bstate.btype. */
typedef struct blockingState {
    /* Generic fields. */
    blocking_type btype;  /* Type of blocking op if CLIENT_BLOCKED. */
    mstime_t timeout;     /* Blocking operation timeout. If UNIX current time
                           * is > timeout then the operation timed out. */
    int unblock_on_nokey; /* Whether to unblock the client when at least one of the keys
                             is deleted or does not exist anymore */
    union {
        listNode *client_waiting_acks_list_node; /* list node in server.clients_waiting_acks list. */
        listNode *postponed_list_node;           /* list node in server.postponed_clients */
        listNode *generic_blocked_list_node;     /* generic placeholder for blocked clients utility lists.
                                                    Since a client cannot be blocked multiple times, we can assume
                                                    it will be held in only one extra utility list, so it is ok to maintain
                                                    a union of these listNode references. */
    };

    /* BLOCKED_LIST, BLOCKED_ZSET and BLOCKED_STREAM or any other Keys related blocking */
    dict *keys; /* The keys we are blocked on */

    /* BLOCKED_WAIT and BLOCKED_WAITAOF */
    int numreplicas;      /* Number of replicas we are waiting for ACK. */
    int numlocal;         /* Indication if WAITAOF is waiting for local fsync. */
    long long reploffset; /* Replication offset to reach. */

    /* BLOCKED_MODULE */
    void *module_blocked_handle; /* ValkeyModuleBlockedClient structure.
                                    which is opaque for the Redis core, only
                                    handled in module.c. */

    void *async_rm_call_handle; /* ValkeyModuleAsyncRMCallPromise structure.
                                   which is opaque for the Redis core, only
                                   handled in module.c. */
} blockingState;

/* The following structure represents a node in the server.ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we iterate over this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that server.ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a database,
 * where we make sure to remember if a given key was already added in the
 * server.ready_keys list. */
typedef struct readyList {
    serverDb *db;
    robj *key;
} readyList;

/* This structure represents a user. This is useful for ACLs, the
 * user is associated to the connection after the connection is authenticated.
 * If there is no associated user, the connection uses the default user. */
#define USER_COMMAND_BITS_COUNT 1024             /* The total number of command bits     \
                                                   in the user structure. The last valid \
                                                   command ID we can set in the user     \
                                                   is USER_COMMAND_BITS_COUNT-1. */
#define USER_FLAG_ENABLED (1 << 0)               /* The user is active. */
#define USER_FLAG_DISABLED (1 << 1)              /* The user is disabled. */
#define USER_FLAG_NOPASS (1 << 2)                /* The user requires no password, any   \
                                                    provided password will work. For the \
                                                    default user, this also means that   \
                                                    no AUTH is needed, and every         \
                                                    connection is immediately            \
                                                    authenticated. */
#define USER_FLAG_SANITIZE_PAYLOAD (1 << 3)      /* The user require a deep RESTORE \
                                                  * payload sanitization. */
#define USER_FLAG_SANITIZE_PAYLOAD_SKIP (1 << 4) /* The user should skip the     \
                                                  * deep sanitization of RESTORE \
                                                  * payload. */

#define SELECTOR_FLAG_ROOT (1 << 0)        /* This is the root user permission \
                                            * selector. */
#define SELECTOR_FLAG_ALLKEYS (1 << 1)     /* The user can mention any key. */
#define SELECTOR_FLAG_ALLCOMMANDS (1 << 2) /* The user can run all commands. */
#define SELECTOR_FLAG_ALLCHANNELS (1 << 3) /* The user can mention any Pub/Sub \
                                              channel. */

typedef struct {
    sds name;         /* The username as an SDS string. */
    uint32_t flags;   /* See USER_FLAG_* */
    list *passwords;  /* A list of SDS valid passwords for this user. */
    list *selectors;  /* A list of selectors this user validates commands
                         against. This list will always contain at least
                         one selector for backwards compatibility. */
    robj *acl_string; /* cached string represent of ACLs */
} user;

/* With multiplexing we need to take per-client state.
 * Clients are taken in a linked list. */

#define CLIENT_ID_AOF (UINT64_MAX)                 /* Reserved ID for the AOF client. If you   \
                                                      need more reserved IDs use UINT64_MAX-1, \
                                                      -2, ... and so forth. */
#define CLIENT_ID_CACHED_RESPONSE (UINT64_MAX - 1) /* Client for cached response, see createCachedResponseClient. */

/* Replication backlog is not a separate memory, it just is one consumer of
 * the global replication buffer. This structure records the reference of
 * replication buffers. Since the replication buffer block list may be very long,
 * it would cost much time to search replication offset on partial resync, so
 * we use one rax tree to index some blocks every REPL_BACKLOG_INDEX_PER_BLOCKS
 * to make searching offset from replication buffer blocks list faster. */
typedef struct replBacklog {
    listNode *ref_repl_buf_node; /* Referenced node of replication buffer blocks,
                                  * see the definition of replBufBlock. */
    size_t unindexed_count;      /* The count from last creating index block. */
    rax *blocks_index;           /* The index of recorded blocks of replication
                                  * buffer for quickly searching replication
                                  * offset on partial resynchronization. */
    long long histlen;           /* Backlog actual data length */
    long long offset;            /* Replication "primary offset" of first
                                  * byte in the replication backlog buffer.*/
} replBacklog;

typedef struct replDataBuf {
    list *blocks; /* List of replDataBufBlock */
    size_t len;   /* Number of bytes stored in all blocks */
    size_t peak;
} replDataBuf;

typedef struct {
    list *clients;
    size_t mem_usage_sum;
} clientMemUsageBucket;

#ifdef LOG_REQ_RES
/* Structure used to log client's requests and their
 * responses (see logreqres.c) */
typedef struct {
    /* General */
    int argv_logged; /* 1 if the command was logged */
    /* Vars for log buffer */
    unsigned char *buf; /* Buffer holding the data (request and response) */
    size_t used;
    size_t capacity;
    /* Vars for offsets within the client's reply */
    struct {
        /* General */
        int saved; /* 1 if we already saved the offset (first time we call addReply*) */
        /* Offset within the static reply buffer */
        int bufpos;
        /* Offset within the reply block list */
        struct {
            int index;
            size_t used;
        } last_node;
    } offset;
} clientReqResInfo;
#endif

typedef enum {
    CLIENT_IDLE = 0,        /* Initial state: client is idle. */
    CLIENT_PENDING_IO = 1,  /* Main-thread sets this state when client is sent to IO-thread for read/write. */
    CLIENT_COMPLETED_IO = 2 /* IO-thread sets this state after completing IO operation. */
} clientIOState;

typedef struct ClientFlags {
    uint64_t primary : 1;                  /* This client is a primary */
    uint64_t replica : 1;                  /* This client is a replica */
    uint64_t monitor : 1;                  /* This client is a replica monitor, see MONITOR */
    uint64_t multi : 1;                    /* This client is in a MULTI context */
    uint64_t blocked : 1;                  /* The client is waiting in a blocking operation */
    uint64_t dirty_cas : 1;                /* Watched keys modified. EXEC will fail. */
    uint64_t close_after_reply : 1;        /* Close after writing entire reply. */
    uint64_t unblocked : 1;                /* This client was unblocked and is stored in server.unblocked_clients */
    uint64_t script : 1;                   /* This is a non connected client used by Lua */
    uint64_t asking : 1;                   /* Client issued the ASKING command */
    uint64_t close_asap : 1;               /* Close this client ASAP */
    uint64_t unix_socket : 1;              /* Client connected via Unix domain socket */
    uint64_t dirty_exec : 1;               /* EXEC will fail for errors while queueing */
    uint64_t primary_force_reply : 1;      /* Queue replies even if is primary */
    uint64_t force_aof : 1;                /* Force AOF propagation of current cmd. */
    uint64_t force_repl : 1;               /* Force replication of current cmd. */
    uint64_t pre_psync : 1;                /* Instance don't understand PSYNC. */
    uint64_t readonly : 1;                 /* Cluster client is in read-only state. */
    uint64_t pubsub : 1;                   /* Client is in Pub/Sub mode. */
    uint64_t prevent_aof_prop : 1;         /* Don't propagate to AOF. */
    uint64_t prevent_repl_prop : 1;        /* Don't propagate to replicas. */
    uint64_t prevent_prop : 1;             /* Don't propagate to AOF or replicas. */
    uint64_t pending_write : 1;            /* Client has output to send but a write handler is yet not installed. */
    uint64_t pending_read : 1;             /* Client has output to send but a write handler is yet not installed. */
    uint64_t reply_off : 1;                /* Don't send replies to client. */
    uint64_t reply_skip_next : 1;          /* Set CLIENT_REPLY_SKIP for next cmd */
    uint64_t reply_skip : 1;               /* Don't send just this reply. */
    uint64_t lua_debug : 1;                /* Run EVAL in debug mode. */
    uint64_t lua_debug_sync : 1;           /* EVAL debugging without fork() */
    uint64_t module : 1;                   /* Non connected client used by some module. */
    uint64_t protected : 1;                /* Client should not be freed for now. */
    uint64_t executing_command : 1;        /* Indicates that the client is currently in the process of handling a command. */
    uint64_t pending_command : 1;          /* Indicates the client has a fully parsed command ready for execution. */
    uint64_t tracking : 1;                 /* Client enabled keys tracking in order to perform client side caching. */
    uint64_t tracking_broken_redir : 1;    /* Target client is invalid. */
    uint64_t tracking_bcast : 1;           /* Tracking in BCAST mode. */
    uint64_t tracking_optin : 1;           /* Tracking in opt-in mode. */
    uint64_t tracking_optout : 1;          /* Tracking in opt-out mode. */
    uint64_t tracking_caching : 1;         /* CACHING yes/no was given, depending on optin/optout mode. */
    uint64_t tracking_noloop : 1;          /* Don't send invalidation messages about writes performed by myself. */
    uint64_t in_to_table : 1;              /* This client is in the timeout table. */
    uint64_t protocol_error : 1;           /* Protocol error chatting with it. */
    uint64_t close_after_command : 1;      /* Close after executing commands and writing entire reply. */
    uint64_t deny_blocking : 1;            /* Indicate that the client should not be blocked. */
    uint64_t repl_rdbonly : 1;             /* This client is a replica that only wants RDB without replication buffer. */
    uint64_t no_evict : 1;                 /* This client is protected against client memory eviction. */
    uint64_t allow_oom : 1;                /* Client used by RM_Call is allowed to fully execute scripts even when in OOM */
    uint64_t no_touch : 1;                 /* This client will not touch LFU/LRU stats. */
    uint64_t pushing : 1;                  /* This client is pushing notifications. */
    uint64_t module_auth_has_result : 1;   /* Indicates a client in the middle of module based auth had been authenticated
                                              from the Module. */
    uint64_t module_prevent_aof_prop : 1;  /* Module client do not want to propagate to AOF */
    uint64_t module_prevent_repl_prop : 1; /* Module client do not want to propagate to replica */
    uint64_t reprocessing_command : 1;     /* The client is re-processing the command. */
    uint64_t replication_done : 1;         /* Indicate that replication has been done on the client */
    uint64_t authenticated : 1;            /* Indicate a client has successfully authenticated */
    uint64_t protected_rdb_channel : 1;    /* Dual channel replication sync: Protects the RDB client from premature \
                                            * release during full sync. This flag is used to ensure that the RDB client, which \
                                            * references the first replication data block required by the replica, is not \
                                            * released prematurely. Protecting the client is crucial for prevention of \
                                            * synchronization failures: \
                                            * If the RDB client is released before the replica initiates PSYNC, the primary \
                                            * will reduce the reference count (o->refcount) of the block needed by the replica.
                                            * \
                                            * This could potentially lead to the removal of the required data block, resulting \
                                            * in synchronization failures. Such failures could occur even in scenarios where \
                                            * the replica only needs an additional 4KB beyond the minimum size of the
                                            * repl_backlog.
                                            * By using this flag, we ensure that the RDB client remains intact until the replica
                                            * \ has successfully initiated PSYNC. */
    uint64_t repl_rdb_channel : 1;         /* Dual channel replication sync: track a connection which is used for rdb snapshot */
    uint64_t dont_cache_primary : 1;       /* In some cases we don't want to cache the primary. For example, the replica
                                            * knows that it does not need the cache and required a full sync. With this
                                            * flag, we won't cache the primary in freeClient. */
    uint64_t fake : 1;                     /* This is a fake client without a real connection. */
    uint64_t import_source : 1;            /* This client is importing data to server and can visit expired key. */
    uint64_t reserved : 4;                 /* Reserved for future use */
} ClientFlags;

typedef struct ClientPubSubData {
    dict *pubsub_channels;      /* channels a client is interested in (SUBSCRIBE) */
    dict *pubsub_patterns;      /* patterns a client is interested in (PSUBSCRIBE) */
    dict *pubsubshard_channels; /* shard level channels a client is interested in (SSUBSCRIBE) */
    /* If this client is in tracking mode and this field is non zero,
     * invalidation messages for keys fetched by this client will be sent to
     * the specified client ID. */
    uint64_t client_tracking_redirection;
    rax *client_tracking_prefixes; /* A dictionary of prefixes we are already
                                      subscribed to in BCAST mode, in the
                                      context of client side caching. */
} ClientPubSubData;

typedef struct ClientReplicationData {
    int repl_state;                      /* Replication state if this is a replica. */
    int repl_start_cmd_stream_on_ack;    /* Install replica write handler on first ACK. */
    int repldbfd;                        /* Replication DB file descriptor. */
    off_t repldboff;                     /* Replication DB file offset. */
    off_t repldbsize;                    /* Replication DB file size. */
    sds replpreamble;                    /* Replication DB preamble. */
    long long read_reploff;              /* Read replication offset if this is a primary. */
    long long reploff;                   /* Applied replication offset if this is a primary. */
    long long repl_applied;              /* Applied replication data count in querybuf, if this is a replica. */
    long long repl_ack_off;              /* Replication ack offset, if this is a replica. */
    long long repl_aof_off;              /* Replication AOF fsync ack offset, if this is a replica. */
    long long repl_ack_time;             /* Replication ack time, if this is a replica. */
    long long repl_last_partial_write;   /* The last time the server did a partial write from the RDB child pipe to this
                                            replica  */
    long long psync_initial_offset;      /* FULLRESYNC reply offset other replicas
                                            copying this replica output buffer
                                            should use. */
    char replid[CONFIG_RUN_ID_SIZE + 1]; /* primary replication ID (if primary). */
    int replica_listening_port;          /* As configured with: REPLCONF listening-port */
    char *replica_addr;                  /* Optionally given by REPLCONF ip-address */
    int replica_version;                 /* Version on the form 0xMMmmpp. */
    short replica_capa;                  /* Replica capabilities: REPLICA_CAPA_* bitwise OR. */
    short replica_req;                   /* Replica requirements: REPLICA_REQ_* */
    uint64_t associated_rdb_client_id;   /* The client id of this replica's rdb connection */
    time_t rdb_client_disconnect_time;   /* Time of the first freeClient call on this client. Used for delaying free. */
    listNode *ref_repl_buf_node;         /* Referenced node of replication buffer blocks,
                                           see the definition of replBufBlock. */
    size_t ref_block_pos;                /* Access position of referenced buffer block,
                                           i.e. the next offset to send. */
} ClientReplicationData;

typedef struct ClientModuleData {
    void *module_blocked_client;               /* Pointer to the ValkeyModuleBlockedClient associated with this
                                                * client. This is set in case of module authentication before the
                                                * unblocked client is reprocessed to handle reply callbacks. */
    void *module_auth_ctx;                     /* Ongoing / attempted module based auth callback's ctx.
                                                * This is only tracked within the context of the command attempting
                                                * authentication. If not NULL, it means module auth is in progress. */
    ValkeyModuleUserChangedFunc auth_callback; /* Module callback to execute
                                                * when the authenticated user
                                                * changes. */
    void *auth_callback_privdata;              /* Private data that is passed when the auth
                                                * changed callback is executed. Opaque for
                                                * the Server Core. */
    void *auth_module;                         /* The module that owns the callback, which is used
                                                * to disconnect the client if the module is
                                                * unloaded for cleanup. Opaque for the Server Core.*/
} ClientModuleData;

typedef struct client {
    /* Basic client information and connection. */
    uint64_t id; /* Client incremental unique ID. */
    connection *conn;
    /* Input buffer and command parsing fields */
    sds querybuf;        /* Buffer we use to accumulate client queries. */
    size_t qb_pos;       /* The position we have read in querybuf. */
    robj **argv;         /* Arguments of current command. */
    int argc;            /* Num of arguments of current command. */
    int argv_len;        /* Size of argv array (may be more than argc) */
    size_t argv_len_sum; /* Sum of lengths of objects in argv list. */
    int reqtype;         /* Request protocol type: PROTO_REQ_* */
    int multibulklen;    /* Number of multi bulk arguments left to read. */
    long bulklen;        /* Length of bulk argument in multi bulk request. */
    long long woff;      /* Last write global replication offset. */
    /* Command execution state and command information */
    struct serverCommand *cmd;           /* Current command. */
    struct serverCommand *lastcmd;       /* Last command executed. */
    struct serverCommand *realcmd;       /* The original command that was executed by the client */
    struct serverCommand *io_parsed_cmd; /* The command that was parsed by the IO thread. */
    time_t last_interaction;             /* Time of the last interaction, used for timeout */
    serverDb *db;                        /* Pointer to currently SELECTed DB. */
    /* Client state structs. */
    ClientPubSubData *pubsub_data;    /* Required for: pubsub commands and tracking. lazily initialized when first needed */
    ClientReplicationData *repl_data; /* Required for Replication operations. lazily initialized when first needed */
    ClientModuleData *module_data;    /* Required for Module operations. lazily initialized when first needed */
    multiState *mstate;               /* MULTI/EXEC state, lazily initialized when first needed */
    blockingState *bstate;            /* Blocking state, lazily initialized when first needed */
    /* Output buffer and reply handling */
    long duration;                       /* Current command duration. Used for measuring latency of blocking/non-blocking cmds */
    char *buf;                           /* Output buffer */
    size_t buf_usable_size;              /* Usable size of buffer. */
    list *reply;                         /* List of reply objects to send to the client. */
    listNode *io_last_reply_block;       /* Last client reply block when sent to IO thread */
    size_t io_last_bufpos;               /* The client's bufpos at the time it was sent to the IO thread */
    unsigned long long reply_bytes;      /* Tot bytes of objects in reply list. */
    size_t sentlen;                      /* Amount of bytes already sent in the current buffer or object being sent. */
    listNode clients_pending_write_node; /* list node in clients_pending_write or in clients_pending_io_write list */
    int bufpos;
    int original_argc;    /* Num of arguments of original command if arguments were rewritten. */
    robj **original_argv; /* Arguments of original command if arguments were rewritten. */
    /* Client flags and state indicators */
    union {
        uint64_t raw_flag;
        struct ClientFlags flag;
    };
    uint16_t write_flags;            /* Client Write flags - used to communicate the client write state. */
    volatile uint8_t io_read_state;  /* Indicate the IO read state of the client */
    volatile uint8_t io_write_state; /* Indicate the IO write state of the client */
    uint8_t resp;                    /* RESP protocol version. Can be 2 or 3. */
    uint8_t cur_tid;                 /* ID of IO thread currently performing IO for this client */
    /* In updateClientMemoryUsage() we track the memory usage of
     * each client and add it to the sum of all the clients of a given type,
     * however we need to remember what was the old contribution of each
     * client, and in which category the client was, in order to remove it
     * before adding it the new value. */
    uint8_t last_memory_type;
    uint8_t capa;                    /* Client capabilities: CLIENT_CAPA* macros. */
    listNode pending_read_list_node; /* IO thread only ?*/
    /* Statistics and metrics */
    unsigned long long net_input_bytes;           /* Total network input bytes read from this client. */
    unsigned long long net_input_bytes_curr_cmd;  /* Total network input bytes read for the* execution of this client's current command. */
    unsigned long long net_output_bytes;          /* Total network output bytes sent to this client. */
    unsigned long long commands_processed;        /* Total count of commands this client executed. */
    unsigned long long net_output_bytes_curr_cmd; /* Total network output bytes sent to this client, by the current command. */
    size_t buf_peak;                              /* Peak used size of buffer in last 5 sec interval. */
    int nwritten;                                 /* Number of bytes of the last write. */
    int nread;                                    /* Number of bytes of the last read. */
    int read_flags;                               /* Client Read flags - used to communicate the client read state. */
    int slot;                                     /* The slot the client is executing against. Set to -1 if no slot is being used */
    listNode *mem_usage_bucket_node;
    clientMemUsageBucket *mem_usage_bucket;
    /* In updateClientMemoryUsage() we track the memory usage of
     * each client and add it to the sum of all the clients of a given type,
     * however we need to remember what was the old contribution of each
     * client, and in which category the client was, in order to remove it
     * before adding it the new value. */
    size_t last_memory_usage;
    /* Fields after this point are less frequently used */
    listNode *client_list_node;        /* list node in client list */
    mstime_t buf_peak_last_reset_time; /* keeps the last time the buffer peak value was reset */
    size_t querybuf_peak;              /* Recent (100ms or more) peak of querybuf size. */
    dictEntry *cur_script;             /* Cached pointer to the dictEntry of the script being executed. */
    user *user;                        /* User associated with this connection */
    time_t obuf_soft_limit_reached_time;
    list *deferred_reply_errors; /* Used for module thread safe contexts. */
    robj *name;                  /* As set by CLIENT SETNAME. */
    robj *lib_name;              /* The client library name as set by CLIENT SETINFO. */
    robj *lib_ver;               /* The client library version as set by CLIENT SETINFO. */
    sds peerid;                  /* Cached peer ID. */
    sds sockname;                /* Cached connection target address. */
    time_t ctime;                /* Client creation time. */
#ifdef LOG_REQ_RES
    clientReqResInfo reqres;
#endif
} client;

/* When a command generates a lot of discrete elements to the client output buffer, it is much faster to
 * skip certain types of initialization. This type is used to indicate a client that has been initialized
 * and can be used with addWritePreparedReply* functions. A client can be cast into this type with
 * prepareClientForFutureWrites(client *c). */
typedef struct writePreparedClient writePreparedClient;

/* ACL information */
typedef struct aclInfo {
    long long user_auth_failures;       /* Auth failure counts on user level */
    long long invalid_cmd_accesses;     /* Invalid command accesses that user doesn't have permission to */
    long long invalid_key_accesses;     /* Invalid key accesses that user doesn't have permission to */
    long long invalid_channel_accesses; /* Invalid channel accesses that user doesn't have permission to */
} aclInfo;

struct saveparam {
    time_t seconds;
    int changes;
};

struct sentinelLoadQueueEntry {
    int argc;
    sds *argv;
    int linenum;
    sds line;
};

struct sentinelConfig {
    list *pre_monitor_cfg;
    list *monitor_cfg;
    list *post_monitor_cfg;
};

struct sharedObjectsStruct {
    robj *ok, *err, *emptybulk, *czero, *cone, *pong, *space, *queued, *null[4], *nullarray[4], *emptymap[4],
        *emptyset[4], *emptyarray, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr, *outofrangeerr, *noscripterr,
        *loadingerr, *slowevalerr, *slowscripterr, *slowmoduleerr, *bgsaveerr, *primarydownerr, *roreplicaerr,
        *execaborterr, *noautherr, *noreplicaserr, *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk,
        *subscribebulk, *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *unlink, *rpop, *lpop, *lpush,
        *rpoplpush, *lmove, *blmove, *zpopmin, *zpopmax, *emptyscan, *multi, *exec, *left, *right, *hset, *srem,
        *xgroup, *xclaim, *script, *replconf, *eval, *persist, *set, *pexpireat, *pexpire, *time, *pxat, *absttl,
        *retrycount, *force, *justid, *entriesread, *lastid, *ping, *setid, *keepttl, *load, *createconsumer, *getack,
        *special_asterisk, *special_equals, *default_username, *redacted, *ssubscribebulk, *sunsubscribebulk,
        *smessagebulk, *select[PROTO_SHARED_SELECT_CMDS], *integers[OBJ_SHARED_INTEGERS],
        *mbulkhdr[OBJ_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
        *bulkhdr[OBJ_SHARED_BULKHDR_LEN],  /* "$<value>\r\n" */
        *maphdr[OBJ_SHARED_BULKHDR_LEN],   /* "%<value>\r\n" */
        *sethdr[OBJ_SHARED_BULKHDR_LEN];   /* "~<value>\r\n" */
    sds minstring, maxstring;
};

/* ZSETs use a specialized version of Skiplists */
typedef struct zskiplistNode {
    sds ele;
    double score;
    struct zskiplistNode *backward;
    struct zskiplistLevel {
        struct zskiplistNode *forward;
        /* At each level we keep the span, which is the number of elements which are on the "subtree"
         * from this node at this level to the next node at the same level.
         * One exception is the value at level 0. In level 0 the span can only be 1 or 0 (in case the last elements in the list)
         * So we use it in order to hold the height of the node, which is the number of levels. */
        unsigned long span;
    } level[];
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    hashtable *ht;
    zskiplist *zsl;
} zset;

typedef struct clientBufferLimitsConfig {
    unsigned long long hard_limit_bytes;
    unsigned long long soft_limit_bytes;
    time_t soft_limit_seconds;
} clientBufferLimitsConfig;

extern clientBufferLimitsConfig clientBufferLimitsDefaults[CLIENT_TYPE_OBUF_COUNT];

/* The serverOp structure defines an Operation, that is an instance of
 * a command with an argument vector, database ID, propagation target
 * (PROPAGATE_*), and command pointer.
 *
 * Currently only used to additionally propagate more commands to AOF/Replication
 * after the propagation of the executed command. */
typedef struct serverOp {
    robj **argv;
    int argc, dbid, target;
} serverOp;

/* Defines an array of Operations. There is an API to add to this
 * structure in an easy way.
 *
 * int serverOpArrayAppend(serverOpArray *oa, int dbid, robj **argv, int argc, int target);
 * void serverOpArrayFree(serverOpArray *oa);
 */
typedef struct serverOpArray {
    serverOp *ops;
    int numops;
    int capacity;
} serverOpArray;

/* This structure is returned by the getMemoryOverheadData() function in
 * order to return memory overhead information. */
struct serverMemOverhead {
    size_t peak_allocated;
    size_t total_allocated;
    size_t startup_allocated;
    size_t repl_backlog;
    size_t clients_replicas;
    size_t clients_normal;
    size_t cluster_links;
    size_t aof_buffer;
    size_t lua_caches;
    size_t functions_caches;
    size_t overhead_total;
    size_t dataset;
    size_t total_keys;
    size_t bytes_per_key;
    float dataset_perc;
    float peak_perc;
    float total_frag;
    ssize_t total_frag_bytes;
    float allocator_frag;
    ssize_t allocator_frag_bytes;
    float allocator_rss;
    ssize_t allocator_rss_bytes;
    float rss_extra;
    size_t rss_extra_bytes;
    size_t num_dbs;
    size_t overhead_db_hashtable_lut;
    size_t overhead_db_hashtable_rehashing;
    unsigned long db_dict_rehashing_count;
    struct {
        size_t dbid;
        size_t overhead_ht_main;
        size_t overhead_ht_expires;
    } *db;
};

/* Replication error behavior determines the replica behavior
 * when it receives an error over the replication stream. In
 * either case the error is logged. */
typedef enum {
    PROPAGATION_ERR_BEHAVIOR_IGNORE = 0,
    PROPAGATION_ERR_BEHAVIOR_PANIC,
    PROPAGATION_ERR_BEHAVIOR_PANIC_ON_REPLICAS
} replicationErrorBehavior;

/* This structure can be optionally passed to RDB save/load functions in
 * order to implement additional functionalities, by storing and loading
 * metadata to the RDB file.
 *
 * For example, to use select a DB at load time, useful in
 * replication in order to make sure that chained replicas (replicas of replicas)
 * select the correct DB and are able to accept the stream coming from the
 * top-level primary. */
typedef struct rdbSaveInfo {
    /* Used saving and loading. */
    int repl_stream_db; /* DB to select in server.primary client. */

    /* Used only loading. */
    int repl_id_is_set;                   /* True if repl_id field is set. */
    char repl_id[CONFIG_RUN_ID_SIZE + 1]; /* Replication ID. */
    long long repl_offset;                /* Replication offset. */
} rdbSaveInfo;

#define RDB_SAVE_INFO_INIT {-1, 0, "0000000000000000000000000000000000000000", -1}

struct malloc_stats {
    size_t zmalloc_used;
    size_t process_rss;
    size_t allocator_allocated;
    size_t allocator_active;
    size_t allocator_resident;
    size_t allocator_muzzy;
    size_t allocator_frag_smallbins_bytes;
};

/*-----------------------------------------------------------------------------
 * Cached state per client connection type flags (bitwise or)
 *-----------------------------------------------------------------------------*/

#define CACHE_CONN_TYPE_TLS (1 << 0)
#define CACHE_CONN_TYPE_IPv6 (1 << 1)
#define CACHE_CONN_TYPE_RESP3 (1 << 2)
#define CACHE_CONN_TYPE_MAX (1 << 3)

/*-----------------------------------------------------------------------------
 * TLS Context Configuration
 *----------------------------------------------------------------------------*/

typedef struct serverTLSContextConfig {
    char *cert_file;            /* Server side and optionally client side cert file name */
    char *key_file;             /* Private key filename for cert_file */
    char *key_file_pass;        /* Optional password for key_file */
    char *client_cert_file;     /* Certificate to use as a client; if none, use cert_file */
    char *client_key_file;      /* Private key filename for client_cert_file */
    char *client_key_file_pass; /* Optional password for client_key_file */
    char *dh_params_file;
    char *ca_cert_file;
    char *ca_cert_dir;
    char *protocols;
    char *ciphers;
    char *ciphersuites;
    int prefer_server_ciphers;
    int session_caching;
    int session_cache_size;
    int session_cache_timeout;
} serverTLSContextConfig;

/*-----------------------------------------------------------------------------
 * Unix Context Configuration
 *----------------------------------------------------------------------------*/

typedef struct serverUnixContextConfig {
    char *group;       /* UNIX socket group */
    unsigned int perm; /* UNIX socket permission (see mode_t) */
} serverUnixContextConfig;

/*-----------------------------------------------------------------------------
 * RDMA Context Configuration
 *----------------------------------------------------------------------------*/
typedef struct serverRdmaContextConfig {
    char *bindaddr[CONFIG_BINDADDR_MAX];
    int bindaddr_count;
    int port;
    int rx_size;
    int completion_vector;
} serverRdmaContextConfig;

/*-----------------------------------------------------------------------------
 * AOF manifest definition
 *----------------------------------------------------------------------------*/
typedef enum {
    AOF_FILE_TYPE_BASE = 'b', /* BASE file */
    AOF_FILE_TYPE_HIST = 'h', /* HISTORY file */
    AOF_FILE_TYPE_INCR = 'i', /* INCR file */
} aof_file_type;

typedef struct {
    sds file_name;           /* file name */
    long long file_seq;      /* file sequence */
    aof_file_type file_type; /* file type */
} aofInfo;

typedef struct {
    aofInfo *base_aof_info;       /* BASE file information. NULL if there is no BASE file. */
    list *incr_aof_list;          /* INCR AOFs list. We may have multiple INCR AOF when rewrite fails. */
    list *history_aof_list;       /* HISTORY AOF list. When the AOFRW success, The aofInfo contained in
                                     `base_aof_info` and `incr_aof_list` will be moved to this list. We
                                     will delete these AOF files when AOFRW finish. */
    long long curr_base_file_seq; /* The sequence number used by the current BASE file. */
    long long curr_incr_file_seq; /* The sequence number used by the current INCR file. */
    int dirty;                    /* 1 Indicates that the aofManifest in the memory is inconsistent with
                                     disk, we need to persist it immediately. */
} aofManifest;

/*-----------------------------------------------------------------------------
 * Global server state
 *----------------------------------------------------------------------------*/

/* AIX defines hz to __hz, we don't use this define and in order to allow
 * the server build on AIX we need to undef it. */
#ifdef _AIX
#undef hz
#endif

#define CHILD_TYPE_NONE 0
#define CHILD_TYPE_RDB 1
#define CHILD_TYPE_AOF 2
#define CHILD_TYPE_LDB 3
#define CHILD_TYPE_MODULE 4

typedef enum childInfoType {
    CHILD_INFO_TYPE_CURRENT_INFO,
    CHILD_INFO_TYPE_AOF_COW_SIZE,
    CHILD_INFO_TYPE_RDB_COW_SIZE,
    CHILD_INFO_TYPE_MODULE_COW_SIZE
} childInfoType;

struct valkeyServer {
    /* General */
    pid_t pid;                /* Main process pid. */
    pthread_t main_thread_id; /* Main thread id */
    char *configfile;         /* Absolute config file path, or NULL */
    char *executable;         /* Absolute executable file path. */
    char **exec_argv;         /* Executable argv vector (copy). */
    mode_t umask;             /* The umask value of the process on startup */
    int hz;                   /* serverCron() calls frequency in hertz */
    int clients_hz;           /* clientsTimeProc() frequency in hertz */
    int in_fork_child;        /* indication that this is a fork child */
    serverDb *db;
    hashtable *commands;      /* Command table */
    hashtable *orig_commands; /* Command table before command renaming. */
    aeEventLoop *el;
    _Atomic AeIoState io_poll_state;     /* Indicates the state of the IO polling. */
    int io_ae_fired_events;              /* Number of poll events received by the IO thread. */
    rax *errors;                         /* Errors table */
    unsigned int lruclock;               /* Clock for LRU eviction */
    volatile sig_atomic_t shutdown_asap; /* Shutdown ordered by signal handler. */
    mstime_t shutdown_mstime;            /* Timestamp to limit graceful shutdown. */
    int last_sig_received;               /* Indicates the last SIGNAL received, if any (e.g., SIGINT or SIGTERM). */
    int shutdown_flags;                  /* Flags passed to prepareForShutdown(). */
    int activerehashing;                 /* Incremental rehash in serverCron() */
    int active_defrag_cpu_percent;       /* Current desired CPU percentage for active defrag */
    char *pidfile;                       /* PID file path */
    int arch_bits;                       /* 32 or 64 depending on sizeof(long) */
    int cronloops;                       /* Number of times the cron function run */
    char runid[CONFIG_RUN_ID_SIZE + 1];  /* ID always different at every exec. */
    int sentinel_mode;                   /* True if this instance is a Sentinel. */
    size_t initial_memory_usage;         /* Bytes used after initialization. */
    int always_show_logo;                /* Show logo even for non-stdout logging. */
    int in_exec;                         /* Are we inside EXEC? */
    int busy_module_yield_flags;         /* Are we inside a busy module? (triggered by RM_Yield). see BUSY_MODULE_YIELD_ flags. */
    const char *busy_module_yield_reply; /* When non-null, we are inside RM_Yield. */
    char *ignore_warnings;               /* Config: warnings that should be ignored. */
    int client_pause_in_transaction;     /* Was a client pause executed during this Exec? */
    int server_del_keys_in_slot;         /* The server is deleting the keys in the dirty slot. */
    int thp_enabled;                     /* If true, THP is enabled. */
    size_t page_size;                    /* The page size of OS. */
    /* Modules */
    dict *moduleapi;                  /* Exported core APIs dictionary for modules. */
    dict *sharedapi;                  /* Like moduleapi but containing the APIs that
                                         modules share with each other. */
    dict *module_configs_queue;       /* Dict that stores module configurations from .conf file until after modules are loaded
                                         during startup or arguments to loadex. */
    list *loadmodule_queue;           /* List of modules to load at startup. */
    int module_pipe[2];               /* Pipe used to awake the event loop by module threads. */
    pid_t child_pid;                  /* PID of current child */
    int child_type;                   /* Type of current child */
    _Atomic int module_gil_acquiring; /* Indicates whether the GIL is being acquiring by the main thread. */
    /* Networking */
    int port;                              /* TCP listening port */
    int tls_port;                          /* TLS listening port */
    int tcp_backlog;                       /* TCP listen() backlog */
    char *bindaddr[CONFIG_BINDADDR_MAX];   /* Addresses we should bind to */
    int bindaddr_count;                    /* Number of addresses in server.bindaddr[] */
    char *bind_source_addr;                /* Source address to bind on for outgoing connections */
    char *unixsocket;                      /* UNIX socket path */
    connListener listeners[CONN_TYPE_MAX]; /* TCP/Unix/TLS even more types */
    uint32_t socket_mark_id;               /* ID for listen socket marking */
    connListener clistener;                /* Cluster bus listener */
    list *clients;                         /* List of active clients */
    list *clients_to_close;                /* Clients to close asynchronously */
    list *clients_pending_write;           /* There is to write or install handler. */
    list *clients_pending_io_read;         /* List of clients with pending read to be process by I/O threads. */
    list *clients_pending_io_write;        /* List of clients with pending write to be process by I/O threads. */
    list *replicas, *monitors;             /* List of replicas and MONITORs */
    rax *replicas_waiting_psync;           /* Radix tree for tracking replicas awaiting partial synchronization.
                                            * Key: RDB client ID
                                            * Value: RDB client object
                                            * This structure holds dual-channel sync replicas from the start of their
                                            * RDB transfer until their main channel establishes partial synchronization. */
    client *current_client;                /* The client that triggered the command execution (External or AOF). */
    client *executing_client;              /* The client executing the current command (possibly script or module). */

#ifdef LOG_REQ_RES
    char *req_res_logfile; /* Path of log file for logging all requests and their replies. If NULL, no logging will be
                              performed */
    unsigned int client_default_resp;
#endif

    /* Stuff for client mem eviction */
    clientMemUsageBucket *client_mem_usage_buckets;

    rax *clients_timeout_table; /* Radix tree for blocked clients timeouts. */
    int execution_nesting;      /* Execution nesting level.
                                 * e.g. call(), async module stuff (timers, events, etc.),
                                 * cron stuff (active expire, eviction) */
    rax *clients_index;         /* Active clients dictionary by client ID. */
    uint32_t paused_actions;    /* Bitmask of actions that are currently paused */
    list *postponed_clients;    /* List of postponed clients */
    pause_event client_pause_per_purpose[NUM_PAUSE_PURPOSES];
    char neterr[ANET_ERR_LEN];                /* Error buffer for anet.c */
    dict *migrate_cached_sockets;             /* MIGRATE cached sockets */
    _Atomic uint64_t next_client_id;          /* Next client unique ID. Incremental. */
    int protected_mode;                       /* Don't accept external connections. */
    int io_threads_num;                       /* Number of IO threads to use. */
    int active_io_threads_num;                /* Current number of active IO threads, includes main thread. */
    int events_per_io_thread;                 /* Number of events on the event loop to trigger IO threads activation. */
    int prefetch_batch_max_size;              /* Maximum number of keys to prefetch in a single batch */
    long long events_processed_while_blocked; /* processEventsWhileBlocked() */
    int enable_protected_configs;             /* Enable the modification of protected configs, see PROTECTED_ACTION_ALLOWED_* */
    int enable_debug_cmd;                     /* Enable DEBUG commands, see PROTECTED_ACTION_ALLOWED_* */
    int enable_module_cmd;                    /* Enable MODULE commands, see PROTECTED_ACTION_ALLOWED_* */
    int enable_debug_assert;                  /* Enable debug asserts */

    /* RDB / AOF loading information */
    volatile sig_atomic_t loading;       /* We are loading data from disk if true */
    volatile sig_atomic_t async_loading; /* We are loading data without blocking the db being served */
    off_t loading_total_bytes;
    off_t loading_rdb_used_mem;
    off_t loading_loaded_bytes;
    time_t loading_start_time;
    off_t loading_process_events_interval_bytes;
    time_t loading_process_events_interval_ms;
    /* Fields used only for stats */
    time_t stat_starttime;                         /* Server start time */
    long long stat_numcommands;                    /* Number of processed commands */
    long long stat_numconnections;                 /* Number of connections received */
    long long stat_expiredkeys;                    /* Number of expired keys */
    double stat_expired_stale_perc;                /* Percentage of keys probably expired */
    long long stat_expired_time_cap_reached_count; /* Early expire cycle stops.*/
    long long stat_expire_cycle_time_used;         /* Cumulative microseconds used. */
    long long stat_evictedkeys;                    /* Number of evicted keys (maxmemory) */
    long long stat_evictedclients;                 /* Number of evicted clients */
    long long stat_evictedscripts;                 /* Number of evicted lua scripts. */
    long long stat_total_eviction_exceeded_time;   /* Total time over the memory limit, unit us */
    monotime stat_last_eviction_exceeded_time;     /* Timestamp of current eviction start, unit us */
    long long stat_keyspace_hits;                  /* Number of successful lookups of keys */
    long long stat_keyspace_misses;                /* Number of failed lookups of keys */
    long long stat_active_defrag_hits;             /* number of allocations moved */
    long long stat_active_defrag_misses;           /* number of allocations scanned but not moved */
    long long stat_active_defrag_key_hits;         /* number of keys with moved allocations */
    long long stat_active_defrag_key_misses;       /* number of keys scanned and not moved */
    long long stat_active_defrag_scanned;          /* number of dictEntries scanned */
    long long stat_total_active_defrag_time;       /* Total time memory fragmentation over the limit, unit us */
    monotime stat_last_active_defrag_time;         /* Timestamp of current active defrag start */
    size_t stat_peak_memory;                       /* Max used memory record */
    long long stat_aof_rewrites;                   /* number of aof file rewrites performed */
    long long stat_aofrw_consecutive_failures;     /* The number of consecutive failures of aofrw */
    long long stat_rdb_saves;                      /* number of rdb saves performed */
    long long stat_fork_time;                      /* Time needed to perform latest fork() */
    double stat_fork_rate;                         /* Fork rate in GB/sec. */
    long long stat_total_forks;                    /* Total count of fork. */
    long long stat_rejected_conn;                  /* Clients rejected because of maxclients */
    long long stat_sync_full;                      /* Number of full resyncs with replicas. */
    long long stat_sync_partial_ok;                /* Number of accepted PSYNC requests. */
    long long stat_sync_partial_err;               /* Number of unaccepted PSYNC requests. */
    commandlog commandlog[COMMANDLOG_TYPE_NUM];    /* Logs of commands. */
    struct malloc_stats cron_malloc_stats;         /* sampled in serverCron(). */
    long long stat_net_input_bytes;                /* Bytes read from network. */
    long long stat_net_output_bytes;               /* Bytes written to network. */
    long long stat_net_repl_input_bytes;           /* Bytes read during replication, added to stat_net_input_bytes in 'info'. */
    /* Bytes written during replication, added to stat_net_output_bytes in 'info'. */
    long long stat_net_repl_output_bytes;
    size_t stat_current_cow_peak;                       /* Peak size of copy on write bytes. */
    size_t stat_current_cow_bytes;                      /* Copy on write bytes while child is active. */
    monotime stat_current_cow_updated;                  /* Last update time of stat_current_cow_bytes */
    size_t stat_current_save_keys_processed;            /* Processed keys while child is active. */
    size_t stat_current_save_keys_total;                /* Number of keys when child started. */
    size_t stat_rdb_cow_bytes;                          /* Copy on write bytes during RDB saving. */
    size_t stat_aof_cow_bytes;                          /* Copy on write bytes during AOF rewrite. */
    size_t stat_module_cow_bytes;                       /* Copy on write bytes during module fork. */
    double stat_module_progress;                        /* Module save progress. */
    size_t stat_clients_type_memory[CLIENT_TYPE_COUNT]; /* Mem usage by type */
    size_t stat_cluster_links_memory;                   /* Mem usage by cluster links */
    long long
        stat_unexpected_error_replies;                 /* Number of unexpected (aof-loading, replica to primary, etc.) error replies */
    long long stat_total_error_replies;                /* Total number of issued error replies ( command + rejected errors ) */
    long long stat_dump_payload_sanitizations;         /* Number deep dump payloads integrity validations. */
    long long stat_io_reads_processed;                 /* Number of read events processed by IO threads */
    long long stat_io_writes_processed;                /* Number of write events processed by IO threads */
    long long stat_io_freed_objects;                   /* Number of objects freed by IO threads */
    long long stat_io_accept_offloaded;                /* Number of offloaded accepts */
    long long stat_poll_processed_by_io_threads;       /* Total number of poll jobs processed by IO */
    long long stat_total_reads_processed;              /* Total number of read events processed */
    long long stat_total_writes_processed;             /* Total number of write events processed */
    long long stat_client_qbuf_limit_disconnections;   /* Total number of clients reached query buf length limit */
    long long stat_client_outbuf_limit_disconnections; /* Total number of clients reached output buf length limit */
    long long stat_total_prefetch_entries;             /* Total number of prefetched dict entries */
    long long stat_total_prefetch_batches;             /* Total number of prefetched batches */
    /* The following two are used to track instantaneous metrics, like
     * number of operations per second, network traffic. */
    struct {
        long long last_sample_base;  /* The divisor of last sample window */
        long long last_sample_value; /* The dividend of last sample window */
        long long samples[STATS_METRIC_SAMPLES];
        int idx;
    } inst_metric[STATS_METRIC_COUNT];
    long long stat_reply_buffer_shrinks; /* Total number of output buffer shrinks */
    long long stat_reply_buffer_expands; /* Total number of output buffer expands */
    monotime el_start;
    /* The following two are used to record the max number of commands executed in one eventloop.
     * Note that commands in transactions are also counted. */
    long long el_cmd_cnt_start;
    long long el_cmd_cnt_max;
    /* The sum of active-expire, active-defrag and all other tasks done by cron and beforeSleep,
       but excluding read, write and AOF, which are counted by other sets of metrics. */
    monotime el_cron_duration;
    durationStats duration_stats[EL_DURATION_TYPE_NUM];

    /* Configuration */
    int verbosity;               /* Loglevel verbosity */
    int hide_user_data_from_log; /* Hide or redact user data, or data that may contain user data, from the log. */
    int maxidletime;             /* Client timeout in seconds */
    int tcpkeepalive;            /* Set SO_KEEPALIVE if non-zero. */
    int active_expire_enabled;   /* Can be disabled for testing purposes. */
    int active_expire_effort;    /* From 1 (default) to 10, active effort. */
    int lazy_expire_disabled;    /* If > 0, don't trigger lazy expire */
    int active_defrag_enabled;
    int sanitize_dump_payload;                   /* Enables deep sanitization for ziplist and listpack in RDB and RESTORE. */
    int skip_checksum_validation;                /* Disable checksum validation for RDB and RESTORE payload. */
    int rdb_version_check;                       /* Try to load RDB produced by a future version. */
    int jemalloc_bg_thread;                      /* Enable jemalloc background thread */
    int active_defrag_configuration_changed;     /* Config changed; need to recompute active_defrag_cpu_percent. */
    size_t active_defrag_ignore_bytes;           /* minimum amount of fragmentation waste to start active defrag */
    int active_defrag_threshold_lower;           /* minimum percentage of fragmentation to start active defrag */
    int active_defrag_threshold_upper;           /* maximum percentage of fragmentation at which we use maximum effort */
    int active_defrag_cpu_min;                   /* minimal effort for defrag in CPU percentage */
    int active_defrag_cpu_max;                   /* maximal effort for defrag in CPU percentage */
    int active_defrag_cycle_us;                  /* standard duration of defrag cycle */
    unsigned long active_defrag_max_scan_fields; /* maximum number of fields of set/hash/zset/list to process from
                                                    within the main dict scan */
    size_t client_max_querybuf_len;              /* Limit for client query buffer length */
    int dbnum;                                   /* Total number of configured DBs */
    int supervised;                              /* 1 if supervised, 0 otherwise. */
    int supervised_mode;                         /* See SUPERVISED_* */
    int daemonize;                               /* True if running as a daemon */
    int set_proc_title;                          /* True if change proc title */
    char *proc_title_template;                   /* Process title template format */
    clientBufferLimitsConfig client_obuf_limits[CLIENT_TYPE_OBUF_COUNT];
    int extended_redis_compat;                 /* True if extended Redis OSS compatibility is enabled */
    int pause_cron;                            /* Don't run cron tasks (debug) */
    int dict_resizing;                         /* Whether to allow main dict and expired dict to be resized (debug) */
    int latency_tracking_enabled;              /* 1 if extended latency tracking is enabled, 0 otherwise. */
    double *latency_tracking_info_percentiles; /* Extended latency tracking info output percentile list configuration. */
    int latency_tracking_info_percentiles_len;
    unsigned int max_new_tls_conns_per_cycle; /* The maximum number of tls connections that will be accepted during each
                                                 invocation of the event loop. */
    unsigned int max_new_conns_per_cycle;     /* The maximum number of tcp connections that will be accepted during each
                                                 invocation of the event loop. */
    /* AOF persistence */
    int aof_enabled;                    /* AOF configuration */
    int aof_state;                      /* AOF_(ON|OFF|WAIT_REWRITE) */
    int aof_fsync;                      /* Kind of fsync() policy */
    char *aof_filename;                 /* Basename of the AOF file and manifest file */
    char *aof_dirname;                  /* Name of the AOF directory */
    int aof_no_fsync_on_rewrite;        /* Don't fsync if a rewrite is in prog. */
    int aof_rewrite_perc;               /* Rewrite AOF if % growth is > M and... */
    off_t aof_rewrite_min_size;         /* the AOF file is at least N bytes. */
    off_t aof_rewrite_base_size;        /* AOF size on latest startup or rewrite. */
    off_t aof_current_size;             /* AOF current size (Including BASE + INCRs). */
    off_t aof_last_incr_size;           /* The size of the latest incr AOF. */
    off_t aof_last_incr_fsync_offset;   /* AOF offset which is already requested to be synced to disk.
                                         * Compare with the aof_last_incr_size. */
    int aof_flush_sleep;                /* Micros to sleep before flush. (used by tests) */
    int aof_rewrite_scheduled;          /* Rewrite once BGSAVE terminates. */
    sds aof_buf;                        /* AOF buffer, written before entering the event loop */
    int aof_fd;                         /* File descriptor of currently selected AOF file */
    int aof_selected_db;                /* Currently selected DB in AOF */
    mstime_t aof_flush_postponed_start; /* mstime of postponed AOF flush */
    mstime_t aof_last_fsync;            /* mstime of last fsync() */
    time_t aof_rewrite_time_last;       /* Time used by last AOF rewrite run. */
    time_t aof_rewrite_time_start;      /* Current AOF rewrite start time. */
    time_t aof_cur_timestamp;           /* Current record timestamp in AOF */
    int aof_timestamp_enabled;          /* Enable record timestamp in AOF */
    int aof_lastbgrewrite_status;       /* C_OK or C_ERR */
    unsigned long aof_delayed_fsync;    /* delayed AOF fsync() counter */
    int aof_rewrite_incremental_fsync;  /* fsync incrementally while aof rewriting? */
    int rdb_save_incremental_fsync;     /* fsync incrementally while rdb saving? */
    int aof_last_write_status;          /* C_OK or C_ERR */
    int aof_last_write_errno;           /* Valid if aof write/fsync status is ERR */
    int aof_load_truncated;             /* Don't stop on unexpected AOF EOF. */
    int aof_use_rdb_preamble;           /* Specify base AOF to use RDB encoding on AOF rewrites. */
    int aof_rewrite_use_rdb_preamble;   /* Base AOF to use RDB encoding on AOF rewrites start. */
    _Atomic int aof_bio_fsync_status;   /* Status of AOF fsync in bio job. */
    _Atomic int aof_bio_fsync_errno;    /* Errno of AOF fsync in bio job. */
    aofManifest *aof_manifest;          /* Used to track AOFs. */
    int aof_disable_auto_gc;            /* If disable automatically deleting HISTORY type AOFs?
                                           default no. (for testings). */

    /* RDB persistence */
    long long dirty;                      /* Changes to DB from the last save */
    long long dirty_before_bgsave;        /* Used to restore dirty on failed BGSAVE */
    long long rdb_last_load_keys_expired; /* number of expired keys when loading RDB */
    long long rdb_last_load_keys_loaded;  /* number of loaded keys when loading RDB */
    struct saveparam *saveparams;         /* Save points array for RDB */
    int saveparamslen;                    /* Number of saving points */
    char *rdb_filename;                   /* Name of RDB file */
    int rdb_compression;                  /* Use compression in RDB? */
    int rdb_checksum;                     /* Use RDB checksum? */
    int rdb_del_sync_files;               /* Remove RDB files used only for SYNC if
                                             the instance does not use persistence. */
    time_t lastsave;                      /* Unix time of last successful save */
    time_t lastbgsave_try;                /* Unix time of last attempted bgsave */
    time_t rdb_save_time_last;            /* Time used by last RDB save run. */
    time_t rdb_save_time_start;           /* Current RDB save start time. */
    int rdb_bgsave_scheduled;             /* BGSAVE when possible if true. */
    int rdb_child_type;                   /* Type of save by active child. */
    int lastbgsave_status;                /* C_OK or C_ERR */
    int stop_writes_on_bgsave_err;        /* Don't allow writes if can't BGSAVE */
    int rdb_pipe_read;                    /* RDB pipe used to transfer the rdb data */
                                          /* to the parent process in diskless repl. */
    int rdb_child_exit_pipe;              /* Used by the diskless parent allow child exit. */
    connection **rdb_pipe_conns;          /* Connections which are currently the */
    int rdb_pipe_numconns;                /* target of diskless rdb fork child. */
    int rdb_pipe_numconns_writing;        /* Number of rdb conns with pending writes. */
    char *rdb_pipe_buff;                  /* In diskless replication, this buffer holds data */
    int rdb_pipe_bufflen;                 /* that was read from the rdb pipe. */
    int rdb_key_save_delay;               /* Delay in microseconds between keys while
                                           * writing aof or rdb. (for testings). negative
                                           * value means fractions of microseconds (on average). */
    int key_load_delay;                   /* Delay in microseconds between keys while
                                           * loading aof or rdb. (for testings). negative
                                           * value means fractions of microseconds (on average). */
    /* Pipe and data structures for child -> parent info sharing. */
    int child_info_pipe[2]; /* Pipe used to write the child_info_data. */
    int child_info_nread;   /* Num of bytes of the last read from pipe */
    /* Propagation of commands in AOF / replication */
    serverOpArray also_propagate; /* Additional command to propagate. */
    int replication_allowed;      /* Are we allowed to replicate? */
    /* Logging */
    char *logfile;            /* Path of log file */
    int syslog_enabled;       /* Is syslog enabled? */
    char *syslog_ident;       /* Syslog ident */
    int syslog_facility;      /* Syslog facility */
    int crashlog_enabled;     /* Enable signal handler for crashlog.
                               * disable for clean core dumps. */
    int crashed;              /* True if the server has crashed, used in catClientInfoString
                               * to indicate that no wait for IO threads is needed. */
    int memcheck_enabled;     /* Enable memory check on crash. */
    int use_exit_on_panic;    /* Use exit() on panic and assert rather than
                               * abort(). useful for Valgrind. */
    int log_format;           /* Print log in specific format */
    int log_timestamp_format; /* Timestamp format in log */
    /* Shutdown */
    int shutdown_timeout;    /* Graceful shutdown time limit in seconds. */
    int shutdown_on_sigint;  /* Shutdown flags configured for SIGINT. */
    int shutdown_on_sigterm; /* Shutdown flags configured for SIGTERM. */

    /* Replication (primary) */
    char replid[CONFIG_RUN_ID_SIZE + 1];       /* My current replication ID. */
    char replid2[CONFIG_RUN_ID_SIZE + 1];      /* replid inherited from primary*/
    long long primary_repl_offset;             /* My current replication offset */
    long long second_replid_offset;            /* Accept offsets up to this for replid2. */
    _Atomic long long fsynced_reploff_pending; /* Largest replication offset to
                                      * potentially have been fsynced, applied to
                                        fsynced_reploff only when AOF state is AOF_ON
                                        (not during the initial rewrite) */
    long long fsynced_reploff;                 /* Largest replication offset that has been confirmed to be fsynced */
    int replicas_eldb;                         /* Last SELECTed DB in replication output */
    int repl_ping_replica_period;              /* Primary pings the replica every N seconds */
    replBacklog *repl_backlog;                 /* Replication backlog for partial syncs */
    long long repl_backlog_size;               /* Backlog circular buffer size */
    replDataBuf pending_repl_data;             /* Replication data buffer for dual-channel-replication */
    time_t repl_backlog_time_limit;            /* Time without replicas after the backlog
                                                  gets released. */
    time_t repl_no_replicas_since;             /* We have no replicas since that time.
                                                Only valid if server.replicas len is 0. */
    int repl_min_replicas_to_write;            /* Min number of replicas to write. */
    int repl_min_replicas_max_lag;             /* Max lag of <count> replicas to write. */
    int repl_good_replicas_count;              /* Number of replicas with lag <= max_lag. */
    int repl_diskless_sync;                    /* Primary send RDB to replicas sockets directly. */
    int repl_diskless_load;                    /* Replica parse RDB directly from the socket.
                                                * see REPL_DISKLESS_LOAD_* enum */
    int repl_diskless_sync_delay;              /* Delay to start a diskless repl BGSAVE. */
    int repl_diskless_sync_max_replicas;       /* Max replicas for diskless repl BGSAVE
                                                * delay (start sooner if they all connect). */
    int dual_channel_replication;              /* Config used to determine if the replica should
                                                * use dual channel replication for full syncs. */
    int wait_before_rdb_client_free;           /* Grace period in seconds for replica main channel
                                                * to establish psync. */
    int debug_pause_after_fork;                /* Debug param that pauses the main process
                                                * after a replication fork() (for bgsave). */
    size_t repl_buffer_mem;                    /* The memory of replication buffer. */
    list *repl_buffer_blocks;                  /* Replication buffers blocks list
                                                * (serving replica clients and repl backlog) */
    /* Replication (replica) */
    char *primary_user;     /* AUTH with this user and primary_auth with primary */
    sds primary_auth;       /* AUTH with this password with primary */
    char *primary_host;     /* Hostname of primary */
    int primary_port;       /* Port of primary */
    int repl_timeout;       /* Timeout after N seconds of primary idle */
    client *primary;        /* Client that is primary for this replica */
    uint64_t rdb_client_id; /* Rdb client id as it defined at primary side */
    struct {
        connection *conn;
        char replid[CONFIG_RUN_ID_SIZE + 1];
        long long reploff;
        long long read_reploff;
        int dbid;
    } repl_provisional_primary;
    client *cached_primary;             /* Cached primary to be reused for PSYNC. */
    rio *loading_rio;                   /* Pointer to the rio object currently used for loading data. */
    int repl_syncio_timeout;            /* Timeout for synchronous I/O calls */
    int repl_state;                     /* Replication status if the instance is a replica */
    int repl_rdb_channel_state;         /* State of the replica's rdb channel during dual-channel-replication */
    off_t repl_transfer_size;           /* Size of RDB to read from primary during sync. */
    off_t repl_transfer_read;           /* Amount of RDB read from primary during sync. */
    off_t repl_transfer_last_fsync_off; /* Offset when we fsync-ed last time. */
    connection *repl_transfer_s;        /* Replica -> Primary SYNC connection */
    connection *repl_rdb_transfer_s;    /* Primary FULL SYNC connection (RDB download) */
    int repl_transfer_fd;               /* Replica -> Primary SYNC temp file descriptor */
    char *repl_transfer_tmpfile;        /* Replica-> Primary SYNC temp file name */
    time_t repl_transfer_lastio;        /* Unix time of the latest read, for timeout */
    int repl_serve_stale_data;          /* Serve stale data when link is down? */
    int repl_replica_ro;                /* Replica is read only? */
    int repl_replica_ignore_maxmemory;  /* If true replicas do not evict. */
    time_t repl_down_since;             /* Unix time at which link with primary went down */
    int repl_disable_tcp_nodelay;       /* Disable TCP_NODELAY after SYNC? */
    int replica_priority;               /* Reported in INFO and used by Sentinel. */
    int replica_announced;              /* If true, replica is announced by Sentinel */
    int replica_announce_port;          /* Give the primary this listening port. */
    char *replica_announce_ip;          /* Give the primary this ip address. */
    int propagation_error_behavior;     /* Configures the behavior of the replica
                                         * when it receives an error on the replication stream */
    int repl_ignore_disk_write_error;   /* Configures whether replicas panic when unable to
                                         * persist writes to AOF. */

    /* The following two fields is where we store primary PSYNC replid/offset
     * while the PSYNC is in progress. At the end we'll copy the fields into
     * the server->primary client structure. */
    char primary_replid[CONFIG_RUN_ID_SIZE + 1]; /* Primary PSYNC runid. */
    long long primary_initial_offset;            /* Primary PSYNC offset. */
    int repl_replica_lazy_flush;                 /* Lazy FLUSHALL before loading DB? */
    /* Import Mode */
    int import_mode; /* If true, server is in import mode and forbid expiration and eviction. */
    /* Synchronous replication. */
    list *clients_waiting_acks; /* Clients waiting in WAIT or WAITAOF. */
    int get_ack_from_replicas;  /* If true we send REPLCONF GETACK. */
    /* Limits */
    unsigned int maxclients;                    /* Max number of simultaneous clients */
    unsigned long long maxmemory;               /* Max number of memory bytes to use */
    ssize_t maxmemory_clients;                  /* Memory limit for total client buffers */
    int maxmemory_policy;                       /* Policy for key eviction */
    int maxmemory_samples;                      /* Precision of random sampling */
    int maxmemory_eviction_tenacity;            /* Aggressiveness of eviction processing */
    int lfu_log_factor;                         /* LFU logarithmic counter factor. */
    int lfu_decay_time;                         /* LFU counter decay factor. */
    long long proto_max_bulk_len;               /* Protocol bulk length maximum size. */
    int oom_score_adj_values[CONFIG_OOM_COUNT]; /* Linux oom_score_adj configuration */
    int oom_score_adj;                          /* If true, oom_score_adj is managed */
    int disable_thp;                            /* If true, disable THP by syscall */
    /* Blocked clients */
    unsigned int blocked_clients; /* # of clients executing a blocking cmd.*/
    unsigned int blocked_clients_by_type[BLOCKED_NUM];
    list *unblocked_clients; /* list of clients to unblock before next loop */
    list *ready_keys;        /* List of readyList structures for BLPOP & co */
    /* Client side caching. */
    unsigned int tracking_clients;  /* # of clients with tracking enabled.*/
    size_t tracking_table_max_keys; /* Max number of keys in tracking table. */
    list *tracking_pending_keys;    /* tracking invalidation keys pending to flush */
    list *pending_push_messages;    /* pending publish or other push messages to flush */
    /* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    int sort_store;
    /* Zip structure config, see redis.conf for more information  */
    size_t hash_max_listpack_entries;
    size_t hash_max_listpack_value;
    size_t set_max_intset_entries;
    size_t set_max_listpack_entries;
    size_t set_max_listpack_value;
    size_t zset_max_listpack_entries;
    size_t zset_max_listpack_value;
    size_t hll_sparse_max_bytes;
    size_t stream_node_max_bytes;
    long long stream_node_max_entries;
    /* List parameters */
    int list_max_listpack_size;
    int list_compress_depth;
    /* time cache */
    time_t unixtime;             /* Unix time sampled every cron cycle. */
    time_t timezone;             /* Cached timezone. As set by tzset(). */
    _Atomic int daylight_active; /* Currently in daylight saving time. */
    mstime_t mstime;             /* 'unixtime' in milliseconds. */
    ustime_t ustime;             /* 'unixtime' in microseconds. */
    mstime_t cmd_time_snapshot;  /* Time snapshot of the root execution nesting. */
    size_t blocking_op_nesting;  /* Nesting level of blocking operation, used to reset blocked_last_cron. */
    long long blocked_last_cron; /* Indicate the mstime of the last time we did cron jobs from a blocking operation */
    /* Pubsub */
    kvstore *pubsub_channels;      /* Map channels to list of subscribed clients */
    dict *pubsub_patterns;         /* A dict of pubsub_patterns */
    int notify_keyspace_events;    /* Events to propagate via Pub/Sub. This is an
                                      xor of NOTIFY_... flags. */
    kvstore *pubsubshard_channels; /* Map shard channels in every slot to list of subscribed clients */
    unsigned int pubsub_clients;   /* # of clients in Pub/Sub mode */
    unsigned int watching_clients; /* # of clients are watching keys */
    /* Cluster */
    int cluster_enabled;                                   /* Is cluster enabled? */
    int cluster_port;                                      /* Set the cluster port for a node. */
    mstime_t cluster_node_timeout;                         /* Cluster node timeout. */
    mstime_t cluster_ping_interval;                        /* A debug configuration for setting how often cluster nodes send ping messages. */
    char *cluster_configfile;                              /* Cluster auto-generated config file name. */
    struct clusterState *cluster;                          /* State of the cluster */
    int cluster_migration_barrier;                         /* Cluster replicas migration barrier. */
    int cluster_allow_replica_migration;                   /* Automatic replica migrations to orphaned primaries and from empty primaries */
    int cluster_replica_validity_factor;                   /* Replica max data age for failover. */
    int cluster_require_full_coverage;                     /* If true, put the cluster down if
                                                              there is at least an uncovered slot.*/
    int cluster_replica_no_failover;                       /* Prevent replica from starting a failover
                                                            if the primary is in failure state. */
    char *cluster_announce_ip;                             /* IP address to announce on cluster bus. */
    char *cluster_announce_client_ipv4;                    /* IPv4 for clients, to announce on cluster bus. */
    char *cluster_announce_client_ipv6;                    /* IPv6 for clients, to announce on cluster bus. */
    char *cluster_announce_hostname;                       /* hostname to announce on cluster bus. */
    char *cluster_announce_human_nodename;                 /* Human readable node name assigned to a node. */
    int cluster_preferred_endpoint_type;                   /* Use the announced hostname when available. */
    int cluster_announce_port;                             /* base port to announce on cluster bus. */
    int cluster_announce_tls_port;                         /* TLS port to announce on cluster bus. */
    int cluster_announce_bus_port;                         /* bus port to announce on cluster bus. */
    int cluster_module_flags;                              /* Set of flags that modules are able
                                                              to set in order to suppress certain
                                                              native Redis Cluster features. Check the
                                                              VALKEYMODULE_CLUSTER_FLAG_*. */
    int cluster_allow_reads_when_down;                     /* Are reads allowed when the cluster
                                                            is down? */
    int cluster_config_file_lock_fd;                       /* cluster config fd, will be flocked. */
    unsigned long long cluster_link_msg_queue_limit_bytes; /* Memory usage limit on individual link msg queue */
    int cluster_drop_packet_filter;                        /* Debug config that allows tactically
                                                            * dropping packets of a specific type */
    unsigned long cluster_blacklist_ttl;                   /* Duration in seconds that a node is denied re-entry into
                                                            * the cluster after it is forgotten with CLUSTER FORGET. */
    int cluster_slot_stats_enabled;                        /* Cluster slot usage statistics tracking enabled. */
    /* Debug config that goes along with cluster_drop_packet_filter. When set, the link is closed on packet drop. */
    uint32_t debug_cluster_close_link_on_packet_drop : 1;
    /* Debug config to control the random ping. When set, we will disable the random ping in clusterCron. */
    uint32_t debug_cluster_disable_random_ping : 1;
    sds cached_cluster_slot_info[CACHE_CONN_TYPE_MAX]; /* Index in array is a bitwise or of CACHE_CONN_TYPE_* */
    /* Scripting */
    mstime_t busy_reply_threshold;  /* Script / module timeout in milliseconds */
    int pre_command_oom_state;      /* OOM before command (script?) was started */
    int script_disable_deny_script; /* Allow running commands marked "noscript" inside a script. */
    /* Lazy free */
    int lazyfree_lazy_eviction;
    int lazyfree_lazy_expire;
    int lazyfree_lazy_server_del;
    int lazyfree_lazy_user_del;
    int lazyfree_lazy_user_flush;
    /* Latency monitor */
    long long latency_monitor_threshold;
    dict *latency_events;
    /* ACLs */
    char *acl_filename;           /* ACL Users file. NULL if not configured. */
    unsigned long acllog_max_len; /* Maximum length of the ACL LOG list. */
    sds requirepass;              /* Remember the cleartext password set with
                                     the old "requirepass" directive for
                                     backward compatibility with Redis <= 5. */
    int acl_pubsub_default;       /* Default ACL pub/sub channels flag */
    aclInfo acl_info;             /* ACL info */
    /* Assert & bug reporting */
    int watchdog_period; /* Software watchdog period in ms. 0 = off */
    /* System hardware info */
    size_t system_memory_size; /* Total memory in system as reported by OS */
    /* TLS Configuration */
    int tls_cluster;
    int tls_replication;
    int tls_auth_clients;
    serverTLSContextConfig tls_ctx_config;
    serverUnixContextConfig unix_ctx_config;
    serverRdmaContextConfig rdma_ctx_config;
    /* cpu affinity */
    char *server_cpulist;      /* cpu affinity list of server main/io thread. */
    char *bio_cpulist;         /* cpu affinity list of bio thread. */
    char *aof_rewrite_cpulist; /* cpu affinity list of aof rewrite process. */
    char *bgsave_cpulist;      /* cpu affinity list of bgsave process. */
    /* Sentinel config */
    struct sentinelConfig *sentinel_config; /* sentinel config to load at startup time. */
    /* Coordinate failover info */
    mstime_t failover_end_time;              /* Deadline for failover command. */
    int force_failover;                      /* If true then failover will be forced at the
                                              * deadline, otherwise failover is aborted. */
    char *target_replica_host;               /* Failover target host. If null during a
                                              * failover then any replica can be used. */
    int target_replica_port;                 /* Failover target port */
    int failover_state;                      /* Failover state */
    int cluster_allow_pubsubshard_when_down; /* Is pubsubshard allowed when the cluster
                                                is down, doesn't affect pubsub global. */
    long reply_buffer_peak_reset_time;       /* The amount of time (in milliseconds) to wait between reply buffer peak resets */
    int reply_buffer_resizing_enabled;       /* Is reply buffer resizing enabled (1 by default) */
    sds availability_zone;                   /* When run in a cloud environment we can configure the availability zone it is running in */
    /* Local environment */
    char *locale_collate;
    char *debug_context; /* A free-form string that has no impact on server except being included in a crash report. */
};

#define MAX_KEYS_BUFFER 256

typedef struct {
    int pos;   /* The position of the key within the client array */
    int flags; /* The flags associated with the key access, see
                  CMD_KEY_* for more information */
} keyReference;

/* A result structure for the various getkeys function calls. It lists the
 * keys as indices to the provided argv. This functionality is also re-used
 * for returning channel information.
 */
typedef struct {
    int numkeys;                           /* Number of key indices return */
    int size;                              /* Available array size */
    keyReference *keys;                    /* Key indices array, points to keysbuf or heap */
    keyReference keysbuf[MAX_KEYS_BUFFER]; /* Pre-allocated buffer, to save heap allocations */
} getKeysResult;

static inline void initGetKeysResult(getKeysResult *result) {
    result->numkeys = 0;
    result->size = MAX_KEYS_BUFFER;
    result->keys = NULL;
}

/* Key specs definitions.
 *
 * Brief: This is a scheme that tries to describe the location
 * of key arguments better than the old [first,last,step] scheme
 * which is limited and doesn't fit many commands.
 *
 * There are two steps:
 * 1. begin_search (BS): in which index should we start searching for keys?
 * 2. find_keys (FK): relative to the output of BS, how can we will which args are keys?
 *
 * There are two types of BS:
 * 1. index: key args start at a constant index
 * 2. keyword: key args start just after a specific keyword
 *
 * There are two kinds of FK:
 * 1. range: keys end at a specific index (or relative to the last argument)
 * 2. keynum: there's an arg that contains the number of key args somewhere before the keys themselves
 */

/* WARNING! Must be synced with generate-command-code.py and ValkeyModuleKeySpecBeginSearchType */
typedef enum {
    KSPEC_BS_INVALID = 0, /* Must be 0 */
    KSPEC_BS_UNKNOWN,
    KSPEC_BS_INDEX,
    KSPEC_BS_KEYWORD
} kspec_bs_type;

/* WARNING! Must be synced with generate-command-code.py and ValkeyModuleKeySpecFindKeysType */
typedef enum {
    KSPEC_FK_INVALID = 0, /* Must be 0 */
    KSPEC_FK_UNKNOWN,
    KSPEC_FK_RANGE,
    KSPEC_FK_KEYNUM
} kspec_fk_type;

/* WARNING! This struct must match ValkeyModuleCommandKeySpec */
typedef struct {
    /* Declarative data */
    const char *notes;
    uint64_t flags;
    kspec_bs_type begin_search_type;
    union {
        struct {
            /* The index from which we start the search for keys */
            int pos;
        } index;
        struct {
            /* The keyword that indicates the beginning of key args */
            const char *keyword;
            /* An index in argv from which to start searching.
             * Can be negative, which means start search from the end, in reverse
             * (Example: -2 means to start in reverse from the penultimate arg) */
            int startfrom;
        } keyword;
    } bs;
    kspec_fk_type find_keys_type;
    union {
        /* NOTE: Indices in this struct are relative to the result of the begin_search step!
         * These are: range.lastkey, keynum.keynumidx, keynum.firstkey */
        struct {
            /* Index of the last key.
             * Can be negative, in which case it's not relative. -1 indicating till the last argument,
             * -2 one before the last and so on. */
            int lastkey;
            /* How many args should we skip after finding a key, in order to find the next one. */
            int keystep;
            /* If lastkey is -1, we use limit to stop the search by a factor. 0 and 1 mean no limit.
             * 2 means 1/2 of the remaining args, 3 means 1/3, and so on. */
            int limit;
        } range;
        struct {
            /* Index of the argument containing the number of keys to come */
            int keynumidx;
            /* Index of the fist key (Usually it's just after keynumidx, in
             * which case it should be set to keynumidx+1). */
            int firstkey;
            /* How many args should we skip after finding a key, in order to find the next one. */
            int keystep;
        } keynum;
    } fk;
} keySpec;

#ifdef LOG_REQ_RES

/* Must be synced with generate-command-code.py */
typedef enum {
    JSON_TYPE_STRING,
    JSON_TYPE_INTEGER,
    JSON_TYPE_BOOLEAN,
    JSON_TYPE_OBJECT,
    JSON_TYPE_ARRAY,
} jsonType;

typedef struct jsonObjectElement {
    jsonType type;
    const char *key;
    union {
        const char *string;
        long long integer;
        int boolean;
        struct jsonObject *object;
        struct {
            struct jsonObject **objects;
            int length;
        } array;
    } value;
} jsonObjectElement;

typedef struct jsonObject {
    struct jsonObjectElement *elements;
    int length;
} jsonObject;

#endif

/* WARNING! This struct must match ValkeyModuleCommandHistoryEntry */
typedef struct {
    const char *since;
    const char *changes;
} commandHistory;

/* Must be synced with COMMAND_GROUP_STR and generate-command-code.py */
typedef enum {
    COMMAND_GROUP_GENERIC,
    COMMAND_GROUP_STRING,
    COMMAND_GROUP_LIST,
    COMMAND_GROUP_SET,
    COMMAND_GROUP_SORTED_SET,
    COMMAND_GROUP_HASH,
    COMMAND_GROUP_PUBSUB,
    COMMAND_GROUP_TRANSACTIONS,
    COMMAND_GROUP_CONNECTION,
    COMMAND_GROUP_SERVER,
    COMMAND_GROUP_SCRIPTING,
    COMMAND_GROUP_HYPERLOGLOG,
    COMMAND_GROUP_CLUSTER,
    COMMAND_GROUP_SENTINEL,
    COMMAND_GROUP_GEO,
    COMMAND_GROUP_STREAM,
    COMMAND_GROUP_BITMAP,
    COMMAND_GROUP_MODULE,
} serverCommandGroup;

typedef void serverCommandProc(client *c);
typedef int serverGetKeysProc(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);

/* Command structure.
 *
 * Note that the command table is in commands.c and it is auto-generated.
 *
 * This is the meaning of the flags:
 *
 * CMD_WRITE:       Write command (may modify the key space).
 *
 * CMD_READONLY:    Commands just reading from keys without changing the content.
 *                  Note that commands that don't read from the keyspace such as
 *                  TIME, SELECT, INFO, administrative commands, and connection
 *                  or transaction related commands (multi, exec, discard, ...)
 *                  are not flagged as read-only commands, since they affect the
 *                  server or the connection in other ways.
 *
 * CMD_DENYOOM:     May increase memory usage once called. Don't allow if out
 *                  of memory.
 *
 * CMD_MODULE:      Command exported by module.
 *
 * CMD_ADMIN:       Administrative command, like SAVE or SHUTDOWN.
 *
 * CMD_PUBSUB:      Pub/Sub related command.
 *
 * CMD_NOSCRIPT:    Command not allowed in scripts.
 *
 * CMD_BLOCKING:    The command has the potential to block the client.
 *
 * CMD_LOADING:     Allow the command while loading the database.
 *
 * CMD_NO_ASYNC_LOADING: Deny during async loading (when a replica uses diskless
 *                       sync swapdb, and allows access to the old dataset)
 *
 * CMD_STALE:       Allow the command while a replica has stale data but is not
 *                  allowed to serve this data. Normally no command is accepted
 *                  in this condition but just a few.
 *
 * CMD_SKIP_MONITOR:  Do not automatically propagate the command on MONITOR.
 *
 * CMD_SKIP_COMMANDLOG:  Do not automatically propagate the command to the commandlog.
 *
 * CMD_ASKING:      Perform an implicit ASKING for this command, so the
 *                  command will be accepted in cluster mode if the slot is marked
 *                  as 'importing'.
 *
 * CMD_FAST:        Fast command: O(1) or O(log(N)) command that should never
 *                  delay its execution as long as the kernel scheduler is giving
 *                  us time. Note that commands that may trigger a DEL as a side
 *                  effect (like SET) are not fast commands.
 *
 * CMD_NO_AUTH:     Command doesn't require authentication
 *
 * CMD_MAY_REPLICATE:   Command may produce replication traffic, but should be
 *                      allowed under circumstances where write commands are disallowed.
 *                      Examples include PUBLISH, which replicates pubsub messages,and
 *                      EVAL, which may execute write commands, which are replicated,
 *                      or may just execute read commands. A command can not be marked
 *                      both CMD_WRITE and CMD_MAY_REPLICATE
 *
 * CMD_SENTINEL:    This command is present in sentinel mode.
 *
 * CMD_ONLY_SENTINEL: This command is present only when in sentinel mode.
 *                    And should be removed from redis.
 *
 * CMD_NO_MANDATORY_KEYS: This key arguments for this command are optional.
 *
 * CMD_PROTECTED: The command is a protected command, see enable-debug-command for more details.
 *
 * CMD_MODULE_GETKEYS: Use the modules getkeys interface.
 *
 * CMD_MODULE_NO_CLUSTER: Deny on cluster.
 *
 * CMD_NO_MULTI: The command is not allowed inside a transaction
 *
 * CMD_MOVABLE_KEYS: The legacy range spec doesn't cover all keys. Populated by
 *                   populateCommandLegacyRangeSpec.
 *
 * CMD_ALLOW_BUSY: The command can run while another command is running for
 *                 a long time (timedout script, module command that yields)
 *
 * CMD_MODULE_GETCHANNELS: Use the modules getchannels interface.
 *
 * CMD_TOUCHES_ARBITRARY_KEYS: The command may touch (and cause lazy-expire)
 *                             arbitrary key (i.e not provided in argv)
 *
 * The following additional flags are only used in order to put commands
 * in a specific ACL category. Commands can have multiple ACL categories.
 * See valkey.conf for the exact meaning of each.
 *
 * @keyspace, @read, @write, @set, @sortedset, @list, @hash, @string, @bitmap,
 * @hyperloglog, @stream, @admin, @fast, @slow, @pubsub, @blocking, @dangerous,
 * @connection, @transaction, @scripting, @geo.
 *
 * Note that:
 *
 * 1) The read-only flag implies the @read ACL category.
 * 2) The write flag implies the @write ACL category.
 * 3) The fast flag implies the @fast ACL category.
 * 4) The admin flag implies the @admin and @dangerous ACL category.
 * 5) The pub-sub flag implies the @pubsub ACL category.
 * 6) The lack of fast flag implies the @slow ACL category.
 * 7) The non obvious "keyspace" category includes the commands
 *    that interact with keys without having anything to do with
 *    specific data structures, such as: DEL, RENAME, MOVE, SELECT,
 *    TYPE, EXPIRE*, PEXPIRE*, TTL, PTTL, ...
 */
struct serverCommand {
    /* Declarative data */
    const char *declared_name;    /* A string representing the command declared_name.
                                   * It is a const char * for native commands and SDS for module commands. */
    const char *summary;          /* Summary of the command (optional). */
    const char *complexity;       /* Complexity description (optional). */
    const char *since;            /* Debut version of the command (optional). */
    int doc_flags;                /* Flags for documentation (see CMD_DOC_*). */
    const char *replaced_by;      /* In case the command is deprecated, this is the successor command. */
    const char *deprecated_since; /* In case the command is deprecated, when did it happen? */
    serverCommandGroup group;     /* Command group */
    commandHistory *history;      /* History of the command */
    int num_history;
    const char **tips; /* An array of strings that are meant to be tips for clients/proxies regarding this command */
    int num_tips;
    serverCommandProc *proc; /* Command implementation */
    int arity;               /* Number of arguments, it is possible to use -N to say >= N */
    uint64_t flags;          /* Command flags, see CMD_*. */
    uint64_t acl_categories; /* ACl categories, see ACL_CATEGORY_*. */
    keySpec *key_specs;
    int key_specs_num;
    /* Use a function to determine keys arguments in a command line.
     * Used for Cluster redirect (may be NULL) */
    serverGetKeysProc *getkeys_proc;
    int num_args; /* Length of args array. */
    /* Array of subcommands (may be NULL) */
    struct serverCommand *subcommands;
    /* Array of arguments (may be NULL) */
    struct serverCommandArg *args;
#ifdef LOG_REQ_RES
    /* Reply schema */
    struct jsonObject *reply_schema;
#endif

    /* Runtime populated data */
    long long microseconds, calls, rejected_calls, failed_calls;
    int id;           /* Command ID. This is a progressive ID starting from 0 that
                         is assigned at runtime, and is used in order to check
                         ACLs. A connection is able to execute a given command if
                         the user associated to the connection has this command
                         bit set in the bitmap of allowed commands. */
    sds fullname;     /* Includes parent name if any: "parentcmd|childcmd". Unchanged if command is renamed. */
    sds current_name; /* Same as fullname, becomes a separate string if command is renamed. */
    struct hdr_histogram
        *latency_histogram;        /* Points to the command latency command histogram (unit of time nanosecond). */
    keySpec legacy_range_key_spec; /* The legacy (first,last,step) key spec is
                                    * still maintained (if applicable) so that
                                    * we can still support the reply format of
                                    * COMMAND INFO and COMMAND GETKEYS */
    hashtable *subcommands_ht;     /* Subcommands hash table. The key is the subcommand sds name
                                    * (not the fullname), and the value is the serverCommand structure pointer. */
    struct serverCommand *parent;
    struct ValkeyModuleCommand *module_cmd; /* A pointer to the module command data (NULL if native command) */
};

struct serverError {
    long long count;
};

struct serverFunctionSym {
    char *name;
    unsigned long pointer;
};

typedef struct _serverSortObject {
    robj *obj;
    union {
        double score;
        robj *cmpobj;
    } u;
} serverSortObject;

typedef struct _serverSortOperation {
    int type;
    robj *pattern;
} serverSortOperation;

/* Structure to hold list iteration abstraction. */
typedef struct {
    robj *subject;
    unsigned char encoding;
    unsigned char direction; /* Iteration direction */

    unsigned char *lpi;  /* listpack iterator */
    quicklistIter *iter; /* quicklist iterator */
} listTypeIterator;

/* Structure for an entry while iterating over a list. */
typedef struct {
    listTypeIterator *li;
    unsigned char *lpe;   /* Entry in listpack */
    quicklistEntry entry; /* Entry in quicklist */
} listTypeEntry;

/* Structure to hold set iteration abstraction. */
typedef struct {
    robj *subject;
    int encoding;
    int ii; /* intset iterator */
    hashtableIterator *hashtable_iterator;
    unsigned char *lpi; /* listpack iterator */
} setTypeIterator;

/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
typedef struct {
    robj *subject;
    int encoding;

    unsigned char *fptr, *vptr;

    hashtableIterator iter;
    void *next;
} hashTypeIterator;

#include "stream.h" /* Stream data type header file. */

#define OBJ_HASH_FIELD 1
#define OBJ_HASH_VALUE 2

/*-----------------------------------------------------------------------------
 * Extern declarations
 *----------------------------------------------------------------------------*/

extern struct valkeyServer server;
extern struct sharedObjectsStruct shared;
extern dictType objectKeyPointerValueDictType;
extern dictType objectKeyHeapPointerValueDictType;
extern hashtableType setHashtableType;
extern dictType BenchmarkDictType;
extern hashtableType zsetHashtableType;
extern hashtableType kvstoreKeysHashtableType;
extern hashtableType kvstoreExpiresHashtableType;
extern double R_Zero, R_PosInf, R_NegInf, R_Nan;
extern dictType hashDictType;
extern hashtableType hashHashtableType;
extern dictType stringSetDictType;
extern dictType externalStringType;
extern dictType sdsHashDictType;
extern dictType clientDictType;
extern dictType objToDictDictType;
extern hashtableType kvstoreChannelHashtableType;
extern dictType modulesDictType;
extern dictType sdsReplyDictType;
extern hashtableType sdsReplyHashtableType;
extern dictType keylistDictType;
extern dict *modules;

/*-----------------------------------------------------------------------------
 * Functions prototypes
 *----------------------------------------------------------------------------*/

/* Command metadata */
void populateCommandLegacyRangeSpec(struct serverCommand *c);

/* Utils */
long long ustime(void);
mstime_t mstime(void);
mstime_t commandTimeSnapshot(void);
void getRandomHexChars(char *p, size_t len);
void getRandomBytes(unsigned char *p, size_t len);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);
void exitFromChild(int retcode);
long long serverPopcount(void *s, long count);
int serverSetProcTitle(char *title);
int validateProcTitleTemplate(const char *template);
int serverCommunicateSystemd(const char *sd_notify_msg);
void serverSetCpuAffinity(const char *cpulist);
void dictVanillaFree(void *val);

/* ERROR STATS constants */

/* Once the errors RAX reaches this limit, instead of tracking custom
 * errors (e.g. LUA), we track the error under the prefix below. */
#define ERRORSTATS_LIMIT 128
#define ERRORSTATS_OVERFLOW_ERR "ERRORSTATS_OVERFLOW"

/* afterErrorReply flags */

/* Indicating that we should not update error stats after sending error reply. */
#define ERR_REPLY_FLAG_NO_STATS_UPDATE (1ULL << 0)
/* Indicates the error message is custom (e.g. from LUA). */
#define ERR_REPLY_FLAG_CUSTOM (1ULL << 1)

/* networking.c -- Networking and Client related operations */

/* Read flags for various read errors and states */
#define READ_FLAGS_QB_LIMIT_REACHED (1 << 0)
#define READ_FLAGS_ERROR_BIG_INLINE_REQUEST (1 << 1)
#define READ_FLAGS_ERROR_BIG_MULTIBULK (1 << 2)
#define READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN (1 << 3)
#define READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN (1 << 4)
#define READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN (1 << 5)
#define READ_FLAGS_ERROR_BIG_BULK_COUNT (1 << 6)
#define READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER (1 << 7)
#define READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN (1 << 8)
#define READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_PRIMARY (1 << 9)
#define READ_FLAGS_ERROR_UNBALANCED_QUOTES (1 << 10)
#define READ_FLAGS_INLINE_ZERO_QUERY_LEN (1 << 11)
#define READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN (1 << 12)
#define READ_FLAGS_PARSING_COMPLETED (1 << 13)
#define READ_FLAGS_PRIMARY (1 << 14)
#define READ_FLAGS_DONT_PARSE (1 << 15)
#define READ_FLAGS_AUTH_REQUIRED (1 << 16)

/* Write flags for various write errors and states */
#define WRITE_FLAGS_WRITE_ERROR (1 << 0)
#define WRITE_FLAGS_IS_REPLICA (1 << 1)

client *createClient(connection *conn);
void freeClient(client *c);
void freeClientAsync(client *c);
void logInvalidUseAndFreeClientAsync(client *c, const char *fmt, ...);
void beforeNextClient(client *c);
void clearClientConnectionState(client *c);
void resetClient(client *c);
void resetClientIOState(client *c);
void freeClientOriginalArgv(client *c);
void freeClientArgv(client *c);
void sendReplyToClient(connection *conn);
void *addReplyDeferredLen(client *c);
void setDeferredArrayLen(client *c, void *node, long length);
void setDeferredMapLen(client *c, void *node, long length);
void setDeferredSetLen(client *c, void *node, long length);
void setDeferredAttributeLen(client *c, void *node, long length);
void setDeferredPushLen(client *c, void *node, long length);
int processInputBuffer(client *c);
void acceptCommonHandler(connection *conn, struct ClientFlags flags, char *ip);
void readQueryFromClient(connection *conn);
int prepareClientToWrite(client *c);
writePreparedClient *prepareClientForFutureWrites(client *c);
void addReplyNull(client *c);
void addReplyNullArray(client *c);
void addReplyBool(client *c, int b);
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext);
void addReplyProto(client *c, const char *s, size_t len);
void AddReplyFromClient(client *c, client *src);
void addReplyBulk(client *c, robj *obj);
void addReplyBulkCString(client *c, const char *s);
void addReplyBulkCBuffer(client *c, const void *p, size_t len);
void addWritePreparedReplyBulkCBuffer(writePreparedClient *c, const void *p, size_t len);
void addReplyBulkLongLong(client *c, long long ll);
void addWritePreparedReplyBulkLongLong(writePreparedClient *c, long long ll);
void addReply(client *c, robj *obj);
void addReplyStatusLength(client *c, const char *s, size_t len);
void addReplySds(client *c, sds s);
void addReplyBulkSds(client *c, sds s);
void addWritePreparedReplyBulkSds(writePreparedClient *c, sds s);
void setDeferredReplyBulkSds(client *c, void *node, sds s);
void addReplyErrorObject(client *c, robj *err);
void addReplyOrErrorObject(client *c, robj *reply);
void afterErrorReply(client *c, const char *s, size_t len, int flags);
void addReplyErrorFormatInternal(client *c, int flags, const char *fmt, va_list ap);
void addReplyErrorSdsEx(client *c, sds err, int flags);
void addReplyErrorSds(client *c, sds err);
void addReplyErrorSdsSafe(client *c, sds err);
void addReplyError(client *c, const char *err);
void addReplyErrorArity(client *c);
void addReplyErrorExpireTime(client *c);
void addReplyStatus(client *c, const char *status);
void addReplyDouble(client *c, double d);
void addReplyBigNum(client *c, const char *num, size_t len);
void addReplyHumanLongDouble(client *c, long double d);
void addReplyLongLong(client *c, long long ll);
void addReplyArrayLen(client *c, long length);
void addWritePreparedReplyArrayLen(writePreparedClient *c, long length);
void addReplyMapLen(client *c, long length);
void addWritePreparedReplyMapLen(writePreparedClient *c, long length);
void addReplySetLen(client *c, long length);
void addReplyAttributeLen(client *c, long length);
void addReplyPushLen(client *c, long length);
void addReplyHelp(client *c, const char **help);
void addExtendedReplyHelp(client *c, const char **help, const char **extended_help);
void addReplySubcommandSyntaxError(client *c);
void addReplyLoadedModules(client *c);
void copyReplicaOutputBuffer(client *dst, client *src);
void addListRangeReply(client *c, robj *o, long start, long end, int reverse);
void deferredAfterErrorReply(client *c, list *errors);
size_t getStringObjectSdsUsedMemory(robj *o);
void freeClientReplyValue(void *o);
void *dupClientReplyValue(void *o);
char *getClientPeerId(client *client);
char *getClientSockName(client *client);
int isClientConnIpV6(client *c);
sds catClientInfoString(sds s, client *client, int hide_user_data);
sds catClientInfoShortString(sds s, client *client, int hide_user_data);
sds getAllClientsInfoString(int type, int hide_user_data);
int clientSetName(client *c, robj *name, const char **err);
void rewriteClientCommandVector(client *c, int argc, ...);
void rewriteClientCommandArgument(client *c, int i, robj *newval);
void replaceClientCommandVector(client *c, int argc, robj **argv);
void redactClientCommandArgument(client *c, int argc);
size_t getClientOutputBufferMemoryUsage(client *c);
size_t getClientMemoryUsage(client *c, size_t *output_buffer_mem_usage);
int freeClientsInAsyncFreeQueue(void);
int closeClientOnOutputBufferLimitReached(client *c, int async);
int getClientType(client *c);
int getClientTypeByName(char *name);
char *getClientTypeName(int class);
void flushReplicasOutputBuffers(void);
void disconnectReplicas(void);
void evictClients(void);
int listenToPort(connListener *fds);
void pauseActions(pause_purpose purpose, mstime_t end, uint32_t actions);
void unpauseActions(pause_purpose purpose);
uint32_t isPausedActions(uint32_t action_bitmask);
uint32_t isPausedActionsWithUpdate(uint32_t action_bitmask);
char *getPausedReason(pause_purpose purpose);
mstime_t getPausedActionTimeout(uint32_t action, pause_purpose *purpose);
void updatePausedActions(void);
void unblockPostponedClients(void);
void processEventsWhileBlocked(void);
void whileBlockedCron(void);
void blockingOperationStarts(void);
void blockingOperationEnds(void);
int handleClientsWithPendingWrites(void);
void adjustThreadedIOIfNeeded(void);
int clientHasPendingReplies(client *c);
int updateClientMemUsageAndBucket(client *c);
void removeClientFromMemUsageBucket(client *c, int allow_eviction);
void unlinkClient(client *c);
void removeFromServerClientList(client *c);
int writeToClient(client *c);
void linkClient(client *c);
void protectClient(client *c);
void unprotectClient(client *c);
void initSharedQueryBuf(void);
void freeSharedQueryBuf(void);
client *lookupClientByID(uint64_t id);
int authRequired(client *c);
void putClientInPendingWriteQueue(client *c);
client *createCachedResponseClient(int resp);
void deleteCachedResponseClient(client *recording_client);
void waitForClientIO(client *c);
void ioThreadReadQueryFromClient(void *data);
void ioThreadWriteToClient(void *data);
int canParseCommand(client *c);
int processIOThreadsReadDone(void);
int processIOThreadsWriteDone(void);

/* logreqres.c - logging of requests and responses */
void reqresReset(client *c, int free_buf);
void reqresSaveClientReplyOffset(client *c);
size_t reqresAppendRequest(client *c);
size_t reqresAppendResponse(client *c);

#ifdef __GNUC__
void addReplyErrorFormatEx(client *c, int flags, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
void addReplyErrorFormat(client *c, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
void addReplyStatusFormat(client *c, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
#else
void addReplyErrorFormatEx(client *c, int flags, const char *fmt, ...);
void addReplyErrorFormat(client *c, const char *fmt, ...);
void addReplyStatusFormat(client *c, const char *fmt, ...);
#endif

/* Client side caching (tracking mode) */
void enableTracking(client *c, uint64_t redirect_to, struct ClientFlags options, robj **prefix, size_t numprefix);
void disableTracking(client *c);
void trackingRememberKeys(client *tracking, client *executing);
void trackingInvalidateKey(client *c, robj *keyobj, int bcast);
void trackingScheduleKeyInvalidation(uint64_t client_id, robj *keyobj);
void trackingHandlePendingKeyInvalidations(void);
void trackingInvalidateKeysOnFlush(int async);
void freeTrackingRadixTree(rax *rt);
void freeTrackingRadixTreeAsync(rax *rt);
void freeErrorsRadixTreeAsync(rax *errors);
void trackingLimitUsedSlots(void);
uint64_t trackingGetTotalItems(void);
uint64_t trackingGetTotalKeys(void);
uint64_t trackingGetTotalPrefixes(void);
void trackingBroadcastInvalidationMessages(void);
int checkPrefixCollisionsOrReply(client *c, robj **prefix, size_t numprefix);

/* List data type */
void listTypePush(robj *subject, robj *value, int where);
robj *listTypePop(robj *subject, int where);
unsigned long listTypeLength(const robj *subject);
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
void listTypeSetIteratorDirection(listTypeIterator *li, listTypeEntry *entry, unsigned char direction);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
robj *listTypeGet(listTypeEntry *entry);
unsigned char *listTypeGetValue(listTypeEntry *entry, size_t *vlen, long long *lval);
void listTypeInsert(listTypeEntry *entry, robj *value, int where);
void listTypeReplace(listTypeEntry *entry, robj *value);
int listTypeEqual(listTypeEntry *entry, robj *o);
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry);
robj *listTypeDup(robj *o);
void listTypeDelRange(robj *o, long start, long stop);
void popGenericCommand(client *c, int where);
void listElementsRemoved(client *c, robj *key, int where, robj *o, long count, int signal, int *deleted);
typedef enum {
    LIST_CONV_AUTO,
    LIST_CONV_GROWING,
    LIST_CONV_SHRINKING,
} list_conv_type;
typedef void (*beforeConvertCB)(void *data);
void listTypeTryConversion(robj *o, list_conv_type lct, beforeConvertCB fn, void *data);
void listTypeTryConversionAppend(robj *o, robj **argv, int start, int end, beforeConvertCB fn, void *data);

/* MULTI/EXEC/WATCH... */
void unwatchAllKeys(client *c);
void initClientMultiState(client *c);
void freeClientMultiState(client *c);
void queueMultiCommand(client *c, uint64_t cmd_flags);
size_t multiStateMemOverhead(client *c);
void touchWatchedKey(serverDb *db, robj *key);
int isWatchedKeyExpired(client *c);
void touchAllWatchedKeysInDb(serverDb *emptied, serverDb *replaced_with);
void discardTransaction(client *c);
void flagTransaction(client *c);
void execCommandAbort(client *c, sds error);

/* Object implementation */
void decrRefCount(robj *o);
void incrRefCount(robj *o);
robj *makeObjectShared(robj *o);
void freeStringObject(robj *o);
void freeListObject(robj *o);
void freeSetObject(robj *o);
void freeZsetObject(robj *o);
void freeHashObject(robj *o);
void dismissObject(robj *o, size_t dump_size);
robj *createObject(int type, void *ptr);
void initObjectLRUOrLFU(robj *o);
robj *createStringObject(const char *ptr, size_t len);
robj *createRawStringObject(const char *ptr, size_t len);
robj *tryCreateRawStringObject(const char *ptr, size_t len);
robj *tryCreateStringObject(const char *ptr, size_t len);
robj *dupStringObject(const robj *o);
int isSdsRepresentableAsLongLong(sds s, long long *llval);
int isObjectRepresentableAsLongLong(robj *o, long long *llongval);
robj *tryObjectEncoding(robj *o);
robj *tryObjectEncodingEx(robj *o, int try_trim);
robj *getDecodedObject(robj *o);
size_t stringObjectLen(robj *o);
robj *createStringObjectFromLongLong(long long value);
robj *createStringObjectFromLongLongForValue(long long value);
robj *createStringObjectFromLongLongWithSds(long long value);
robj *createStringObjectFromLongDouble(long double value, int humanfriendly);
robj *createQuicklistObject(int fill, int compress);
robj *createListListpackObject(void);
robj *createSetObject(void);
robj *createIntsetObject(void);
robj *createSetListpackObject(void);
robj *createHashObject(void);
robj *createZsetObject(void);
robj *createZsetListpackObject(void);
robj *createStreamObject(void);
robj *createModuleObject(moduleType *mt, void *value);
int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int getPositiveLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int getRangeLongFromObjectOrReply(client *c, robj *o, long min, long max, long *target, const char *msg);
int checkType(client *c, robj *o, int type);
int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg);
int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg);
int getDoubleFromObject(const robj *o, double *target);
int getLongLongFromObject(robj *o, long long *target);
int getLongDoubleFromObject(robj *o, long double *target);
int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg);
int getIntFromObjectOrReply(client *c, robj *o, int *target, const char *msg);
char *strEncoding(int encoding);
int compareStringObjects(const robj *a, const robj *b);
int collateStringObjects(const robj *a, const robj *b);
int equalStringObjects(robj *a, robj *b);
unsigned long long estimateObjectIdleTime(robj *o);
void trimStringObjectIfNeeded(robj *o, int trim_small_values);
#define sdsEncodedObject(objptr) (objptr->encoding == OBJ_ENCODING_RAW || objptr->encoding == OBJ_ENCODING_EMBSTR)

/* Objects with key attached, AKA valkey (val+key) objects */
robj *createObjectWithKeyAndExpire(int type, void *ptr, const sds key, long long expire);
robj *objectSetKeyAndExpire(robj *val, sds key, long long expire);
robj *objectSetExpire(robj *val, long long expire);
sds objectGetKey(const robj *val);
long long objectGetExpire(const robj *val);

/* Synchronous I/O with timeout */
ssize_t syncWrite(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncRead(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncReadLine(int fd, char *ptr, ssize_t size, long long timeout);

/* Replication */
void replicationFeedReplicas(int dictid, robj **argv, int argc);
void replicationFeedStreamFromPrimaryStream(char *buf, size_t buflen);
void resetReplicationBuffer(void);
void feedReplicationBuffer(char *buf, size_t len);
void freeReplicaReferencedReplBuffer(client *replica);
void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc);
void updateReplicasWaitingBgsave(int bgsaveerr, int type);
void replicationCron(void);
void replicationStartPendingFork(void);
void replicationHandlePrimaryDisconnection(void);
void replicationCachePrimary(client *c);
void resizeReplicationBacklog(void);
void replicationSetPrimary(char *ip, int port, int full_sync_required);
void replicationUnsetPrimary(void);
void refreshGoodReplicasCount(void);
int checkGoodReplicasStatus(void);
void processClientsWaitingReplicas(void);
void unblockClientWaitingReplicas(client *c);
int replicationCountAcksByOffset(long long offset);
int replicationCountAOFAcksByOffset(long long offset);
void replicationSendNewlineToPrimary(void);
long long replicationGetReplicaOffset(void);
char *replicationGetReplicaName(client *c);
long long getPsyncInitialOffset(void);
int replicationSetupReplicaForFullResync(client *replica, long long offset);
void changeReplicationId(void);
void clearReplicationId2(void);
void createReplicationBacklog(void);
void freeReplicationBacklog(void);
void replicationCachePrimaryUsingMyself(void);
void feedReplicationBacklog(void *ptr, size_t len);
void incrementalTrimReplicationBacklog(size_t blocks);
int canFeedReplicaReplBuffer(client *replica);
void rebaseReplicationBuffer(long long base_repl_offset);
void showLatestBacklog(void);
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
void rdbPipeWriteHandlerConnRemoved(struct connection *conn);
int rdbRegisterAuxField(char *auxfield, rdbAuxFieldEncoder encoder, rdbAuxFieldDecoder decoder);
void clearFailoverState(void);
void updateFailoverStatus(void);
void abortFailover(const char *err);
const char *getFailoverStateString(void);
int sendCurrentOffsetToReplica(client *replica);
void addRdbReplicaToPsyncWait(client *replica);
void initClientReplicationData(client *c);
void freeClientReplicationData(client *c);

/* Generic persistence functions */
void startLoadingFile(size_t size, char *filename, int rdbflags);
void startLoading(size_t size, int rdbflags, int async);
void loadingAbsProgress(off_t pos);
void loadingIncrProgress(off_t size);
void stopLoading(int success);
void updateLoadingFileName(char *filename);
void startSaving(int rdbflags);
void stopSaving(int success);
int allPersistenceDisabled(void);

#define DISK_ERROR_TYPE_AOF 1  /* Don't accept writes: AOF errors. */
#define DISK_ERROR_TYPE_RDB 2  /* Don't accept writes: RDB errors. */
#define DISK_ERROR_TYPE_NONE 0 /* No problems, we can accept writes. */
int writeCommandsDeniedByDiskError(void);
sds writeCommandsGetDiskErrorMessage(int);

/* RDB persistence */
#include "rdb.h"
void killRDBChild(void);
int bg_unlink(const char *filename);

/* AOF persistence */
void flushAppendOnlyFile(int force);
void feedAppendOnlyFile(int dictid, robj **argv, int argc);
void aofRemoveTempFile(pid_t childpid);
int rewriteAppendOnlyFileBackground(void);
int loadAppendOnlyFiles(aofManifest *am);
void stopAppendOnly(void);
int startAppendOnly(void);
void backgroundRewriteDoneHandler(int exitcode, int bysignal);
void killAppendOnlyChild(void);
void restartAOFAfterSYNC(void);
void aofLoadManifestFromDisk(void);
void aofOpenIfNeededOnServerStart(void);
void aofManifestFree(aofManifest *am);
int aofDelHistoryFiles(void);
int aofRewriteLimited(void);

/* Child info */
void openChildInfoPipe(void);
void closeChildInfoPipe(void);
void sendChildInfoGeneric(childInfoType info_type, size_t keys, double progress, char *pname);
void sendChildCowInfo(childInfoType info_type, char *pname);
void sendChildInfo(childInfoType info_type, size_t keys, char *pname);
void receiveChildInfo(void);

/* Fork helpers */
int serverFork(int purpose);
int hasActiveChildProcess(void);
void resetChildState(void);
int isMutuallyExclusiveChildType(int type);

/* acl.c -- Authentication related prototypes. */
extern rax *Users;
extern user *DefaultUser;
void ACLInit(void);
/* Return values for ACLCheckAllPerm(). */
#define ACL_OK 0
#define ACL_DENIED_CMD 1
#define ACL_DENIED_KEY 2
#define ACL_DENIED_AUTH 3    /* Only used for ACL LOG entries. */
#define ACL_DENIED_CHANNEL 4 /* Only used for pub/sub commands */

/* Context values for addACLLogEntry(). */
#define ACL_LOG_CTX_TOPLEVEL 0
#define ACL_LOG_CTX_LUA 1
#define ACL_LOG_CTX_MULTI 2
#define ACL_LOG_CTX_MODULE 3

/* ACL key permission types */
#define ACL_READ_PERMISSION (1 << 0)
#define ACL_WRITE_PERMISSION (1 << 1)
#define ACL_ALL_PERMISSION (ACL_READ_PERMISSION | ACL_WRITE_PERMISSION)

/* Return codes for Authentication functions to indicate the result. */
typedef enum {
    AUTH_OK = 0,
    AUTH_ERR,
    AUTH_NOT_HANDLED,
    AUTH_BLOCKED
} AuthResult;

int ACLCheckUserCredentials(robj *username, robj *password);
int ACLAuthenticateUser(client *c, robj *username, robj *password, robj **err);
int checkModuleAuthentication(client *c, robj *username, robj *password, robj **err);
void addAuthErrReply(client *c, robj *err);
unsigned long ACLGetCommandID(sds cmdname);
user *ACLGetUserByName(const char *name, size_t namelen);
int ACLUserCheckKeyPerm(user *u, const char *key, int keylen, int flags);
int ACLUserCheckChannelPerm(user *u, sds channel, int literal);
int ACLCheckAllUserCommandPerm(user *u, struct serverCommand *cmd, robj **argv, int argc, int *idxptr);
int ACLUserCheckCmdWithUnrestrictedKeyAccess(user *u, struct serverCommand *cmd, robj **argv, int argc, int flags);
int ACLCheckAllPerm(client *c, int *idxptr);
int ACLSetUser(user *u, const char *op, ssize_t oplen);
sds ACLStringSetUser(user *u, sds username, sds *argv, int argc);
uint64_t ACLGetCommandCategoryFlagByName(const char *name);
int ACLAddCommandCategory(const char *name, uint64_t flag);
void ACLCleanupCategoriesOnFailure(size_t num_acl_categories_added);
int ACLAppendUserForLoading(sds *argv, int argc, int *argc_err);
const char *ACLSetUserStringError(void);
robj *ACLDescribeUser(user *u);
void ACLLoadUsersAtStartup(void);
void addReplyCommandCategories(client *c, struct serverCommand *cmd);
user *ACLCreateUnlinkedUser(void);
void ACLFreeUserAndKillClients(user *u);
void addACLLogEntry(client *c, int reason, int context, int argpos, sds username, sds object);
sds getAclErrorMessage(int acl_res, user *user, struct serverCommand *cmd, sds errored_val, int verbose);
void ACLUpdateDefaultUserPassword(sds password);
sds genValkeyInfoStringACLStats(sds info);
void ACLRecomputeCommandBitsFromCommandRulesAllUsers(void);

/* Sorted sets data type */

/* Input flags. */
#define ZADD_IN_NONE 0
#define ZADD_IN_INCR (1 << 0) /* Increment the score instead of setting it. */
#define ZADD_IN_NX (1 << 1)   /* Don't touch elements not already existing. */
#define ZADD_IN_XX (1 << 2)   /* Only touch elements already existing. */
#define ZADD_IN_GT (1 << 3)   /* Only update existing when new scores are higher. */
#define ZADD_IN_LT (1 << 4)   /* Only update existing when new scores are lower. */

/* Output flags. */
#define ZADD_OUT_NOP (1 << 0)     /* Operation not performed because of conditionals.*/
#define ZADD_OUT_NAN (1 << 1)     /* Only touch elements already existing. */
#define ZADD_OUT_ADDED (1 << 2)   /* The element was new and was added. */
#define ZADD_OUT_UPDATED (1 << 3) /* The element already existed, score updated. */

/* Struct to hold an inclusive/exclusive range spec by score comparison. */
typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} zrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
    sds min, max;     /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

/* flags for incrCommandFailedCalls */
#define ERROR_COMMAND_REJECTED (1 << 0) /* Indicate to update the command rejected stats */
#define ERROR_COMMAND_FAILED (1 << 1)   /* Indicate to update the command failed stats */

zskiplist *zslCreate(void);
void zslFree(zskiplist *zsl);
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele);
zskiplistNode *zslNthInRange(zskiplist *zsl, zrangespec *range, long n);
double zzlGetScore(unsigned char *sptr);
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range);
unsigned long zsetLength(const robj *zobj);
void zsetConvert(robj *zobj, int encoding);
void zsetConvertToListpackIfNeeded(robj *zobj, size_t maxelelen, size_t totelelen);
int zsetScore(robj *zobj, sds member, double *score);
int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore);
int zsetDel(robj *zobj, sds ele);
robj *zsetDup(robj *o);
void genericZpopCommand(client *c,
                        robj **keyv,
                        int keyc,
                        int where,
                        int emitkey,
                        long count,
                        int use_nested_array,
                        int reply_nil_when_empty,
                        int *deleted);
sds lpGetObject(unsigned char *sptr);
int zslValueGteMin(double value, zrangespec *spec);
int zslValueLteMax(double value, zrangespec *spec);
void zslFreeLexRange(zlexrangespec *spec);
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec);
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range);
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range);
zskiplistNode *zslNthInLexRange(zskiplist *zsl, zlexrangespec *range, long n);
int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec);
int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec);
int zslLexValueGteMin(sds value, zlexrangespec *spec);
int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Core functions */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level);
size_t freeMemoryGetNotCountedMemory(void);
int overMaxmemoryAfterAlloc(size_t moremem);
uint64_t getCommandFlags(client *c);
int processCommand(client *c);
int processPendingCommandAndInputBuffer(client *c);
int processCommandAndResetClient(client *c);
void setupSignalHandlers(void);
int createSocketAcceptHandler(connListener *sfd, aeFileProc *accept_handler);
connListener *listenerByType(const char *typename);
int changeListener(connListener *listener);
struct serverCommand *lookupSubcommand(struct serverCommand *container, sds sub_name);
struct serverCommand *lookupCommand(robj **argv, int argc);
struct serverCommand *lookupCommandBySdsLogic(hashtable *commands, sds s);
struct serverCommand *lookupCommandBySds(sds s);
struct serverCommand *lookupCommandByCStringLogic(hashtable *commands, const char *s);
struct serverCommand *lookupCommandByCString(const char *s);
struct serverCommand *lookupCommandOrOriginal(robj **argv, int argc);
int commandCheckExistence(client *c, sds *err);
int commandCheckArity(struct serverCommand *cmd, int argc, sds *err);
void startCommandExecution(void);
int incrCommandStatsOnError(struct serverCommand *cmd, int flags);
void call(client *c, int flags);
void alsoPropagate(int dbid, robj **argv, int argc, int target);
void postExecutionUnitOperations(void);
void serverOpArrayFree(serverOpArray *oa);
void forceCommandPropagation(client *c, int flags);
void preventCommandPropagation(client *c);
void preventCommandAOF(client *c);
void preventCommandReplication(client *c);
void commandlogPushCurrentCommand(client *c, struct serverCommand *cmd);
void updateCommandLatencyHistogram(struct hdr_histogram **latency_histogram, int64_t duration_hist);
int prepareForShutdown(client *c, int flags);
void replyToClientsBlockedOnShutdown(void);
int abortShutdown(void);
void afterCommand(client *c);
int mustObeyClient(client *c);
#ifdef __GNUC__
void _serverLog(int level, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
void serverLogFromHandler(int level, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
#else
void serverLogFromHandler(int level, const char *fmt, ...);
void _serverLog(int level, const char *fmt, ...);
#endif
void serverLogRaw(int level, const char *msg);
void serverLogRawFromHandler(int level, const char *msg);
void usage(void);
void updateDictResizePolicy(void);
void populateCommandTable(void);
void resetCommandTableStats(hashtable *commands);
void resetErrorTableStats(void);
void adjustOpenFilesLimit(void);
void incrementErrorCount(const char *fullerr, size_t namelen);
void closeListeningSockets(int unlink_unix_socket);
void updateCachedTime(int update_daylight_info);
void bytesToHuman(char *s, size_t size, unsigned long long n);
void enterExecutionUnit(int update_cached_time, long long us);
void exitExecutionUnit(void);
void resetServerStats(void);
void monitorActiveDefrag(void);
void defragWhileBlocked(void);
unsigned int getLRUClock(void);
unsigned int LRU_CLOCK(void);
const char *evictPolicyToString(void);
struct serverMemOverhead *getMemoryOverheadData(void);
void freeMemoryOverheadData(struct serverMemOverhead *mh);
void checkChildrenDone(void);
int setOOMScoreAdj(int process_class);
void rejectCommandFormat(client *c, const char *fmt, ...);
void *activeDefragAlloc(void *ptr);
robj *activeDefragStringOb(robj *ob);
void dismissSds(sds s);
void dismissMemoryInChild(void);

#define RESTART_SERVER_NONE 0
#define RESTART_SERVER_GRACEFULLY (1 << 0)     /* Do proper shutdown. */
#define RESTART_SERVER_CONFIG_REWRITE (1 << 1) /* CONFIG REWRITE before restart.*/
int restartServer(client *c, int flags, mstime_t delay);
int getKeySlot(sds key);
int calculateKeySlot(sds key);

/* kvstore wrappers */
int dbExpand(serverDb *db, uint64_t db_size, int try_expand);
int dbExpandExpires(serverDb *db, uint64_t db_size, int try_expand);
robj *dbFind(serverDb *db, sds key);
robj *dbFindExpires(serverDb *db, sds key);
unsigned long long dbSize(serverDb *db);
unsigned long long dbScan(serverDb *db, unsigned long long cursor, hashtableScanFunction scan_cb, void *privdata);

/* Set data type */
robj *setTypeCreate(sds value, size_t size_hint);
int setTypeAdd(robj *subject, sds value);
int setTypeAddAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds);
int setTypeRemove(robj *subject, sds value);
int setTypeRemoveAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds);
int setTypeIsMember(robj *subject, sds value);
int setTypeIsMemberAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds);
setTypeIterator *setTypeInitIterator(robj *subject);
void setTypeReleaseIterator(setTypeIterator *si);
int setTypeNext(setTypeIterator *si, char **str, size_t *len, int64_t *llele);
sds setTypeNextObject(setTypeIterator *si);
int setTypeRandomElement(robj *setobj, char **str, size_t *len, int64_t *llele);
unsigned long setTypeSize(const robj *subject);
void setTypeConvert(robj *subject, int enc);
int setTypeConvertAndExpand(robj *setobj, int enc, unsigned long cap, int panic);
robj *setTypeDup(robj *o);

/* Hash data type */
#define HASH_SET_TAKE_FIELD (1 << 0)
#define HASH_SET_TAKE_VALUE (1 << 1)
#define HASH_SET_COPY 0

typedef struct hashTypeEntry hashTypeEntry;
hashTypeEntry *hashTypeCreateEntry(sds field, sds value);
sds hashTypeEntryGetField(const hashTypeEntry *entry);
sds hashTypeEntryGetValue(const hashTypeEntry *entry);
size_t hashTypeEntryAllocSize(hashTypeEntry *entry);
hashTypeEntry *hashTypeEntryDefrag(hashTypeEntry *entry, void *(*defragfn)(void *), sds (*sdsdefragfn)(sds));
void dismissHashTypeEntry(hashTypeEntry *entry);
void freeHashTypeEntry(hashTypeEntry *entry);

void hashTypeConvert(robj *o, int enc);
void hashTypeTryConversion(robj *subject, robj **argv, int start, int end);
int hashTypeExists(robj *o, sds key);
int hashTypeDelete(robj *o, sds key);
unsigned long hashTypeLength(const robj *o);
void hashTypeInitIterator(robj *subject, hashTypeIterator *hi);
void hashTypeResetIterator(hashTypeIterator *hi);
int hashTypeNext(hashTypeIterator *hi);
void hashTypeCurrentFromListpack(hashTypeIterator *hi,
                                 int what,
                                 unsigned char **vstr,
                                 unsigned int *vlen,
                                 long long *vll);
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what);
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what);
robj *hashTypeLookupWriteOrCreate(client *c, robj *key);
robj *hashTypeGetValueObject(robj *o, sds field);
int hashTypeSet(robj *o, sds field, sds value, int flags);
robj *hashTypeDup(robj *o);

/* Pub / Sub */
int pubsubUnsubscribeAllChannels(client *c, int notify);
int pubsubUnsubscribeShardAllChannels(client *c, int notify);
void pubsubShardUnsubscribeAllChannelsInSlot(unsigned int slot);
int pubsubUnsubscribeAllPatterns(client *c, int notify);
int pubsubPublishMessage(robj *channel, robj *message, int sharded);
int pubsubPublishMessageAndPropagateToCluster(robj *channel, robj *message, int sharded);
void addReplyPubsubMessage(client *c, robj *channel, robj *msg, robj *message_bulk);
int serverPubsubSubscriptionCount(void);
int serverPubsubShardSubscriptionCount(void);
size_t pubsubMemOverhead(client *c);
void unmarkClientAsPubSub(client *c);
int pubsubTotalSubscriptions(void);
dict *getClientPubSubChannels(client *c);
dict *getClientPubSubShardChannels(client *c);
void initClientPubSubData(client *c);
void freeClientPubSubData(client *c);

/* Keyspace events notification */
void notifyKeyspaceEvent(int type, char *event, robj *key, int dbid);
int keyspaceEventsStringToFlags(char *classes);
sds keyspaceEventsFlagsToString(int flags);

/* Configuration */
/* Configuration Flags */
#define MODIFIABLE_CONFIG 0             /* This is the implied default for a standard \
                                         * config, which is mutable. */
#define IMMUTABLE_CONFIG (1ULL << 0)    /* Can this value only be set at startup? */
#define SENSITIVE_CONFIG (1ULL << 1)    /* Does this value contain sensitive information */
#define DEBUG_CONFIG (1ULL << 2)        /* Values that are useful for debugging. */
#define MULTI_ARG_CONFIG (1ULL << 3)    /* This config receives multiple arguments. */
#define HIDDEN_CONFIG (1ULL << 4)       /* This config is hidden in `config get <pattern>` (used for tests/debugging) */
#define PROTECTED_CONFIG (1ULL << 5)    /* Becomes immutable if enable-protected-configs is enabled. */
#define DENY_LOADING_CONFIG (1ULL << 6) /* This config is forbidden during loading. */
#define ALIAS_CONFIG (1ULL << 7)        /* For configs with multiple names, this flag is set on the alias. */
#define MODULE_CONFIG (1ULL << 8)       /* This config is a module config */
#define VOLATILE_CONFIG (1ULL << 9)     /* The config is a reference to the config data and not the config data itself (ex. \
                                         * a file name containing more configuration like a tls key). In this case we want  \
                                         * to apply the configuration change even if the new config value is the same as    \
                                         * the old. */

#define INTEGER_CONFIG 0        /* No flags means a simple integer configuration */
#define MEMORY_CONFIG (1 << 0)  /* Indicates if this value can be loaded as a memory value */
#define PERCENT_CONFIG (1 << 1) /* Indicates if this value can be loaded as a percent (and stored as a negative int) */
#define OCTAL_CONFIG (1 << 2)   /* This value uses octal representation */

/* Enum Configs contain an array of configEnum objects that match a string with an integer. */
typedef struct configEnum {
    char *name;
    int val;
} configEnum;

/* Type of configuration. */
typedef enum {
    BOOL_CONFIG,
    NUMERIC_CONFIG,
    STRING_CONFIG,
    SDS_CONFIG,
    ENUM_CONFIG,
    SPECIAL_CONFIG,
} configType;

void loadServerConfig(char *filename, char config_from_stdin, char *options);
void appendServerSaveParams(time_t seconds, int changes);
void resetServerSaveParams(void);
struct rewriteConfigState; /* Forward declaration to export API. */
int rewriteConfigRewriteLine(struct rewriteConfigState *state, const char *option, sds line, int force);
void rewriteConfigMarkAsProcessed(struct rewriteConfigState *state, const char *option);
int rewriteConfig(char *path, int force_write);
void initConfigValues(void);
void removeConfig(sds name);
sds getConfigDebugInfo(void);
int allowProtectedAction(int config, client *c);
void createSharedObjectsWithCompat(void);
void initServerClientMemUsageBuckets(void);
void freeServerClientMemUsageBuckets(void);

/* Module Configuration */
typedef struct ModuleConfig ModuleConfig;
int performModuleConfigSetFromName(sds name, sds value, const char **err);
int performModuleConfigSetDefaultFromName(sds name, const char **err);
void addModuleBoolConfig(const char *module_name, const char *name, int flags, void *privdata, int default_val);
void addModuleStringConfig(const char *module_name, const char *name, int flags, void *privdata, sds default_val);
void addModuleEnumConfig(const char *module_name,
                         const char *name,
                         int flags,
                         void *privdata,
                         int default_val,
                         configEnum *enum_vals);
void addModuleNumericConfig(const char *module_name,
                            const char *name,
                            int flags,
                            void *privdata,
                            long long default_val,
                            int conf_flags,
                            long long lower,
                            long long upper);
void addModuleConfigApply(list *module_configs, ModuleConfig *module_config);
int moduleConfigApplyConfig(list *module_configs, const char **err, const char **err_arg_name);
int getModuleBoolConfig(ModuleConfig *module_config);
int setModuleBoolConfig(ModuleConfig *config, int val, const char **err);
sds getModuleStringConfig(ModuleConfig *module_config);
int setModuleStringConfig(ModuleConfig *config, sds strval, const char **err);
int getModuleEnumConfig(ModuleConfig *module_config);
int setModuleEnumConfig(ModuleConfig *config, int val, const char **err);
long long getModuleNumericConfig(ModuleConfig *module_config);
int setModuleNumericConfig(ModuleConfig *config, long long val, const char **err);

/* db.c -- Keyspace access API */
int removeExpire(serverDb *db, robj *key);
void deleteExpiredKeyAndPropagate(serverDb *db, robj *keyobj);
void deleteExpiredKeyFromOverwriteAndPropagate(client *c, robj *keyobj);
void propagateDeletion(serverDb *db, robj *key, int lazy);
int keyIsExpired(serverDb *db, robj *key);
long long getExpire(serverDb *db, robj *key);
robj *setExpire(client *c, serverDb *db, robj *key, long long when);
int checkAlreadyExpired(long long when);
robj *lookupKeyRead(serverDb *db, robj *key);
robj *lookupKeyWrite(serverDb *db, robj *key);
robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply);
robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply);
robj *lookupKeyReadWithFlags(serverDb *db, robj *key, int flags);
robj *lookupKeyWriteWithFlags(serverDb *db, robj *key, int flags);
robj *objectCommandLookup(client *c, robj *key);
robj *objectCommandLookupOrReply(client *c, robj *key, robj *reply);
int objectSetLRUOrLFU(robj *val, long long lfu_freq, long long lru_idle, long long lru_clock, int lru_multiplier);
#define LOOKUP_NONE 0
#define LOOKUP_NOTOUCH (1 << 0)  /* Don't update LRU. */
#define LOOKUP_NONOTIFY (1 << 1) /* Don't trigger keyspace event on key misses. */
#define LOOKUP_NOSTATS (1 << 2)  /* Don't update keyspace hits/misses counters. */
#define LOOKUP_WRITE (1 << 3)    /* Delete expired keys even in replicas. */
#define LOOKUP_NOEXPIRE (1 << 4) /* Avoid deleting lazy expired keys. */
#define LOOKUP_NOEFFECTS \
    (LOOKUP_NONOTIFY | LOOKUP_NOSTATS | LOOKUP_NOTOUCH | LOOKUP_NOEXPIRE) /* Avoid any effects from fetching the key */

void dbAdd(serverDb *db, robj *key, robj **valref);
int dbAddRDBLoad(serverDb *db, sds key, robj **valref);
void dbReplaceValue(serverDb *db, robj *key, robj **valref);

#define SETKEY_KEEPTTL 1
#define SETKEY_NO_SIGNAL 2
#define SETKEY_ALREADY_EXIST 4
#define SETKEY_DOESNT_EXIST 8
#define SETKEY_ADD_OR_UPDATE 16 /* Key most likely doesn't exists */
void setKey(client *c, serverDb *db, robj *key, robj **valref, int flags);
robj *dbRandomKey(serverDb *db);
int dbGenericDelete(serverDb *db, robj *key, int async, int flags);
int dbSyncDelete(serverDb *db, robj *key);
int dbDelete(serverDb *db, robj *key);
robj *dbUnshareStringValue(serverDb *db, robj *key, robj *o);

#define EMPTYDB_NO_FLAGS 0           /* No flags. */
#define EMPTYDB_ASYNC (1 << 0)       /* Reclaim memory in another thread. */
#define EMPTYDB_NOFUNCTIONS (1 << 1) /* Indicate not to flush the functions. */
long long emptyData(int dbnum, int flags, void(callback)(hashtable *));
long long emptyDbStructure(serverDb *dbarray, int dbnum, int async, void(callback)(hashtable *));
void flushAllDataAndResetRDB(int flags);
long long dbTotalServerKeyCount(void);
serverDb *initTempDb(void);
void discardTempDb(serverDb *tempDb);
int selectDb(client *c, int id);
void signalModifiedKey(client *c, serverDb *db, robj *key);
void signalFlushedDb(int dbid, int async);
void scanGenericCommand(client *c, robj *o, unsigned long long cursor);
int parseScanCursorOrReply(client *c, robj *o, unsigned long long *cursor);
int dbAsyncDelete(serverDb *db, robj *key);
void emptyDbAsync(serverDb *db);
size_t lazyfreeGetPendingObjectsCount(void);
size_t lazyfreeGetFreedObjectsCount(void);
void lazyfreeResetStats(void);
void freeObjAsync(robj *key, robj *obj, int dbid);
void freeReplicationBacklogRefMemAsync(list *blocks, rax *index);

/* API to get key arguments from commands */
#define GET_KEYSPEC_DEFAULT 0
#define GET_KEYSPEC_INCLUDE_NOT_KEYS (1 << 0) /* Consider 'fake' keys as keys */
#define GET_KEYSPEC_RETURN_PARTIAL (1 << 1)   /* Return all keys that can be found */

int getKeysFromCommandWithSpecs(struct serverCommand *cmd,
                                robj **argv,
                                int argc,
                                int search_flags,
                                getKeysResult *result);
keyReference *getKeysPrepareResult(getKeysResult *result, int numkeys);
int getKeysFromCommand(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int doesCommandHaveKeys(struct serverCommand *cmd);
int getChannelsFromCommand(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int doesCommandHaveChannelsWithFlags(struct serverCommand *cmd, int flags);
void getKeysFreeResult(getKeysResult *result);
int sintercardGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int zunionInterDiffGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int zunionInterDiffStoreGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int evalGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int functionGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int sortGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int sortROGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int migrateGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int georadiusGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int xreadGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int lmpopGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int blmpopGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int zmpopGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int bzmpopGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int setGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int bitfieldGetKeys(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);

unsigned short crc16(const char *buf, int len);

/* Sentinel */
void initSentinelConfig(void);
void initSentinel(void);
void sentinelTimer(void);
const char *sentinelHandleConfiguration(char **argv, int argc);
void queueSentinelConfig(sds *argv, int argc, int linenum, sds line);
void loadSentinelConfigFromQueue(void);
void sentinelIsRunning(void);
void sentinelCheckConfigFile(void);
void sentinelCommand(client *c);
void sentinelInfoCommand(client *c);
void sentinelPublishCommand(client *c);
void sentinelRoleCommand(client *c);

/* valkey-check-rdb & aof */
int redis_check_rdb(char *rdbfilename, FILE *fp);
int redis_check_rdb_main(int argc, char **argv, FILE *fp);
int redis_check_aof_main(int argc, char **argv);

/* Scripting */
void freeEvalScripts(dict *scripts, list *scripts_lru_list, list *engine_callbacks);
void freeEvalScriptsAsync(dict *scripts, list *scripts_lru_list, list *engine_callbacks);
void freeFunctionsAsync(functionsLibCtx *lib_ctx);
void sha1hex(char *digest, char *script, size_t len);
unsigned long evalMemory(void);
dict *evalScriptsDict(void);
unsigned long evalScriptsMemory(void);
uint64_t evalGetCommandFlags(client *c, uint64_t orig_flags);
uint64_t fcallGetCommandFlags(client *c, uint64_t orig_flags);
int isInsideYieldingLongCommand(void);

/* Cache of recently used small arguments to avoid malloc calls. */
#define LUA_CMD_OBJCACHE_SIZE 32
#define LUA_CMD_OBJCACHE_MAX_LEN 64

/* Blocked clients API */
void processUnblockedClients(void);
void initClientBlockingState(client *c);
void freeClientBlockingState(client *c);
void blockClient(client *c, int btype);
void unblockClient(client *c, int queue_for_reprocessing);
void unblockClientOnTimeout(client *c);
void unblockClientOnError(client *c, const char *err_str);
void queueClientForReprocessing(client *c);
void replyToBlockedClientTimedOut(client *c);
int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit);
void disconnectAllBlockedClients(void);
void handleClientsBlockedOnKeys(void);
void signalKeyAsReady(serverDb *db, robj *key, int type);
void blockForKeys(client *c, int btype, robj **keys, int numkeys, mstime_t timeout, int unblock_on_nokey);
void blockClientShutdown(client *c);
void blockPostponeClient(client *c);
void blockClientForReplicaAck(client *c, mstime_t timeout, long long offset, long numreplicas, int numlocal);
void replicationRequestAckFromReplicas(void);
void signalDeletedKeyAsReady(serverDb *db, robj *key, int type);
void updateStatsOnUnblock(client *c, long blocked_us, long reply_us, int failed_or_rejected);
void scanDatabaseForDeletedKeys(serverDb *emptied, serverDb *replaced_with);
void totalNumberOfStatefulKeys(unsigned long *blocking_keys,
                               unsigned long *blocking_keys_on_nokey,
                               unsigned long *watched_keys);
void blockedBeforeSleep(void);

/* timeout.c -- Blocked clients timeout and connections timeout. */
void addClientToTimeoutTable(client *c);
void removeClientFromTimeoutTable(client *c);
void handleBlockedClientsTimeout(void);
int clientsCronHandleTimeout(client *c, mstime_t now_ms);

/* expire.c -- Handling of expired keys */
void activeExpireCycle(int type);
void expireReplicaKeys(void);
void rememberReplicaKeyWithExpire(serverDb *db, robj *key);
void flushReplicaKeysWithExpireList(void);
size_t getReplicaKeyWithExpireCount(void);

/* evict.c -- maxmemory handling and LRU eviction. */
void evictionPoolAlloc(void);
#define LFU_INIT_VAL 5
unsigned long LFUGetTimeInMinutes(void);
uint8_t LFULogIncr(uint8_t value);
unsigned long LFUDecrAndReturn(robj *o);
#define EVICT_OK 0
#define EVICT_RUNNING 1
#define EVICT_FAIL 2
int performEvictions(void);
void startEvictionTimeProc(void);

/* Keys hashing/comparison functions for dict.c and hashtable.c hash tables. */
uint64_t dictSdsHash(const void *key);
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCompare(const void *key1, const void *key2);
int hashtableSdsKeyCompare(const void *key1, const void *key2);
int dictSdsKeyCaseCompare(const void *key1, const void *key2);
void dictSdsDestructor(void *val);
void dictListDestructor(void *val);
void *dictSdsDup(const void *key);

/* Git SHA1 */
char *serverGitSHA1(void);
char *serverGitDirty(void);
uint64_t serverBuildId(void);
const char *serverBuildIdRaw(void);
char *serverBuildIdString(void);

/* Commands prototypes */
void authCommand(client *c);
void pingCommand(client *c);
void echoCommand(client *c);
void commandCommand(client *c);
void commandCountCommand(client *c);
void commandListCommand(client *c);
void commandInfoCommand(client *c);
void commandGetKeysCommand(client *c);
void commandGetKeysAndFlagsCommand(client *c);
void commandHelpCommand(client *c);
void commandDocsCommand(client *c);
void setCommand(client *c);
void setnxCommand(client *c);
void setexCommand(client *c);
void psetexCommand(client *c);
void getCommand(client *c);
void getexCommand(client *c);
void getdelCommand(client *c);
void delCommand(client *c);
void unlinkCommand(client *c);
void existsCommand(client *c);
void setbitCommand(client *c);
void getbitCommand(client *c);
void bitfieldCommand(client *c);
void bitfieldroCommand(client *c);
void setrangeCommand(client *c);
void getrangeCommand(client *c);
void incrCommand(client *c);
void decrCommand(client *c);
void incrbyCommand(client *c);
void decrbyCommand(client *c);
void incrbyfloatCommand(client *c);
void selectCommand(client *c);
void swapdbCommand(client *c);
void randomkeyCommand(client *c);
void keysCommand(client *c);
void scanCommand(client *c);
void dbsizeCommand(client *c);
void lastsaveCommand(client *c);
void saveCommand(client *c);
void bgsaveCommand(client *c);
void bgrewriteaofCommand(client *c);
void shutdownCommand(client *c);
void slowlogCommand(client *c);
void commandlogCommand(client *c);
void moveCommand(client *c);
void copyCommand(client *c);
void renameCommand(client *c);
void renamenxCommand(client *c);
void lpushCommand(client *c);
void rpushCommand(client *c);
void lpushxCommand(client *c);
void rpushxCommand(client *c);
void linsertCommand(client *c);
void lpopCommand(client *c);
void rpopCommand(client *c);
void lmpopCommand(client *c);
void llenCommand(client *c);
void lindexCommand(client *c);
void lrangeCommand(client *c);
void ltrimCommand(client *c);
void typeCommand(client *c);
void lsetCommand(client *c);
void saddCommand(client *c);
void sremCommand(client *c);
void smoveCommand(client *c);
void sismemberCommand(client *c);
void smismemberCommand(client *c);
void scardCommand(client *c);
void spopCommand(client *c);
void srandmemberCommand(client *c);
void sinterCommand(client *c);
void sinterCardCommand(client *c);
void sinterstoreCommand(client *c);
void sunionCommand(client *c);
void sunionstoreCommand(client *c);
void sdiffCommand(client *c);
void sdiffstoreCommand(client *c);
void sscanCommand(client *c);
void syncCommand(client *c);
void flushdbCommand(client *c);
void flushallCommand(client *c);
void sortCommand(client *c);
void sortroCommand(client *c);
void lremCommand(client *c);
void lposCommand(client *c);
void rpoplpushCommand(client *c);
void lmoveCommand(client *c);
void infoCommand(client *c);
void mgetCommand(client *c);
void monitorCommand(client *c);
void expireCommand(client *c);
void expireatCommand(client *c);
void pexpireCommand(client *c);
void pexpireatCommand(client *c);
void getsetCommand(client *c);
void ttlCommand(client *c);
void touchCommand(client *c);
void pttlCommand(client *c);
void expiretimeCommand(client *c);
void pexpiretimeCommand(client *c);
void persistCommand(client *c);
void replicaofCommand(client *c);
void roleCommand(client *c);
void debugCommand(client *c);
void msetCommand(client *c);
void msetnxCommand(client *c);
void zaddCommand(client *c);
void zincrbyCommand(client *c);
void zrangeCommand(client *c);
void zrangebyscoreCommand(client *c);
void zrevrangebyscoreCommand(client *c);
void zrangebylexCommand(client *c);
void zrevrangebylexCommand(client *c);
void zcountCommand(client *c);
void zlexcountCommand(client *c);
void zrevrangeCommand(client *c);
void zcardCommand(client *c);
void zremCommand(client *c);
void zscoreCommand(client *c);
void zmscoreCommand(client *c);
void zremrangebyscoreCommand(client *c);
void zremrangebylexCommand(client *c);
void zpopminCommand(client *c);
void zpopmaxCommand(client *c);
void zmpopCommand(client *c);
void bzpopminCommand(client *c);
void bzpopmaxCommand(client *c);
void bzmpopCommand(client *c);
void zrandmemberCommand(client *c);
void multiCommand(client *c);
void execCommand(client *c);
void discardCommand(client *c);
void blpopCommand(client *c);
void brpopCommand(client *c);
void blmpopCommand(client *c);
void brpoplpushCommand(client *c);
void blmoveCommand(client *c);
void appendCommand(client *c);
void strlenCommand(client *c);
void zrankCommand(client *c);
void zrevrankCommand(client *c);
void hsetCommand(client *c);
void hsetnxCommand(client *c);
void hgetCommand(client *c);
void hmgetCommand(client *c);
void hdelCommand(client *c);
void hlenCommand(client *c);
void hstrlenCommand(client *c);
void zremrangebyrankCommand(client *c);
void zunionstoreCommand(client *c);
void zinterstoreCommand(client *c);
void zdiffstoreCommand(client *c);
void zunionCommand(client *c);
void zinterCommand(client *c);
void zinterCardCommand(client *c);
void zrangestoreCommand(client *c);
void zdiffCommand(client *c);
void zscanCommand(client *c);
void hkeysCommand(client *c);
void hvalsCommand(client *c);
void hgetallCommand(client *c);
void hexistsCommand(client *c);
void hscanCommand(client *c);
void hrandfieldCommand(client *c);
void configSetCommand(client *c);
void configGetCommand(client *c);
void configResetStatCommand(client *c);
void configRewriteCommand(client *c);
void configHelpCommand(client *c);
void hincrbyCommand(client *c);
void hincrbyfloatCommand(client *c);
void subscribeCommand(client *c);
void unsubscribeCommand(client *c);
void psubscribeCommand(client *c);
void punsubscribeCommand(client *c);
void publishCommand(client *c);
void pubsubCommand(client *c);
void spublishCommand(client *c);
void ssubscribeCommand(client *c);
void sunsubscribeCommand(client *c);
void watchCommand(client *c);
void unwatchCommand(client *c);
void clusterCommand(client *c);
void clusterSlotStatsCommand(client *c);
void restoreCommand(client *c);
void migrateCommand(client *c);
void askingCommand(client *c);
void readonlyCommand(client *c);
void readwriteCommand(client *c);
int verifyDumpPayload(unsigned char *p, size_t len, uint16_t *rdbver_ptr);
void dumpCommand(client *c);
void objectCommand(client *c);
void memoryCommand(client *c);
void clientCommand(client *c);
void clientHelpCommand(client *c);
void clientIDCommand(client *c);
void clientInfoCommand(client *c);
void clientListCommand(client *c);
void clientReplyCommand(client *c);
void clientNoEvictCommand(client *c);
void clientKillCommand(client *c);
void clientUnblockCommand(client *c);
void clientSetNameCommand(client *c);
void clientGetNameCommand(client *c);
void clientUnpauseCommand(client *c);
void clientPauseCommand(client *c);
void clientTrackingCommand(client *c);
void clientCachingCommand(client *c);
void clientGetredirCommand(client *c);
void clientTrackingInfoCommand(client *c);
void clientNoTouchCommand(client *c);
void clientCapaCommand(client *c);
void clientImportSourceCommand(client *c);
void helloCommand(client *c);
void clientSetinfoCommand(client *c);
void evalCommand(client *c);
void evalRoCommand(client *c);
void evalShaCommand(client *c);
void evalShaRoCommand(client *c);
void scriptCommand(client *c);
void fcallCommand(client *c);
void fcallroCommand(client *c);
void functionLoadCommand(client *c);
void functionDeleteCommand(client *c);
void functionKillCommand(client *c);
void functionStatsCommand(client *c);
void functionListCommand(client *c);
void functionHelpCommand(client *c);
void functionFlushCommand(client *c);
void functionRestoreCommand(client *c);
void functionDumpCommand(client *c);
void timeCommand(client *c);
void bitopCommand(client *c);
void bitcountCommand(client *c);
void bitposCommand(client *c);
void replconfCommand(client *c);
void waitCommand(client *c);
void waitaofCommand(client *c);
void georadiusbymemberCommand(client *c);
void georadiusbymemberroCommand(client *c);
void georadiusCommand(client *c);
void georadiusroCommand(client *c);
void geoaddCommand(client *c);
void geohashCommand(client *c);
void geoposCommand(client *c);
void geodistCommand(client *c);
void geosearchCommand(client *c);
void geosearchstoreCommand(client *c);
void pfselftestCommand(client *c);
void pfaddCommand(client *c);
void pfcountCommand(client *c);
void pfmergeCommand(client *c);
void pfdebugCommand(client *c);
void latencyCommand(client *c);
void moduleCommand(client *c);
void securityWarningCommand(client *c);
void xaddCommand(client *c);
void xrangeCommand(client *c);
void xrevrangeCommand(client *c);
void xlenCommand(client *c);
void xreadCommand(client *c);
void xgroupCommand(client *c);
void xsetidCommand(client *c);
void xackCommand(client *c);
void xpendingCommand(client *c);
void xclaimCommand(client *c);
void xautoclaimCommand(client *c);
void xinfoCommand(client *c);
void xdelCommand(client *c);
void xtrimCommand(client *c);
void lolwutCommand(client *c);
void aclCommand(client *c);
void lcsCommand(client *c);
void quitCommand(client *c);
void resetCommand(client *c);
void failoverCommand(client *c);

#if defined(__GNUC__)
void *calloc(size_t count, size_t size) __attribute__((deprecated));
void free(void *ptr) __attribute__((deprecated));
void *malloc(size_t size) __attribute__((deprecated));
void *realloc(void *ptr, size_t size) __attribute__((deprecated));
#endif

/* Debugging stuff */
void _serverAssertWithInfo(const client *c, const robj *o, const char *estr, const char *file, int line);
void _serverAssert(const char *estr, const char *file, int line);
#ifdef __GNUC__
void _serverPanic(const char *file, int line, const char *msg, ...) __attribute__((format(printf, 3, 4)));
#else
void _serverPanic(const char *file, int line, const char *msg, ...);
#endif
void serverLogObjectDebugInfo(const robj *o);
void setupDebugSigHandlers(void);
void setupSigSegvHandler(void);
void removeSigSegvHandlers(void);
const char *getSafeInfoString(const char *s, size_t len, char **tmp);
dict *genInfoSectionDict(robj **argv, int argc, char **defaults, int *out_all, int *out_everything);
void releaseInfoSectionDict(dict *sec);
sds genValkeyInfoString(dict *section_dict, int all_sections, int everything);
sds genModulesInfoString(sds info);
void applyWatchdogPeriod(void);
void watchdogScheduleSignal(int period);
void serverLogHexDump(int level, char *descr, void *value, size_t len);
int memtest_preserving_test(unsigned long *m, size_t bytes, int passes);
void mixDigest(unsigned char *digest, const void *ptr, size_t len);
void xorDigest(unsigned char *digest, const void *ptr, size_t len);
sds catSubCommandFullname(const char *parent_name, const char *sub_name);
void commandAddSubcommand(struct serverCommand *parent, struct serverCommand *subcommand);
void debugDelay(int usec);
void killThreads(void);
void makeThreadKillable(void);
void swapMainDbWithTempDb(serverDb *tempDb);
sds getVersion(void);
void debugPauseProcess(void);

/* Use macro for checking log level to avoid evaluating arguments in cases log
 * should be ignored due to low level. */
#define serverLog(level, ...)                           \
    do {                                                \
        if (((level) & 0xff) < server.verbosity) break; \
        _serverLog(level, __VA_ARGS__);                 \
    } while (0)

/* dualChannelServerLog - Log messages related to dual-channel operations
 * This macro wraps the serverLog function, prepending "<Dual Channel>"
 * to the log message. */
#define dualChannelServerLog(level, ...) serverLog(level, "Dual channel replication: " __VA_ARGS__)

#define serverDebug(fmt, ...) printf("DEBUG %s:%d > " fmt "\n", __FILE__, __LINE__, __VA_ARGS__)
#define serverDebugMark() printf("-- MARK %s:%d --\n", __FILE__, __LINE__)

int iAmPrimary(void);

#define STRINGIFY_(x) #x
#define STRINGIFY(x) STRINGIFY_(x)

#endif
