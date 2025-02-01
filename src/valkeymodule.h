/*
 * valkeymodule.h
 *
 * This header file is forked from redismodule.h to reflect the new naming conventions adopted in Valkey.
 *
 * Key Changes:
 * - Symbolic names have been changed from containing RedisModule, REDISMODULE, etc. to ValkeyModule, VALKEYMODULE, etc.
 * to align with the new module naming convention. Developers must use these new symbolic names in their module
 *   implementations.
 * - Terminology has been updated to be more inclusive: "slave" is now "replica", and "master"
 *   is now "primary". These changes are part of an effort to use more accurate and inclusive language.
 *
 * When developing modules for Valkey, ensure to include "valkeymodule.h". This header file contains
 * the updated definitions and should be used to maintain compatibility with the changes made in Valkey.
 */

#ifndef VALKEYMODULE_H
#define VALKEYMODULE_H

#include <sys/types.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>


typedef struct ValkeyModuleString ValkeyModuleString;
typedef struct ValkeyModuleKey ValkeyModuleKey;

/* -------------- Defines NOT common between core and modules ------------- */

#if defined VALKEYMODULE_CORE
/* Things only defined for the modules core (server), not exported to modules
 * that include this file. */

#define ValkeyModuleString robj

#endif /* defined VALKEYMODULE_CORE */

#if !defined VALKEYMODULE_CORE && !defined VALKEYMODULE_CORE_MODULE
/* Things defined for modules, but not for core-modules. */

typedef long long mstime_t;
typedef long long ustime_t;

#endif /* !defined VALKEYMODULE_CORE && !defined VALKEYMODULE_CORE_MODULE */

/* ---------------- Defines common between core and modules --------------- */

/* Error status return values. */
#define VALKEYMODULE_OK 0
#define VALKEYMODULE_ERR 1

/* Module Based Authentication status return values. */
#define VALKEYMODULE_AUTH_HANDLED 0
#define VALKEYMODULE_AUTH_NOT_HANDLED 1

/* API versions. */
#define VALKEYMODULE_APIVER_1 1

/* Version of the ValkeyModuleTypeMethods structure. Once the ValkeyModuleTypeMethods
 * structure is changed, this version number needs to be changed synchronistically. */
#define VALKEYMODULE_TYPE_METHOD_VERSION 5

/* API flags and constants */
#define VALKEYMODULE_READ (1 << 0)
#define VALKEYMODULE_WRITE (1 << 1)

/* ValkeyModule_OpenKey extra flags for the 'mode' argument.
 * Avoid touching the LRU/LFU of the key when opened. */
#define VALKEYMODULE_OPEN_KEY_NOTOUCH (1 << 16)
/* Don't trigger keyspace event on key misses. */
#define VALKEYMODULE_OPEN_KEY_NONOTIFY (1 << 17)
/* Don't update keyspace hits/misses counters. */
#define VALKEYMODULE_OPEN_KEY_NOSTATS (1 << 18)
/* Avoid deleting lazy expired keys. */
#define VALKEYMODULE_OPEN_KEY_NOEXPIRE (1 << 19)
/* Avoid any effects from fetching the key */
#define VALKEYMODULE_OPEN_KEY_NOEFFECTS (1 << 20)
/* Mask of all VALKEYMODULE_OPEN_KEY_* values. Any new mode should be added to this list.
 * Should not be used directly by the module, use RM_GetOpenKeyModesAll instead.
 * Located here so when we will add new modes we will not forget to update it. */
#define _VALKEYMODULE_OPEN_KEY_ALL                                                                            \
    VALKEYMODULE_READ | VALKEYMODULE_WRITE | VALKEYMODULE_OPEN_KEY_NOTOUCH | VALKEYMODULE_OPEN_KEY_NONOTIFY | \
        VALKEYMODULE_OPEN_KEY_NOSTATS | VALKEYMODULE_OPEN_KEY_NOEXPIRE | VALKEYMODULE_OPEN_KEY_NOEFFECTS

/* List push and pop */
#define VALKEYMODULE_LIST_HEAD 0
#define VALKEYMODULE_LIST_TAIL 1

/* Key types. */
#define VALKEYMODULE_KEYTYPE_EMPTY 0
#define VALKEYMODULE_KEYTYPE_STRING 1
#define VALKEYMODULE_KEYTYPE_LIST 2
#define VALKEYMODULE_KEYTYPE_HASH 3
#define VALKEYMODULE_KEYTYPE_SET 4
#define VALKEYMODULE_KEYTYPE_ZSET 5
#define VALKEYMODULE_KEYTYPE_MODULE 6
#define VALKEYMODULE_KEYTYPE_STREAM 7

/* Reply types. */
#define VALKEYMODULE_REPLY_UNKNOWN -1
#define VALKEYMODULE_REPLY_STRING 0
#define VALKEYMODULE_REPLY_ERROR 1
#define VALKEYMODULE_REPLY_INTEGER 2
#define VALKEYMODULE_REPLY_ARRAY 3
#define VALKEYMODULE_REPLY_NULL 4
#define VALKEYMODULE_REPLY_MAP 5
#define VALKEYMODULE_REPLY_SET 6
#define VALKEYMODULE_REPLY_BOOL 7
#define VALKEYMODULE_REPLY_DOUBLE 8
#define VALKEYMODULE_REPLY_BIG_NUMBER 9
#define VALKEYMODULE_REPLY_VERBATIM_STRING 10
#define VALKEYMODULE_REPLY_ATTRIBUTE 11
#define VALKEYMODULE_REPLY_PROMISE 12

/* Postponed array length. */
#define VALKEYMODULE_POSTPONED_ARRAY_LEN -1 /* Deprecated, please use VALKEYMODULE_POSTPONED_LEN */
#define VALKEYMODULE_POSTPONED_LEN -1

/* Expire */
#define VALKEYMODULE_NO_EXPIRE -1

/* Sorted set API flags. */
#define VALKEYMODULE_ZADD_XX (1 << 0)
#define VALKEYMODULE_ZADD_NX (1 << 1)
#define VALKEYMODULE_ZADD_ADDED (1 << 2)
#define VALKEYMODULE_ZADD_UPDATED (1 << 3)
#define VALKEYMODULE_ZADD_NOP (1 << 4)
#define VALKEYMODULE_ZADD_GT (1 << 5)
#define VALKEYMODULE_ZADD_LT (1 << 6)

/* Hash API flags. */
#define VALKEYMODULE_HASH_NONE 0
#define VALKEYMODULE_HASH_NX (1 << 0)
#define VALKEYMODULE_HASH_XX (1 << 1)
#define VALKEYMODULE_HASH_CFIELDS (1 << 2)
#define VALKEYMODULE_HASH_EXISTS (1 << 3)
#define VALKEYMODULE_HASH_COUNT_ALL (1 << 4)

#define VALKEYMODULE_CONFIG_DEFAULT 0                /* This is the default for a module config. */
#define VALKEYMODULE_CONFIG_IMMUTABLE (1ULL << 0)    /* Can this value only be set at startup? */
#define VALKEYMODULE_CONFIG_SENSITIVE (1ULL << 1)    /* Does this value contain sensitive information */
#define VALKEYMODULE_CONFIG_HIDDEN (1ULL << 4)       /* This config is hidden in `config get <pattern>` (used for tests/debugging) */
#define VALKEYMODULE_CONFIG_PROTECTED (1ULL << 5)    /* Becomes immutable if enable-protected-configs is enabled. */
#define VALKEYMODULE_CONFIG_DENY_LOADING (1ULL << 6) /* This config is forbidden during loading. */

#define VALKEYMODULE_CONFIG_MEMORY (1ULL << 7)   /* Indicates if this value can be set as a memory value */
#define VALKEYMODULE_CONFIG_BITFLAGS (1ULL << 8) /* Indicates if this value can be set as a multiple enum values */

/* StreamID type. */
typedef struct ValkeyModuleStreamID {
    uint64_t ms;
    uint64_t seq;
} ValkeyModuleStreamID;

/* StreamAdd() flags. */
#define VALKEYMODULE_STREAM_ADD_AUTOID (1 << 0)
/* StreamIteratorStart() flags. */
#define VALKEYMODULE_STREAM_ITERATOR_EXCLUSIVE (1 << 0)
#define VALKEYMODULE_STREAM_ITERATOR_REVERSE (1 << 1)
/* StreamIteratorTrim*() flags. */
#define VALKEYMODULE_STREAM_TRIM_APPROX (1 << 0)

/* Context Flags: Info about the current context returned by
 * RM_GetContextFlags(). */

/* The command is running in the context of a Lua script */
#define VALKEYMODULE_CTX_FLAGS_LUA (1 << 0)
/* The command is running inside a Valkey transaction */
#define VALKEYMODULE_CTX_FLAGS_MULTI (1 << 1)
/* The instance is a primary */
#define VALKEYMODULE_CTX_FLAGS_PRIMARY (1 << 2)
/* The instance is a replic */
#define VALKEYMODULE_CTX_FLAGS_REPLICA (1 << 3)
/* The instance is read-only (usually meaning it's a replica as well) */
#define VALKEYMODULE_CTX_FLAGS_READONLY (1 << 4)
/* The instance is running in cluster mode */
#define VALKEYMODULE_CTX_FLAGS_CLUSTER (1 << 5)
/* The instance has AOF enabled */
#define VALKEYMODULE_CTX_FLAGS_AOF (1 << 6)
/* The instance has RDB enabled */
#define VALKEYMODULE_CTX_FLAGS_RDB (1 << 7)
/* The instance has Maxmemory set */
#define VALKEYMODULE_CTX_FLAGS_MAXMEMORY (1 << 8)
/* Maxmemory is set and has an eviction policy that may delete keys */
#define VALKEYMODULE_CTX_FLAGS_EVICT (1 << 9)
/* Valkey is out of memory according to the maxmemory flag. */
#define VALKEYMODULE_CTX_FLAGS_OOM (1 << 10)
/* Less than 25% of memory available according to maxmemory. */
#define VALKEYMODULE_CTX_FLAGS_OOM_WARNING (1 << 11)
/* The command was sent over the replication link. */
#define VALKEYMODULE_CTX_FLAGS_REPLICATED (1 << 12)
/* Valkey is currently loading either from AOF or RDB. */
#define VALKEYMODULE_CTX_FLAGS_LOADING (1 << 13)
/* The replica has no link with its primary, note that
 * there is the inverse flag as well:
 *
 *  VALKEYMODULE_CTX_FLAGS_REPLICA_IS_ONLINE
 *
 * The two flags are exclusive, one or the other can be set. */
#define VALKEYMODULE_CTX_FLAGS_REPLICA_IS_STALE (1 << 14)
/* The replica is trying to connect with the primary.
 * (REPL_STATE_CONNECT and REPL_STATE_CONNECTING states) */
#define VALKEYMODULE_CTX_FLAGS_REPLICA_IS_CONNECTING (1 << 15)
/* THe replica is receiving an RDB file from its primary. */
#define VALKEYMODULE_CTX_FLAGS_REPLICA_IS_TRANSFERRING (1 << 16)
/* The replica is online, receiving updates from its primary. */
#define VALKEYMODULE_CTX_FLAGS_REPLICA_IS_ONLINE (1 << 17)
/* There is currently some background process active. */
#define VALKEYMODULE_CTX_FLAGS_ACTIVE_CHILD (1 << 18)
/* The next EXEC will fail due to dirty CAS (touched keys). */
#define VALKEYMODULE_CTX_FLAGS_MULTI_DIRTY (1 << 19)
/* Valkey is currently running inside background child process. */
#define VALKEYMODULE_CTX_FLAGS_IS_CHILD (1 << 20)
/* The current client does not allow blocking, either called from
 * within multi, lua, or from another module using RM_Call */
#define VALKEYMODULE_CTX_FLAGS_DENY_BLOCKING (1 << 21)
/* The current client uses RESP3 protocol */
#define VALKEYMODULE_CTX_FLAGS_RESP3 (1 << 22)
/* Valkey is currently async loading database for diskless replication. */
#define VALKEYMODULE_CTX_FLAGS_ASYNC_LOADING (1 << 23)
/* Valkey is starting. */
#define VALKEYMODULE_CTX_FLAGS_SERVER_STARTUP (1 << 24)

/* Next context flag, must be updated when adding new flags above!
This flag should not be used directly by the module.
 * Use ValkeyModule_GetContextFlagsAll instead. */
#define _VALKEYMODULE_CTX_FLAGS_NEXT (1 << 25)

/* Keyspace changes notification classes. Every class is associated with a
 * character for configuration purposes.
 * NOTE: These have to be in sync with NOTIFY_* in server.h */
#define VALKEYMODULE_NOTIFY_KEYSPACE (1 << 0)  /* K */
#define VALKEYMODULE_NOTIFY_KEYEVENT (1 << 1)  /* E */
#define VALKEYMODULE_NOTIFY_GENERIC (1 << 2)   /* g */
#define VALKEYMODULE_NOTIFY_STRING (1 << 3)    /* $ */
#define VALKEYMODULE_NOTIFY_LIST (1 << 4)      /* l */
#define VALKEYMODULE_NOTIFY_SET (1 << 5)       /* s */
#define VALKEYMODULE_NOTIFY_HASH (1 << 6)      /* h */
#define VALKEYMODULE_NOTIFY_ZSET (1 << 7)      /* z */
#define VALKEYMODULE_NOTIFY_EXPIRED (1 << 8)   /* x */
#define VALKEYMODULE_NOTIFY_EVICTED (1 << 9)   /* e */
#define VALKEYMODULE_NOTIFY_STREAM (1 << 10)   /* t */
#define VALKEYMODULE_NOTIFY_KEY_MISS (1 << 11) /* m (Note: This one is excluded from VALKEYMODULE_NOTIFY_ALL on purpose) */
#define VALKEYMODULE_NOTIFY_LOADED (1 << 12)   /* module only key space notification, indicate a key loaded from rdb */
#define VALKEYMODULE_NOTIFY_MODULE (1 << 13)   /* d, module key space notification */
#define VALKEYMODULE_NOTIFY_NEW (1 << 14)      /* n, new key notification */

/* Next notification flag, must be updated when adding new flags above!
This flag should not be used directly by the module.
 * Use ValkeyModule_GetKeyspaceNotificationFlagsAll instead. */
#define _VALKEYMODULE_NOTIFY_NEXT (1 << 15)

#define VALKEYMODULE_NOTIFY_ALL                                                                                        \
    (VALKEYMODULE_NOTIFY_GENERIC | VALKEYMODULE_NOTIFY_STRING | VALKEYMODULE_NOTIFY_LIST | VALKEYMODULE_NOTIFY_SET |   \
     VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_ZSET | VALKEYMODULE_NOTIFY_EXPIRED | VALKEYMODULE_NOTIFY_EVICTED | \
     VALKEYMODULE_NOTIFY_STREAM | VALKEYMODULE_NOTIFY_MODULE) /* A */

/* A special pointer that we can use between the core and the module to signal
 * field deletion, and that is impossible to be a valid pointer. */
#define VALKEYMODULE_HASH_DELETE ((ValkeyModuleString *)(long)1)

/* Error messages. */
#define VALKEYMODULE_ERRORMSG_WRONGTYPE "WRONGTYPE Operation against a key holding the wrong kind of value"

#define VALKEYMODULE_POSITIVE_INFINITE (1.0 / 0.0)
#define VALKEYMODULE_NEGATIVE_INFINITE (-1.0 / 0.0)

/* Cluster API defines. */
#define VALKEYMODULE_NODE_ID_LEN 40
#define VALKEYMODULE_NODE_MYSELF (1 << 0)
#define VALKEYMODULE_NODE_PRIMARY (1 << 1)
#define VALKEYMODULE_NODE_REPLICA (1 << 2)
#define VALKEYMODULE_NODE_PFAIL (1 << 3)
#define VALKEYMODULE_NODE_FAIL (1 << 4)
#define VALKEYMODULE_NODE_NOFAILOVER (1 << 5)

#define VALKEYMODULE_CLUSTER_FLAG_NONE 0
#define VALKEYMODULE_CLUSTER_FLAG_NO_FAILOVER (1 << 1)
#define VALKEYMODULE_CLUSTER_FLAG_NO_REDIRECTION (1 << 2)

#define VALKEYMODULE_NOT_USED(V) ((void)V)

/* Logging level strings */
#define VALKEYMODULE_LOGLEVEL_DEBUG "debug"
#define VALKEYMODULE_LOGLEVEL_VERBOSE "verbose"
#define VALKEYMODULE_LOGLEVEL_NOTICE "notice"
#define VALKEYMODULE_LOGLEVEL_WARNING "warning"

/* Bit flags for aux_save_triggers and the aux_load and aux_save callbacks */
#define VALKEYMODULE_AUX_BEFORE_RDB (1 << 0)
#define VALKEYMODULE_AUX_AFTER_RDB (1 << 1)

/* RM_Yield flags */
#define VALKEYMODULE_YIELD_FLAG_NONE (1 << 0)
#define VALKEYMODULE_YIELD_FLAG_CLIENTS (1 << 1)

/* RM_BlockClientOnKeysWithFlags flags */
#define VALKEYMODULE_BLOCK_UNBLOCK_DEFAULT (0)
#define VALKEYMODULE_BLOCK_UNBLOCK_DELETED (1 << 0)

/* This type represents a timer handle, and is returned when a timer is
 * registered and used in order to invalidate a timer. It's just a 64 bit
 * number, because this is how each timer is represented inside the radix tree
 * of timers that are going to expire, sorted by expire time. */
typedef uint64_t ValkeyModuleTimerID;

/* CommandFilter Flags */

/* Do filter ValkeyModule_Call() commands initiated by module itself. */
#define VALKEYMODULE_CMDFILTER_NOSELF (1 << 0)

/* Declare that the module can handle errors with ValkeyModule_SetModuleOptions. */
#define VALKEYMODULE_OPTIONS_HANDLE_IO_ERRORS (1 << 0)

/* When set, Valkey will not call ValkeyModule_SignalModifiedKey(), implicitly in
 * ValkeyModule_CloseKey, and the module needs to do that when manually when keys
 * are modified from the user's perspective, to invalidate WATCH. */
#define VALKEYMODULE_OPTION_NO_IMPLICIT_SIGNAL_MODIFIED (1 << 1)

/* Declare that the module can handle diskless async replication with ValkeyModule_SetModuleOptions. */
#define VALKEYMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD (1 << 2)

/* Declare that the module want to get nested key space notifications.
 * If enabled, the module is responsible to break endless loop. */
#define VALKEYMODULE_OPTIONS_ALLOW_NESTED_KEYSPACE_NOTIFICATIONS (1 << 3)

/* Next option flag, must be updated when adding new module flags above!
 * This flag should not be used directly by the module.
 * Use ValkeyModule_GetModuleOptionsAll instead. */
#define _VALKEYMODULE_OPTIONS_FLAGS_NEXT (1 << 4)

/* Definitions for ValkeyModule_SetCommandInfo. */

typedef enum {
    VALKEYMODULE_ARG_TYPE_STRING,
    VALKEYMODULE_ARG_TYPE_INTEGER,
    VALKEYMODULE_ARG_TYPE_DOUBLE,
    VALKEYMODULE_ARG_TYPE_KEY, /* A string, but represents a keyname */
    VALKEYMODULE_ARG_TYPE_PATTERN,
    VALKEYMODULE_ARG_TYPE_UNIX_TIME,
    VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
    VALKEYMODULE_ARG_TYPE_ONEOF, /* Must have sub-arguments */
    VALKEYMODULE_ARG_TYPE_BLOCK  /* Must have sub-arguments */
} ValkeyModuleCommandArgType;

#define VALKEYMODULE_CMD_ARG_NONE (0)
#define VALKEYMODULE_CMD_ARG_OPTIONAL (1 << 0)       /* The argument is optional (like GET in SET command) */
#define VALKEYMODULE_CMD_ARG_MULTIPLE (1 << 1)       /* The argument may repeat itself (like key in DEL) */
#define VALKEYMODULE_CMD_ARG_MULTIPLE_TOKEN (1 << 2) /* The argument may repeat itself, and so does its token (like `GET pattern` in SORT) */
#define _VALKEYMODULE_CMD_ARG_NEXT (1 << 3)

typedef enum {
    VALKEYMODULE_KSPEC_BS_INVALID = 0, /* Must be zero. An implicitly value of
                                        * zero is provided when the field is
                                        * absent in a struct literal. */
    VALKEYMODULE_KSPEC_BS_UNKNOWN,
    VALKEYMODULE_KSPEC_BS_INDEX,
    VALKEYMODULE_KSPEC_BS_KEYWORD
} ValkeyModuleKeySpecBeginSearchType;

typedef enum {
    VALKEYMODULE_KSPEC_FK_OMITTED = 0, /* Used when the field is absent in a
                                        * struct literal. Don't use this value
                                        * explicitly. */
    VALKEYMODULE_KSPEC_FK_UNKNOWN,
    VALKEYMODULE_KSPEC_FK_RANGE,
    VALKEYMODULE_KSPEC_FK_KEYNUM
} ValkeyModuleKeySpecFindKeysType;

/* Key-spec flags. For details, see the documentation of
 * ValkeyModule_SetCommandInfo and the key-spec flags in server.h. */
#define VALKEYMODULE_CMD_KEY_RO (1ULL << 0)
#define VALKEYMODULE_CMD_KEY_RW (1ULL << 1)
#define VALKEYMODULE_CMD_KEY_OW (1ULL << 2)
#define VALKEYMODULE_CMD_KEY_RM (1ULL << 3)
#define VALKEYMODULE_CMD_KEY_ACCESS (1ULL << 4)
#define VALKEYMODULE_CMD_KEY_UPDATE (1ULL << 5)
#define VALKEYMODULE_CMD_KEY_INSERT (1ULL << 6)
#define VALKEYMODULE_CMD_KEY_DELETE (1ULL << 7)
#define VALKEYMODULE_CMD_KEY_NOT_KEY (1ULL << 8)
#define VALKEYMODULE_CMD_KEY_INCOMPLETE (1ULL << 9)
#define VALKEYMODULE_CMD_KEY_VARIABLE_FLAGS (1ULL << 10)

/* Channel flags, for details see the documentation of
 * ValkeyModule_ChannelAtPosWithFlags. */
#define VALKEYMODULE_CMD_CHANNEL_PATTERN (1ULL << 0)
#define VALKEYMODULE_CMD_CHANNEL_PUBLISH (1ULL << 1)
#define VALKEYMODULE_CMD_CHANNEL_SUBSCRIBE (1ULL << 2)
#define VALKEYMODULE_CMD_CHANNEL_UNSUBSCRIBE (1ULL << 3)

typedef struct ValkeyModuleCommandArg {
    const char *name;
    ValkeyModuleCommandArgType type;
    int key_spec_index; /* If type is KEY, this is a zero-based index of
                         * the key_spec in the command. For other types,
                         * you may specify -1. */
    const char *token;  /* If type is PURE_TOKEN, this is the token. */
    const char *summary;
    const char *since;
    int flags; /* The VALKEYMODULE_CMD_ARG_* macros. */
    const char *deprecated_since;
    struct ValkeyModuleCommandArg *subargs;
    const char *display_text;
} ValkeyModuleCommandArg;

typedef struct {
    const char *since;
    const char *changes;
} ValkeyModuleCommandHistoryEntry;

typedef struct {
    const char *notes;
    uint64_t flags; /* VALKEYMODULE_CMD_KEY_* macros. */
    ValkeyModuleKeySpecBeginSearchType begin_search_type;
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
    ValkeyModuleKeySpecFindKeysType find_keys_type;
    union {
        struct {
            /* Index of the last key relative to the result of the begin search
             * step. Can be negative, in which case it's not relative. -1
             * indicating till the last argument, -2 one before the last and so
             * on. */
            int lastkey;
            /* How many args should we skip after finding a key, in order to
             * find the next one. */
            int keystep;
            /* If lastkey is -1, we use limit to stop the search by a factor. 0
             * and 1 mean no limit. 2 means 1/2 of the remaining args, 3 means
             * 1/3, and so on. */
            int limit;
        } range;
        struct {
            /* Index of the argument containing the number of keys to come
             * relative to the result of the begin search step */
            int keynumidx;
            /* Index of the fist key. (Usually it's just after keynumidx, in
             * which case it should be set to keynumidx + 1.) */
            int firstkey;
            /* How many args should we skip after finding a key, in order to
             * find the next one, relative to the result of the begin search
             * step. */
            int keystep;
        } keynum;
    } fk;
} ValkeyModuleCommandKeySpec;

typedef struct {
    int version;
    size_t sizeof_historyentry;
    size_t sizeof_keyspec;
    size_t sizeof_arg;
} ValkeyModuleCommandInfoVersion;

static const ValkeyModuleCommandInfoVersion ValkeyModule_CurrentCommandInfoVersion = {
    .version = 1,
    .sizeof_historyentry = sizeof(ValkeyModuleCommandHistoryEntry),
    .sizeof_keyspec = sizeof(ValkeyModuleCommandKeySpec),
    .sizeof_arg = sizeof(ValkeyModuleCommandArg)};

#define VALKEYMODULE_COMMAND_INFO_VERSION (&ValkeyModule_CurrentCommandInfoVersion)

typedef struct {
    /* Always set version to VALKEYMODULE_COMMAND_INFO_VERSION */
    const ValkeyModuleCommandInfoVersion *version;
    const char *summary;                      /* Summary of the command */
    const char *complexity;                   /* Complexity description */
    const char *since;                        /* Debut module version of the command */
    ValkeyModuleCommandHistoryEntry *history; /* History */
    /* A string of space-separated tips meant for clients/proxies regarding this
     * command */
    const char *tips;
    /* Number of arguments, it is possible to use -N to say >= N */
    int arity;
    ValkeyModuleCommandKeySpec *key_specs;
    ValkeyModuleCommandArg *args;
} ValkeyModuleCommandInfo;

/* Eventloop definitions. */
#define VALKEYMODULE_EVENTLOOP_READABLE 1
#define VALKEYMODULE_EVENTLOOP_WRITABLE 2
typedef void (*ValkeyModuleEventLoopFunc)(int fd, void *user_data, int mask);
typedef void (*ValkeyModuleEventLoopOneShotFunc)(void *user_data);

/* Server events definitions.
 * Those flags should not be used directly by the module, instead
 * the module should use ValkeyModuleEvent_* variables.
 * Note: This must be synced with moduleEventVersions */
#define VALKEYMODULE_EVENT_REPLICATION_ROLE_CHANGED 0
#define VALKEYMODULE_EVENT_PERSISTENCE 1
#define VALKEYMODULE_EVENT_FLUSHDB 2
#define VALKEYMODULE_EVENT_LOADING 3
#define VALKEYMODULE_EVENT_CLIENT_CHANGE 4
#define VALKEYMODULE_EVENT_SHUTDOWN 5
#define VALKEYMODULE_EVENT_REPLICA_CHANGE 6
#define VALKEYMODULE_EVENT_PRIMARY_LINK_CHANGE 7
#define VALKEYMODULE_EVENT_CRON_LOOP 8
#define VALKEYMODULE_EVENT_MODULE_CHANGE 9
#define VALKEYMODULE_EVENT_LOADING_PROGRESS 10
#define VALKEYMODULE_EVENT_SWAPDB 11
#define VALKEYMODULE_EVENT_REPL_BACKUP 12 /* Not used anymore. */
#define VALKEYMODULE_EVENT_FORK_CHILD 13
#define VALKEYMODULE_EVENT_REPL_ASYNC_LOAD 14
#define VALKEYMODULE_EVENT_EVENTLOOP 15
#define VALKEYMODULE_EVENT_CONFIG 16
#define VALKEYMODULE_EVENT_KEY 17
#define _VALKEYMODULE_EVENT_NEXT 18 /* Next event flag, should be updated if a new event added. */

typedef struct ValkeyModuleEvent {
    uint64_t id;      /* VALKEYMODULE_EVENT_... defines. */
    uint64_t dataver; /* Version of the structure we pass as 'data'. */
} ValkeyModuleEvent;

struct ValkeyModuleCtx;
struct ValkeyModuleDefragCtx;
typedef void (*ValkeyModuleEventCallback)(struct ValkeyModuleCtx *ctx,
                                          ValkeyModuleEvent eid,
                                          uint64_t subevent,
                                          void *data);

/* IMPORTANT: When adding a new version of one of below structures that contain
 * event data (ValkeyModuleFlushInfoV1 for example) we have to avoid renaming the
 * old ValkeyModuleEvent structure.
 * For example, if we want to add ValkeyModuleFlushInfoV2, the ValkeyModuleEvent
 * structures should be:
 *      ValkeyModuleEvent_FlushDB = {
 *          VALKEYMODULE_EVENT_FLUSHDB,
 *          1
 *      },
 *      ValkeyModuleEvent_FlushDBV2 = {
 *          VALKEYMODULE_EVENT_FLUSHDB,
 *          2
 *      }
 * and NOT:
 *      ValkeyModuleEvent_FlushDBV1 = {
 *          VALKEYMODULE_EVENT_FLUSHDB,
 *          1
 *      },
 *      ValkeyModuleEvent_FlushDB = {
 *          VALKEYMODULE_EVENT_FLUSHDB,
 *          2
 *      }
 * The reason for that is forward-compatibility: We want that module that
 * compiled with a new valkeymodule.h to be able to work with a old server,
 * unless the author explicitly decided to use the newer event type.
 */
static const ValkeyModuleEvent ValkeyModuleEvent_ReplicationRoleChanged = {VALKEYMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                                                                           1},
                               ValkeyModuleEvent_Persistence = {VALKEYMODULE_EVENT_PERSISTENCE, 1},
                               ValkeyModuleEvent_FlushDB = {VALKEYMODULE_EVENT_FLUSHDB, 1},
                               ValkeyModuleEvent_Loading = {VALKEYMODULE_EVENT_LOADING, 1},
                               ValkeyModuleEvent_ClientChange = {VALKEYMODULE_EVENT_CLIENT_CHANGE, 1},
                               ValkeyModuleEvent_Shutdown = {VALKEYMODULE_EVENT_SHUTDOWN, 1},
                               ValkeyModuleEvent_ReplicaChange = {VALKEYMODULE_EVENT_REPLICA_CHANGE, 1},
                               ValkeyModuleEvent_CronLoop = {VALKEYMODULE_EVENT_CRON_LOOP, 1},
                               ValkeyModuleEvent_PrimaryLinkChange = {VALKEYMODULE_EVENT_PRIMARY_LINK_CHANGE, 1},
                               ValkeyModuleEvent_ModuleChange = {VALKEYMODULE_EVENT_MODULE_CHANGE, 1},
                               ValkeyModuleEvent_LoadingProgress = {VALKEYMODULE_EVENT_LOADING_PROGRESS, 1},
                               ValkeyModuleEvent_SwapDB = {VALKEYMODULE_EVENT_SWAPDB, 1},
                               ValkeyModuleEvent_ReplAsyncLoad = {VALKEYMODULE_EVENT_REPL_ASYNC_LOAD, 1},
                               ValkeyModuleEvent_ForkChild = {VALKEYMODULE_EVENT_FORK_CHILD, 1},
                               ValkeyModuleEvent_EventLoop = {VALKEYMODULE_EVENT_EVENTLOOP, 1},
                               ValkeyModuleEvent_Config = {VALKEYMODULE_EVENT_CONFIG, 1},
                               ValkeyModuleEvent_Key = {VALKEYMODULE_EVENT_KEY, 1};

/* Those are values that are used for the 'subevent' callback argument. */
#define VALKEYMODULE_SUBEVENT_PERSISTENCE_RDB_START 0
#define VALKEYMODULE_SUBEVENT_PERSISTENCE_AOF_START 1
#define VALKEYMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START 2
#define VALKEYMODULE_SUBEVENT_PERSISTENCE_ENDED 3
#define VALKEYMODULE_SUBEVENT_PERSISTENCE_FAILED 4
#define VALKEYMODULE_SUBEVENT_PERSISTENCE_SYNC_AOF_START 5
#define _VALKEYMODULE_SUBEVENT_PERSISTENCE_NEXT 6

#define VALKEYMODULE_SUBEVENT_LOADING_RDB_START 0
#define VALKEYMODULE_SUBEVENT_LOADING_AOF_START 1
#define VALKEYMODULE_SUBEVENT_LOADING_REPL_START 2
#define VALKEYMODULE_SUBEVENT_LOADING_ENDED 3
#define VALKEYMODULE_SUBEVENT_LOADING_FAILED 4
#define _VALKEYMODULE_SUBEVENT_LOADING_NEXT 5

#define VALKEYMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED 0
#define VALKEYMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED 1
#define _VALKEYMODULE_SUBEVENT_CLIENT_CHANGE_NEXT 2

#define VALKEYMODULE_SUBEVENT_PRIMARY_LINK_UP 0
#define VALKEYMODULE_SUBEVENT_PRIMARY_LINK_DOWN 1
#define _VALKEYMODULE_SUBEVENT_PRIMARY_NEXT 2

#define VALKEYMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE 0
#define VALKEYMODULE_SUBEVENT_REPLICA_CHANGE_OFFLINE 1
#define _VALKEYMODULE_SUBEVENT_REPLICA_CHANGE_NEXT 2

#define VALKEYMODULE_EVENT_REPLROLECHANGED_NOW_PRIMARY 0
#define VALKEYMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA 1
#define _VALKEYMODULE_EVENT_REPLROLECHANGED_NEXT 2

#define VALKEYMODULE_SUBEVENT_FLUSHDB_START 0
#define VALKEYMODULE_SUBEVENT_FLUSHDB_END 1
#define _VALKEYMODULE_SUBEVENT_FLUSHDB_NEXT 2

#define VALKEYMODULE_SUBEVENT_MODULE_LOADED 0
#define VALKEYMODULE_SUBEVENT_MODULE_UNLOADED 1
#define _VALKEYMODULE_SUBEVENT_MODULE_NEXT 2

#define VALKEYMODULE_SUBEVENT_CONFIG_CHANGE 0
#define _VALKEYMODULE_SUBEVENT_CONFIG_NEXT 1

#define VALKEYMODULE_SUBEVENT_LOADING_PROGRESS_RDB 0
#define VALKEYMODULE_SUBEVENT_LOADING_PROGRESS_AOF 1
#define _VALKEYMODULE_SUBEVENT_LOADING_PROGRESS_NEXT 2

#define VALKEYMODULE_SUBEVENT_REPL_ASYNC_LOAD_STARTED 0
#define VALKEYMODULE_SUBEVENT_REPL_ASYNC_LOAD_ABORTED 1
#define VALKEYMODULE_SUBEVENT_REPL_ASYNC_LOAD_COMPLETED 2
#define _VALKEYMODULE_SUBEVENT_REPL_ASYNC_LOAD_NEXT 3

#define VALKEYMODULE_SUBEVENT_FORK_CHILD_BORN 0
#define VALKEYMODULE_SUBEVENT_FORK_CHILD_DIED 1
#define _VALKEYMODULE_SUBEVENT_FORK_CHILD_NEXT 2

#define VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP 0
#define VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP 1
#define _VALKEYMODULE_SUBEVENT_EVENTLOOP_NEXT 2

#define VALKEYMODULE_SUBEVENT_KEY_DELETED 0
#define VALKEYMODULE_SUBEVENT_KEY_EXPIRED 1
#define VALKEYMODULE_SUBEVENT_KEY_EVICTED 2
#define VALKEYMODULE_SUBEVENT_KEY_OVERWRITTEN 3
#define _VALKEYMODULE_SUBEVENT_KEY_NEXT 4

#define _VALKEYMODULE_SUBEVENT_SHUTDOWN_NEXT 0
#define _VALKEYMODULE_SUBEVENT_CRON_LOOP_NEXT 0
#define _VALKEYMODULE_SUBEVENT_SWAPDB_NEXT 0

/* ValkeyModuleClientInfo flags. */
#define VALKEYMODULE_CLIENTINFO_FLAG_SSL (1 << 0)
#define VALKEYMODULE_CLIENTINFO_FLAG_PUBSUB (1 << 1)
#define VALKEYMODULE_CLIENTINFO_FLAG_BLOCKED (1 << 2)
#define VALKEYMODULE_CLIENTINFO_FLAG_TRACKING (1 << 3)
#define VALKEYMODULE_CLIENTINFO_FLAG_UNIXSOCKET (1 << 4)
#define VALKEYMODULE_CLIENTINFO_FLAG_MULTI (1 << 5)

/* Here we take all the structures that the module pass to the core
 * and the other way around. Notably the list here contains the structures
 * used by the hooks API ValkeyModule_RegisterToServerEvent().
 *
 * The structures always start with a 'version' field. This is useful
 * when we want to pass a reference to the structure to the core APIs,
 * for the APIs to fill the structure. In that case, the structure 'version'
 * field is initialized before passing it to the core, so that the core is
 * able to cast the pointer to the appropriate structure version. In this
 * way we obtain ABI compatibility.
 *
 * Here we'll list all the structure versions in case they evolve over time,
 * however using a define, we'll make sure to use the last version as the
 * public name for the module to use. */

#define VALKEYMODULE_CLIENTINFO_VERSION 1
typedef struct ValkeyModuleClientInfo {
    uint64_t version; /* Version of this structure for ABI compat. */
    uint64_t flags;   /* VALKEYMODULE_CLIENTINFO_FLAG_* */
    uint64_t id;      /* Client ID. */
    char addr[46];    /* IPv4 or IPv6 address. */
    uint16_t port;    /* TCP port. */
    uint16_t db;      /* Selected DB. */
} ValkeyModuleClientInfoV1;

#define ValkeyModuleClientInfo ValkeyModuleClientInfoV1

#define VALKEYMODULE_CLIENTINFO_INITIALIZER_V1 {.version = 1}

#define VALKEYMODULE_REPLICATIONINFO_VERSION 1
typedef struct ValkeyModuleReplicationInfo {
    uint64_t version;      /* Not used since this structure is never passed
                              from the module to the core right now. Here
                              for future compatibility. */
    int primary;           /* true if primary, false if replica */
    char *primary_host;    /* primary instance hostname for NOW_REPLICA */
    int primary_port;      /* primary instance port for NOW_REPLICA */
    char *replid1;         /* Main replication ID */
    char *replid2;         /* Secondary replication ID */
    uint64_t repl1_offset; /* Main replication offset */
    uint64_t repl2_offset; /* Offset of replid2 validity */
} ValkeyModuleReplicationInfoV1;

#define ValkeyModuleReplicationInfo ValkeyModuleReplicationInfoV1

#define VALKEYMODULE_FLUSHINFO_VERSION 1
typedef struct ValkeyModuleFlushInfo {
    uint64_t version; /* Not used since this structure is never passed
                         from the module to the core right now. Here
                         for future compatibility. */
    int32_t sync;     /* Synchronous or threaded flush?. */
    int32_t dbnum;    /* Flushed database number, -1 for ALL. */
} ValkeyModuleFlushInfoV1;

#define ValkeyModuleFlushInfo ValkeyModuleFlushInfoV1

#define VALKEYMODULE_MODULE_CHANGE_VERSION 1
typedef struct ValkeyModuleModuleChange {
    uint64_t version;        /* Not used since this structure is never passed
                                from the module to the core right now. Here
                                for future compatibility. */
    const char *module_name; /* Name of module loaded or unloaded. */
    int32_t module_version;  /* Module version. */
} ValkeyModuleModuleChangeV1;

#define ValkeyModuleModuleChange ValkeyModuleModuleChangeV1

#define VALKEYMODULE_CONFIGCHANGE_VERSION 1
typedef struct ValkeyModuleConfigChange {
    uint64_t version;          /* Not used since this structure is never passed
                                  from the module to the core right now. Here
                                  for future compatibility. */
    uint32_t num_changes;      /* how many Valkey config options were changed */
    const char **config_names; /* the config names that were changed */
} ValkeyModuleConfigChangeV1;

#define ValkeyModuleConfigChange ValkeyModuleConfigChangeV1

#define VALKEYMODULE_CRON_LOOP_VERSION 1
typedef struct ValkeyModuleCronLoopInfo {
    uint64_t version; /* Not used since this structure is never passed
                         from the module to the core right now. Here
                         for future compatibility. */
    int32_t hz;       /* Approximate number of events per second. */
} ValkeyModuleCronLoopV1;

#define ValkeyModuleCronLoop ValkeyModuleCronLoopV1

#define VALKEYMODULE_LOADING_PROGRESS_VERSION 1
typedef struct ValkeyModuleLoadingProgressInfo {
    uint64_t version; /* Not used since this structure is never passed
                         from the module to the core right now. Here
                         for future compatibility. */
    int32_t hz;       /* Approximate number of events per second. */
    int32_t progress; /* Approximate progress between 0 and 1024, or -1
                       * if unknown. */
} ValkeyModuleLoadingProgressV1;

#define ValkeyModuleLoadingProgress ValkeyModuleLoadingProgressV1

#define VALKEYMODULE_SWAPDBINFO_VERSION 1
typedef struct ValkeyModuleSwapDbInfo {
    uint64_t version;     /* Not used since this structure is never passed
                             from the module to the core right now. Here
                             for future compatibility. */
    int32_t dbnum_first;  /* Swap Db first dbnum */
    int32_t dbnum_second; /* Swap Db second dbnum */
} ValkeyModuleSwapDbInfoV1;

#define ValkeyModuleSwapDbInfo ValkeyModuleSwapDbInfoV1

#define VALKEYMODULE_KEYINFO_VERSION 1
typedef struct ValkeyModuleKeyInfo {
    uint64_t version;     /* Not used since this structure is never passed
                             from the module to the core right now. Here
                             for future compatibility. */
    ValkeyModuleKey *key; /* Opened key. */
} ValkeyModuleKeyInfoV1;

#define ValkeyModuleKeyInfo ValkeyModuleKeyInfoV1

typedef enum {
    VALKEYMODULE_ACL_LOG_AUTH = 0, /* Authentication failure */
    VALKEYMODULE_ACL_LOG_CMD,      /* Command authorization failure */
    VALKEYMODULE_ACL_LOG_KEY,      /* Key authorization failure */
    VALKEYMODULE_ACL_LOG_CHANNEL   /* Channel authorization failure */
} ValkeyModuleACLLogEntryReason;

/* Incomplete structures needed by both the core and modules. */
typedef struct ValkeyModuleCtx ValkeyModuleCtx;
typedef struct ValkeyModuleIO ValkeyModuleIO;
typedef struct ValkeyModuleDigest ValkeyModuleDigest;
typedef struct ValkeyModuleInfoCtx ValkeyModuleInfoCtx;
typedef struct ValkeyModuleDefragCtx ValkeyModuleDefragCtx;

/* Function pointers needed by both the core and modules, these needs to be
 * exposed since you can't cast a function pointer to (void *). */
typedef void (*ValkeyModuleInfoFunc)(ValkeyModuleInfoCtx *ctx, int for_crash_report);
typedef void (*ValkeyModuleDefragFunc)(ValkeyModuleDefragCtx *ctx);
typedef void (*ValkeyModuleUserChangedFunc)(uint64_t client_id, void *privdata);

/* Current ABI version for scripting engine modules. */
#define VALKEYMODULE_SCRIPTING_ENGINE_ABI_VERSION 1UL

/* Type definitions for implementing scripting engines modules. */
typedef void ValkeyModuleScriptingEngineCtx;
typedef void ValkeyModuleScriptingEngineFunctionCtx;

/* This struct represents a scripting engine function that results from the
 * compilation of a script by the engine implementation.
 *
 * IMPORTANT: If we ever need to add/remove fields from this struct, we need
 * to bump the version number defined in the
 * `VALKEYMODULE_SCRIPTING_ENGINE_ABI_VERSION` constant.
 */
typedef struct ValkeyModuleScriptingEngineCompiledFunction {
    ValkeyModuleString *name; /* Function name */
    void *function;           /* Opaque object representing a function, usually it'
                                 the function compiled code. */
    ValkeyModuleString *desc; /* Function description */
    uint64_t f_flags;         /* Function flags */
} ValkeyModuleScriptingEngineCompiledFunction;

/* This struct is used to return the memory information of the scripting
 * engine.
 *
 * IMPORTANT: If we ever need to add/remove fields from this struct, we need
 * to bump the version number defined in the
 * `VALKEYMODULE_SCRIPTING_ENGINE_ABI_VERSION` constant.
 */
typedef struct ValkeyModuleScriptingEngineMemoryInfo {
    /* The memory used by the scripting engine runtime. */
    size_t used_memory;
    /* The memory used by the scripting engine data structures. */
    size_t engine_memory_overhead;
} ValkeyModuleScriptingEngineMemoryInfo;

/* The callback function called when `FUNCTION LOAD` command is called to load
 * a library of functions.
 * This callback function evaluates the source code passed to `FUNCTION LOAD`
 * and registers the functions declared in the source code.
 *
 * - `engine_ctx`: the engine specific context pointer.
 *
 * - `code`: string pointer to the source code.
 *
 * - `timeout`: timeout for the library creation (0 for no timeout).
 *
 * - `out_num_compiled_functions`: out param with the number of objects
 *   returned by this function.
 *
 * - `err` - out param with the description of error (if occurred).
 *
 * Returns an array of compiled function objects, or `NULL` if some error
 * occurred.
 */
typedef ValkeyModuleScriptingEngineCompiledFunction **(*ValkeyModuleScriptingEngineCreateFunctionsLibraryFunc)(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx,
    const char *code,
    size_t timeout,
    size_t *out_num_compiled_functions,
    ValkeyModuleString **err);

/* The callback function called when `FCALL` command is called on a function
 * registered in the scripting engine.
 * This callback function executes the `compiled_function` code.
 *
 * - `module_ctx`: the module runtime context.
 *
 * - `engine_ctx`: the engine specific context pointer.
 *
 * - `func_ctx`: the context opaque structure that represents the runtime
 *               context for the function.
 *
 * - `compiled_function`: pointer to the compiled function registered by the
 *   engine.
 *
 * - `keys`: the array of key strings passed in the `FCALL` command.
 *
 * - `nkeys`: the number of elements present in the `keys` array.
 *
 * - `args`: the array of string arguments passed in the `FCALL` command.
 *
 * - `nargs`: the number of elements present in the `args` array.
 */
typedef void (*ValkeyModuleScriptingEngineCallFunctionFunc)(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx,
    ValkeyModuleScriptingEngineFunctionCtx *func_ctx,
    void *compiled_function,
    ValkeyModuleString **keys,
    size_t nkeys,
    ValkeyModuleString **args,
    size_t nargs);


/* Return memory overhead for a given function, such memory is not counted as
 * engine memory but as general structs memory that hold different information
 */
typedef size_t (*ValkeyModuleScriptingEngineGetFunctionMemoryOverheadFunc)(
    ValkeyModuleCtx *module_ctx,
    void *compiled_function);

/* Free the given function */
typedef void (*ValkeyModuleScriptingEngineFreeFunctionFunc)(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx,
    void *compiled_function);

typedef ValkeyModuleScriptingEngineMemoryInfo (*ValkeyModuleScriptingEngineGetMemoryInfoFunc)(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx);

typedef struct ValkeyModuleScriptingEngineMethodsV1 {
    uint64_t version; /* Version of this structure for ABI compat. */

    /* Library create function callback. When a new script is loaded, this
     * callback will be called with the script code, and returns a list of
     * ValkeyModuleScriptingEngineCompiledFunc objects. */
    ValkeyModuleScriptingEngineCreateFunctionsLibraryFunc create_functions_library;

    /* The callback function called when `FCALL` command is called on a function
     * registered in this engine. */
    ValkeyModuleScriptingEngineCallFunctionFunc call_function;

    /* Function callback to free the memory of a registered engine function. */
    ValkeyModuleScriptingEngineFreeFunctionFunc free_function;

    /* Function callback to return memory overhead for a given function. */
    ValkeyModuleScriptingEngineGetFunctionMemoryOverheadFunc get_function_memory_overhead;

    /* Function callback to get the used memory by the engine. */
    ValkeyModuleScriptingEngineGetMemoryInfoFunc get_memory_info;

} ValkeyModuleScriptingEngineMethodsV1;

#define ValkeyModuleScriptingEngineMethods ValkeyModuleScriptingEngineMethodsV1

/* ------------------------- End of common defines ------------------------ */

/* ----------- The rest of the defines are only for modules ----------------- */
#if !defined VALKEYMODULE_CORE || defined VALKEYMODULE_CORE_MODULE
/* Things defined for modules and core-modules. */

/* Macro definitions specific to individual compilers */
#ifndef VALKEYMODULE_ATTR_UNUSED
#ifdef __GNUC__
#define VALKEYMODULE_ATTR_UNUSED __attribute__((unused))
#else
#define VALKEYMODULE_ATTR_UNUSED
#endif
#endif

#ifndef VALKEYMODULE_ATTR_PRINTF
#ifdef __GNUC__
#define VALKEYMODULE_ATTR_PRINTF(idx, cnt) __attribute__((format(printf, idx, cnt)))
#else
#define VALKEYMODULE_ATTR_PRINTF(idx, cnt)
#endif
#endif

#ifndef VALKEYMODULE_ATTR_COMMON
#if defined(__GNUC__) && !(defined(__clang__) && defined(__cplusplus))
#define VALKEYMODULE_ATTR_COMMON __attribute__((__common__))
#else
#define VALKEYMODULE_ATTR_COMMON
#endif
#endif

/* Incomplete structures for compiler checks but opaque access. */
typedef struct ValkeyModuleCommand ValkeyModuleCommand;
typedef struct ValkeyModuleCallReply ValkeyModuleCallReply;
typedef struct ValkeyModuleType ValkeyModuleType;
typedef struct ValkeyModuleBlockedClient ValkeyModuleBlockedClient;
typedef struct ValkeyModuleClusterInfo ValkeyModuleClusterInfo;
typedef struct ValkeyModuleDict ValkeyModuleDict;
typedef struct ValkeyModuleDictIter ValkeyModuleDictIter;
typedef struct ValkeyModuleCommandFilterCtx ValkeyModuleCommandFilterCtx;
typedef struct ValkeyModuleCommandFilter ValkeyModuleCommandFilter;
typedef struct ValkeyModuleServerInfoData ValkeyModuleServerInfoData;
typedef struct ValkeyModuleScanCursor ValkeyModuleScanCursor;
typedef struct ValkeyModuleUser ValkeyModuleUser;
typedef struct ValkeyModuleKeyOptCtx ValkeyModuleKeyOptCtx;
typedef struct ValkeyModuleRdbStream ValkeyModuleRdbStream;

typedef int (*ValkeyModuleCmdFunc)(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
typedef void (*ValkeyModuleDisconnectFunc)(ValkeyModuleCtx *ctx, ValkeyModuleBlockedClient *bc);
typedef int (*ValkeyModuleNotificationFunc)(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key);
typedef void (*ValkeyModulePostNotificationJobFunc)(ValkeyModuleCtx *ctx, void *pd);
typedef void *(*ValkeyModuleTypeLoadFunc)(ValkeyModuleIO *rdb, int encver);
typedef void (*ValkeyModuleTypeSaveFunc)(ValkeyModuleIO *rdb, void *value);
typedef int (*ValkeyModuleTypeAuxLoadFunc)(ValkeyModuleIO *rdb, int encver, int when);
typedef void (*ValkeyModuleTypeAuxSaveFunc)(ValkeyModuleIO *rdb, int when);
typedef void (*ValkeyModuleTypeRewriteFunc)(ValkeyModuleIO *aof, ValkeyModuleString *key, void *value);
typedef size_t (*ValkeyModuleTypeMemUsageFunc)(const void *value);
typedef size_t (*ValkeyModuleTypeMemUsageFunc2)(ValkeyModuleKeyOptCtx *ctx, const void *value, size_t sample_size);
typedef void (*ValkeyModuleTypeDigestFunc)(ValkeyModuleDigest *digest, void *value);
typedef void (*ValkeyModuleTypeFreeFunc)(void *value);
typedef size_t (*ValkeyModuleTypeFreeEffortFunc)(ValkeyModuleString *key, const void *value);
typedef size_t (*ValkeyModuleTypeFreeEffortFunc2)(ValkeyModuleKeyOptCtx *ctx, const void *value);
typedef void (*ValkeyModuleTypeUnlinkFunc)(ValkeyModuleString *key, const void *value);
typedef void (*ValkeyModuleTypeUnlinkFunc2)(ValkeyModuleKeyOptCtx *ctx, const void *value);
typedef void *(*ValkeyModuleTypeCopyFunc)(ValkeyModuleString *fromkey, ValkeyModuleString *tokey, const void *value);
typedef void *(*ValkeyModuleTypeCopyFunc2)(ValkeyModuleKeyOptCtx *ctx, const void *value);
typedef int (*ValkeyModuleTypeDefragFunc)(ValkeyModuleDefragCtx *ctx, ValkeyModuleString *key, void **value);
typedef void (*ValkeyModuleClusterMessageReceiver)(ValkeyModuleCtx *ctx,
                                                   const char *sender_id,
                                                   uint8_t type,
                                                   const unsigned char *payload,
                                                   uint32_t len);
typedef void (*ValkeyModuleTimerProc)(ValkeyModuleCtx *ctx, void *data);
typedef void (*ValkeyModuleCommandFilterFunc)(ValkeyModuleCommandFilterCtx *filter);
typedef void (*ValkeyModuleForkDoneHandler)(int exitcode, int bysignal, void *user_data);
typedef void (*ValkeyModuleScanCB)(ValkeyModuleCtx *ctx,
                                   ValkeyModuleString *keyname,
                                   ValkeyModuleKey *key,
                                   void *privdata);
typedef void (*ValkeyModuleScanKeyCB)(ValkeyModuleKey *key,
                                      ValkeyModuleString *field,
                                      ValkeyModuleString *value,
                                      void *privdata);
typedef ValkeyModuleString *(*ValkeyModuleConfigGetStringFunc)(const char *name, void *privdata);
typedef long long (*ValkeyModuleConfigGetNumericFunc)(const char *name, void *privdata);
typedef int (*ValkeyModuleConfigGetBoolFunc)(const char *name, void *privdata);
typedef int (*ValkeyModuleConfigGetEnumFunc)(const char *name, void *privdata);
typedef int (*ValkeyModuleConfigSetStringFunc)(const char *name,
                                               ValkeyModuleString *val,
                                               void *privdata,
                                               ValkeyModuleString **err);
typedef int (*ValkeyModuleConfigSetNumericFunc)(const char *name,
                                                long long val,
                                                void *privdata,
                                                ValkeyModuleString **err);
typedef int (*ValkeyModuleConfigSetBoolFunc)(const char *name, int val, void *privdata, ValkeyModuleString **err);
typedef int (*ValkeyModuleConfigSetEnumFunc)(const char *name, int val, void *privdata, ValkeyModuleString **err);
typedef int (*ValkeyModuleConfigApplyFunc)(ValkeyModuleCtx *ctx, void *privdata, ValkeyModuleString **err);
typedef void (*ValkeyModuleOnUnblocked)(ValkeyModuleCtx *ctx, ValkeyModuleCallReply *reply, void *private_data);
typedef int (*ValkeyModuleAuthCallback)(ValkeyModuleCtx *ctx,
                                        ValkeyModuleString *username,
                                        ValkeyModuleString *password,
                                        ValkeyModuleString **err);

typedef struct ValkeyModuleTypeMethods {
    uint64_t version;
    ValkeyModuleTypeLoadFunc rdb_load;
    ValkeyModuleTypeSaveFunc rdb_save;
    ValkeyModuleTypeRewriteFunc aof_rewrite;
    ValkeyModuleTypeMemUsageFunc mem_usage;
    ValkeyModuleTypeDigestFunc digest;
    ValkeyModuleTypeFreeFunc free;
    ValkeyModuleTypeAuxLoadFunc aux_load;
    ValkeyModuleTypeAuxSaveFunc aux_save;
    int aux_save_triggers;
    ValkeyModuleTypeFreeEffortFunc free_effort;
    ValkeyModuleTypeUnlinkFunc unlink;
    ValkeyModuleTypeCopyFunc copy;
    ValkeyModuleTypeDefragFunc defrag;
    ValkeyModuleTypeMemUsageFunc2 mem_usage2;
    ValkeyModuleTypeFreeEffortFunc2 free_effort2;
    ValkeyModuleTypeUnlinkFunc2 unlink2;
    ValkeyModuleTypeCopyFunc2 copy2;
    ValkeyModuleTypeAuxSaveFunc aux_save2;
} ValkeyModuleTypeMethods;

#define VALKEYMODULE_GET_API(name) ValkeyModule_GetApi("ValkeyModule_" #name, ((void **)&ValkeyModule_##name))

/* Default API declaration prefix (not 'extern' for backwards compatibility) */
#ifndef VALKEYMODULE_API
#define VALKEYMODULE_API
#endif

/* Default API declaration suffix (compiler attributes) */
#ifndef VALKEYMODULE_ATTR
#define VALKEYMODULE_ATTR VALKEYMODULE_ATTR_COMMON
#endif

VALKEYMODULE_API void *(*ValkeyModule_Alloc)(size_t bytes)VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_TryAlloc)(size_t bytes)VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_Realloc)(void *ptr, size_t bytes)VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_TryRealloc)(void *ptr, size_t bytes)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_Free)(void *ptr) VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_Calloc)(size_t nmemb, size_t size)VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_TryCalloc)(size_t nmemb, size_t size)VALKEYMODULE_ATTR;
VALKEYMODULE_API char *(*ValkeyModule_Strdup)(const char *str)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetApi)(const char *, void *) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CreateCommand)(ValkeyModuleCtx *ctx,
                                                   const char *name,
                                                   ValkeyModuleCmdFunc cmdfunc,
                                                   const char *strflags,
                                                   int firstkey,
                                                   int lastkey,
                                                   int keystep) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCommand *(*ValkeyModule_GetCommand)(ValkeyModuleCtx *ctx,
                                                                 const char *name)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CreateSubcommand)(ValkeyModuleCommand *parent,
                                                      const char *name,
                                                      ValkeyModuleCmdFunc cmdfunc,
                                                      const char *strflags,
                                                      int firstkey,
                                                      int lastkey,
                                                      int keystep) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetCommandInfo)(ValkeyModuleCommand *command,
                                                    const ValkeyModuleCommandInfo *info) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetCommandACLCategories)(ValkeyModuleCommand *command,
                                                             const char *ctgrsflags) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_AddACLCategory)(ValkeyModuleCtx *ctx, const char *name) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SetModuleAttribs)(ValkeyModuleCtx *ctx, const char *name, int ver, int apiver)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_IsModuleNameBusy)(const char *name) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_WrongArity)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_UpdateRuntimeArgs)(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithLongLong)(ValkeyModuleCtx *ctx, long long ll) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetSelectedDb)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SelectDb)(ValkeyModuleCtx *ctx, int newid) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_KeyExists)(ValkeyModuleCtx *ctx, ValkeyModuleString *keyname) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleKey *(*ValkeyModule_OpenKey)(ValkeyModuleCtx *ctx,
                                                          ValkeyModuleString *keyname,
                                                          int mode)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetOpenKeyModesAll)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_CloseKey)(ValkeyModuleKey *kp) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_KeyType)(ValkeyModuleKey *kp) VALKEYMODULE_ATTR;
VALKEYMODULE_API size_t (*ValkeyModule_ValueLength)(ValkeyModuleKey *kp) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ListPush)(ValkeyModuleKey *kp,
                                              int where,
                                              ValkeyModuleString *ele) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_ListPop)(ValkeyModuleKey *key, int where)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_ListGet)(ValkeyModuleKey *key, long index)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ListSet)(ValkeyModuleKey *key,
                                             long index,
                                             ValkeyModuleString *value) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ListInsert)(ValkeyModuleKey *key,
                                                long index,
                                                ValkeyModuleString *value) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ListDelete)(ValkeyModuleKey *key, long index) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCallReply *(*ValkeyModule_Call)(ValkeyModuleCtx *ctx,
                                                             const char *cmdname,
                                                             const char *fmt,
                                                             ...)VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_CallReplyProto)(ValkeyModuleCallReply *reply, size_t *len)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_FreeCallReply)(ValkeyModuleCallReply *reply) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CallReplyType)(ValkeyModuleCallReply *reply) VALKEYMODULE_ATTR;
VALKEYMODULE_API long long (*ValkeyModule_CallReplyInteger)(ValkeyModuleCallReply *reply) VALKEYMODULE_ATTR;
VALKEYMODULE_API double (*ValkeyModule_CallReplyDouble)(ValkeyModuleCallReply *reply) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CallReplyBool)(ValkeyModuleCallReply *reply) VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_CallReplyBigNumber)(ValkeyModuleCallReply *reply,
                                                                size_t *len)VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_CallReplyVerbatim)(ValkeyModuleCallReply *reply,
                                                               size_t *len,
                                                               const char **format)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCallReply *(*ValkeyModule_CallReplySetElement)(ValkeyModuleCallReply *reply,
                                                                            size_t idx)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CallReplyMapElement)(ValkeyModuleCallReply *reply,
                                                         size_t idx,
                                                         ValkeyModuleCallReply **key,
                                                         ValkeyModuleCallReply **val) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CallReplyAttributeElement)(ValkeyModuleCallReply *reply,
                                                               size_t idx,
                                                               ValkeyModuleCallReply **key,
                                                               ValkeyModuleCallReply **val) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_CallReplyPromiseSetUnblockHandler)(ValkeyModuleCallReply *reply,
                                                                        ValkeyModuleOnUnblocked on_unblock,
                                                                        void *private_data) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CallReplyPromiseAbort)(ValkeyModuleCallReply *reply,
                                                           void **private_data) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCallReply *(*ValkeyModule_CallReplyAttribute)(ValkeyModuleCallReply *reply)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API size_t (*ValkeyModule_CallReplyLength)(ValkeyModuleCallReply *reply) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCallReply *(*ValkeyModule_CallReplyArrayElement)(ValkeyModuleCallReply *reply,
                                                                              size_t idx)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_CreateString)(ValkeyModuleCtx *ctx,
                                                                  const char *ptr,
                                                                  size_t len)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_CreateStringFromLongLong)(ValkeyModuleCtx *ctx,
                                                                              long long ll)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_CreateStringFromULongLong)(ValkeyModuleCtx *ctx,
                                                                               unsigned long long ull)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_CreateStringFromDouble)(ValkeyModuleCtx *ctx,
                                                                            double d)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_CreateStringFromLongDouble)(ValkeyModuleCtx *ctx,
                                                                                long double ld,
                                                                                int humanfriendly)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(
    *ValkeyModule_CreateStringFromString)(ValkeyModuleCtx *ctx, const ValkeyModuleString *str)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(
    *ValkeyModule_CreateStringFromStreamID)(ValkeyModuleCtx *ctx, const ValkeyModuleStreamID *id)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_CreateStringPrintf)(ValkeyModuleCtx *ctx, const char *fmt, ...)
    VALKEYMODULE_ATTR_PRINTF(2, 3) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_FreeString)(ValkeyModuleCtx *ctx, ValkeyModuleString *str) VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_StringPtrLen)(const ValkeyModuleString *str, size_t *len)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithError)(ValkeyModuleCtx *ctx, const char *err) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithErrorFormat)(ValkeyModuleCtx *ctx, const char *fmt, ...) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithSimpleString)(ValkeyModuleCtx *ctx, const char *msg) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithArray)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithMap)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithSet)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithAttribute)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithNullArray)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithEmptyArray)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ReplySetArrayLength)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ReplySetMapLength)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ReplySetSetLength)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ReplySetAttributeLength)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ReplySetPushLength)(ValkeyModuleCtx *ctx, long len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithStringBuffer)(ValkeyModuleCtx *ctx,
                                                           const char *buf,
                                                           size_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithCString)(ValkeyModuleCtx *ctx, const char *buf) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithString)(ValkeyModuleCtx *ctx, ValkeyModuleString *str) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithEmptyString)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithVerbatimString)(ValkeyModuleCtx *ctx,
                                                             const char *buf,
                                                             size_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithVerbatimStringType)(ValkeyModuleCtx *ctx,
                                                                 const char *buf,
                                                                 size_t len,
                                                                 const char *ext) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithNull)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithBool)(ValkeyModuleCtx *ctx, int b) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithLongDouble)(ValkeyModuleCtx *ctx, long double d) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithDouble)(ValkeyModuleCtx *ctx, double d) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithBigNumber)(ValkeyModuleCtx *ctx,
                                                        const char *bignum,
                                                        size_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplyWithCallReply)(ValkeyModuleCtx *ctx,
                                                        ValkeyModuleCallReply *reply) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringToLongLong)(const ValkeyModuleString *str, long long *ll) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringToULongLong)(const ValkeyModuleString *str,
                                                       unsigned long long *ull) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringToDouble)(const ValkeyModuleString *str, double *d) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringToLongDouble)(const ValkeyModuleString *str,
                                                        long double *d) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringToStreamID)(const ValkeyModuleString *str,
                                                      ValkeyModuleStreamID *id) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_AutoMemory)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_Replicate)(ValkeyModuleCtx *ctx, const char *cmdname, const char *fmt, ...)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ReplicateVerbatim)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_CallReplyStringPtr)(ValkeyModuleCallReply *reply,
                                                                size_t *len)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_CreateStringFromCallReply)(ValkeyModuleCallReply *reply)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DeleteKey)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_UnlinkKey)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringSet)(ValkeyModuleKey *key, ValkeyModuleString *str) VALKEYMODULE_ATTR;
VALKEYMODULE_API char *(*ValkeyModule_StringDMA)(ValkeyModuleKey *key, size_t *len, int mode)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringTruncate)(ValkeyModuleKey *key, size_t newlen) VALKEYMODULE_ATTR;
VALKEYMODULE_API mstime_t (*ValkeyModule_GetExpire)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetExpire)(ValkeyModuleKey *key, mstime_t expire) VALKEYMODULE_ATTR;
VALKEYMODULE_API mstime_t (*ValkeyModule_GetAbsExpire)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetAbsExpire)(ValkeyModuleKey *key, mstime_t expire) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ResetDataset)(int restart_aof, int async) VALKEYMODULE_ATTR;
VALKEYMODULE_API unsigned long long (*ValkeyModule_DbSize)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_RandomKey)(ValkeyModuleCtx *ctx)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetAdd)(ValkeyModuleKey *key, double score, ValkeyModuleString *ele, int *flagsptr)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetIncrby)(ValkeyModuleKey *key,
                                                double score,
                                                ValkeyModuleString *ele,
                                                int *flagsptr,
                                                double *newscore) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetScore)(ValkeyModuleKey *key,
                                               ValkeyModuleString *ele,
                                               double *score) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetRem)(ValkeyModuleKey *key,
                                             ValkeyModuleString *ele,
                                             int *deleted) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ZsetRangeStop)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetFirstInScoreRange)(ValkeyModuleKey *key,
                                                           double min,
                                                           double max,
                                                           int minex,
                                                           int maxex) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetLastInScoreRange)(ValkeyModuleKey *key,
                                                          double min,
                                                          double max,
                                                          int minex,
                                                          int maxex) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetFirstInLexRange)(ValkeyModuleKey *key,
                                                         ValkeyModuleString *min,
                                                         ValkeyModuleString *max) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetLastInLexRange)(ValkeyModuleKey *key,
                                                        ValkeyModuleString *min,
                                                        ValkeyModuleString *max) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_ZsetRangeCurrentElement)(ValkeyModuleKey *key,
                                                                             double *score)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetRangeNext)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetRangePrev)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ZsetRangeEndReached)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_HashSet)(ValkeyModuleKey *key, int flags, ...) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_HashGet)(ValkeyModuleKey *key, int flags, ...) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StreamAdd)(ValkeyModuleKey *key,
                                               int flags,
                                               ValkeyModuleStreamID *id,
                                               ValkeyModuleString **argv,
                                               int64_t numfields) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StreamDelete)(ValkeyModuleKey *key, ValkeyModuleStreamID *id) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StreamIteratorStart)(ValkeyModuleKey *key,
                                                         int flags,
                                                         ValkeyModuleStreamID *startid,
                                                         ValkeyModuleStreamID *endid) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StreamIteratorStop)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StreamIteratorNextID)(ValkeyModuleKey *key,
                                                          ValkeyModuleStreamID *id,
                                                          long *numfields) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StreamIteratorNextField)(ValkeyModuleKey *key,
                                                             ValkeyModuleString **field_ptr,
                                                             ValkeyModuleString **value_ptr) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StreamIteratorDelete)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API long long (*ValkeyModule_StreamTrimByLength)(ValkeyModuleKey *key,
                                                              int flags,
                                                              long long length) VALKEYMODULE_ATTR;
VALKEYMODULE_API long long (*ValkeyModule_StreamTrimByID)(ValkeyModuleKey *key,
                                                          int flags,
                                                          ValkeyModuleStreamID *id) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_IsKeysPositionRequest)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_KeyAtPos)(ValkeyModuleCtx *ctx, int pos) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_KeyAtPosWithFlags)(ValkeyModuleCtx *ctx, int pos, int flags) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_IsChannelsPositionRequest)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ChannelAtPosWithFlags)(ValkeyModuleCtx *ctx, int pos, int flags) VALKEYMODULE_ATTR;
VALKEYMODULE_API unsigned long long (*ValkeyModule_GetClientId)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_GetClientUserNameById)(ValkeyModuleCtx *ctx,
                                                                           uint64_t id)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetClientInfoById)(void *ci, uint64_t id) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_GetClientNameById)(ValkeyModuleCtx *ctx,
                                                                       uint64_t id)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetClientNameById)(uint64_t id, ValkeyModuleString *name) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_PublishMessage)(ValkeyModuleCtx *ctx,
                                                    ValkeyModuleString *channel,
                                                    ValkeyModuleString *message) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_PublishMessageShard)(ValkeyModuleCtx *ctx,
                                                         ValkeyModuleString *channel,
                                                         ValkeyModuleString *message) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetContextFlags)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_AvoidReplicaTraffic)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_PoolAlloc)(ValkeyModuleCtx *ctx, size_t bytes)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleType *(*ValkeyModule_CreateDataType)(ValkeyModuleCtx *ctx,
                                                                  const char *name,
                                                                  int encver,
                                                                  ValkeyModuleTypeMethods *typemethods)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ModuleTypeSetValue)(ValkeyModuleKey *key,
                                                        ValkeyModuleType *mt,
                                                        void *value) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ModuleTypeReplaceValue)(ValkeyModuleKey *key,
                                                            ValkeyModuleType *mt,
                                                            void *new_value,
                                                            void **old_value) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleType *(*ValkeyModule_ModuleTypeGetType)(ValkeyModuleKey *key)VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_ModuleTypeGetValue)(ValkeyModuleKey *key)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_IsIOError)(ValkeyModuleIO *io) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SetModuleOptions)(ValkeyModuleCtx *ctx, int options) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SignalModifiedKey)(ValkeyModuleCtx *ctx,
                                                       ValkeyModuleString *keyname) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SaveUnsigned)(ValkeyModuleIO *io, uint64_t value) VALKEYMODULE_ATTR;
VALKEYMODULE_API uint64_t (*ValkeyModule_LoadUnsigned)(ValkeyModuleIO *io) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SaveSigned)(ValkeyModuleIO *io, int64_t value) VALKEYMODULE_ATTR;
VALKEYMODULE_API int64_t (*ValkeyModule_LoadSigned)(ValkeyModuleIO *io) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_EmitAOF)(ValkeyModuleIO *io, const char *cmdname, const char *fmt, ...)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SaveString)(ValkeyModuleIO *io, ValkeyModuleString *s) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SaveStringBuffer)(ValkeyModuleIO *io,
                                                       const char *str,
                                                       size_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_LoadString)(ValkeyModuleIO *io)VALKEYMODULE_ATTR;
VALKEYMODULE_API char *(*ValkeyModule_LoadStringBuffer)(ValkeyModuleIO *io, size_t *lenptr)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SaveDouble)(ValkeyModuleIO *io, double value) VALKEYMODULE_ATTR;
VALKEYMODULE_API double (*ValkeyModule_LoadDouble)(ValkeyModuleIO *io) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SaveFloat)(ValkeyModuleIO *io, float value) VALKEYMODULE_ATTR;
VALKEYMODULE_API float (*ValkeyModule_LoadFloat)(ValkeyModuleIO *io) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SaveLongDouble)(ValkeyModuleIO *io, long double value) VALKEYMODULE_ATTR;
VALKEYMODULE_API long double (*ValkeyModule_LoadLongDouble)(ValkeyModuleIO *io) VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_LoadDataTypeFromString)(const ValkeyModuleString *str,
                                                              const ValkeyModuleType *mt)VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_LoadDataTypeFromStringEncver)(const ValkeyModuleString *str,
                                                                    const ValkeyModuleType *mt,
                                                                    int encver)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_SaveDataTypeToString)(ValkeyModuleCtx *ctx,
                                                                          void *data,
                                                                          const ValkeyModuleType *mt)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_Log)(ValkeyModuleCtx *ctx, const char *level, const char *fmt, ...)
    VALKEYMODULE_ATTR VALKEYMODULE_ATTR_PRINTF(3, 4);
VALKEYMODULE_API void (*ValkeyModule_LogIOError)(ValkeyModuleIO *io, const char *levelstr, const char *fmt, ...)
    VALKEYMODULE_ATTR VALKEYMODULE_ATTR_PRINTF(3, 4);
VALKEYMODULE_API void (*ValkeyModule__Assert)(const char *estr, const char *file, int line) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_LatencyAddSample)(const char *event, mstime_t latency) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringAppendBuffer)(ValkeyModuleCtx *ctx,
                                                        ValkeyModuleString *str,
                                                        const char *buf,
                                                        size_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_TrimStringAllocation)(ValkeyModuleString *str) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_RetainString)(ValkeyModuleCtx *ctx, ValkeyModuleString *str) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_HoldString)(ValkeyModuleCtx *ctx,
                                                                ValkeyModuleString *str)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StringCompare)(const ValkeyModuleString *a,
                                                   const ValkeyModuleString *b) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCtx *(*ValkeyModule_GetContextFromIO)(ValkeyModuleIO *io)VALKEYMODULE_ATTR;
VALKEYMODULE_API const ValkeyModuleString *(*ValkeyModule_GetKeyNameFromIO)(ValkeyModuleIO *io)VALKEYMODULE_ATTR;
VALKEYMODULE_API const ValkeyModuleString *(*ValkeyModule_GetKeyNameFromModuleKey)(ValkeyModuleKey *key)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetDbIdFromModuleKey)(ValkeyModuleKey *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetDbIdFromIO)(ValkeyModuleIO *io) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetDbIdFromOptCtx)(ValkeyModuleKeyOptCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetToDbIdFromOptCtx)(ValkeyModuleKeyOptCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API const ValkeyModuleString *(*ValkeyModule_GetKeyNameFromOptCtx)(ValkeyModuleKeyOptCtx *ctx)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API const ValkeyModuleString *(*ValkeyModule_GetToKeyNameFromOptCtx)(ValkeyModuleKeyOptCtx *ctx)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API mstime_t (*ValkeyModule_Milliseconds)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API uint64_t (*ValkeyModule_MonotonicMicroseconds)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API ustime_t (*ValkeyModule_Microseconds)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API ustime_t (*ValkeyModule_CachedMicroseconds)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_DigestAddStringBuffer)(ValkeyModuleDigest *md,
                                                            const char *ele,
                                                            size_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_DigestAddLongLong)(ValkeyModuleDigest *md, long long ele) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_DigestEndSequence)(ValkeyModuleDigest *md) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetDbIdFromDigest)(ValkeyModuleDigest *dig) VALKEYMODULE_ATTR;
VALKEYMODULE_API const ValkeyModuleString *(*ValkeyModule_GetKeyNameFromDigest)(ValkeyModuleDigest *dig)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleDict *(*ValkeyModule_CreateDict)(ValkeyModuleCtx *ctx)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_FreeDict)(ValkeyModuleCtx *ctx, ValkeyModuleDict *d) VALKEYMODULE_ATTR;
VALKEYMODULE_API uint64_t (*ValkeyModule_DictSize)(ValkeyModuleDict *d) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictSetC)(ValkeyModuleDict *d, void *key, size_t keylen, void *ptr)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictReplaceC)(ValkeyModuleDict *d, void *key, size_t keylen, void *ptr)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictSet)(ValkeyModuleDict *d, ValkeyModuleString *key, void *ptr) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictReplace)(ValkeyModuleDict *d,
                                                 ValkeyModuleString *key,
                                                 void *ptr) VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_DictGetC)(ValkeyModuleDict *d, void *key, size_t keylen, int *nokey)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_DictGet)(ValkeyModuleDict *d,
                                               ValkeyModuleString *key,
                                               int *nokey)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictDelC)(ValkeyModuleDict *d, void *key, size_t keylen, void *oldval)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictDel)(ValkeyModuleDict *d,
                                             ValkeyModuleString *key,
                                             void *oldval) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleDictIter *(*ValkeyModule_DictIteratorStartC)(ValkeyModuleDict *d,
                                                                          const char *op,
                                                                          void *key,
                                                                          size_t keylen)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleDictIter *(*ValkeyModule_DictIteratorStart)(ValkeyModuleDict *d,
                                                                         const char *op,
                                                                         ValkeyModuleString *key)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_DictIteratorStop)(ValkeyModuleDictIter *di) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictIteratorReseekC)(ValkeyModuleDictIter *di,
                                                         const char *op,
                                                         void *key,
                                                         size_t keylen) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictIteratorReseek)(ValkeyModuleDictIter *di,
                                                        const char *op,
                                                        ValkeyModuleString *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_DictNextC)(ValkeyModuleDictIter *di,
                                                 size_t *keylen,
                                                 void **dataptr)VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_DictPrevC)(ValkeyModuleDictIter *di,
                                                 size_t *keylen,
                                                 void **dataptr)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_DictNext)(ValkeyModuleCtx *ctx,
                                                              ValkeyModuleDictIter *di,
                                                              void **dataptr)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_DictPrev)(ValkeyModuleCtx *ctx,
                                                              ValkeyModuleDictIter *di,
                                                              void **dataptr)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictCompareC)(ValkeyModuleDictIter *di, const char *op, void *key, size_t keylen)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DictCompare)(ValkeyModuleDictIter *di,
                                                 const char *op,
                                                 ValkeyModuleString *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RegisterInfoFunc)(ValkeyModuleCtx *ctx, ValkeyModuleInfoFunc cb) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_RegisterAuthCallback)(ValkeyModuleCtx *ctx,
                                                           ValkeyModuleAuthCallback cb) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_InfoAddSection)(ValkeyModuleInfoCtx *ctx, const char *name) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_InfoBeginDictField)(ValkeyModuleInfoCtx *ctx, const char *name) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_InfoEndDictField)(ValkeyModuleInfoCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_InfoAddFieldString)(ValkeyModuleInfoCtx *ctx,
                                                        const char *field,
                                                        ValkeyModuleString *value) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_InfoAddFieldCString)(ValkeyModuleInfoCtx *ctx,
                                                         const char *field,
                                                         const char *value) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_InfoAddFieldDouble)(ValkeyModuleInfoCtx *ctx,
                                                        const char *field,
                                                        double value) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_InfoAddFieldLongLong)(ValkeyModuleInfoCtx *ctx,
                                                          const char *field,
                                                          long long value) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_InfoAddFieldULongLong)(ValkeyModuleInfoCtx *ctx,
                                                           const char *field,
                                                           unsigned long long value) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleServerInfoData *(*ValkeyModule_GetServerInfo)(ValkeyModuleCtx *ctx,
                                                                           const char *section)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_FreeServerInfo)(ValkeyModuleCtx *ctx,
                                                     ValkeyModuleServerInfoData *data) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_ServerInfoGetField)(ValkeyModuleCtx *ctx,
                                                                        ValkeyModuleServerInfoData *data,
                                                                        const char *field)VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_ServerInfoGetFieldC)(ValkeyModuleServerInfoData *data,
                                                                 const char *field)VALKEYMODULE_ATTR;
VALKEYMODULE_API long long (*ValkeyModule_ServerInfoGetFieldSigned)(ValkeyModuleServerInfoData *data,
                                                                    const char *field,
                                                                    int *out_err) VALKEYMODULE_ATTR;
VALKEYMODULE_API unsigned long long (*ValkeyModule_ServerInfoGetFieldUnsigned)(ValkeyModuleServerInfoData *data,
                                                                               const char *field,
                                                                               int *out_err) VALKEYMODULE_ATTR;
VALKEYMODULE_API double (*ValkeyModule_ServerInfoGetFieldDouble)(ValkeyModuleServerInfoData *data,
                                                                 const char *field,
                                                                 int *out_err) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SubscribeToServerEvent)(ValkeyModuleCtx *ctx,
                                                            ValkeyModuleEvent event,
                                                            ValkeyModuleEventCallback callback) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetLRU)(ValkeyModuleKey *key, mstime_t lru_idle) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetLRU)(ValkeyModuleKey *key, mstime_t *lru_idle) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetLFU)(ValkeyModuleKey *key, long long lfu_freq) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetLFU)(ValkeyModuleKey *key, long long *lfu_freq) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleBlockedClient *(*ValkeyModule_BlockClientOnKeys)(ValkeyModuleCtx *ctx,
                                                                              ValkeyModuleCmdFunc reply_callback,
                                                                              ValkeyModuleCmdFunc timeout_callback,
                                                                              void (*free_privdata)(ValkeyModuleCtx *,
                                                                                                    void *),
                                                                              long long timeout_ms,
                                                                              ValkeyModuleString **keys,
                                                                              int numkeys,
                                                                              void *privdata)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleBlockedClient *(*ValkeyModule_BlockClientOnKeysWithFlags)(
    ValkeyModuleCtx *ctx,
    ValkeyModuleCmdFunc reply_callback,
    ValkeyModuleCmdFunc timeout_callback,
    void (*free_privdata)(ValkeyModuleCtx *, void *),
    long long timeout_ms,
    ValkeyModuleString **keys,
    int numkeys,
    void *privdata,
    int flags)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SignalKeyAsReady)(ValkeyModuleCtx *ctx, ValkeyModuleString *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_GetBlockedClientReadyKey)(ValkeyModuleCtx *ctx)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleScanCursor *(*ValkeyModule_ScanCursorCreate)(void)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ScanCursorRestart)(ValkeyModuleScanCursor *cursor) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ScanCursorDestroy)(ValkeyModuleScanCursor *cursor) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_Scan)(ValkeyModuleCtx *ctx,
                                          ValkeyModuleScanCursor *cursor,
                                          ValkeyModuleScanCB fn,
                                          void *privdata) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ScanKey)(ValkeyModuleKey *key,
                                             ValkeyModuleScanCursor *cursor,
                                             ValkeyModuleScanKeyCB fn,
                                             void *privdata) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetContextFlagsAll)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetModuleOptionsAll)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetKeyspaceNotificationFlagsAll)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_IsSubEventSupported)(ValkeyModuleEvent event, uint64_t subevent) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetServerVersion)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetTypeMethodVersion)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_Yield)(ValkeyModuleCtx *ctx, int flags, const char *busy_reply) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleBlockedClient *(*ValkeyModule_BlockClient)(ValkeyModuleCtx *ctx,
                                                                        ValkeyModuleCmdFunc reply_callback,
                                                                        ValkeyModuleCmdFunc timeout_callback,
                                                                        void (*free_privdata)(ValkeyModuleCtx *,
                                                                                              void *),
                                                                        long long timeout_ms)VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_BlockClientGetPrivateData)(ValkeyModuleBlockedClient *blocked_client)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_BlockClientSetPrivateData)(ValkeyModuleBlockedClient *blocked_client,
                                                                void *private_data) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleBlockedClient *(*ValkeyModule_BlockClientOnAuth)(
    ValkeyModuleCtx *ctx,
    ValkeyModuleAuthCallback reply_callback,
    void (*free_privdata)(ValkeyModuleCtx *, void *))VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_UnblockClient)(ValkeyModuleBlockedClient *bc, void *privdata) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_IsBlockedReplyRequest)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_IsBlockedTimeoutRequest)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_GetBlockedClientPrivateData)(ValkeyModuleCtx *ctx)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleBlockedClient *(*ValkeyModule_GetBlockedClientHandle)(ValkeyModuleCtx *ctx)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_AbortBlock)(ValkeyModuleBlockedClient *bc) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_BlockedClientMeasureTimeStart)(ValkeyModuleBlockedClient *bc) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_BlockedClientMeasureTimeEnd)(ValkeyModuleBlockedClient *bc) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCtx *(*ValkeyModule_GetThreadSafeContext)(ValkeyModuleBlockedClient *bc)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCtx *(*ValkeyModule_GetDetachedThreadSafeContext)(ValkeyModuleCtx *ctx)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_FreeThreadSafeContext)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ThreadSafeContextLock)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ThreadSafeContextTryLock)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ThreadSafeContextUnlock)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SubscribeToKeyspaceEvents)(ValkeyModuleCtx *ctx,
                                                               int types,
                                                               ValkeyModuleNotificationFunc cb) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_AddPostNotificationJob)(ValkeyModuleCtx *ctx,
                                                            ValkeyModulePostNotificationJobFunc callback,
                                                            void *pd,
                                                            void (*free_pd)(void *)) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_NotifyKeyspaceEvent)(ValkeyModuleCtx *ctx,
                                                         int type,
                                                         const char *event,
                                                         ValkeyModuleString *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetNotifyKeyspaceEvents)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_BlockedClientDisconnected)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_RegisterClusterMessageReceiver)(ValkeyModuleCtx *ctx,
                                                                     uint8_t type,
                                                                     ValkeyModuleClusterMessageReceiver callback)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SendClusterMessage)(ValkeyModuleCtx *ctx,
                                                        const char *target_id,
                                                        uint8_t type,
                                                        const char *msg,
                                                        uint32_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetClusterNodeInfo)(ValkeyModuleCtx *ctx,
                                                        const char *id,
                                                        char *ip,
                                                        char *primary_id,
                                                        int *port,
                                                        int *flags) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetClusterNodeInfoForClient)(ValkeyModuleCtx *ctx,
                                                                 uint64_t client_id,
                                                                 const char *node_id,
                                                                 char *ip,
                                                                 char *primary_id,
                                                                 int *port,
                                                                 int *flags) VALKEYMODULE_ATTR;
VALKEYMODULE_API char **(*ValkeyModule_GetClusterNodesList)(ValkeyModuleCtx *ctx, size_t *numnodes)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_FreeClusterNodesList)(char **ids) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleTimerID (*ValkeyModule_CreateTimer)(ValkeyModuleCtx *ctx,
                                                                 mstime_t period,
                                                                 ValkeyModuleTimerProc callback,
                                                                 void *data) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_StopTimer)(ValkeyModuleCtx *ctx,
                                               ValkeyModuleTimerID id,
                                               void **data) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetTimerInfo)(ValkeyModuleCtx *ctx,
                                                  ValkeyModuleTimerID id,
                                                  uint64_t *remaining,
                                                  void **data) VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_GetMyClusterID)(void)VALKEYMODULE_ATTR;
VALKEYMODULE_API size_t (*ValkeyModule_GetClusterSize)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_GetRandomBytes)(unsigned char *dst, size_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_GetRandomHexChars)(char *dst, size_t len) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SetDisconnectCallback)(ValkeyModuleBlockedClient *bc,
                                                            ValkeyModuleDisconnectFunc callback) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SetClusterFlags)(ValkeyModuleCtx *ctx, uint64_t flags) VALKEYMODULE_ATTR;
VALKEYMODULE_API unsigned int (*ValkeyModule_ClusterKeySlot)(ValkeyModuleString *key) VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_ClusterCanonicalKeyNameInSlot)(unsigned int slot)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ExportSharedAPI)(ValkeyModuleCtx *ctx,
                                                     const char *apiname,
                                                     void *func) VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_GetSharedAPI)(ValkeyModuleCtx *ctx, const char *apiname)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleCommandFilter *(*ValkeyModule_RegisterCommandFilter)(ValkeyModuleCtx *ctx,
                                                                                  ValkeyModuleCommandFilterFunc cb,
                                                                                  int flags)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_UnregisterCommandFilter)(ValkeyModuleCtx *ctx,
                                                             ValkeyModuleCommandFilter *filter) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CommandFilterArgsCount)(ValkeyModuleCommandFilterCtx *fctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_CommandFilterArgGet)(ValkeyModuleCommandFilterCtx *fctx,
                                                                         int pos)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CommandFilterArgInsert)(ValkeyModuleCommandFilterCtx *fctx,
                                                            int pos,
                                                            ValkeyModuleString *arg) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CommandFilterArgReplace)(ValkeyModuleCommandFilterCtx *fctx,
                                                             int pos,
                                                             ValkeyModuleString *arg) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_CommandFilterArgDelete)(ValkeyModuleCommandFilterCtx *fctx,
                                                            int pos) VALKEYMODULE_ATTR;
VALKEYMODULE_API unsigned long long (*ValkeyModule_CommandFilterGetClientId)(ValkeyModuleCommandFilterCtx *fctx)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_Fork)(ValkeyModuleForkDoneHandler cb, void *user_data) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SendChildHeartbeat)(double progress) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ExitFromChild)(int retcode) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_KillForkChild)(int child_pid) VALKEYMODULE_ATTR;
VALKEYMODULE_API float (*ValkeyModule_GetUsedMemoryRatio)(void) VALKEYMODULE_ATTR;
VALKEYMODULE_API size_t (*ValkeyModule_MallocSize)(void *ptr) VALKEYMODULE_ATTR;
VALKEYMODULE_API size_t (*ValkeyModule_MallocUsableSize)(void *ptr) VALKEYMODULE_ATTR;
VALKEYMODULE_API size_t (*ValkeyModule_MallocSizeString)(ValkeyModuleString *str) VALKEYMODULE_ATTR;
VALKEYMODULE_API size_t (*ValkeyModule_MallocSizeDict)(ValkeyModuleDict *dict) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleUser *(*ValkeyModule_CreateModuleUser)(const char *name)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_FreeModuleUser)(ValkeyModuleUser *user) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_SetContextUser)(ValkeyModuleCtx *ctx,
                                                     const ValkeyModuleUser *user) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetModuleUserACL)(ValkeyModuleUser *user, const char *acl) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_SetModuleUserACLString)(ValkeyModuleCtx *ctx,
                                                            ValkeyModuleUser *user,
                                                            const char *acl,
                                                            ValkeyModuleString **error) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_GetModuleUserACLString)(ValkeyModuleUser *user)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_GetCurrentUserName)(ValkeyModuleCtx *ctx)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleUser *(*ValkeyModule_GetModuleUserFromUserName)(ValkeyModuleString *name)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ACLCheckCommandPermissions)(ValkeyModuleUser *user,
                                                                ValkeyModuleString **argv,
                                                                int argc) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ACLCheckKeyPermissions)(ValkeyModuleUser *user,
                                                            ValkeyModuleString *key,
                                                            int flags) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_ACLCheckChannelPermissions)(ValkeyModuleUser *user,
                                                                ValkeyModuleString *ch,
                                                                int literal) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ACLAddLogEntry)(ValkeyModuleCtx *ctx,
                                                     ValkeyModuleUser *user,
                                                     ValkeyModuleString *object,
                                                     ValkeyModuleACLLogEntryReason reason) VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_ACLAddLogEntryByUserName)(ValkeyModuleCtx *ctx,
                                                               ValkeyModuleString *user,
                                                               ValkeyModuleString *object,
                                                               ValkeyModuleACLLogEntryReason reason) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_AuthenticateClientWithACLUser)(ValkeyModuleCtx *ctx,
                                                                   const char *name,
                                                                   size_t len,
                                                                   ValkeyModuleUserChangedFunc callback,
                                                                   void *privdata,
                                                                   uint64_t *client_id) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_AuthenticateClientWithUser)(ValkeyModuleCtx *ctx,
                                                                ValkeyModuleUser *user,
                                                                ValkeyModuleUserChangedFunc callback,
                                                                void *privdata,
                                                                uint64_t *client_id) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DeauthenticateAndCloseClient)(ValkeyModuleCtx *ctx,
                                                                  uint64_t client_id) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RedactClientCommandArgument)(ValkeyModuleCtx *ctx, int pos) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_GetClientCertificate)(ValkeyModuleCtx *ctx,
                                                                          uint64_t id)VALKEYMODULE_ATTR;
VALKEYMODULE_API int *(*ValkeyModule_GetCommandKeys)(ValkeyModuleCtx *ctx,
                                                     ValkeyModuleString **argv,
                                                     int argc,
                                                     int *num_keys)VALKEYMODULE_ATTR;
VALKEYMODULE_API int *(*ValkeyModule_GetCommandKeysWithFlags)(ValkeyModuleCtx *ctx,
                                                              ValkeyModuleString **argv,
                                                              int argc,
                                                              int *num_keys,
                                                              int **out_flags)VALKEYMODULE_ATTR;
VALKEYMODULE_API const char *(*ValkeyModule_GetCurrentCommandName)(ValkeyModuleCtx *ctx)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RegisterDefragFunc)(ValkeyModuleCtx *ctx,
                                                        ValkeyModuleDefragFunc func) VALKEYMODULE_ATTR;
VALKEYMODULE_API void *(*ValkeyModule_DefragAlloc)(ValkeyModuleDefragCtx *ctx, void *ptr)VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleString *(*ValkeyModule_DefragValkeyModuleString)(ValkeyModuleDefragCtx *ctx,
                                                                              ValkeyModuleString *str)VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DefragShouldStop)(ValkeyModuleDefragCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DefragCursorSet)(ValkeyModuleDefragCtx *ctx,
                                                     unsigned long cursor) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_DefragCursorGet)(ValkeyModuleDefragCtx *ctx,
                                                     unsigned long *cursor) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_GetDbIdFromDefragCtx)(ValkeyModuleDefragCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API const ValkeyModuleString *(*ValkeyModule_GetKeyNameFromDefragCtx)(ValkeyModuleDefragCtx *ctx)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_EventLoopAdd)(int fd, int mask, ValkeyModuleEventLoopFunc func, void *user_data)
    VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_EventLoopDel)(int fd, int mask) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_EventLoopAddOneShot)(ValkeyModuleEventLoopOneShotFunc func,
                                                         void *user_data) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RegisterBoolConfig)(ValkeyModuleCtx *ctx,
                                                        const char *name,
                                                        int default_val,
                                                        unsigned int flags,
                                                        ValkeyModuleConfigGetBoolFunc getfn,
                                                        ValkeyModuleConfigSetBoolFunc setfn,
                                                        ValkeyModuleConfigApplyFunc applyfn,
                                                        void *privdata) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RegisterNumericConfig)(ValkeyModuleCtx *ctx,
                                                           const char *name,
                                                           long long default_val,
                                                           unsigned int flags,
                                                           long long min,
                                                           long long max,
                                                           ValkeyModuleConfigGetNumericFunc getfn,
                                                           ValkeyModuleConfigSetNumericFunc setfn,
                                                           ValkeyModuleConfigApplyFunc applyfn,
                                                           void *privdata) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RegisterStringConfig)(ValkeyModuleCtx *ctx,
                                                          const char *name,
                                                          const char *default_val,
                                                          unsigned int flags,
                                                          ValkeyModuleConfigGetStringFunc getfn,
                                                          ValkeyModuleConfigSetStringFunc setfn,
                                                          ValkeyModuleConfigApplyFunc applyfn,
                                                          void *privdata) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RegisterEnumConfig)(ValkeyModuleCtx *ctx,
                                                        const char *name,
                                                        int default_val,
                                                        unsigned int flags,
                                                        const char **enum_values,
                                                        const int *int_values,
                                                        int num_enum_vals,
                                                        ValkeyModuleConfigGetEnumFunc getfn,
                                                        ValkeyModuleConfigSetEnumFunc setfn,
                                                        ValkeyModuleConfigApplyFunc applyfn,
                                                        void *privdata) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_LoadConfigs)(ValkeyModuleCtx *ctx) VALKEYMODULE_ATTR;
VALKEYMODULE_API ValkeyModuleRdbStream *(*ValkeyModule_RdbStreamCreateFromFile)(const char *filename)VALKEYMODULE_ATTR;
VALKEYMODULE_API void (*ValkeyModule_RdbStreamFree)(ValkeyModuleRdbStream *stream) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RdbLoad)(ValkeyModuleCtx *ctx,
                                             ValkeyModuleRdbStream *stream,
                                             int flags) VALKEYMODULE_ATTR;
VALKEYMODULE_API int (*ValkeyModule_RdbSave)(ValkeyModuleCtx *ctx,
                                             ValkeyModuleRdbStream *stream,
                                             int flags) VALKEYMODULE_ATTR;

VALKEYMODULE_API int (*ValkeyModule_RegisterScriptingEngine)(ValkeyModuleCtx *module_ctx,
                                                             const char *engine_name,
                                                             ValkeyModuleScriptingEngineCtx *engine_ctx,
                                                             ValkeyModuleScriptingEngineMethods *engine_methods) VALKEYMODULE_ATTR;

VALKEYMODULE_API int (*ValkeyModule_UnregisterScriptingEngine)(ValkeyModuleCtx *module_ctx,
                                                               const char *engine_name) VALKEYMODULE_ATTR;

#define ValkeyModule_IsAOFClient(id) ((id) == UINT64_MAX)

/* This is included inline inside each Valkey module. */
static int ValkeyModule_Init(ValkeyModuleCtx *ctx, const char *name, int ver, int apiver) VALKEYMODULE_ATTR_UNUSED;
static int ValkeyModule_Init(ValkeyModuleCtx *ctx, const char *name, int ver, int apiver) {
    void *getapifuncptr = ((void **)ctx)[0];
    ValkeyModule_GetApi = (int (*)(const char *, void *))(unsigned long)getapifuncptr;
    VALKEYMODULE_GET_API(Alloc);
    VALKEYMODULE_GET_API(TryAlloc);
    VALKEYMODULE_GET_API(Calloc);
    VALKEYMODULE_GET_API(TryCalloc);
    VALKEYMODULE_GET_API(Free);
    VALKEYMODULE_GET_API(Realloc);
    VALKEYMODULE_GET_API(TryRealloc);
    VALKEYMODULE_GET_API(Strdup);
    VALKEYMODULE_GET_API(CreateCommand);
    VALKEYMODULE_GET_API(GetCommand);
    VALKEYMODULE_GET_API(CreateSubcommand);
    VALKEYMODULE_GET_API(SetCommandInfo);
    VALKEYMODULE_GET_API(SetCommandACLCategories);
    VALKEYMODULE_GET_API(AddACLCategory);
    VALKEYMODULE_GET_API(SetModuleAttribs);
    VALKEYMODULE_GET_API(IsModuleNameBusy);
    VALKEYMODULE_GET_API(WrongArity);
    VALKEYMODULE_GET_API(UpdateRuntimeArgs);
    VALKEYMODULE_GET_API(ReplyWithLongLong);
    VALKEYMODULE_GET_API(ReplyWithError);
    VALKEYMODULE_GET_API(ReplyWithErrorFormat);
    VALKEYMODULE_GET_API(ReplyWithSimpleString);
    VALKEYMODULE_GET_API(ReplyWithArray);
    VALKEYMODULE_GET_API(ReplyWithMap);
    VALKEYMODULE_GET_API(ReplyWithSet);
    VALKEYMODULE_GET_API(ReplyWithAttribute);
    VALKEYMODULE_GET_API(ReplyWithNullArray);
    VALKEYMODULE_GET_API(ReplyWithEmptyArray);
    VALKEYMODULE_GET_API(ReplySetArrayLength);
    VALKEYMODULE_GET_API(ReplySetMapLength);
    VALKEYMODULE_GET_API(ReplySetSetLength);
    VALKEYMODULE_GET_API(ReplySetAttributeLength);
    VALKEYMODULE_GET_API(ReplySetPushLength);
    VALKEYMODULE_GET_API(ReplyWithStringBuffer);
    VALKEYMODULE_GET_API(ReplyWithCString);
    VALKEYMODULE_GET_API(ReplyWithString);
    VALKEYMODULE_GET_API(ReplyWithEmptyString);
    VALKEYMODULE_GET_API(ReplyWithVerbatimString);
    VALKEYMODULE_GET_API(ReplyWithVerbatimStringType);
    VALKEYMODULE_GET_API(ReplyWithNull);
    VALKEYMODULE_GET_API(ReplyWithBool);
    VALKEYMODULE_GET_API(ReplyWithCallReply);
    VALKEYMODULE_GET_API(ReplyWithDouble);
    VALKEYMODULE_GET_API(ReplyWithBigNumber);
    VALKEYMODULE_GET_API(ReplyWithLongDouble);
    VALKEYMODULE_GET_API(GetSelectedDb);
    VALKEYMODULE_GET_API(SelectDb);
    VALKEYMODULE_GET_API(KeyExists);
    VALKEYMODULE_GET_API(OpenKey);
    VALKEYMODULE_GET_API(GetOpenKeyModesAll);
    VALKEYMODULE_GET_API(CloseKey);
    VALKEYMODULE_GET_API(KeyType);
    VALKEYMODULE_GET_API(ValueLength);
    VALKEYMODULE_GET_API(ListPush);
    VALKEYMODULE_GET_API(ListPop);
    VALKEYMODULE_GET_API(ListGet);
    VALKEYMODULE_GET_API(ListSet);
    VALKEYMODULE_GET_API(ListInsert);
    VALKEYMODULE_GET_API(ListDelete);
    VALKEYMODULE_GET_API(StringToLongLong);
    VALKEYMODULE_GET_API(StringToULongLong);
    VALKEYMODULE_GET_API(StringToDouble);
    VALKEYMODULE_GET_API(StringToLongDouble);
    VALKEYMODULE_GET_API(StringToStreamID);
    VALKEYMODULE_GET_API(Call);
    VALKEYMODULE_GET_API(CallReplyProto);
    VALKEYMODULE_GET_API(FreeCallReply);
    VALKEYMODULE_GET_API(CallReplyInteger);
    VALKEYMODULE_GET_API(CallReplyDouble);
    VALKEYMODULE_GET_API(CallReplyBool);
    VALKEYMODULE_GET_API(CallReplyBigNumber);
    VALKEYMODULE_GET_API(CallReplyVerbatim);
    VALKEYMODULE_GET_API(CallReplySetElement);
    VALKEYMODULE_GET_API(CallReplyMapElement);
    VALKEYMODULE_GET_API(CallReplyAttributeElement);
    VALKEYMODULE_GET_API(CallReplyPromiseSetUnblockHandler);
    VALKEYMODULE_GET_API(CallReplyPromiseAbort);
    VALKEYMODULE_GET_API(CallReplyAttribute);
    VALKEYMODULE_GET_API(CallReplyType);
    VALKEYMODULE_GET_API(CallReplyLength);
    VALKEYMODULE_GET_API(CallReplyArrayElement);
    VALKEYMODULE_GET_API(CallReplyStringPtr);
    VALKEYMODULE_GET_API(CreateStringFromCallReply);
    VALKEYMODULE_GET_API(CreateString);
    VALKEYMODULE_GET_API(CreateStringFromLongLong);
    VALKEYMODULE_GET_API(CreateStringFromULongLong);
    VALKEYMODULE_GET_API(CreateStringFromDouble);
    VALKEYMODULE_GET_API(CreateStringFromLongDouble);
    VALKEYMODULE_GET_API(CreateStringFromString);
    VALKEYMODULE_GET_API(CreateStringFromStreamID);
    VALKEYMODULE_GET_API(CreateStringPrintf);
    VALKEYMODULE_GET_API(FreeString);
    VALKEYMODULE_GET_API(StringPtrLen);
    VALKEYMODULE_GET_API(AutoMemory);
    VALKEYMODULE_GET_API(Replicate);
    VALKEYMODULE_GET_API(ReplicateVerbatim);
    VALKEYMODULE_GET_API(DeleteKey);
    VALKEYMODULE_GET_API(UnlinkKey);
    VALKEYMODULE_GET_API(StringSet);
    VALKEYMODULE_GET_API(StringDMA);
    VALKEYMODULE_GET_API(StringTruncate);
    VALKEYMODULE_GET_API(GetExpire);
    VALKEYMODULE_GET_API(SetExpire);
    VALKEYMODULE_GET_API(GetAbsExpire);
    VALKEYMODULE_GET_API(SetAbsExpire);
    VALKEYMODULE_GET_API(ResetDataset);
    VALKEYMODULE_GET_API(DbSize);
    VALKEYMODULE_GET_API(RandomKey);
    VALKEYMODULE_GET_API(ZsetAdd);
    VALKEYMODULE_GET_API(ZsetIncrby);
    VALKEYMODULE_GET_API(ZsetScore);
    VALKEYMODULE_GET_API(ZsetRem);
    VALKEYMODULE_GET_API(ZsetRangeStop);
    VALKEYMODULE_GET_API(ZsetFirstInScoreRange);
    VALKEYMODULE_GET_API(ZsetLastInScoreRange);
    VALKEYMODULE_GET_API(ZsetFirstInLexRange);
    VALKEYMODULE_GET_API(ZsetLastInLexRange);
    VALKEYMODULE_GET_API(ZsetRangeCurrentElement);
    VALKEYMODULE_GET_API(ZsetRangeNext);
    VALKEYMODULE_GET_API(ZsetRangePrev);
    VALKEYMODULE_GET_API(ZsetRangeEndReached);
    VALKEYMODULE_GET_API(HashSet);
    VALKEYMODULE_GET_API(HashGet);
    VALKEYMODULE_GET_API(StreamAdd);
    VALKEYMODULE_GET_API(StreamDelete);
    VALKEYMODULE_GET_API(StreamIteratorStart);
    VALKEYMODULE_GET_API(StreamIteratorStop);
    VALKEYMODULE_GET_API(StreamIteratorNextID);
    VALKEYMODULE_GET_API(StreamIteratorNextField);
    VALKEYMODULE_GET_API(StreamIteratorDelete);
    VALKEYMODULE_GET_API(StreamTrimByLength);
    VALKEYMODULE_GET_API(StreamTrimByID);
    VALKEYMODULE_GET_API(IsKeysPositionRequest);
    VALKEYMODULE_GET_API(KeyAtPos);
    VALKEYMODULE_GET_API(KeyAtPosWithFlags);
    VALKEYMODULE_GET_API(IsChannelsPositionRequest);
    VALKEYMODULE_GET_API(ChannelAtPosWithFlags);
    VALKEYMODULE_GET_API(GetClientId);
    VALKEYMODULE_GET_API(GetClientUserNameById);
    VALKEYMODULE_GET_API(GetContextFlags);
    VALKEYMODULE_GET_API(AvoidReplicaTraffic);
    VALKEYMODULE_GET_API(PoolAlloc);
    VALKEYMODULE_GET_API(CreateDataType);
    VALKEYMODULE_GET_API(ModuleTypeSetValue);
    VALKEYMODULE_GET_API(ModuleTypeReplaceValue);
    VALKEYMODULE_GET_API(ModuleTypeGetType);
    VALKEYMODULE_GET_API(ModuleTypeGetValue);
    VALKEYMODULE_GET_API(IsIOError);
    VALKEYMODULE_GET_API(SetModuleOptions);
    VALKEYMODULE_GET_API(SignalModifiedKey);
    VALKEYMODULE_GET_API(SaveUnsigned);
    VALKEYMODULE_GET_API(LoadUnsigned);
    VALKEYMODULE_GET_API(SaveSigned);
    VALKEYMODULE_GET_API(LoadSigned);
    VALKEYMODULE_GET_API(SaveString);
    VALKEYMODULE_GET_API(SaveStringBuffer);
    VALKEYMODULE_GET_API(LoadString);
    VALKEYMODULE_GET_API(LoadStringBuffer);
    VALKEYMODULE_GET_API(SaveDouble);
    VALKEYMODULE_GET_API(LoadDouble);
    VALKEYMODULE_GET_API(SaveFloat);
    VALKEYMODULE_GET_API(LoadFloat);
    VALKEYMODULE_GET_API(SaveLongDouble);
    VALKEYMODULE_GET_API(LoadLongDouble);
    VALKEYMODULE_GET_API(SaveDataTypeToString);
    VALKEYMODULE_GET_API(LoadDataTypeFromString);
    VALKEYMODULE_GET_API(LoadDataTypeFromStringEncver);
    VALKEYMODULE_GET_API(EmitAOF);
    VALKEYMODULE_GET_API(Log);
    VALKEYMODULE_GET_API(LogIOError);
    VALKEYMODULE_GET_API(_Assert);
    VALKEYMODULE_GET_API(LatencyAddSample);
    VALKEYMODULE_GET_API(StringAppendBuffer);
    VALKEYMODULE_GET_API(TrimStringAllocation);
    VALKEYMODULE_GET_API(RetainString);
    VALKEYMODULE_GET_API(HoldString);
    VALKEYMODULE_GET_API(StringCompare);
    VALKEYMODULE_GET_API(GetContextFromIO);
    VALKEYMODULE_GET_API(GetKeyNameFromIO);
    VALKEYMODULE_GET_API(GetKeyNameFromModuleKey);
    VALKEYMODULE_GET_API(GetDbIdFromModuleKey);
    VALKEYMODULE_GET_API(GetDbIdFromIO);
    VALKEYMODULE_GET_API(GetKeyNameFromOptCtx);
    VALKEYMODULE_GET_API(GetToKeyNameFromOptCtx);
    VALKEYMODULE_GET_API(GetDbIdFromOptCtx);
    VALKEYMODULE_GET_API(GetToDbIdFromOptCtx);
    VALKEYMODULE_GET_API(Milliseconds);
    VALKEYMODULE_GET_API(MonotonicMicroseconds);
    VALKEYMODULE_GET_API(Microseconds);
    VALKEYMODULE_GET_API(CachedMicroseconds);
    VALKEYMODULE_GET_API(DigestAddStringBuffer);
    VALKEYMODULE_GET_API(DigestAddLongLong);
    VALKEYMODULE_GET_API(DigestEndSequence);
    VALKEYMODULE_GET_API(GetKeyNameFromDigest);
    VALKEYMODULE_GET_API(GetDbIdFromDigest);
    VALKEYMODULE_GET_API(CreateDict);
    VALKEYMODULE_GET_API(FreeDict);
    VALKEYMODULE_GET_API(DictSize);
    VALKEYMODULE_GET_API(DictSetC);
    VALKEYMODULE_GET_API(DictReplaceC);
    VALKEYMODULE_GET_API(DictSet);
    VALKEYMODULE_GET_API(DictReplace);
    VALKEYMODULE_GET_API(DictGetC);
    VALKEYMODULE_GET_API(DictGet);
    VALKEYMODULE_GET_API(DictDelC);
    VALKEYMODULE_GET_API(DictDel);
    VALKEYMODULE_GET_API(DictIteratorStartC);
    VALKEYMODULE_GET_API(DictIteratorStart);
    VALKEYMODULE_GET_API(DictIteratorStop);
    VALKEYMODULE_GET_API(DictIteratorReseekC);
    VALKEYMODULE_GET_API(DictIteratorReseek);
    VALKEYMODULE_GET_API(DictNextC);
    VALKEYMODULE_GET_API(DictPrevC);
    VALKEYMODULE_GET_API(DictNext);
    VALKEYMODULE_GET_API(DictPrev);
    VALKEYMODULE_GET_API(DictCompare);
    VALKEYMODULE_GET_API(DictCompareC);
    VALKEYMODULE_GET_API(RegisterInfoFunc);
    VALKEYMODULE_GET_API(RegisterAuthCallback);
    VALKEYMODULE_GET_API(InfoAddSection);
    VALKEYMODULE_GET_API(InfoBeginDictField);
    VALKEYMODULE_GET_API(InfoEndDictField);
    VALKEYMODULE_GET_API(InfoAddFieldString);
    VALKEYMODULE_GET_API(InfoAddFieldCString);
    VALKEYMODULE_GET_API(InfoAddFieldDouble);
    VALKEYMODULE_GET_API(InfoAddFieldLongLong);
    VALKEYMODULE_GET_API(InfoAddFieldULongLong);
    VALKEYMODULE_GET_API(GetServerInfo);
    VALKEYMODULE_GET_API(FreeServerInfo);
    VALKEYMODULE_GET_API(ServerInfoGetField);
    VALKEYMODULE_GET_API(ServerInfoGetFieldC);
    VALKEYMODULE_GET_API(ServerInfoGetFieldSigned);
    VALKEYMODULE_GET_API(ServerInfoGetFieldUnsigned);
    VALKEYMODULE_GET_API(ServerInfoGetFieldDouble);
    VALKEYMODULE_GET_API(GetClientInfoById);
    VALKEYMODULE_GET_API(GetClientNameById);
    VALKEYMODULE_GET_API(SetClientNameById);
    VALKEYMODULE_GET_API(PublishMessage);
    VALKEYMODULE_GET_API(PublishMessageShard);
    VALKEYMODULE_GET_API(SubscribeToServerEvent);
    VALKEYMODULE_GET_API(SetLRU);
    VALKEYMODULE_GET_API(GetLRU);
    VALKEYMODULE_GET_API(SetLFU);
    VALKEYMODULE_GET_API(GetLFU);
    VALKEYMODULE_GET_API(BlockClientOnKeys);
    VALKEYMODULE_GET_API(BlockClientOnKeysWithFlags);
    VALKEYMODULE_GET_API(SignalKeyAsReady);
    VALKEYMODULE_GET_API(GetBlockedClientReadyKey);
    VALKEYMODULE_GET_API(ScanCursorCreate);
    VALKEYMODULE_GET_API(ScanCursorRestart);
    VALKEYMODULE_GET_API(ScanCursorDestroy);
    VALKEYMODULE_GET_API(Scan);
    VALKEYMODULE_GET_API(ScanKey);
    VALKEYMODULE_GET_API(GetContextFlagsAll);
    VALKEYMODULE_GET_API(GetModuleOptionsAll);
    VALKEYMODULE_GET_API(GetKeyspaceNotificationFlagsAll);
    VALKEYMODULE_GET_API(IsSubEventSupported);
    VALKEYMODULE_GET_API(GetServerVersion);
    VALKEYMODULE_GET_API(GetTypeMethodVersion);
    VALKEYMODULE_GET_API(Yield);
    VALKEYMODULE_GET_API(GetThreadSafeContext);
    VALKEYMODULE_GET_API(GetDetachedThreadSafeContext);
    VALKEYMODULE_GET_API(FreeThreadSafeContext);
    VALKEYMODULE_GET_API(ThreadSafeContextLock);
    VALKEYMODULE_GET_API(ThreadSafeContextTryLock);
    VALKEYMODULE_GET_API(ThreadSafeContextUnlock);
    VALKEYMODULE_GET_API(BlockClient);
    VALKEYMODULE_GET_API(BlockClientGetPrivateData);
    VALKEYMODULE_GET_API(BlockClientSetPrivateData);
    VALKEYMODULE_GET_API(BlockClientOnAuth);
    VALKEYMODULE_GET_API(UnblockClient);
    VALKEYMODULE_GET_API(IsBlockedReplyRequest);
    VALKEYMODULE_GET_API(IsBlockedTimeoutRequest);
    VALKEYMODULE_GET_API(GetBlockedClientPrivateData);
    VALKEYMODULE_GET_API(GetBlockedClientHandle);
    VALKEYMODULE_GET_API(AbortBlock);
    VALKEYMODULE_GET_API(BlockedClientMeasureTimeStart);
    VALKEYMODULE_GET_API(BlockedClientMeasureTimeEnd);
    VALKEYMODULE_GET_API(SetDisconnectCallback);
    VALKEYMODULE_GET_API(SubscribeToKeyspaceEvents);
    VALKEYMODULE_GET_API(AddPostNotificationJob);
    VALKEYMODULE_GET_API(NotifyKeyspaceEvent);
    VALKEYMODULE_GET_API(GetNotifyKeyspaceEvents);
    VALKEYMODULE_GET_API(BlockedClientDisconnected);
    VALKEYMODULE_GET_API(RegisterClusterMessageReceiver);
    VALKEYMODULE_GET_API(SendClusterMessage);
    VALKEYMODULE_GET_API(GetClusterNodeInfo);
    VALKEYMODULE_GET_API(GetClusterNodeInfoForClient);
    VALKEYMODULE_GET_API(GetClusterNodesList);
    VALKEYMODULE_GET_API(FreeClusterNodesList);
    VALKEYMODULE_GET_API(CreateTimer);
    VALKEYMODULE_GET_API(StopTimer);
    VALKEYMODULE_GET_API(GetTimerInfo);
    VALKEYMODULE_GET_API(GetMyClusterID);
    VALKEYMODULE_GET_API(GetClusterSize);
    VALKEYMODULE_GET_API(GetRandomBytes);
    VALKEYMODULE_GET_API(GetRandomHexChars);
    VALKEYMODULE_GET_API(SetClusterFlags);
    VALKEYMODULE_GET_API(ClusterKeySlot);
    VALKEYMODULE_GET_API(ClusterCanonicalKeyNameInSlot);
    VALKEYMODULE_GET_API(ExportSharedAPI);
    VALKEYMODULE_GET_API(GetSharedAPI);
    VALKEYMODULE_GET_API(RegisterCommandFilter);
    VALKEYMODULE_GET_API(UnregisterCommandFilter);
    VALKEYMODULE_GET_API(CommandFilterArgsCount);
    VALKEYMODULE_GET_API(CommandFilterArgGet);
    VALKEYMODULE_GET_API(CommandFilterArgInsert);
    VALKEYMODULE_GET_API(CommandFilterArgReplace);
    VALKEYMODULE_GET_API(CommandFilterArgDelete);
    VALKEYMODULE_GET_API(CommandFilterGetClientId);
    VALKEYMODULE_GET_API(Fork);
    VALKEYMODULE_GET_API(SendChildHeartbeat);
    VALKEYMODULE_GET_API(ExitFromChild);
    VALKEYMODULE_GET_API(KillForkChild);
    VALKEYMODULE_GET_API(GetUsedMemoryRatio);
    VALKEYMODULE_GET_API(MallocSize);
    VALKEYMODULE_GET_API(MallocUsableSize);
    VALKEYMODULE_GET_API(MallocSizeString);
    VALKEYMODULE_GET_API(MallocSizeDict);
    VALKEYMODULE_GET_API(CreateModuleUser);
    VALKEYMODULE_GET_API(FreeModuleUser);
    VALKEYMODULE_GET_API(SetContextUser);
    VALKEYMODULE_GET_API(SetModuleUserACL);
    VALKEYMODULE_GET_API(SetModuleUserACLString);
    VALKEYMODULE_GET_API(GetModuleUserACLString);
    VALKEYMODULE_GET_API(GetCurrentUserName);
    VALKEYMODULE_GET_API(GetModuleUserFromUserName);
    VALKEYMODULE_GET_API(ACLCheckCommandPermissions);
    VALKEYMODULE_GET_API(ACLCheckKeyPermissions);
    VALKEYMODULE_GET_API(ACLCheckChannelPermissions);
    VALKEYMODULE_GET_API(ACLAddLogEntry);
    VALKEYMODULE_GET_API(ACLAddLogEntryByUserName);
    VALKEYMODULE_GET_API(DeauthenticateAndCloseClient);
    VALKEYMODULE_GET_API(AuthenticateClientWithACLUser);
    VALKEYMODULE_GET_API(AuthenticateClientWithUser);
    VALKEYMODULE_GET_API(RedactClientCommandArgument);
    VALKEYMODULE_GET_API(GetClientCertificate);
    VALKEYMODULE_GET_API(GetCommandKeys);
    VALKEYMODULE_GET_API(GetCommandKeysWithFlags);
    VALKEYMODULE_GET_API(GetCurrentCommandName);
    VALKEYMODULE_GET_API(RegisterDefragFunc);
    VALKEYMODULE_GET_API(DefragAlloc);
    VALKEYMODULE_GET_API(DefragValkeyModuleString);
    VALKEYMODULE_GET_API(DefragShouldStop);
    VALKEYMODULE_GET_API(DefragCursorSet);
    VALKEYMODULE_GET_API(DefragCursorGet);
    VALKEYMODULE_GET_API(GetKeyNameFromDefragCtx);
    VALKEYMODULE_GET_API(GetDbIdFromDefragCtx);
    VALKEYMODULE_GET_API(EventLoopAdd);
    VALKEYMODULE_GET_API(EventLoopDel);
    VALKEYMODULE_GET_API(EventLoopAddOneShot);
    VALKEYMODULE_GET_API(RegisterBoolConfig);
    VALKEYMODULE_GET_API(RegisterNumericConfig);
    VALKEYMODULE_GET_API(RegisterStringConfig);
    VALKEYMODULE_GET_API(RegisterEnumConfig);
    VALKEYMODULE_GET_API(LoadConfigs);
    VALKEYMODULE_GET_API(RdbStreamCreateFromFile);
    VALKEYMODULE_GET_API(RdbStreamFree);
    VALKEYMODULE_GET_API(RdbLoad);
    VALKEYMODULE_GET_API(RdbSave);
    VALKEYMODULE_GET_API(RegisterScriptingEngine);
    VALKEYMODULE_GET_API(UnregisterScriptingEngine);

    if (ValkeyModule_IsModuleNameBusy && ValkeyModule_IsModuleNameBusy(name)) return VALKEYMODULE_ERR;
    ValkeyModule_SetModuleAttribs(ctx, name, ver, apiver);
    return VALKEYMODULE_OK;
}

#define ValkeyModule_Assert(_e) ((_e) ? (void)0 : (ValkeyModule__Assert(#_e, __FILE__, __LINE__), exit(1)))

#define RMAPI_FUNC_SUPPORTED(func) (func != NULL)

#endif /* VALKEYMODULE_CORE */
#endif /* VALKEYMODULE_H */
