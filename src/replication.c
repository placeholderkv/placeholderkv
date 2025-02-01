/* Asynchronous replication implementation.
 *
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


#include "server.h"
#include "cluster.h"
#include "cluster_slot_stats.h"
#include "bio.h"
#include "functions.h"
#include "connection.h"
#include "module.h"

#include <memory.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <ctype.h>

void replicationDiscardCachedPrimary(void);
void replicationResurrectCachedPrimary(connection *conn);
void replicationResurrectProvisionalPrimary(void);
void replicationSendAck(void);
int replicaPutOnline(client *replica);
void replicaStartCommandStream(client *replica);
int cancelReplicationHandshake(int reconnect);
void replicationSteadyStateInit(void);
void dualChannelSetupMainConnForPsync(connection *conn);
void dualChannelSyncHandleRdbLoadCompletion(void);
static void dualChannelFullSyncWithPrimary(connection *conn);

/* We take a global flag to remember if this instance generated an RDB
 * because of replication, so that we can remove the RDB file in case
 * the instance is configured to have no persistence. */
int RDBGeneratedByReplication = 0;

/* --------------------------- Utility functions ---------------------------- */
static ConnectionType *connTypeOfReplication(void) {
    if (server.tls_replication) {
        return connectionTypeTls();
    }

    return connectionTypeTcp();
}

/* Return the pointer to a string representing the replica ip:listening_port
 * pair. Mostly useful for logging, since we want to log a replica using its
 * IP address and its listening port which is more clear for the user, for
 * example: "Closing connection with replica 10.1.2.3:6380". */
char *replicationGetReplicaName(client *c) {
    static char buf[NET_HOST_PORT_STR_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';
    if (c->repl_data->replica_addr || connAddrPeerName(c->conn, ip, sizeof(ip), NULL) != -1) {
        char *addr = c->repl_data->replica_addr ? c->repl_data->replica_addr : ip;
        if (c->repl_data->replica_listening_port)
            formatAddr(buf, sizeof(buf), addr, c->repl_data->replica_listening_port);
        else
            snprintf(buf, sizeof(buf), "%s:<unknown-replica-port>", addr);
    } else {
        snprintf(buf, sizeof(buf), "client id #%llu", (unsigned long long)c->id);
    }
    return buf;
}

/* Plain unlink() can block for quite some time in order to actually apply
 * the file deletion to the filesystem. This call removes the file in a
 * background thread instead. We actually just do close() in the thread,
 * by using the fact that if there is another instance of the same file open,
 * the foreground unlink() will only remove the fs name, and deleting the
 * file's storage space will only happen once the last reference is lost. */
int bg_unlink(const char *filename) {
    int fd = open(filename, O_RDONLY | O_NONBLOCK);
    if (fd == -1) {
        /* Can't open the file? Fall back to unlinking in the main thread. */
        return unlink(filename);
    } else {
        /* The following unlink() removes the name but doesn't free the
         * file contents because a process still has it open. */
        int retval = unlink(filename);
        if (retval == -1) {
            /* If we got an unlink error, we just return it, closing the
             * new reference we have to the file. */
            int old_errno = errno;
            close(fd); /* This would overwrite our errno. So we saved it. */
            errno = old_errno;
            return -1;
        }
        bioCreateCloseJob(fd, 0, 0);
        return 0; /* Success. */
    }
}

/* ---------------------------------- PRIMARY -------------------------------- */

void createReplicationBacklog(void) {
    serverAssert(server.repl_backlog == NULL);
    server.repl_backlog = zmalloc(sizeof(replBacklog));
    server.repl_backlog->ref_repl_buf_node = NULL;
    server.repl_backlog->unindexed_count = 0;
    server.repl_backlog->blocks_index = raxNew();
    server.repl_backlog->histlen = 0;
    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream. */
    server.repl_backlog->offset = server.primary_repl_offset + 1;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to resize the buffer and setup it
 * so that it contains the same data as the previous one (possibly less data,
 * but the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
void resizeReplicationBacklog(void) {
    if (server.repl_backlog_size < CONFIG_REPL_BACKLOG_MIN_SIZE)
        server.repl_backlog_size = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog) incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
}

void freeReplicationBacklog(void) {
    serverAssert(listLength(server.replicas) == 0);
    if (server.repl_backlog == NULL) return;

    /* Decrease the start buffer node reference count. */
    if (server.repl_backlog->ref_repl_buf_node) {
        replBufBlock *o = listNodeValue(server.repl_backlog->ref_repl_buf_node);
        serverAssert(o->refcount == 1); /* Last reference. */
        o->refcount--;
    }

    /* Replication buffer blocks are completely released when we free the
     * backlog, since the backlog is released only when there are no replicas
     * and the backlog keeps the last reference of all blocks. */
    freeReplicationBacklogRefMemAsync(server.repl_buffer_blocks, server.repl_backlog->blocks_index);
    resetReplicationBuffer();
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/* To make search offset from replication buffer blocks quickly
 * when replicas ask partial resynchronization, we create one index
 * block every REPL_BACKLOG_INDEX_PER_BLOCKS blocks. */
void createReplicationBacklogIndex(listNode *ln) {
    server.repl_backlog->unindexed_count++;
    if (server.repl_backlog->unindexed_count >= REPL_BACKLOG_INDEX_PER_BLOCKS) {
        replBufBlock *o = listNodeValue(ln);
        uint64_t encoded_offset = htonu64(o->repl_offset);
        raxInsert(server.repl_backlog->blocks_index, (unsigned char *)&encoded_offset, sizeof(uint64_t), ln, NULL);
        server.repl_backlog->unindexed_count = 0;
    }
}

/* Rebase replication buffer blocks' offset since the initial
 * setting offset starts from 0 when primary restart. */
void rebaseReplicationBuffer(long long base_repl_offset) {
    raxFree(server.repl_backlog->blocks_index);
    server.repl_backlog->blocks_index = raxNew();
    server.repl_backlog->unindexed_count = 0;

    listIter li;
    listNode *ln;
    listRewind(server.repl_buffer_blocks, &li);
    while ((ln = listNext(&li))) {
        replBufBlock *o = listNodeValue(ln);
        o->repl_offset += base_repl_offset;
        createReplicationBacklogIndex(ln);
    }
}

/* Return a client by ID, or NULL if the client ID is not in the set
 * of replicas waiting psync clients. */
static inline client *lookupRdbClientByID(uint64_t id) {
    id = htonu64(id);
    void *c = NULL;
    raxFind(server.replicas_waiting_psync, (unsigned char *)&id, sizeof(id), &c);
    return c;
}

/* Replication: Primary side - connections association.
 * During dual channel sync, association is used to keep replication data
 * in the backlog until the replica requests PSYNC.
 * Association occurs in two forms:
 * 1. If there's an existing buffer block at fork time, the replica is attached to the tail.
 * 2. If there's no tail, the replica is attached when a new buffer block is created
 *    (see the Retrospect function below).
 * The replica RDB client ID is used as a unique key for this association.
 * If a COB overrun occurs, the association is deleted and the RDB connection is dropped. */
void addRdbReplicaToPsyncWait(client *replica_rdb_client) {
    listNode *ln = NULL;
    replBufBlock *tail = NULL;
    if (server.repl_backlog == NULL) {
        createReplicationBacklog();
    } else {
        ln = listLast(server.repl_buffer_blocks);
        tail = ln ? listNodeValue(ln) : NULL;
        if (tail) {
            tail->refcount++;
        }
    }
    dualChannelServerLog(LL_DEBUG, "Add rdb replica %s to waiting psync, with cid %llu, %s ",
                         replicationGetReplicaName(replica_rdb_client), (unsigned long long)replica_rdb_client->id,
                         tail ? "tracking repl-backlog tail" : "no repl-backlog to track");
    replica_rdb_client->repl_data->ref_repl_buf_node = tail ? ln : NULL;
    /* Prevent rdb client from being freed before psync is established. */
    replica_rdb_client->flag.protected_rdb_channel = 1;
    uint64_t id = htonu64(replica_rdb_client->id);
    raxInsert(server.replicas_waiting_psync, (unsigned char *)&id, sizeof(id), replica_rdb_client, NULL);
}

/* Attach waiting psync replicas with new replication backlog head. */
void backfillRdbReplicasToPsyncWait(void) {
    listNode *ln = listFirst(server.repl_buffer_blocks);
    replBufBlock *head = ln ? listNodeValue(ln) : NULL;
    raxIterator iter;

    if (head == NULL) return;
    /* Update waiting psync replicas to wait on new buffer block */
    raxStart(&iter, server.replicas_waiting_psync);
    raxSeek(&iter, "^", NULL, 0);
    while (raxNext(&iter)) {
        client *replica_rdb_client = iter.data;
        if (replica_rdb_client->repl_data->ref_repl_buf_node) continue;
        replica_rdb_client->repl_data->ref_repl_buf_node = ln;
        head->refcount++;
        dualChannelServerLog(LL_DEBUG, "Attach replica rdb client %llu to repl buf block",
                             (long long unsigned int)replica_rdb_client->id);
    }
    raxStop(&iter);
}

void removeReplicaFromPsyncWait(client *replica_main_client) {
    listNode *ln;
    replBufBlock *o;
    /* Get replBufBlock pointed by this replica */
    client *replica_rdb_client = lookupRdbClientByID(replica_main_client->repl_data->associated_rdb_client_id);
    ln = replica_rdb_client->repl_data->ref_repl_buf_node;
    o = ln ? listNodeValue(ln) : NULL;
    if (o != NULL) {
        serverAssert(o->refcount > 0);
        o->refcount--;
    }
    replica_rdb_client->repl_data->ref_repl_buf_node = NULL;
    replica_rdb_client->flag.protected_rdb_channel = 0;
    dualChannelServerLog(LL_DEBUG, "Remove psync waiting replica %s with cid %llu, repl buffer block %s",
                         replicationGetReplicaName(replica_main_client),
                         (long long unsigned int)replica_main_client->repl_data->associated_rdb_client_id,
                         o ? "ref count decreased" : "doesn't exist");
    uint64_t id = htonu64(replica_rdb_client->id);
    raxRemove(server.replicas_waiting_psync, (unsigned char *)&id, sizeof(id), NULL);
}

void resetReplicationBuffer(void) {
    server.repl_buffer_mem = 0;
    server.repl_buffer_blocks = listCreate();
    listSetFreeMethod(server.repl_buffer_blocks, zfree);
}

int canFeedReplicaReplBuffer(client *replica) {
    /* Don't feed replicas that only want the RDB. */
    if (replica->flag.repl_rdbonly) return 0;

    /* Don't feed replicas that are still waiting for BGSAVE to start. */
    if (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_START) return 0;

    return 1;
}

/* Similar with 'prepareClientToWrite', note that we must call this function
 * before feeding replication stream into global replication buffer, since
 * clientHasPendingReplies in prepareClientToWrite will access the global
 * replication buffer to make judgements. */
int prepareReplicasToWrite(void) {
    listIter li;
    listNode *ln;
    int prepared = 0;

    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;
        if (!canFeedReplicaReplBuffer(replica)) continue;
        if (prepareClientToWrite(replica) == C_ERR) continue;
        prepared++;
    }

    return prepared;
}

/* Wrapper for feedReplicationBuffer() that takes string Objects
 * as input. */
void feedReplicationBufferWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr, sizeof(llstr), (long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBuffer(p, len);
}

/* Generally, we only have one replication buffer block to trim when replication
 * backlog size exceeds our setting and no replica reference it. But if replica
 * clients disconnect, we need to free many replication buffer blocks that are
 * referenced. It would cost much time if there are a lots blocks to free, that
 * will freeze server, so we trim replication backlog incrementally. */
void incrementalTrimReplicationBacklog(size_t max_blocks) {
    serverAssert(server.repl_backlog != NULL);

    size_t trimmed_blocks = 0;
    while (server.repl_backlog->histlen > server.repl_backlog_size && trimmed_blocks < max_blocks) {
        /* We never trim backlog to less than one block. */
        if (listLength(server.repl_buffer_blocks) <= 1) break;

        /* Replicas increment the refcount of the first replication buffer block
         * they refer to, in that case, we don't trim the backlog even if
         * backlog_histlen exceeds backlog_size. This implicitly makes backlog
         * bigger than our setting, but makes the primary accept partial resync as
         * much as possible. So that backlog must be the last reference of
         * replication buffer blocks. */
        listNode *first = listFirst(server.repl_buffer_blocks);
        serverAssert(first == server.repl_backlog->ref_repl_buf_node);
        replBufBlock *fo = listNodeValue(first);
        if (fo->refcount != 1) break;

        /* We don't try trim backlog if backlog valid size will be lessen than
         * setting backlog size once we release the first repl buffer block. */
        if (server.repl_backlog->histlen - (long long)fo->size <= server.repl_backlog_size) break;

        /* Decr refcount and release the first block later. */
        fo->refcount--;
        trimmed_blocks++;
        server.repl_backlog->histlen -= fo->size;

        /* Go to use next replication buffer block node. */
        listNode *next = listNextNode(first);
        server.repl_backlog->ref_repl_buf_node = next;
        serverAssert(server.repl_backlog->ref_repl_buf_node != NULL);
        /* Incr reference count to keep the new head node. */
        ((replBufBlock *)listNodeValue(next))->refcount++;

        /* Remove the node in recorded blocks. */
        uint64_t encoded_offset = htonu64(fo->repl_offset);
        raxRemove(server.repl_backlog->blocks_index, (unsigned char *)&encoded_offset, sizeof(uint64_t), NULL);

        /* Delete the first node from global replication buffer. */
        serverAssert(fo->refcount == 0 && fo->used == fo->size);
        server.repl_buffer_mem -= (fo->size + sizeof(listNode) + sizeof(replBufBlock));
        listDelNode(server.repl_buffer_blocks, first);
    }

    /* Set the offset of the first byte we have in the backlog. */
    server.repl_backlog->offset = server.primary_repl_offset - server.repl_backlog->histlen + 1;
}

/* Free replication buffer blocks that are referenced by this client. */
void freeReplicaReferencedReplBuffer(client *replica) {
    if (replica->flag.repl_rdb_channel) {
        uint64_t rdb_cid = htonu64(replica->id);
        if (raxRemove(server.replicas_waiting_psync, (unsigned char *)&rdb_cid, sizeof(rdb_cid), NULL)) {
            dualChannelServerLog(LL_DEBUG, "Remove psync waiting replica %s with cid %llu from replicas rax.",
                                 replicationGetReplicaName(replica), (long long unsigned int)replica->id);
        }
    }
    if (replica->repl_data->ref_repl_buf_node != NULL) {
        /* Decrease the start buffer node reference count. */
        replBufBlock *o = listNodeValue(replica->repl_data->ref_repl_buf_node);
        serverAssert(o->refcount > 0);
        o->refcount--;
        incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
    }
    replica->repl_data->ref_repl_buf_node = NULL;
    replica->repl_data->ref_block_pos = 0;
}

/* Replication: Primary side.
 * Append bytes into the global replication buffer list, replication backlog and
 * all replica clients use replication buffers collectively, this function replace
 * 'addReply*', 'feedReplicationBacklog' for replicas and replication backlog,
 * First we add buffer into global replication buffer block list, and then
 * update replica / replication-backlog referenced node and block position. */
void feedReplicationBuffer(char *s, size_t len) {
    static long long repl_block_id = 0;

    if (server.repl_backlog == NULL) return;

    clusterSlotStatsIncrNetworkBytesOutForReplication(len);

    while (len > 0) {
        size_t start_pos = 0;        /* The position of referenced block to start sending. */
        listNode *start_node = NULL; /* Replica/backlog starts referenced node. */
        int add_new_block = 0;       /* Create new block if current block is total used. */
        listNode *ln = listLast(server.repl_buffer_blocks);
        replBufBlock *tail = ln ? listNodeValue(ln) : NULL;
        int empty_backlog = (tail == NULL);

        /* Append to tail string when possible. */
        if (tail && tail->size > tail->used) {
            start_node = listLast(server.repl_buffer_blocks);
            start_pos = tail->used;
            /* Copy the part we can fit into the tail, and leave the rest for a
             * new node */
            size_t avail = tail->size - tail->used;
            size_t copy = (avail >= len) ? len : avail;
            memcpy(tail->buf + tail->used, s, copy);
            tail->used += copy;
            s += copy;
            len -= copy;
            server.primary_repl_offset += copy;
            server.repl_backlog->histlen += copy;
        }
        if (len) {
            /* Create a new node, make sure it is allocated to at
             * least PROTO_REPLY_CHUNK_BYTES */
            size_t usable_size;
            /* Avoid creating nodes smaller than PROTO_REPLY_CHUNK_BYTES, so that we can append more data into them,
             * and also avoid creating nodes bigger than repl_backlog_size / 16, so that we won't have huge nodes that
             * can't trim when we only still need to hold a small portion from them. */
            size_t limit = max((size_t)server.repl_backlog_size / 16, (size_t)PROTO_REPLY_CHUNK_BYTES);
            size_t size = min(max(len, (size_t)PROTO_REPLY_CHUNK_BYTES), limit);
            tail = zmalloc_usable(size + sizeof(replBufBlock), &usable_size);
            /* Take over the allocation's internal fragmentation */
            tail->size = usable_size - sizeof(replBufBlock);
            size_t copy = (tail->size >= len) ? len : tail->size;
            tail->used = copy;
            tail->refcount = 0;
            tail->repl_offset = server.primary_repl_offset + 1;
            tail->id = repl_block_id++;
            memcpy(tail->buf, s, copy);
            listAddNodeTail(server.repl_buffer_blocks, tail);
            /* We also count the list node memory into replication buffer memory. */
            server.repl_buffer_mem += (usable_size + sizeof(listNode));
            add_new_block = 1;
            if (start_node == NULL) {
                start_node = listLast(server.repl_buffer_blocks);
                start_pos = 0;
            }
            s += copy;
            len -= copy;
            server.primary_repl_offset += copy;
            server.repl_backlog->histlen += copy;
        }
        if (empty_backlog && raxSize(server.replicas_waiting_psync) > 0) {
            /* Increase refcount for pending replicas. */
            backfillRdbReplicasToPsyncWait();
        }

        /* For output buffer of replicas. */
        listIter li;
        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            client *replica = ln->value;
            if (!canFeedReplicaReplBuffer(replica) && !(replica->flag.protected_rdb_channel)) continue;
            /* Update shared replication buffer start position. */
            if (replica->repl_data->ref_repl_buf_node == NULL) {
                replica->repl_data->ref_repl_buf_node = start_node;
                replica->repl_data->ref_block_pos = start_pos;
                /* Only increase the start block reference count. */
                ((replBufBlock *)listNodeValue(start_node))->refcount++;
            }

            /* Check output buffer limit only when add new block. */
            if (add_new_block) closeClientOnOutputBufferLimitReached(replica, 1);
        }

        /* For replication backlog */
        if (server.repl_backlog->ref_repl_buf_node == NULL) {
            server.repl_backlog->ref_repl_buf_node = start_node;
            /* Only increase the start block reference count. */
            ((replBufBlock *)listNodeValue(start_node))->refcount++;

            /* Replication buffer must be empty before adding replication stream
             * into replication backlog. */
            serverAssert(add_new_block == 1 && start_pos == 0);
        }
        if (add_new_block) {
            createReplicationBacklogIndex(listLast(server.repl_buffer_blocks));
            /* It is important to trim after adding replication data to keep the backlog size close to
             * repl_backlog_size in the common case. We wait until we add a new block to avoid repeated
             * unnecessary trimming attempts when small amounts of data are added. See comments in
             * freeMemoryGetNotCountedMemory() for details on replication backlog memory tracking. */
            incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
        }
    }
}

/* Propagate write commands to replication stream.
 *
 * This function is used if the instance is a primary: we use the commands
 * received by our clients in order to create the replication stream.
 * Instead if the instance is a replica and has sub-replicas attached, we use
 * replicationFeedStreamFromPrimaryStream() */
void replicationFeedReplicas(int dictid, robj **argv, int argc) {
    int j, len;
    char llstr[LONG_STR_SIZE];

    /* In case we propagate a command that doesn't touch keys (PING, REPLCONF) we
     * pass dbid=-1 that indicate there is no need to replicate `select` command. */
    serverAssert(dictid == -1 || (dictid >= 0 && dictid < server.dbnum));

    /* If the instance is not a top level primary, return ASAP: we'll just proxy
     * the stream of data we receive from our primary instead, in order to
     * propagate *identical* replication stream. In this way this replica can
     * advertise the same replication ID as the primary (since it shares the
     * primary replication history and has the same backlog and offsets). */
    if (server.primary_host != NULL) return;

    /* If there aren't replicas, and there is no backlog buffer to populate,
     * we can return ASAP. */
    if (server.repl_backlog == NULL && listLength(server.replicas) == 0) {
        /* We increment the repl_offset anyway, since we use that for tracking AOF fsyncs
         * even when there's no replication active. This code will not be reached if AOF
         * is also disabled. */
        server.primary_repl_offset += 1;
        return;
    }

    /* We can't have replicas attached and no backlog. */
    serverAssert(!(listLength(server.replicas) != 0 && server.repl_backlog == NULL));

    /* Must install write handler for all replicas first before feeding
     * replication stream. */
    prepareReplicasToWrite();

    /* Send SELECT command to every replica if needed. */
    if (dictid != -1 && server.replicas_eldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr, sizeof(llstr), dictid);
            selectcmd = createObject(
                OBJ_STRING, sdscatprintf(sdsempty(), "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", dictid_len, llstr));
        }

        feedReplicationBufferWithObject(selectcmd);

        /* Although the SELECT command is not associated with any slot,
         * its per-slot network-bytes-out accumulation is made by the above function call.
         * To cancel-out this accumulation, below adjustment is made. */
        clusterSlotStatsDecrNetworkBytesOutForReplication(sdslen(selectcmd->ptr));

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS) decrRefCount(selectcmd);

        server.replicas_eldb = dictid;
    }

    /* Write the command to the replication buffer if any. */
    char aux[LONG_STR_SIZE + 3];

    /* Add the multi bulk reply length. */
    aux[0] = '*';
    len = ll2string(aux + 1, sizeof(aux) - 1, argc);
    aux[len + 1] = '\r';
    aux[len + 2] = '\n';
    feedReplicationBuffer(aux, len + 3);

    for (j = 0; j < argc; j++) {
        long objlen = stringObjectLen(argv[j]);

        /* We need to feed the buffer with the object as a bulk reply
         * not just as a plain string, so create the $..CRLF payload len
         * and add the final CRLF */
        aux[0] = '$';
        len = ll2string(aux + 1, sizeof(aux) - 1, objlen);
        aux[len + 1] = '\r';
        aux[len + 2] = '\n';
        feedReplicationBuffer(aux, len + 3);
        feedReplicationBufferWithObject(argv[j]);
        feedReplicationBuffer(aux + len + 1, 2);
    }
}

/* This is a debugging function that gets called when we detect something
 * wrong with the replication protocol: the goal is to peek into the
 * replication backlog and show a few final bytes to make simpler to
 * guess what kind of bug it could be. */
void showLatestBacklog(void) {
    if (server.repl_backlog == NULL) return;
    if (listLength(server.repl_buffer_blocks) == 0) return;
    if (server.hide_user_data_from_log) {
        serverLog(LL_NOTICE,
                  "hide-user-data-from-log is on, skip logging backlog content to avoid spilling user data.");
        return;
    }

    size_t dumplen = 256;
    if (server.repl_backlog->histlen < (long long)dumplen) dumplen = server.repl_backlog->histlen;

    sds dump = sdsempty();
    listNode *node = listLast(server.repl_buffer_blocks);
    while (dumplen) {
        if (node == NULL) break;
        replBufBlock *o = listNodeValue(node);
        size_t thislen = o->used >= dumplen ? dumplen : o->used;
        sds head = sdscatrepr(sdsempty(), o->buf + o->used - thislen, thislen);
        sds tmp = sdscatsds(head, dump);
        sdsfree(dump);
        dump = tmp;
        dumplen -= thislen;
        node = listPrevNode(node);
    }

    /* Finally log such bytes: this is vital debugging info to
     * understand what happened. */
    serverLog(LL_NOTICE, "Latest backlog is: '%s'", dump);
    sdsfree(dump);
}

/* This function is used in order to proxy what we receive from our primary
 * to our sub-replicas. */
#include <ctype.h>
void replicationFeedStreamFromPrimaryStream(char *buf, size_t buflen) {
    /* Debugging: this is handy to see the stream sent from primary
     * to replicas. Disabled with if(0). */
    if (0) {
        if (!server.hide_user_data_from_log) {
            printf("%zu:", buflen);
            for (size_t j = 0; j < buflen; j++) {
                printf("%c", isprint(buf[j]) ? buf[j] : '.');
            }
            printf("\n");
        }
    }

    /* There must be replication backlog if having attached replicas. */
    if (listLength(server.replicas)) serverAssert(server.repl_backlog != NULL);
    if (server.repl_backlog) {
        /* Must install write handler for all replicas first before feeding
         * replication stream. */
        prepareReplicasToWrite();
        feedReplicationBuffer(buf, buflen);
    }
}

void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    /* Fast path to return if the monitors list is empty or the server is in loading. */
    if (monitors == NULL || listLength(monitors) == 0 || server.loading) return;
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    cmdrepr = sdscatprintf(cmdrepr, "%ld.%06ld ", (long)tv.tv_sec, (long)tv.tv_usec);
    if (c->flag.script) {
        cmdrepr = sdscatprintf(cmdrepr, "[%d lua] ", dictid);
    } else if (c->flag.unix_socket) {
        cmdrepr = sdscatprintf(cmdrepr, "[%d unix:%s] ", dictid, server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr, "[%d %s] ", dictid, getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr, (char *)argv[j]->ptr, sdslen(argv[j]->ptr));
        }
        if (j != argc - 1) cmdrepr = sdscatlen(cmdrepr, " ", 1);
    }
    cmdrepr = sdscatlen(cmdrepr, "\r\n", 2);
    cmdobj = createObject(OBJ_STRING, cmdrepr);

    listRewind(monitors, &li);
    while ((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor, cmdobj);
        updateClientMemUsageAndBucket(monitor);
    }
    decrRefCount(cmdobj);
}

/* Feed the replica 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
long long addReplyReplicationBacklog(client *c, long long offset) {
    long long skip;

    serverLog(LL_DEBUG, "[PSYNC] Replica request offset: %lld", offset);

    if (server.repl_backlog->histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld", server.repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld", server.repl_backlog->offset);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld", server.repl_backlog->histlen);

    /* Compute the amount of bytes we need to discard. */
    skip = offset - server.repl_backlog->offset;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);

    /* Iterate recorded blocks, quickly search the approximate node. */
    listNode *node = NULL;
    if (raxSize(server.repl_backlog->blocks_index) > 0) {
        uint64_t encoded_offset = htonu64(offset);
        raxIterator ri;
        raxStart(&ri, server.repl_backlog->blocks_index);
        raxSeek(&ri, ">", (unsigned char *)&encoded_offset, sizeof(uint64_t));
        if (raxEOF(&ri)) {
            /* No found, so search from the last recorded node. */
            raxSeek(&ri, "$", NULL, 0);
            raxPrev(&ri);
            node = (listNode *)ri.data;
        } else {
            raxPrev(&ri); /* Skip the sought node. */
            /* We should search from the prev node since the offset of current
             * sought node exceeds searching offset. */
            if (raxPrev(&ri))
                node = (listNode *)ri.data;
            else
                node = server.repl_backlog->ref_repl_buf_node;
        }
        raxStop(&ri);
    } else {
        /* No recorded blocks, just from the start node to search. */
        node = server.repl_backlog->ref_repl_buf_node;
    }

    /* Search the exact node. */
    while (node != NULL) {
        replBufBlock *o = listNodeValue(node);
        if (o->repl_offset + (long long)o->used >= offset) break;
        node = listNextNode(node);
    }
    serverAssert(node != NULL);

    /* Install a writer handler first.*/
    prepareClientToWrite(c);
    /* Setting output buffer of the replica. */
    replBufBlock *o = listNodeValue(node);
    o->refcount++;
    c->repl_data->ref_repl_buf_node = node;
    c->repl_data->ref_block_pos = offset - o->repl_offset;

    return server.repl_backlog->histlen - skip;
}

/* Return the offset to provide as reply to the PSYNC command received
 * from the replica. The returned value is only valid immediately after
 * the BGSAVE process started and before executing any other command
 * from clients. */
long long getPsyncInitialOffset(void) {
    return server.primary_repl_offset;
}

/* Send a FULLRESYNC reply in the specific case of a full resynchronization,
 * as a side effect setup the replica for a full sync in different ways:
 *
 * 1) Remember, into the replica client structure, the replication offset
 *    we sent here, so that if new replicas will later attach to the same
 *    background RDB saving process (by duplicating this client output
 *    buffer), we can get the right offset from this replica.
 * 2) Set the replication state of the replica to WAIT_BGSAVE_END so that
 *    we start accumulating differences from this point.
 * 3) Force the replication stream to re-emit a SELECT statement so
 *    the new replica incremental differences will start selecting the
 *    right database number.
 *
 * Normally this function should be called immediately after a successful
 * BGSAVE for replication was started, or when there is one already in
 * progress that we attached our replica to. */
int replicationSetupReplicaForFullResync(client *replica, long long offset) {
    char buf[128];
    int buflen;

    replica->repl_data->psync_initial_offset = offset;
    replica->repl_data->repl_state = REPLICA_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * replica as well. Set replicas_eldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    server.replicas_eldb = -1;

    /* Don't send this reply to replicas that approached us with
     * the old SYNC command. */
    if (!(replica->flag.pre_psync)) {
        buflen = snprintf(buf, sizeof(buf), "+FULLRESYNC %s %lld\r\n", server.replid, offset);
        if (connWrite(replica->conn, buf, buflen) != buflen) {
            freeClientAsync(replica);
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function handles the PSYNC command from the point of view of a
 * primary receiving a request for partial resynchronization.
 *
 * On success return C_OK, otherwise C_ERR is returned and we proceed
 * with the usual full resync. */
int primaryTryPartialResynchronization(client *c, long long psync_offset) {
    long long psync_len;
    char *primary_replid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    /* Is the replication ID of this primary the same advertised by the wannabe
     * replica via PSYNC? If the replication ID changed this primary has a
     * different replication history, and there is no way to continue.
     *
     * Note that there are two potentially valid replication IDs: the ID1
     * and the ID2. The ID2 however is only valid up to a specific offset. */
    if (strcasecmp(primary_replid, server.replid) &&
        (strcasecmp(primary_replid, server.replid2) || psync_offset > server.second_replid_offset)) {
        /* Replid "?" is used by replicas that want to force a full resync. */
        if (primary_replid[0] != '?') {
            if (strcasecmp(primary_replid, server.replid) && strcasecmp(primary_replid, server.replid2)) {
                serverLog(LL_NOTICE,
                          "Partial resynchronization not accepted: "
                          "Replication ID mismatch (Replica asked for '%s', my "
                          "replication IDs are '%s' and '%s')",
                          primary_replid, server.replid, server.replid2);
            } else {
                serverLog(LL_NOTICE,
                          "Partial resynchronization not accepted: "
                          "Requested offset for second ID was %lld, but I can reply "
                          "up to %lld",
                          psync_offset, server.second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE, "Full resync requested by replica %s", replicationGetReplicaName(c));
        }
        goto need_full_resync;
    }

    /* We still have the data our replica is asking for? */
    if (!server.repl_backlog || psync_offset < server.repl_backlog->offset ||
        psync_offset > (server.repl_backlog->offset + server.repl_backlog->histlen)) {
        serverLog(LL_NOTICE,
                  "Unable to partial resync with replica %s for lack of backlog (Replica request was: %lld).",
                  replicationGetReplicaName(c), psync_offset);
        if (psync_offset > server.primary_repl_offset) {
            serverLog(LL_WARNING,
                      "Warning: replica %s tried to PSYNC with an offset that is greater than the primary replication "
                      "offset.",
                      replicationGetReplicaName(c));
        }
        goto need_full_resync;
    }

    /* There are two scenarios that lead to this point. One is that we are able
     * to perform a partial resync with the replica. The second is that the replica
     * is using dual-channel-replication, while loading the snapshot in the background.
     * in both cases:
     * 1) Make sure no IO operations are being performed before changing the client state.
     * 2) Set client state to make it a replica.
     * 3) Inform the client we can continue with +CONTINUE
     * 4) Send the backlog data (from the offset to the end) to the replica. */
    waitForClientIO(c);
    c->flag.replica = 1;
    if (c->repl_data->associated_rdb_client_id && lookupRdbClientByID(c->repl_data->associated_rdb_client_id)) {
        c->repl_data->repl_state = REPLICA_STATE_BG_RDB_LOAD;
        removeReplicaFromPsyncWait(c);
    } else {
        c->repl_data->repl_state = REPLICA_STATE_ONLINE;
    }
    c->repl_data->repl_ack_time = server.unixtime;
    c->repl_data->repl_start_cmd_stream_on_ack = 0;
    listAddNodeTail(server.replicas, c);
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * empty so this write will never fail actually. */
    if (c->repl_data->replica_capa & REPLICA_CAPA_PSYNC2) {
        buflen = snprintf(buf, sizeof(buf), "+CONTINUE %s\r\n", server.replid);
    } else {
        buflen = snprintf(buf, sizeof(buf), "+CONTINUE\r\n");
    }
    if (connWrite(c->conn, buf, buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }
    psync_len = addReplyReplicationBacklog(c, psync_offset);
    serverLog(
        LL_NOTICE,
        "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
        replicationGetReplicaName(c), psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at server.replicas_eldb
     * to -1 to force the primary to emit SELECT, since the replica already
     * has this state from the previous connection with the primary. */

    refreshGoodReplicasCount();

    /* Fire the replica change modules event. */
    moduleFireServerEvent(VALKEYMODULE_EVENT_REPLICA_CHANGE, VALKEYMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE, NULL);

    return C_OK; /* The caller can return, no full resync needed. */

need_full_resync:
    /* We need a full resync for some reason... Note that we can't
     * reply to PSYNC right now if a full SYNC is needed. The reply
     * must include the primary offset at the time the RDB file we transfer
     * is generated, so we need to delay the reply to that moment. */
    return C_ERR;
}

/* Start a BGSAVE for replication goals, which is, selecting the disk or
 * socket target depending on the configuration, and making sure that
 * the script cache is flushed before to start.
 *
 * The mincapa argument is the bitwise AND among all the replicas capabilities
 * of the replicas waiting for this BGSAVE, so represents the replica capabilities
 * all the replicas support. Can be tested via REPLICA_CAPA_* macros.
 *
 * Side effects, other than starting a BGSAVE:
 *
 * 1) Handle the replicas in WAIT_START state, by preparing them for a full
 *    sync if the BGSAVE was successfully started, or sending them an error
 *    and dropping them from the list of replicas.
 *
 * 2) Flush the Lua scripting script cache if the BGSAVE was actually
 *    started.
 *
 * Returns C_OK on success or C_ERR otherwise. */
int startBgsaveForReplication(int mincapa, int req) {
    int retval;
    int socket_target = 0;
    listIter li;
    listNode *ln;

    /* We use a socket target if replica can handle the EOF marker and we're configured to do diskless syncs.
     * Note that in case we're creating a "filtered" RDB (functions-only, for example) we also force socket replication
     * to avoid overwriting the snapshot RDB file with filtered data. */
    socket_target = (server.repl_diskless_sync || req & REPLICA_REQ_RDB_MASK) && (mincapa & REPLICA_CAPA_EOF);
    /* `SYNC` should have failed with error if we don't support socket and require a filter, assert this here */
    serverAssert(socket_target || !(req & REPLICA_REQ_RDB_MASK));

    serverLog(LL_NOTICE, "Starting BGSAVE for SYNC with target: %s using: %s",
              socket_target ? "replicas sockets" : "disk",
              (req & REPLICA_REQ_RDB_CHANNEL) ? "dual-channel" : "normal sync");

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    /* Only do rdbSave* when rsiptr is not NULL,
     * otherwise replica will miss repl-stream-db. */
    if (rsiptr) {
        if (socket_target)
            retval = rdbSaveToReplicasSockets(req, rsiptr);
        else {
            /* Keep the page cache since it'll get used soon */
            retval = rdbSaveBackground(req, server.rdb_filename, rsiptr, RDBFLAGS_REPLICATION | RDBFLAGS_KEEP_CACHE);
        }
        if (server.debug_pause_after_fork) debugPauseProcess();
    } else {
        serverLog(LL_WARNING, "BGSAVE for replication: replication information not available, can't generate the RDB "
                              "file right now. Try later.");
        retval = C_ERR;
    }

    /* If we succeeded to start a BGSAVE with disk target, let's remember
     * this fact, so that we can later delete the file if needed. Note
     * that we don't set the flag to 1 if the feature is disabled, otherwise
     * it would never be cleared: the file is not deleted. This way if
     * the user enables it later with CONFIG SET, we are fine. */
    if (retval == C_OK && !socket_target && server.rdb_del_sync_files) RDBGeneratedByReplication = 1;

    /* If we failed to BGSAVE, remove the replicas waiting for a full
     * resynchronization from the list of replicas, inform them with
     * an error about what happened, close the connection ASAP. */
    if (retval == C_ERR) {
        serverLog(LL_WARNING, "BGSAVE for replication failed");
        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            client *replica = ln->value;

            if (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_START) {
                replica->repl_data->repl_state = REPL_STATE_NONE;
                replica->flag.replica = 0;
                listDelNode(server.replicas, ln);
                addReplyError(replica, "BGSAVE failed, replication can't continue");
                replica->flag.close_after_reply = 1;
            }
        }
        return retval;
    }

    /* If the target is socket, rdbSaveToReplicasSockets() already setup
     * the replicas for a full resync. Otherwise for disk target do it now.*/
    if (!socket_target) {
        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            client *replica = ln->value;

            if (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_START) {
                /* Check replica has the exact requirements */
                if (replica->repl_data->replica_req != req) continue;
                replicationSetupReplicaForFullResync(replica, getPsyncInitialOffset());
            }
        }
    }

    return retval;
}

/* SYNC and PSYNC command implementation. */
void syncCommand(client *c) {
    /* ignore SYNC if already replica or in monitor mode */
    if (c->flag.replica) return;

    initClientReplicationData(c);

    /* Wait for any IO pending operation to finish before changing the client state to replica */
    waitForClientIO(c);

    /* Check if this is a failover request to a replica with the same replid and
     * become a primary if so. */
    if (c->argc > 3 && !strcasecmp(c->argv[0]->ptr, "psync") && !strcasecmp(c->argv[3]->ptr, "failover")) {
        serverLog(LL_NOTICE, "Failover request received for replid %s.", (unsigned char *)c->argv[1]->ptr);
        if (!server.primary_host) {
            addReplyError(c, "PSYNC FAILOVER can't be sent to a master.");
            return;
        }

        if (!strcasecmp(c->argv[1]->ptr, server.replid)) {
            if (server.cluster_enabled) {
                clusterPromoteSelfToPrimary();
            } else {
                replicationUnsetPrimary();
            }
            sds client = catClientInfoShortString(sdsempty(), c, server.hide_user_data_from_log);
            serverLog(LL_NOTICE, "PRIMARY MODE enabled (failover request from '%s')", client);
            sdsfree(client);
        } else {
            addReplyError(c, "PSYNC FAILOVER replid must match my replid.");
            return;
        }
    }

    /* Don't let replicas sync with us while we're failing over */
    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c, "-NOMASTERLINK Can't SYNC while failing over");
        return;
    }

    /* Refuse SYNC requests if we are a replica but the link with our primary
     * is not ok... */
    if (server.primary_host && server.repl_state != REPL_STATE_CONNECTED) {
        addReplyError(c, "-NOMASTERLINK Can't SYNC while not connected with my master");
        return;
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other replicas if needed. */
    if (clientHasPendingReplies(c)) {
        addReplyError(c, "SYNC and PSYNC are invalid with pending output");
        return;
    }

    /* Fail sync if replica doesn't support EOF capability but wants a filtered RDB. This is because we force filtered
     * RDB's to be generated over a socket and not through a file to avoid conflicts with the snapshot files. Forcing
     * use of a socket is handled, if needed, in `startBgsaveForReplication`. */
    if (c->repl_data->replica_req & REPLICA_REQ_RDB_MASK && !(c->repl_data->replica_capa & REPLICA_CAPA_EOF)) {
        addReplyError(c, "Filtered replica requires EOF capability");
        return;
    }

    serverLog(LL_NOTICE, "Replica %s asks for synchronization", replicationGetReplicaName(c));

    /* Try a partial resynchronization if this is a PSYNC command.
     * If it fails, we continue with usual full resynchronization, however
     * when this happens replicationSetupReplicaForFullResync will replied
     * with:
     *
     * +FULLRESYNC <replid> <offset>
     *
     * So the replica knows the new replid and offset to try a PSYNC later
     * if the connection with the primary is lost. */
    if (!strcasecmp(c->argv[0]->ptr, "psync")) {
        long long psync_offset;
        if (getLongLongFromObjectOrReply(c, c->argv[2], &psync_offset, NULL) != C_OK) {
            serverLog(LL_WARNING, "Replica %s asks for synchronization but with a wrong offset",
                      replicationGetReplicaName(c));
            return;
        }

        if (primaryTryPartialResynchronization(c, psync_offset) == C_OK) {
            server.stat_sync_partial_ok++;
            return; /* No full resync needed, return. */
        } else {
            char *primary_replid = c->argv[1]->ptr;

            /* Increment stats for failed PSYNCs, but only if the
             * replid is not "?", as this is used by replicas to force a full
             * resync on purpose when they are not able to partially
             * resync. */
            if (primary_replid[0] != '?') server.stat_sync_partial_err++;
            if (c->repl_data->replica_capa & REPLICA_CAPA_DUAL_CHANNEL) {
                dualChannelServerLog(LL_NOTICE,
                                     "Replica %s is capable of dual channel synchronization, and partial sync "
                                     "isn't possible. "
                                     "Full sync will continue with dedicated RDB channel.",
                                     replicationGetReplicaName(c));
                const char *buf = "+DUALCHANNELSYNC\r\n";
                if (connWrite(c->conn, buf, strlen(buf)) != (int)strlen(buf)) {
                    freeClientAsync(c);
                }
                return;
            }
        }
    } else {
        /* If a replica uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like valkey-cli --replica). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        c->flag.pre_psync = 1;
    }

    /* Full resynchronization. */
    server.stat_sync_full++;

    /* Setup the replica as one waiting for BGSAVE to start. The following code
     * paths will change the state if we handle the replica differently. */
    c->repl_data->repl_state = REPLICA_STATE_WAIT_BGSAVE_START;
    if (server.repl_disable_tcp_nodelay) connDisableTcpNoDelay(c->conn); /* Non critical if it fails. */
    c->repl_data->repldbfd = -1;
    c->flag.replica = 1;
    listAddNodeTail(server.replicas, c);

    /* Create the replication backlog if needed. */
    if (listLength(server.replicas) == 1 && server.repl_backlog == NULL) {
        /* When we create the backlog from scratch, we always use a new
         * replication ID and clear the ID2, since there is no valid
         * past history. */
        changeReplicationId();
        clearReplicationId2();
        createReplicationBacklog();
        serverLog(LL_NOTICE,
                  "Replication backlog created, my new "
                  "replication IDs are '%s' and '%s'",
                  server.replid, server.replid2);
    }

    /* CASE 1: BGSAVE is in progress, with disk target. */
    if (server.child_type == CHILD_TYPE_RDB && server.rdb_child_type == RDB_CHILD_TYPE_DISK) {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another replica that is
         * registering differences since the server forked to save. */
        client *replica;
        listNode *ln;
        listIter li;

        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            replica = ln->value;
            /* If the client needs a buffer of commands, we can't use
             * a replica without replication buffer. */
            if (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END &&
                (!(replica->flag.repl_rdbonly) || (c->flag.repl_rdbonly)))
                break;
        }
        /* To attach this replica, we check that it has at least all the
         * capabilities of the replica that triggered the current BGSAVE
         * and its exact requirements. */
        if (ln && ((c->repl_data->replica_capa & replica->repl_data->replica_capa) == replica->repl_data->replica_capa) &&
            c->repl_data->replica_req == replica->repl_data->replica_req) {
            /* Perfect, the server is already registering differences for
             * another replica. Set the right state, and copy the buffer.
             * We don't copy buffer if clients don't want. */
            if (!c->flag.repl_rdbonly) copyReplicaOutputBuffer(c, replica);
            replicationSetupReplicaForFullResync(c, replica->repl_data->psync_initial_offset);
            serverLog(LL_NOTICE, "Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences. */
            serverLog(LL_NOTICE, "Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

        /* CASE 2: BGSAVE is in progress, with socket target. */
    } else if (server.child_type == CHILD_TYPE_RDB && server.rdb_child_type == RDB_CHILD_TYPE_SOCKET) {
        /* There is an RDB child process but it is writing directly to
         * children sockets. We need to wait for the next BGSAVE
         * in order to synchronize. */
        serverLog(LL_NOTICE, "Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC");

        /* CASE 3: There is no BGSAVE is in progress. */
    } else {
        if (server.repl_diskless_sync && (c->repl_data->replica_capa & REPLICA_CAPA_EOF) && server.repl_diskless_sync_delay) {
            /* Diskless replication RDB child is created inside
             * replicationCron() since we want to delay its start a
             * few seconds to wait for more replicas to arrive. */
            serverLog(LL_NOTICE, "Delay next BGSAVE for diskless SYNC");
        } else {
            /* We don't have a BGSAVE in progress, let's start one. Diskless
             * or disk-based mode is determined by replica's capacity. */
            if (!hasActiveChildProcess()) {
                startBgsaveForReplication(c->repl_data->replica_capa, c->repl_data->replica_req);
            } else {
                serverLog(LL_NOTICE, "No BGSAVE in progress, but another BG operation is active. "
                                     "BGSAVE for replication delayed");
            }
        }
    }
    return;
}

/* Check if there is any other replica waiting dumping RDB finished expect me.
 * This function is useful to judge current dumping RDB can be used for full
 * synchronization or not. */
int anyOtherReplicaWaitRdb(client *except_me) {
    listIter li;
    listNode *ln;

    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;
        if (replica != except_me && replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END) {
            return 1;
        }
    }
    return 0;
}

void initClientReplicationData(client *c) {
    if (c->repl_data) return;
    c->repl_data = (ClientReplicationData *)zcalloc(sizeof(ClientReplicationData));
}

void freeClientReplicationData(client *c) {
    if (!c->repl_data) return;
    freeReplicaReferencedReplBuffer(c);
    /* Primary/replica cleanup Case 1:
     * we lost the connection with a replica. */
    if (c->flag.replica) {
        /* If there is no any other replica waiting dumping RDB finished, the
         * current child process need not continue to dump RDB, then we kill it.
         * So child process won't use more memory, and we also can fork a new
         * child process asap to dump rdb for next full synchronization or bgsave.
         * But we also need to check if users enable 'save' RDB, if enable, we
         * should not remove directly since that means RDB is important for users
         * to keep data safe and we may delay configured 'save' for full sync. */
        if (server.saveparamslen == 0 && c->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END &&
            server.child_type == CHILD_TYPE_RDB && server.rdb_child_type == RDB_CHILD_TYPE_DISK &&
            anyOtherReplicaWaitRdb(c) == 0) {
            serverLog(LL_NOTICE, "Background saving, persistence disabled, last replica dropped, killing fork child.");
            killRDBChild();
        }
        if (c->repl_data->repl_state == REPLICA_STATE_SEND_BULK) {
            if (c->repl_data->repldbfd != -1) close(c->repl_data->repldbfd);
            if (c->repl_data->replpreamble) sdsfree(c->repl_data->replpreamble);
        }
        list *l = (c->flag.monitor) ? server.monitors : server.replicas;
        listNode *ln = listSearchKey(l, c);
        serverAssert(ln != NULL);
        listDelNode(l, ln);
        /* We need to remember the time when we started to have zero
         * attached replicas, as after some time we'll free the replication
         * backlog. */
        if (getClientType(c) == CLIENT_TYPE_REPLICA && listLength(server.replicas) == 0)
            server.repl_no_replicas_since = server.unixtime;
        refreshGoodReplicasCount();
        /* Fire the replica change modules event. */
        if (c->repl_data->repl_state == REPLICA_STATE_ONLINE)
            moduleFireServerEvent(VALKEYMODULE_EVENT_REPLICA_CHANGE, VALKEYMODULE_SUBEVENT_REPLICA_CHANGE_OFFLINE,
                                  NULL);
    }
    if (c->flag.primary) replicationHandlePrimaryDisconnection();
    sdsfree(c->repl_data->replica_addr);
    zfree(c->repl_data);
    c->repl_data = NULL;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a replica in order to configure the replication
 * process before starting it with the SYNC command.
 * This command is also used by a primary in order to get the replication
 * offset from a replica.
 *
 * Currently we support these options:
 *
 * - listening-port <port>
 * - ip-address <ip>
 * What is the listening ip and port of the Replica instance, so that
 * the primary can accurately lists replicas and their listening ports in the
 * INFO output.
 *
 * - capa <eof|psync2|dual-channel>
 * What is the capabilities of this instance.
 * eof: supports EOF-style RDB transfer for diskless replication.
 * psync2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
 * dual-channel: supports full sync using rdb channel.
 *
 * - ack <offset> [fack <aofofs>]
 * Replica informs the primary the amount of replication stream that it
 * processed so far, and optionally the replication offset fsynced to the AOF file.
 * This special pattern doesn't reply to the caller.
 *
 * - getack <dummy>
 * Unlike other subcommands, this is used by primary to get the replication
 * offset from a replica.
 *
 * - rdb-only <0|1>
 * Only wants RDB snapshot without replication buffer.
 *
 * - rdb-filter-only <include-filters>
 * Define "include" filters for the RDB snapshot. Currently we only support
 * a single include filter: "functions". Passing an empty string "" will
 * result in an empty RDB.
 *
 * - version <major.minor.patch>
 * The replica reports its version.
 *
 * - rdb-channel <1|0>
 * Used to identify the client as a replica's rdb connection in an dual channel
 * sync session.
 * */
void replconfCommand(client *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    initClientReplicationData(c);

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j += 2) {
        if (!strcasecmp(c->argv[j]->ptr, "listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c, c->argv[j + 1], &port, NULL) != C_OK)) return;
            c->repl_data->replica_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr, "ip-address")) {
            sds addr = c->argv[j + 1]->ptr;
            if (sdslen(addr) < NET_HOST_STR_LEN) {
                if (c->repl_data->replica_addr) sdsfree(c->repl_data->replica_addr);
                c->repl_data->replica_addr = sdsdup(addr);
            } else {
                addReplyErrorFormat(c,
                                    "REPLCONF ip-address provided by "
                                    "replica instance is too long: %zd bytes",
                                    sdslen(addr));
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr, "capa")) {
            /* Ignore capabilities not understood by this primary. */
            if (!strcasecmp(c->argv[j + 1]->ptr, "eof"))
                c->repl_data->replica_capa |= REPLICA_CAPA_EOF;
            else if (!strcasecmp(c->argv[j + 1]->ptr, "psync2"))
                c->repl_data->replica_capa |= REPLICA_CAPA_PSYNC2;
            else if (!strcasecmp(c->argv[j + 1]->ptr, "dual-channel") && server.dual_channel_replication &&
                     server.repl_diskless_sync) {
                /* If dual-channel is disable on this primary, treat this command as unrecognized
                 * replconf option. */
                c->repl_data->replica_capa |= REPLICA_CAPA_DUAL_CHANNEL;
            }
        } else if (!strcasecmp(c->argv[j]->ptr, "ack")) {
            /* REPLCONF ACK is used by replica to inform the primary the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            long long offset;

            if (!c->flag.replica) return;
            if ((getLongLongFromObject(c->argv[j + 1], &offset) != C_OK)) return;
            if (offset > c->repl_data->repl_ack_off) c->repl_data->repl_ack_off = offset;
            if (c->argc > j + 3 && !strcasecmp(c->argv[j + 2]->ptr, "fack")) {
                if ((getLongLongFromObject(c->argv[j + 3], &offset) != C_OK)) return;
                if (offset > c->repl_data->repl_aof_off) c->repl_data->repl_aof_off = offset;
            }
            c->repl_data->repl_ack_time = server.unixtime;
            /* If this was a diskless replication, we need to really put
             * the replica online when the first ACK is received (which
             * confirms replica is online and ready to get more data). This
             * allows for simpler and less CPU intensive EOF detection
             * when streaming RDB files.
             * There's a chance the ACK got to us before we detected that the
             * bgsave is done (since that depends on cron ticks), so run a
             * quick check first (instead of waiting for the next ACK. */
            if (server.child_type == CHILD_TYPE_RDB && c->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END)
                checkChildrenDone();
            if (c->repl_data->repl_start_cmd_stream_on_ack && c->repl_data->repl_state == REPLICA_STATE_ONLINE) replicaStartCommandStream(c);
            if (c->repl_data->repl_state == REPLICA_STATE_BG_RDB_LOAD) {
                replicaPutOnline(c);
            }
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr, "getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the replica. */
            if (server.primary_host && server.primary) replicationSendAck();
            return;
        } else if (!strcasecmp(c->argv[j]->ptr, "rdb-only")) {
            /* REPLCONF RDB-ONLY is used to identify the client only wants
             * RDB snapshot without replication buffer. */
            long rdb_only = 0;
            if (getRangeLongFromObjectOrReply(c, c->argv[j + 1], 0, 1, &rdb_only, NULL) != C_OK) return;
            if (rdb_only == 1)
                c->flag.repl_rdbonly = 1;
            else
                c->flag.repl_rdbonly = 0;
        } else if (!strcasecmp(c->argv[j]->ptr, "rdb-filter-only")) {
            /* REPLCONFG RDB-FILTER-ONLY is used to define "include" filters
             * for the RDB snapshot. Currently we only support a single
             * include filter: "functions". In the future we may want to add
             * other filters like key patterns, key types, non-volatile, module
             * aux fields, ...
             * We might want to add the complementing "RDB-FILTER-EXCLUDE" to
             * filter out certain data. */
            int filter_count, i;
            sds *filters;
            if (!(filters = sdssplitargs(c->argv[j + 1]->ptr, &filter_count))) {
                addReplyError(c, "Missing rdb-filter-only values");
                return;
            }
            /* By default filter out all parts of the rdb */
            c->repl_data->replica_req |= REPLICA_REQ_RDB_EXCLUDE_DATA;
            c->repl_data->replica_req |= REPLICA_REQ_RDB_EXCLUDE_FUNCTIONS;
            for (i = 0; i < filter_count; i++) {
                if (!strcasecmp(filters[i], "functions"))
                    c->repl_data->replica_req &= ~REPLICA_REQ_RDB_EXCLUDE_FUNCTIONS;
                else {
                    addReplyErrorFormat(c, "Unsupported rdb-filter-only option: %s", (char *)filters[i]);
                    sdsfreesplitres(filters, filter_count);
                    return;
                }
            }
            sdsfreesplitres(filters, filter_count);
        } else if (!strcasecmp(c->argv[j]->ptr, "version")) {
            /* REPLCONF VERSION x.y.z */
            int version = version2num(c->argv[j + 1]->ptr);
            if (version >= 0) {
                c->repl_data->replica_version = version;
            } else {
                addReplyErrorFormat(c, "Unrecognized version format: %s", (char *)c->argv[j + 1]->ptr);
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr, "rdb-channel")) {
            long start_with_offset = 0;
            if (getRangeLongFromObjectOrReply(c, c->argv[j + 1], 0, 1, &start_with_offset, NULL) != C_OK) {
                return;
            }
            if (start_with_offset == 1) {
                c->flag.repl_rdb_channel = 1;
                c->repl_data->replica_req |= REPLICA_REQ_RDB_CHANNEL;
            } else {
                c->flag.repl_rdb_channel = 0;
                c->repl_data->replica_req &= ~REPLICA_REQ_RDB_CHANNEL;
            }
        } else if (!strcasecmp(c->argv[j]->ptr, "set-rdb-client-id")) {
            /* REPLCONF identify <client-id> is used to identify the current replica main channel with existing
             * rdb-connection with the given id. */
            long long client_id = 0;
            if (getLongLongFromObjectOrReply(c, c->argv[j + 1], &client_id, NULL) != C_OK) {
                return;
            }
            if (!lookupRdbClientByID(client_id)) {
                addReplyErrorFormat(c, "Unrecognized RDB client id %lld", client_id);
                return;
            }
            c->repl_data->associated_rdb_client_id = (uint64_t)client_id;
        } else {
            addReplyErrorFormat(c, "Unrecognized REPLCONF option: %s", (char *)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c, shared.ok);
}

/* This function puts a replica in the online state, and should be called just
 * after a replica received the RDB file for the initial synchronization.
 *
 * It does a few things:
 * 1) Put the replica in ONLINE state.
 * 2) Update the count of "good replicas".
 * 3) Trigger the module event.
 *
 * the return value indicates that the replica should be disconnected.
 * */
int replicaPutOnline(client *replica) {
    if (replica->flag.repl_rdbonly) {
        replica->repl_data->repl_state = REPLICA_STATE_RDB_TRANSMITTED;
        /* The client asked for RDB only so we should close it ASAP */
        serverLog(LL_NOTICE, "RDB transfer completed, rdb only replica (%s) should be disconnected asap",
                  replicationGetReplicaName(replica));
        return 0;
    }
    replica->repl_data->repl_state = REPLICA_STATE_ONLINE;
    replica->repl_data->repl_ack_time = server.unixtime; /* Prevent false timeout. */

    refreshGoodReplicasCount();
    /* Fire the replica change modules event. */
    moduleFireServerEvent(VALKEYMODULE_EVENT_REPLICA_CHANGE, VALKEYMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE, NULL);
    serverLog(LL_NOTICE, "Synchronization with replica %s succeeded", replicationGetReplicaName(replica));

    return 1;
}

/* This function should be called just after a replica received the RDB file
 * for the initial synchronization, and we are finally ready to send the
 * incremental stream of commands.
 *
 * It does a few things:
 * 1) Close the replica's connection async if it doesn't need replication
 *    commands buffer stream, since it actually isn't a valid replica.
 * 2) Make sure the writable event is re-installed, since when calling the SYNC
 *    command we had no replies and it was disabled, and then we could
 *    accumulate output buffer data without sending it to the replica so it
 *    won't get mixed with the RDB stream. */
void replicaStartCommandStream(client *replica) {
    serverAssert(!(replica->flag.repl_rdbonly));
    replica->repl_data->repl_start_cmd_stream_on_ack = 0;

    putClientInPendingWriteQueue(replica);
}

/* We call this function periodically to remove an RDB file that was
 * generated because of replication, in an instance that is otherwise
 * without any persistence. We don't want instances without persistence
 * to take RDB files around, this violates certain policies in certain
 * environments. */
void removeRDBUsedToSyncReplicas(void) {
    /* If the feature is disabled, return ASAP but also clear the
     * RDBGeneratedByReplication flag in case it was set. Otherwise if the
     * feature was enabled, but gets disabled later with CONFIG SET, the
     * flag may remain set to one: then next time the feature is re-enabled
     * via CONFIG SET we have it set even if no RDB was generated
     * because of replication recently. */
    if (!server.rdb_del_sync_files) {
        RDBGeneratedByReplication = 0;
        return;
    }

    if (allPersistenceDisabled() && RDBGeneratedByReplication) {
        client *replica;
        listNode *ln;
        listIter li;

        int delrdb = 1;
        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            replica = ln->value;
            if (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_START ||
                replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END ||
                replica->repl_data->repl_state == REPLICA_STATE_SEND_BULK) {
                delrdb = 0;
                break; /* No need to check the other replicas. */
            }
        }
        if (delrdb) {
            struct stat sb;
            if (lstat(server.rdb_filename, &sb) != -1) {
                RDBGeneratedByReplication = 0;
                serverLog(LL_NOTICE, "Removing the RDB file used to feed replicas "
                                     "in a persistence-less instance");
                bg_unlink(server.rdb_filename);
            }
        }
    }
}

/* Close the repldbfd and reclaim the page cache if the client hold
 * the last reference to replication DB */
void closeRepldbfd(client *myself) {
    listNode *ln;
    listIter li;
    int reclaim = 1;
    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;
        if (replica != myself && replica->repl_data->repl_state == REPLICA_STATE_SEND_BULK) {
            reclaim = 0;
            break;
        }
    }

    if (reclaim) {
        bioCreateCloseJob(myself->repl_data->repldbfd, 0, 1);
    } else {
        close(myself->repl_data->repldbfd);
    }
    myself->repl_data->repldbfd = -1;
}

void sendBulkToReplica(connection *conn) {
    client *replica = connGetPrivateData(conn);
    char buf[PROTO_IOBUF_LEN];
    ssize_t nwritten, buflen;

    /* Before sending the RDB file, we send the preamble as configured by the
     * replication process. Currently the preamble is just the bulk count of
     * the file in the form "$<length>\r\n". */
    if (replica->repl_data->replpreamble) {
        nwritten = connWrite(conn, replica->repl_data->replpreamble, sdslen(replica->repl_data->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_WARNING, "Write error sending RDB preamble to replica: %s", connGetLastError(conn));
            freeClient(replica);
            return;
        }
        server.stat_net_repl_output_bytes += nwritten;
        sdsrange(replica->repl_data->replpreamble, nwritten, -1);
        if (sdslen(replica->repl_data->replpreamble) == 0) {
            sdsfree(replica->repl_data->replpreamble);
            replica->repl_data->replpreamble = NULL;
            /* fall through sending data. */
        } else {
            return;
        }
    }

    /* If the preamble was already transferred, send the RDB bulk data. */
    lseek(replica->repl_data->repldbfd, replica->repl_data->repldboff, SEEK_SET);
    buflen = read(replica->repl_data->repldbfd, buf, PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING, "Read error sending DB to replica: %s",
                  (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(replica);
        return;
    }
    if ((nwritten = connWrite(conn, buf, buflen)) == -1) {
        if (connGetState(conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_WARNING, "Write error sending DB to replica: %s", connGetLastError(conn));
            freeClient(replica);
        }
        return;
    }
    replica->repl_data->repldboff += nwritten;
    server.stat_net_repl_output_bytes += nwritten;
    if (replica->repl_data->repldboff == replica->repl_data->repldbsize) {
        closeRepldbfd(replica);
        connSetWriteHandler(replica->conn, NULL);
        if (!replicaPutOnline(replica)) {
            freeClient(replica);
            return;
        }
        replicaStartCommandStream(replica);
    }
}

/* Remove one write handler from the list of connections waiting to be writable
 * during rdb pipe transfer. */
void rdbPipeWriteHandlerConnRemoved(struct connection *conn) {
    if (!connHasWriteHandler(conn)) return;
    connSetWriteHandler(conn, NULL);
    client *replica = connGetPrivateData(conn);
    replica->repl_data->repl_last_partial_write = 0;
    server.rdb_pipe_numconns_writing--;
    /* if there are no more writes for now for this conn, or write error: */
    if (server.rdb_pipe_numconns_writing == 0) {
        if (aeCreateFileEvent(server.el, server.rdb_pipe_read, AE_READABLE, rdbPipeReadHandler, NULL) == AE_ERR) {
            serverPanic("Unrecoverable error creating server.rdb_pipe_read file event.");
        }
    }
}

/* Called in diskless primary during transfer of data from the rdb pipe, when
 * the replica becomes writable again. */
void rdbPipeWriteHandler(struct connection *conn) {
    serverAssert(server.rdb_pipe_bufflen > 0);
    client *replica = connGetPrivateData(conn);
    ssize_t nwritten;
    if ((nwritten = connWrite(conn, server.rdb_pipe_buff + replica->repl_data->repldboff,
                              server.rdb_pipe_bufflen - replica->repl_data->repldboff)) == -1) {
        if (connGetState(conn) == CONN_STATE_CONNECTED) return; /* equivalent to EAGAIN */
        serverLog(LL_WARNING, "Write error sending DB to replica: %s", connGetLastError(conn));
        freeClient(replica);
        return;
    } else {
        replica->repl_data->repldboff += nwritten;
        server.stat_net_repl_output_bytes += nwritten;
        if (replica->repl_data->repldboff < server.rdb_pipe_bufflen) {
            replica->repl_data->repl_last_partial_write = server.unixtime;
            return; /* more data to write.. */
        }
    }
    rdbPipeWriteHandlerConnRemoved(conn);
}

/* Called in diskless primary, when there's data to read from the child's rdb pipe */
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    int i;
    if (!server.rdb_pipe_buff) server.rdb_pipe_buff = zmalloc(PROTO_IOBUF_LEN);
    serverAssert(server.rdb_pipe_numconns_writing == 0);

    while (1) {
        server.rdb_pipe_bufflen = read(fd, server.rdb_pipe_buff, PROTO_IOBUF_LEN);
        if (server.rdb_pipe_bufflen < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            serverLog(LL_WARNING, "Diskless rdb transfer, read error sending DB to replicas: %s", strerror(errno));
            for (i = 0; i < server.rdb_pipe_numconns; i++) {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn) continue;
                client *replica = connGetPrivateData(conn);
                freeClient(replica);
                server.rdb_pipe_conns[i] = NULL;
            }
            killRDBChild();
            return;
        }

        if (server.rdb_pipe_bufflen == 0) {
            /* EOF - write end was closed. */
            int stillUp = 0;
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            for (i = 0; i < server.rdb_pipe_numconns; i++) {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn) continue;
                stillUp++;
            }
            if (stillUp) {
                serverLog(LL_NOTICE, "Diskless rdb transfer, done reading from pipe, %d replicas still up.", stillUp);
            }
            /* Now that the replicas have finished reading, notify the child that it's safe to exit.
             * When the server detects the child has exited, it can mark the replica as online, and
             * start streaming the replication buffers. */
            close(server.rdb_child_exit_pipe);
            server.rdb_child_exit_pipe = -1;
            return;
        }

        for (i = 0; i < server.rdb_pipe_numconns; i++) {
            ssize_t nwritten;
            connection *conn = server.rdb_pipe_conns[i];
            if (!conn) continue;

            client *replica = connGetPrivateData(conn);
            if ((nwritten = connWrite(conn, server.rdb_pipe_buff, server.rdb_pipe_bufflen)) == -1) {
                if (connGetState(conn) != CONN_STATE_CONNECTED) {
                    serverLog(LL_WARNING, "Diskless rdb transfer, write error sending DB to replica: %s",
                              connGetLastError(conn));
                    freeClient(replica);
                    server.rdb_pipe_conns[i] = NULL;
                    continue;
                }
                /* An error and still in connected state, is equivalent to EAGAIN */
                replica->repl_data->repldboff = 0;
            } else {
                /* Note: when use diskless replication, 'repldboff' is the offset
                 * of 'rdb_pipe_buff' sent rather than the offset of entire RDB. */
                replica->repl_data->repldboff = nwritten;
                server.stat_net_repl_output_bytes += nwritten;
            }
            /* If we were unable to write all the data to one of the replicas,
             * setup write handler (and disable pipe read handler, below) */
            if (nwritten != server.rdb_pipe_bufflen) {
                replica->repl_data->repl_last_partial_write = server.unixtime;
                server.rdb_pipe_numconns_writing++;
                connSetWriteHandler(conn, rdbPipeWriteHandler);
            }
        }

        /*  Remove the pipe read handler if at least one write handler was set. */
        if (server.rdb_pipe_numconns_writing) {
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            break;
        }
    }
}

/* This function is called at the end of every background saving.
 *
 * The argument bgsaveerr is C_OK if the background saving succeeded
 * otherwise C_ERR is passed to the function.
 * The 'type' argument is the type of the child that terminated
 * (if it had a disk or socket target). */
void updateReplicasWaitingBgsave(int bgsaveerr, int type) {
    listNode *ln;
    listIter li;

    /* Note: there's a chance we got here from within the REPLCONF ACK command
     * so we must avoid using freeClient, otherwise we'll crash on our way up. */

    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;

        if (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END) {
            int repldbfd;
            struct valkey_stat buf;

            if (bgsaveerr != C_OK) {
                /* If bgsaveerr is error, there is no need to protect the rdb channel. */
                replica->flag.protected_rdb_channel = 0;
                freeClientAsync(replica);
                serverLog(LL_WARNING, "SYNC failed. BGSAVE child returned an error");
                continue;
            }

            /* If this was an RDB on disk save, we have to prepare to send
             * the RDB from disk to the replica socket. Otherwise if this was
             * already an RDB -> Replicas socket transfer, used in the case of
             * diskless replication, our work is trivial, we can just put
             * the replica online. */
            if (type == RDB_CHILD_TYPE_SOCKET) {
                serverLog(LL_NOTICE,
                          "Streamed RDB transfer with replica %s succeeded (socket). Waiting for REPLCONF ACK from "
                          "replica to enable streaming",
                          replicationGetReplicaName(replica));
                /* Note: we wait for a REPLCONF ACK message from the replica in
                 * order to really put it online (install the write handler
                 * so that the accumulated data can be transferred). However
                 * we change the replication state ASAP, since our replica
                 * is technically online now.
                 *
                 * So things work like that:
                 *
                 * 1. We end transferring the RDB file via socket.
                 * 2. The replica is put ONLINE but the write handler
                 *    is not installed.
                 * 3. The replica however goes really online, and pings us
                 *    back via REPLCONF ACK commands.
                 * 4. Now we finally install the write handler, and send
                 *    the buffers accumulated so far to the replica.
                 *
                 * But why we do that? Because the replica, when we stream
                 * the RDB directly via the socket, must detect the RDB
                 * EOF (end of file), that is a special random string at the
                 * end of the RDB (for streamed RDBs we don't know the length
                 * in advance). Detecting such final EOF string is much
                 * simpler and less CPU intensive if no more data is sent
                 * after such final EOF. So we don't want to glue the end of
                 * the RDB transfer with the start of the other replication
                 * data. */
                if (!replicaPutOnline(replica)) {
                    freeClientAsync(replica);
                    continue;
                }
                replica->repl_data->repl_start_cmd_stream_on_ack = 1;
            } else {
                repldbfd = open(server.rdb_filename, O_RDONLY);
                if (repldbfd == -1) {
                    freeClientAsync(replica);
                    serverLog(LL_WARNING, "SYNC failed. Can't open DB after BGSAVE: %s", strerror(errno));
                    continue;
                }
                if (valkey_fstat(repldbfd, &buf) == -1) {
                    freeClientAsync(replica);
                    serverLog(LL_WARNING, "SYNC failed. Can't stat DB after BGSAVE: %s", strerror(errno));
                    close(repldbfd);
                    continue;
                }
                replica->repl_data->repldbfd = repldbfd;
                replica->repl_data->repldboff = 0;
                replica->repl_data->repldbsize = buf.st_size;
                replica->repl_data->repl_state = REPLICA_STATE_SEND_BULK;
                replica->repl_data->replpreamble = sdscatprintf(sdsempty(), "$%lld\r\n", (unsigned long long)replica->repl_data->repldbsize);

                /* When repl_state changes to REPLICA_STATE_SEND_BULK, we will release
                 * the resources in freeClient. */
                connSetWriteHandler(replica->conn, NULL);
                if (connSetWriteHandler(replica->conn, sendBulkToReplica) == C_ERR) {
                    freeClientAsync(replica);
                    continue;
                }
            }
        }
    }
}

/* Change the current instance replication ID with a new, random one.
 * This will prevent successful PSYNCs between this primary and other
 * replicas, so the command should be called when something happens that
 * alters the current story of the dataset. */
void changeReplicationId(void) {
    getRandomHexChars(server.replid, CONFIG_RUN_ID_SIZE);
    server.replid[CONFIG_RUN_ID_SIZE] = '\0';
}

/* Clear (invalidate) the secondary replication ID. This happens, for
 * example, after a full resynchronization, when we start a new replication
 * history. */
void clearReplicationId2(void) {
    memset(server.replid2, '0', sizeof(server.replid));
    server.replid2[CONFIG_RUN_ID_SIZE] = '\0';
    server.second_replid_offset = -1;
}

/* Use the current replication ID / offset as secondary replication
 * ID, and change the current one in order to start a new history.
 * This should be used when an instance is switched from replica to primary
 * so that it can serve PSYNC requests performed using the primary
 * replication ID. */
void shiftReplicationId(void) {
    memcpy(server.replid2, server.replid, sizeof(server.replid));
    /* We set the second replid offset to the primary offset + 1, since
     * the replica will ask for the first byte it has not yet received, so
     * we need to add one to the offset: for example if, as a replica, we are
     * sure we have the same history as the primary for 50 bytes, after we
     * are turned into a primary, we can accept a PSYNC request with offset
     * 51, since the replica asking has the same history up to the 50th
     * byte, and is asking for the new bytes starting at offset 51. */
    server.second_replid_offset = server.primary_repl_offset + 1;
    changeReplicationId();
    serverLog(LL_NOTICE, "Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s",
              server.replid2, server.second_replid_offset, server.replid);
}

/* ----------------------------------- REPLICA -------------------------------- */

/* Returns 1 if the given replication state is a handshake state,
 * 0 otherwise. */
int replicaIsInHandshakeState(void) {
    return server.repl_state >= REPL_STATE_RECEIVE_PING_REPLY && server.repl_state <= REPL_STATE_RECEIVE_PSYNC_REPLY;
}

/* Avoid the primary to detect the replica is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entirely or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyData(), and while we load the new data received as an
 * RDB file from the primary. */
void replicationSendNewlineToPrimary(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        /* Pinging back in this stage is best-effort. */
        if (server.repl_transfer_s) connWrite(server.repl_transfer_s, "\n", 1);
    }
}

/* Callback used by emptyData() while flushing away old data to load
 * the new dataset received by the primary and by discardTempDb()
 * after loading succeeded or failed. */
void replicationEmptyDbCallback(hashtable *d) {
    UNUSED(d);
    if (server.repl_state == REPL_STATE_TRANSFER) replicationSendNewlineToPrimary();
}

/* Once we have a link with the primary and the synchronization was
 * performed, this function materializes the primary client we store
 * at server.primary, starting from the specified file descriptor. */
void replicationCreatePrimaryClientWithHandler(connection *conn, int dbid, ConnectionCallbackFunc handler) {
    server.primary = createClient(conn);
    if (conn) connSetReadHandler(server.primary->conn, handler);

    /**
     * Important note:
     * The CLIENT_DENY_BLOCKING flag is not, and should not, be set here.
     * For commands like BLPOP, it makes no sense to block the primary
     * connection, and such blocking attempt will probably cause deadlock and
     * break the replication. We consider such a thing as a bug because
     * commands as BLPOP should never be sent on the replication link.
     * A possible use-case for blocking the replication link is if a module wants
     * to pass the execution to a background thread and unblock after the
     * execution is done. This is the reason why we allow blocking the replication
     * connection. */
    server.primary->flag.primary = 1;
    server.primary->flag.authenticated = 1;

    /* Allocate a private query buffer for the primary client instead of using the shared query buffer.
     * This is done because the primary's query buffer data needs to be preserved for my sub-replicas to use. */
    server.primary->querybuf = sdsempty();
    initClientReplicationData(server.primary);
    server.primary->repl_data->reploff = server.primary_initial_offset;
    server.primary->repl_data->read_reploff = server.primary->repl_data->reploff;
    server.primary->user = NULL; /* This client can do everything. */
    memcpy(server.primary->repl_data->replid, server.primary_replid, sizeof(server.primary_replid));
    /* If primary offset is set to -1, this primary is old and is not
     * PSYNC capable, so we flag it accordingly. */
    if (server.primary->repl_data->reploff == -1) server.primary->flag.pre_psync = 1;
    if (dbid != -1) selectDb(server.primary, dbid);
}

/* Wrapper for replicationCreatePrimaryClientWithHandler, init primary connection handler
 * with ordinary client connection handler */
void replicationCreatePrimaryClient(connection *conn, int dbid) {
    replicationCreatePrimaryClientWithHandler(conn, dbid, readQueryFromClient);
}

/* This function will try to re-enable the AOF file after the
 * primary-replica synchronization: if it fails after multiple attempts
 * the replica cannot be considered reliable and exists with an
 * error. */
void restartAOFAfterSYNC(void) {
    unsigned int tries, max_tries = 10;
    for (tries = 0; tries < max_tries; ++tries) {
        if (startAppendOnly() == C_OK) break;
        serverLog(LL_WARNING, "Failed enabling the AOF after successful primary synchronization! "
                              "Trying it again in one second.");
        sleep(1);
    }
    if (tries == max_tries) {
        serverLog(LL_WARNING, "FATAL: this replica instance finished the synchronization with "
                              "its primary, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

static int useDisklessLoad(void) {
    /* compute boolean decision to use diskless load */
    int enabled = server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB ||
                  server.repl_diskless_load == REPL_DISKLESS_LOAD_FLUSH_BEFORE_LOAD ||
                  (server.repl_diskless_load == REPL_DISKLESS_LOAD_WHEN_DB_EMPTY && dbTotalServerKeyCount() == 0);

    if (enabled) {
        /* Check all modules handle read errors, otherwise it's not safe to use diskless load. */
        if (!moduleAllDatatypesHandleErrors()) {
            serverLog(LL_NOTICE, "Skipping diskless-load because there are modules that don't handle read errors.");
            enabled = 0;
        }
        /* Check all modules handle async replication, otherwise it's not safe to use diskless load. */
        else if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB && !moduleAllModulesHandleReplAsyncLoad()) {
            serverLog(LL_NOTICE,
                      "Skipping diskless-load because there are modules that are not aware of async replication.");
            enabled = 0;
        }
    }
    return enabled;
}

/* Helper function for readSyncBulkPayload() to initialize tempDb
 * before socket-loading the new db from primary. The tempDb may be populated
 * by swapMainDbWithTempDb or freed by disklessLoadDiscardTempDb later. */
serverDb *disklessLoadInitTempDb(void) {
    return initTempDb();
}

/* Helper function for readSyncBulkPayload() to discard our tempDb
 * when the loading succeeded or failed. */
void disklessLoadDiscardTempDb(serverDb *tempDb) {
    discardTempDb(tempDb);
}

/* Helper function for to initialize temp function lib context.
 * The temp ctx may be populated by functionsLibCtxSwapWithCurrent or
 * freed by disklessLoadDiscardFunctionsLibCtx later. */
functionsLibCtx *disklessLoadFunctionsLibCtxCreate(void) {
    return functionsLibCtxCreate();
}

/* Helper function to discard our temp function lib context
 * when the loading succeeded or failed. */
void disklessLoadDiscardFunctionsLibCtx(functionsLibCtx *temp_functions_lib_ctx) {
    freeFunctionsAsync(temp_functions_lib_ctx);
}

/* If we know we got an entirely different data set from our primary
 * we have no way to incrementally feed our replicas after that.
 * We want our replicas to resync with us as well, if we have any sub-replicas.
 * This is useful on readSyncBulkPayload in places where we just finished transferring db. */
void replicationAttachToNewPrimary(void) {
    /* Replica starts to apply data from new primary, we must discard the cached
     * primary structure. */
    serverAssert(server.primary == NULL);
    replicationDiscardCachedPrimary();

    disconnectReplicas();     /* Force our replicas to resync with us as well. */
    freeReplicationBacklog(); /* Don't allow our chained replicas to PSYNC. */
}

/* Asynchronously read the SYNC payload we receive from a primary */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024 * 1024 * 8) /* 8 MB */
void readSyncBulkPayload(connection *conn) {
    char buf[PROTO_IOBUF_LEN];
    ssize_t nread, readlen, nwritten;
    int use_diskless_load = useDisklessLoad();
    serverDb *diskless_load_tempDb = NULL;
    functionsLibCtx *temp_functions_lib_ctx = NULL;
    int empty_db_flags = server.repl_replica_lazy_flush ? EMPTYDB_ASYNC : EMPTYDB_NO_FLAGS;
    off_t left;

    /* Static vars used to hold the EOF mark, and the last bytes received
     * from the server: when they match, we reached the end of the transfer. */
    static char eofmark[RDB_EOF_MARK_SIZE];
    static char lastbytes[RDB_EOF_MARK_SIZE];
    static int usemark = 0;

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the primary reply. */
    if (server.repl_transfer_size == -1) {
        nread = connSyncReadLine(conn, buf, 1024, server.repl_syncio_timeout * 1000);
        if (nread == -1) {
            serverLog(LL_WARNING, "I/O error reading bulk count from PRIMARY: %s", connGetLastError(conn));
            goto error;
        } else {
            /* nread here is returned by connSyncReadLine(), which calls syncReadLine() and
             * convert "\r\n" to '\0' so 1 byte is lost. */
            server.stat_net_repl_input_bytes += nread + 1;
        }

        if (buf[0] == '-') {
            serverLog(LL_WARNING, "PRIMARY aborted replication with an error: %s", buf + 1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            serverLog(LL_WARNING,
                      "Bad protocol from PRIMARY, the first byte is not '$' (we received '%s'), are you sure the host "
                      "and port are right?",
                      buf);
            goto error;
        }

        /* There are two possible forms for the bulk payload. One is the
         * usual $<count> bulk format. The other is used for diskless transfers
         * when the primary does not know beforehand the size of the file to
         * transfer. In the latter case, the following format is used:
         *
         * $EOF:<40 bytes delimiter>
         *
         * At the end of the file the announced delimiter is transmitted. The
         * delimiter is long and random enough that the probability of a
         * collision with the actual file content can be ignored. */
        if (strncmp(buf + 1, "EOF:", 4) == 0 && strlen(buf + 5) >= RDB_EOF_MARK_SIZE) {
            usemark = 1;
            memcpy(eofmark, buf + 5, RDB_EOF_MARK_SIZE);
            memset(lastbytes, 0, RDB_EOF_MARK_SIZE);
            /* Set any repl_transfer_size to avoid entering this code path
             * at the next call. */
            server.repl_transfer_size = 0;
            serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: receiving streamed RDB from primary with EOF %s",
                      use_diskless_load ? "to parser" : "to disk");
        } else {
            usemark = 0;
            server.repl_transfer_size = strtol(buf + 1, NULL, 10);
            serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: receiving %lld bytes from primary %s",
                      (long long)server.repl_transfer_size, use_diskless_load ? "to parser" : "to disk");
        }
        return;
    }

    if (!use_diskless_load) {
        /* Read the data from the socket, store it to a file and search
         * for the EOF. */
        if (usemark) {
            readlen = sizeof(buf);
        } else {
            left = server.repl_transfer_size - server.repl_transfer_read;
            readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
        }

        nread = connRead(conn, buf, readlen);
        if (nread <= 0) {
            if (connGetState(conn) == CONN_STATE_CONNECTED) {
                /* equivalent to EAGAIN */
                return;
            }
            serverLog(LL_WARNING, "I/O error trying to sync with PRIMARY: %s",
                      (nread == -1) ? connGetLastError(conn) : "connection lost");
            goto error;
        }
        server.stat_net_repl_input_bytes += nread;

        /* When a mark is used, we want to detect EOF asap in order to avoid
         * writing the EOF mark into the file... */
        int eof_reached = 0;

        if (usemark) {
            /* Update the last bytes array, and check if it matches our
             * delimiter. */
            if (nread >= RDB_EOF_MARK_SIZE) {
                memcpy(lastbytes, buf + nread - RDB_EOF_MARK_SIZE, RDB_EOF_MARK_SIZE);
            } else {
                int rem = RDB_EOF_MARK_SIZE - nread;
                memmove(lastbytes, lastbytes + nread, rem);
                memcpy(lastbytes + rem, buf, nread);
            }
            if (memcmp(lastbytes, eofmark, RDB_EOF_MARK_SIZE) == 0) eof_reached = 1;
        }

        /* Update the last I/O time for the replication transfer (used in
         * order to detect timeouts during replication), and write what we
         * got from the socket to the dump file on disk. */
        server.repl_transfer_lastio = server.unixtime;
        if ((nwritten = write(server.repl_transfer_fd, buf, nread)) != nread) {
            serverLog(LL_WARNING,
                      "Write error or short write writing to the DB dump file "
                      "needed for PRIMARY <-> REPLICA synchronization: %s",
                      (nwritten == -1) ? strerror(errno) : "short write");
            goto error;
        }
        server.repl_transfer_read += nread;

        /* Delete the last 40 bytes from the file if we reached EOF. */
        if (usemark && eof_reached) {
            if (ftruncate(server.repl_transfer_fd, server.repl_transfer_read - RDB_EOF_MARK_SIZE) == -1) {
                serverLog(LL_WARNING,
                          "Error truncating the RDB file received from the primary "
                          "for SYNC: %s",
                          strerror(errno));
                goto error;
            }
        }

        /* Sync data on disk from time to time, otherwise at the end of the
         * transfer we may suffer a big delay as the memory buffers are copied
         * into the actual disk. */
        if (server.repl_transfer_read >= server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC) {
            off_t sync_size = server.repl_transfer_read - server.repl_transfer_last_fsync_off;
            rdb_fsync_range(server.repl_transfer_fd, server.repl_transfer_last_fsync_off, sync_size);
            server.repl_transfer_last_fsync_off += sync_size;
        }

        /* Check if the transfer is now complete */
        if (!usemark) {
            if (server.repl_transfer_read == server.repl_transfer_size) eof_reached = 1;
        }

        /* If the transfer is yet not complete, we need to read more, so
         * return ASAP and wait for the handler to be called again. */
        if (!eof_reached) return;
    }

    /* We reach this point in one of the following cases:
     *
     * 1. The replica is using diskless replication, that is, it reads data
     *    directly from the socket to the server memory, without using
     *    a temporary RDB file on disk. In that case we just block and
     *    read everything from the socket.
     *
     * 2. Or when we are done reading from the socket to the RDB file, in
     *    such case we want just to read the RDB file in memory. */

    /* We need to stop any AOF rewriting child before flushing and parsing
     * the RDB, otherwise we'll create a copy-on-write disaster. */
    if (server.aof_state != AOF_OFF) stopAppendOnly();
    /* Also try to stop save RDB child before flushing and parsing the RDB:
     * 1. Ensure background save doesn't overwrite synced data after being loaded.
     * 2. Avoid copy-on-write disaster. */
    if (server.child_type == CHILD_TYPE_RDB) {
        if (!use_diskless_load) {
            serverLog(LL_NOTICE,
                      "Replica is about to load the RDB file received from the "
                      "primary, but there is a pending RDB child running. "
                      "Killing process %ld and removing its temp file to avoid "
                      "any race",
                      (long)server.child_pid);
        }
        killRDBChild();
    }

    if (use_diskless_load && server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
        /* Initialize empty tempDb dictionaries. */
        diskless_load_tempDb = disklessLoadInitTempDb();
        temp_functions_lib_ctx = disklessLoadFunctionsLibCtxCreate();

        moduleFireServerEvent(VALKEYMODULE_EVENT_REPL_ASYNC_LOAD, VALKEYMODULE_SUBEVENT_REPL_ASYNC_LOAD_STARTED, NULL);
    }

    /* Before loading the DB into memory we need to delete the readable
     * handler, otherwise it will get called recursively since
     * rdbLoad() will call the event loop to process events from time to
     * time for non blocking loading. */
    connSetReadHandler(conn, NULL);

    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
    if (use_diskless_load) {
        rio rdb;
        serverDb *dbarray;
        functionsLibCtx *functions_lib_ctx;
        int asyncLoading = 0;

        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* Async loading means we continue serving read commands during full resync, and
             * "swap" the new db with the old db only when loading is done.
             * It is enabled only on SWAPDB diskless replication when primary replication ID hasn't changed,
             * because in that state the old content of the db represents a different point in time of the same
             * data set we're currently receiving from the primary. */
            if (memcmp(server.replid, server.primary_replid, CONFIG_RUN_ID_SIZE) == 0) {
                asyncLoading = 1;
            }
            dbarray = diskless_load_tempDb;
            functions_lib_ctx = temp_functions_lib_ctx;
        } else {
            /* We will soon start loading the RDB from socket, the replication history is changed,
             * we must discard the cached primary structure and force resync of sub-replicas. */
            replicationAttachToNewPrimary();

            /* Even though we are on-empty-db and the database is empty, we still call emptyData. */
            serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Flushing old data");
            emptyData(-1, empty_db_flags, replicationEmptyDbCallback);

            dbarray = server.db;
            functions_lib_ctx = functionsLibCtxGetCurrent();
        }

        rioInitWithConn(&rdb, conn, server.repl_transfer_size);

        /* Put the socket in blocking mode to simplify RDB transfer.
         * We'll restore it when the RDB is received. */
        connBlock(conn);
        connRecvTimeout(conn, server.repl_timeout * 1000);

        serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Loading DB in memory");
        startLoading(server.repl_transfer_size, RDBFLAGS_REPLICATION, asyncLoading);

        int loadingFailed = 0;
        rdbLoadingCtx loadingCtx = {.dbarray = dbarray, .functions_lib_ctx = functions_lib_ctx};
        if (rdbLoadRioWithLoadingCtxScopedRdb(&rdb, RDBFLAGS_REPLICATION, &rsi, &loadingCtx) != C_OK) {
            /* RDB loading failed. */
            serverLog(LL_WARNING, "Failed trying to load the PRIMARY synchronization DB "
                                  "from socket, check server logs.");
            loadingFailed = 1;
        } else if (usemark) {
            /* Verify the end mark is correct. */
            if (!rioRead(&rdb, buf, RDB_EOF_MARK_SIZE) || memcmp(buf, eofmark, RDB_EOF_MARK_SIZE) != 0) {
                serverLog(LL_WARNING, "Replication stream EOF marker is broken");
                loadingFailed = 1;
            }
        }

        if (loadingFailed) {
            stopLoading(0);
            rioFreeConn(&rdb, NULL);

            if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
                /* Discard potentially partially loaded tempDb. */
                moduleFireServerEvent(VALKEYMODULE_EVENT_REPL_ASYNC_LOAD, VALKEYMODULE_SUBEVENT_REPL_ASYNC_LOAD_ABORTED,
                                      NULL);

                disklessLoadDiscardTempDb(diskless_load_tempDb);
                disklessLoadDiscardFunctionsLibCtx(temp_functions_lib_ctx);
                serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Discarding temporary DB in background");
            } else {
                /* Remove the half-loaded data in case we started with an empty replica. */
                serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Discarding the half-loaded data");
                emptyData(-1, empty_db_flags, replicationEmptyDbCallback);
            }

            /* Note that there's no point in restarting the AOF on SYNC
             * failure, it'll be restarted when sync succeeds or the replica
             * gets promoted. */
            goto error;
        }

        /* RDB loading succeeded if we reach this point. */
        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* We will soon swap main db with tempDb and replicas will start
             * to apply data from new primary, we must discard the cached
             * primary structure and force resync of sub-replicas. */
            replicationAttachToNewPrimary();

            serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Swapping active DB with loaded DB");
            swapMainDbWithTempDb(diskless_load_tempDb);

            /* swap existing functions ctx with the temporary one */
            functionsLibCtxSwapWithCurrent(temp_functions_lib_ctx, 1);

            moduleFireServerEvent(VALKEYMODULE_EVENT_REPL_ASYNC_LOAD, VALKEYMODULE_SUBEVENT_REPL_ASYNC_LOAD_COMPLETED,
                                  NULL);

            /* Delete the old db as it's useless now. */
            disklessLoadDiscardTempDb(diskless_load_tempDb);
            serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Discarding old DB in background");
        }

        /* Inform about db change, as replication was diskless and didn't cause a save. */
        server.dirty++;

        stopLoading(1);

        /* Cleanup and restore the socket to the original state to continue
         * with the normal replication. */
        rioFreeConn(&rdb, NULL);
        connNonBlock(conn);
        connRecvTimeout(conn, 0);
    } else {
        /* Make sure the new file (also used for persistence) is fully synced
         * (not covered by earlier calls to rdb_fsync_range). */
        if (fsync(server.repl_transfer_fd) == -1) {
            serverLog(LL_WARNING,
                      "Failed trying to sync the temp DB to disk in "
                      "PRIMARY <-> REPLICA synchronization: %s",
                      strerror(errno));
            goto error;
        }

        /* Rename rdb like renaming rewrite aof asynchronously. */
        int old_rdb_fd = open(server.rdb_filename, O_RDONLY | O_NONBLOCK);
        if (rename(server.repl_transfer_tmpfile, server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                      "Failed trying to rename the temp DB into %s in "
                      "PRIMARY <-> REPLICA synchronization: %s",
                      server.rdb_filename, strerror(errno));
            if (old_rdb_fd != -1) close(old_rdb_fd);
            goto error;
        }
        /* Close old rdb asynchronously. */
        if (old_rdb_fd != -1) bioCreateCloseJob(old_rdb_fd, 0, 0);

        /* Sync the directory to ensure rename is persisted */
        if (fsyncFileDir(server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                      "Failed trying to sync DB directory %s in "
                      "PRIMARY <-> REPLICA synchronization: %s",
                      server.rdb_filename, strerror(errno));
            goto error;
        }

        /* We will soon start loading the RDB from disk, the replication history is changed,
         * we must discard the cached primary structure and force resync of sub-replicas. */
        replicationAttachToNewPrimary();

        /* Empty the databases only after the RDB file is ok, that is, before the RDB file
         * is actually loaded, in case we encounter an error and drop the replication stream
         * and leave an empty database. */
        serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Flushing old data");
        emptyData(-1, empty_db_flags, replicationEmptyDbCallback);

        serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Loading DB in memory");
        if (rdbLoad(server.rdb_filename, &rsi, RDBFLAGS_REPLICATION) != RDB_OK) {
            serverLog(LL_WARNING, "Failed trying to load the PRIMARY synchronization "
                                  "DB from disk, check server logs.");
            if (server.rdb_del_sync_files && allPersistenceDisabled()) {
                serverLog(LL_NOTICE, "Removing the RDB file obtained from "
                                     "the primary. This replica has persistence "
                                     "disabled");
                bg_unlink(server.rdb_filename);
            }

            /* If disk-based RDB loading fails, remove the half-loaded dataset. */
            serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Discarding the half-loaded data");
            emptyData(-1, empty_db_flags, replicationEmptyDbCallback);

            /* Note that there's no point in restarting the AOF on sync failure,
               it'll be restarted when sync succeeds or replica promoted. */
            goto error;
        }

        /* Cleanup. */
        if (server.rdb_del_sync_files && allPersistenceDisabled()) {
            serverLog(LL_NOTICE, "Removing the RDB file obtained from "
                                 "the primary. This replica has persistence "
                                 "disabled");
            bg_unlink(server.rdb_filename);
        }

        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);
        server.repl_transfer_fd = -1;
        server.repl_transfer_tmpfile = NULL;
    }

    /* Final setup of the connected replica <- primary link */
    if (conn == server.repl_rdb_transfer_s) {
        dualChannelSyncHandleRdbLoadCompletion();
    } else {
        replicationCreatePrimaryClient(server.repl_transfer_s, rsi.repl_stream_db);
        server.repl_state = REPL_STATE_CONNECTED;
        server.repl_down_since = 0;
        /* Send the initial ACK immediately to put this replica in online state. */
        replicationSendAck();
    }

    /* Fire the primary link modules event. */
    moduleFireServerEvent(VALKEYMODULE_EVENT_PRIMARY_LINK_CHANGE, VALKEYMODULE_SUBEVENT_PRIMARY_LINK_UP, NULL);
    if (server.repl_state == REPL_STATE_CONNECTED) {
        /* After a full resynchronization we use the replication ID and
         * offset of the primary. The secondary ID / offset are cleared since
         * we are starting a new history. */
        memcpy(server.replid, server.primary->repl_data->replid, sizeof(server.replid));
        server.primary_repl_offset = server.primary->repl_data->reploff;
    }
    clearReplicationId2();

    /* Let's create the replication backlog if needed. Replicas need to
     * accumulate the backlog regardless of the fact they have sub-replicas
     * or not, in order to behave correctly if they are promoted to
     * primaries after a failover. */
    if (server.repl_backlog == NULL) createReplicationBacklog();
    serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Finished with success");

    if (server.supervised_mode == SUPERVISED_SYSTEMD) {
        serverCommunicateSystemd("STATUS=PRIMARY <-> REPLICA sync: Finished with success. Ready to accept connections "
                                 "in read-write mode.\n");
    }

    /* Restart the AOF subsystem now that we finished the sync. This
     * will trigger an AOF rewrite, and when done will start appending
     * to the new file. */
    if (server.aof_enabled) restartAOFAfterSYNC();

    /* In case of dual channel replication sync we want to close the RDB connection
     * once the connection is established */
    if (conn == server.repl_rdb_transfer_s) {
        connClose(conn);
        server.repl_rdb_transfer_s = NULL;
    }
    return;

error:
    cancelReplicationHandshake(1);
    return;
}

char *receiveSynchronousResponse(connection *conn) {
    char buf[256];
    /* Read the reply from the server. */
    if (connSyncReadLine(conn, buf, sizeof(buf), server.repl_syncio_timeout * 1000) == -1) {
        serverLog(LL_WARNING, "Failed to read response from the server: %s", connGetLastError(conn));
        return NULL;
    }
    server.repl_transfer_lastio = server.unixtime;
    return sdsnew(buf);
}

/* Send a pre-formatted multi-bulk command to the connection. */
char *sendCommandRaw(connection *conn, sds cmd) {
    if (connSyncWrite(conn, cmd, sdslen(cmd), server.repl_syncio_timeout * 1000) == -1) {
        return sdscatprintf(sdsempty(), "-Writing to master: %s", connGetLastError(conn));
    }
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection.
 * Used to send AUTH and REPLCONF commands to the primary before starting the
 * replication.
 *
 * Takes a list of char* arguments, terminated by a NULL argument.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendCommand(connection *conn, ...) {
    va_list ap;
    sds cmd = sdsempty();
    sds cmdargs = sdsempty();
    size_t argslen = 0;
    char *arg;

    /* Create the command to send to the primary, we use binary
     * protocol to make sure correct arguments are sent. This function
     * is not safe for all binary data. */
    va_start(ap, conn);
    while (1) {
        arg = va_arg(ap, char *);
        if (arg == NULL) break;
        cmdargs = sdscatprintf(cmdargs, "$%zu\r\n%s\r\n", strlen(arg), arg);
        argslen++;
    }

    cmd = sdscatprintf(cmd, "*%zu\r\n", argslen);
    cmd = sdscatsds(cmd, cmdargs);
    sdsfree(cmdargs);

    va_end(ap);
    char *err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if (err) return err;
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection.
 * Used to send AUTH and REPLCONF commands to the primary before starting the
 * replication.
 *
 * argv_lens is optional, when NULL, strlen is used.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendCommandArgv(connection *conn, int argc, char **argv, size_t *argv_lens) {
    sds cmd = sdsempty();
    char *arg;
    int i;

    /* Create the command to send to the primary. */
    cmd = sdscatfmt(cmd, "*%i\r\n", argc);
    for (i = 0; i < argc; i++) {
        int len;
        arg = argv[i];
        len = argv_lens ? argv_lens[i] : strlen(arg);
        cmd = sdscatfmt(cmd, "$%i\r\n", len);
        cmd = sdscatlen(cmd, arg, len);
        cmd = sdscatlen(cmd, "\r\n", 2);
    }
    char *err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if (err) return err;
    return NULL;
}

/* Replication: Replica side.
 * Returns an sds represent this replica port to be used by the primary (mostly
 * for logs) */
sds getReplicaPortString(void) {
    long long replica_port;
    if (server.replica_announce_port) {
        replica_port = server.replica_announce_port;
    } else if (server.tls_replication && server.tls_port) {
        replica_port = server.tls_port;
    } else {
        replica_port = server.port;
    }
    return sdsfromlonglong(replica_port);
}

/* Replication: Replica side.
 * Free replica's local replication buffer */
void freePendingReplDataBuf(void) {
    listRelease(server.pending_repl_data.blocks);
    server.pending_repl_data.blocks = NULL;
    server.pending_repl_data.len = 0;
}

/* Replication: Replica side.
 * Upon dual-channel sync failure, close rdb-connection, reset repl-state, reset
 * provisional primary struct, and free local replication buffer. */
void replicationAbortDualChannelSyncTransfer(void) {
    serverAssert(server.repl_rdb_channel_state != REPL_DUAL_CHANNEL_STATE_NONE);
    dualChannelServerLog(LL_NOTICE, "Aborting dual channel sync");
    if (server.repl_rdb_transfer_s) {
        connClose(server.repl_rdb_transfer_s);
        server.repl_rdb_transfer_s = NULL;
    }
    zfree(server.repl_transfer_tmpfile);
    server.repl_transfer_tmpfile = NULL;
    if (server.repl_transfer_fd != -1) {
        close(server.repl_transfer_fd);
        server.repl_transfer_fd = -1;
    }
    server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_STATE_NONE;
    server.repl_provisional_primary.read_reploff = 0;
    server.repl_provisional_primary.reploff = 0;
    server.repl_provisional_primary.conn = NULL;
    server.repl_provisional_primary.dbid = -1;
    server.rdb_client_id = -1;
    freePendingReplDataBuf();
    return;
}

/* Replication: Primary side.
 * Send current replication offset to replica. Use the following structure:
 * $ENDOFF:<repl-offset> <primary-repl-id> <current-db-id> <client-id> */
int sendCurrentOffsetToReplica(client *replica) {
    char buf[128];
    int buflen;
    buflen = snprintf(buf, sizeof(buf), "$ENDOFF:%lld %s %d %llu\r\n", server.primary_repl_offset, server.replid,
                      server.db->id, (long long unsigned int)replica->id);
    dualChannelServerLog(LL_NOTICE, "Sending to replica %s RDB end offset %lld and client-id %llu",
                         replicationGetReplicaName(replica), server.primary_repl_offset,
                         (long long unsigned int)replica->id);
    if (connSyncWrite(replica->conn, buf, buflen, server.repl_syncio_timeout * 1000) != buflen) {
        freeClientAsync(replica);
        return C_ERR;
    }
    return C_OK;
}

static int dualChannelReplHandleHandshake(connection *conn, sds *err) {
    dualChannelServerLog(LL_DEBUG, "Received first reply from primary using rdb connection.");
    /* AUTH with the primary if required. */
    if (server.primary_auth) {
        char *args[] = {"AUTH", NULL, NULL};
        size_t lens[] = {4, 0, 0};
        int argc = 1;
        if (server.primary_user) {
            args[argc] = server.primary_user;
            lens[argc] = strlen(server.primary_user);
            argc++;
        }
        args[argc] = server.primary_auth;
        lens[argc] = sdslen(server.primary_auth);
        argc++;
        *err = sendCommandArgv(conn, argc, args, lens);
        if (*err) {
            dualChannelServerLog(LL_WARNING, "Sending command to primary in dual channel replication handshake: %s", *err);
            return C_ERR;
        }
    }
    /* Send replica listening port to primary for clarification */
    sds portstr = getReplicaPortString();
    *err = sendCommand(conn, "REPLCONF", "capa", "eof", "rdb-only", "1", "rdb-channel", "1", "listening-port", portstr,
                       NULL);
    sdsfree(portstr);
    if (*err) {
        dualChannelServerLog(LL_WARNING, "Sending command to primary in dual channel replication handshake: %s", *err);
        return C_ERR;
    }

    if (connSetReadHandler(conn, dualChannelFullSyncWithPrimary) == C_ERR) {
        char conninfo[CONN_INFO_LEN];
        dualChannelServerLog(LL_WARNING, "Can't create readable event for SYNC: %s (%s)", strerror(errno),
                             connGetInfo(conn, conninfo, sizeof(conninfo)));
        return C_ERR;
    }
    return C_OK;
}

static int dualChannelReplHandleAuthReply(connection *conn, sds *err) {
    *err = receiveSynchronousResponse(conn);
    if (*err == NULL) {
        dualChannelServerLog(LL_WARNING, "Primary did not respond to auth command during SYNC handshake");
        return C_ERR;
    }
    if ((*err)[0] == '-') {
        dualChannelServerLog(LL_WARNING, "Unable to AUTH to Primary: %s", *err);
        return C_ERR;
    }
    server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_RECEIVE_REPLCONF_REPLY;
    return C_OK;
}

static int dualChannelReplHandleReplconfReply(connection *conn, sds *err) {
    *err = receiveSynchronousResponse(conn);
    if (*err == NULL) {
        dualChannelServerLog(LL_WARNING, "Primary did not respond to replconf command during SYNC handshake");
        return C_ERR;
    }

    if (*err[0] == '-') {
        dualChannelServerLog(LL_NOTICE, "Server does not support sync with offset, dual channel sync approach cannot be used: %s",
                             *err);
        return C_ERR;
    }
    if (connSyncWrite(conn, "SYNC\r\n", 6, server.repl_syncio_timeout * 1000) == -1) {
        dualChannelServerLog(LL_WARNING, "I/O error writing to Primary: %s", connGetLastError(conn));
        return C_ERR;
    }
    return C_OK;
}

static int dualChannelReplHandleEndOffsetResponse(connection *conn, sds *err) {
    uint64_t rdb_client_id;
    *err = receiveSynchronousResponse(conn);
    if (*err == NULL) {
        return C_ERR;
    }
    if (*err[0] == '\0') {
        /* Retry again later */
        dualChannelServerLog(LL_DEBUG, "Received empty $ENDOFF response");
        return C_RETRY;
    }
    long long reploffset;
    char primary_replid[CONFIG_RUN_ID_SIZE + 1];
    int dbid;
    /* Parse end offset response */
    char *endoff_format = "$ENDOFF:%lld %40s %d %llu";
    if (sscanf(*err, endoff_format, &reploffset, primary_replid, &dbid, &rdb_client_id) != 4) {
        dualChannelServerLog(LL_WARNING, "Received unexpected $ENDOFF response: %s", *err);
        return C_ERR;
    }
    server.rdb_client_id = rdb_client_id;
    server.primary_initial_offset = reploffset;

    /* Initiate repl_provisional_primary to act as this replica temp primary until RDB is loaded */
    server.repl_provisional_primary.conn = server.repl_transfer_s;
    memcpy(server.repl_provisional_primary.replid, primary_replid, sizeof(server.repl_provisional_primary.replid));
    server.repl_provisional_primary.reploff = reploffset;
    server.repl_provisional_primary.read_reploff = reploffset;
    server.repl_provisional_primary.dbid = dbid;

    /* Now that we have the snapshot end-offset, we can ask for psync from that offset. Prepare the
     * main connection accordingly.*/
    server.repl_transfer_s->state = CONN_STATE_CONNECTED;
    server.repl_state = REPL_STATE_SEND_HANDSHAKE;
    serverAssert(connSetReadHandler(server.repl_transfer_s, dualChannelSetupMainConnForPsync) != C_ERR);
    dualChannelSetupMainConnForPsync(server.repl_transfer_s);

    /* As the next block we will receive using this connection is the rdb, we need to prepare
     * the connection accordingly */
    serverAssert(connSetReadHandler(server.repl_rdb_transfer_s, readSyncBulkPayload) != C_ERR);
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_lastio = server.unixtime;

    return C_OK;
}

/* Replication: Replica side.
 * This connection handler is used to initialize the RDB connection (dual-channel-replication).
 * Once a replica with dual-channel-replication enabled, denied from PSYNC with its primary,
 * dualChannelFullSyncWithPrimary begins its role. The connection handler prepares server.repl_rdb_transfer_s
 * for a rdb stream, and server.repl_transfer_s for incremental replication data stream. */
static void dualChannelFullSyncWithPrimary(connection *conn) {
    char *err = NULL;
    int ret = 0;
    serverAssert(conn == server.repl_rdb_transfer_s);
    /* If this event fired after the user turned the instance into a primary
     * with REPLICAOF NO ONE we must just return ASAP. */
    if (server.repl_state == REPL_STATE_NONE) {
        goto error;
    }
    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state. */
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        dualChannelServerLog(LL_WARNING, "Error condition on socket for dual channel replication: %s",
                             connGetLastError(conn));
        goto error;
    }
    switch (server.repl_rdb_channel_state) {
    case REPL_DUAL_CHANNEL_SEND_HANDSHAKE:
        ret = dualChannelReplHandleHandshake(conn, &err);
        if (ret == C_OK) server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_RECEIVE_AUTH_REPLY;
        break;
    case REPL_DUAL_CHANNEL_RECEIVE_AUTH_REPLY:
        if (server.primary_auth) {
            ret = dualChannelReplHandleAuthReply(conn, &err);
            if (ret == C_OK) server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_RECEIVE_REPLCONF_REPLY;
            /* Wait for next bulk before trying to read replconf reply. */
            break;
        }
        server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_RECEIVE_REPLCONF_REPLY;
        /* fall through */
    case REPL_DUAL_CHANNEL_RECEIVE_REPLCONF_REPLY:
        ret = dualChannelReplHandleReplconfReply(conn, &err);
        if (ret == C_OK) server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_RECEIVE_ENDOFF;
        break;
    case REPL_DUAL_CHANNEL_RECEIVE_ENDOFF:
        ret = dualChannelReplHandleEndOffsetResponse(conn, &err);
        if (ret == C_OK) server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_RDB_LOAD;
        break;
    default:
        serverPanic("Unexpected dual replication state: %d", server.repl_rdb_channel_state);
    }
    if (ret == C_ERR) goto error;
    sdsfree(err);
    return;

error:
    if (err) {
        serverLog(LL_WARNING, "Dual channel sync failed with error %s", err);
        sdsfree(err);
    }
    if (server.repl_transfer_s) {
        connClose(server.repl_transfer_s);
        server.repl_transfer_s = NULL;
    }
    if (server.repl_rdb_transfer_s) {
        connClose(server.repl_rdb_transfer_s);
        server.repl_rdb_transfer_s = NULL;
    }
    if (server.repl_transfer_fd != -1) close(server.repl_transfer_fd);
    server.repl_transfer_fd = -1;
    server.repl_state = REPL_STATE_CONNECT;
    replicationAbortDualChannelSyncTransfer();
}

/* Replication: Replica side.
 * Initialize server.pending_repl_data infrastructure, we will allocate the buffer
 * itself once we need it */
void replDataBufInit(void) {
    serverAssert(server.pending_repl_data.blocks == NULL);
    server.pending_repl_data.len = 0;
    server.pending_repl_data.peak = 0;
    server.pending_repl_data.blocks = listCreate();
    server.pending_repl_data.blocks->free = zfree;
}

/* Replication: Replica side.
 * Track the local repl-data buffer streaming progress and serve clients from time to time */
void replStreamProgressCallback(size_t offset, int readlen, time_t *last_progress_callback) {
    time_t now = mstime();
    if (server.loading_process_events_interval_bytes &&
        ((offset + readlen) / server.loading_process_events_interval_bytes >
         offset / server.loading_process_events_interval_bytes) &&
        (now - *last_progress_callback > server.loading_process_events_interval_ms)) {
        replicationSendNewlineToPrimary();
        processEventsWhileBlocked();
        *last_progress_callback = now;
    }
}

/* Link list block, used by replDataBuf during dual-channel-replication to store
 * replication data */
typedef struct replDataBufBlock {
    size_t size, used;
    char buf[];
} replDataBufBlock;

/* Replication: Replica side.
 * Reads replication data from primary into specified repl buffer block */
int readIntoReplDataBlock(connection *conn, replDataBufBlock *data_block, size_t read) {
    int nread = connRead(conn, data_block->buf + data_block->used, read);
    if (nread <= 0) {
        if (nread == 0 || connGetState(conn) != CONN_STATE_CONNECTED) {
            dualChannelServerLog(LL_WARNING, "Provisional primary closed connection");
            /* Signal ongoing RDB load to terminate gracefully */
            if (server.loading_rio) rioCloseASAP(server.loading_rio);
            cancelReplicationHandshake(1);
        }
        return C_ERR;
    }
    data_block->used += nread;
    server.stat_total_reads_processed++;
    return read - nread;
}

/* Replication: Replica side.
 * Read handler for buffering incoming repl data during RDB download/loading. */
void bufferReplData(connection *conn) {
    size_t readlen = PROTO_IOBUF_LEN;
    int remaining_bytes = 0;

    while (readlen > 0) {
        listNode *ln = listLast(server.pending_repl_data.blocks);
        replDataBufBlock *tail = ln ? listNodeValue(ln) : NULL;

        /* Append to tail string when possible */
        if (tail && tail->used < tail->size) {
            size_t avail = tail->size - tail->used;
            remaining_bytes = min(readlen, avail);
            readlen -= remaining_bytes;
            remaining_bytes = readIntoReplDataBlock(conn, tail, remaining_bytes);
        }
        if (readlen && remaining_bytes == 0) {
            if (server.client_obuf_limits[CLIENT_TYPE_REPLICA].hard_limit_bytes &&
                server.pending_repl_data.len > server.client_obuf_limits[CLIENT_TYPE_REPLICA].hard_limit_bytes) {
                dualChannelServerLog(LL_NOTICE, "Replication buffer limit reached, stopping buffering.");
                /* Stop accumulating primary commands. */
                connSetReadHandler(conn, NULL);
                break;
            }
            /* Create a new node, make sure it is allocated to at least PROTO_REPLY_CHUNK_BYTES.
             * Use the same upper boundary as the shared replication buffer (feedReplicationBuffer),
             * as they share the same purpose */
            size_t usable_size;
            size_t limit = max((size_t)server.repl_backlog_size / 16, (size_t)PROTO_REPLY_CHUNK_BYTES);
            size_t size = min(max(readlen, (size_t)PROTO_REPLY_CHUNK_BYTES), limit);
            tail = zmalloc_usable(size + sizeof(replDataBufBlock), &usable_size);
            tail->size = usable_size - sizeof(replDataBufBlock);
            tail->used = 0;
            listAddNodeTail(server.pending_repl_data.blocks, tail);
            server.pending_repl_data.len += tail->size;
            /* Update buffer's peak */
            if (server.pending_repl_data.peak < server.pending_repl_data.len)
                server.pending_repl_data.peak = server.pending_repl_data.len;

            remaining_bytes = min(readlen, tail->size);
            readlen -= remaining_bytes;
            remaining_bytes = readIntoReplDataBlock(conn, tail, remaining_bytes);
        }
        if (remaining_bytes > 0) {
            /* Stop reading in case we read less than we anticipated */
            break;
        }
        if (remaining_bytes == C_ERR) {
            return;
        }
    }
}

/* Replication: Replica side.
 * Streams accumulated replication data into the database while freeing read nodes */
int streamReplDataBufToDb(client *c) {
    serverAssert(c->flag.primary);
    blockingOperationStarts();
    size_t used, offset = 0;
    listNode *cur = NULL;
    time_t last_progress_callback = mstime();
    while (server.pending_repl_data.blocks && (cur = listFirst(server.pending_repl_data.blocks))) {
        /* Read and process repl data block */
        replDataBufBlock *o = listNodeValue(cur);
        used = o->used;
        c->querybuf = sdscatlen(c->querybuf, o->buf, used);
        c->repl_data->read_reploff += used;
        processInputBuffer(c);
        server.pending_repl_data.len -= used;
        offset += used;
        listDelNode(server.pending_repl_data.blocks, cur);
        replStreamProgressCallback(offset, used, &last_progress_callback);
    }
    blockingOperationEnds();
    if (!server.pending_repl_data.blocks) {
        /* If we encounter a `replicaof` command during the replStreamProgressCallback,
         * pending_repl_data.blocks will be NULL, and we should return an error and
         * abort the current sync session. */
        return C_ERR;
    }
    return C_OK;
}

/* Replication: Replica side.
 * After done loading the snapshot using the rdb-channel prepare this replica for steady state by
 * initializing the primary client, amd stream local incremental buffer into memory. */
void dualChannelSyncSuccess(void) {
    server.primary_initial_offset = server.repl_provisional_primary.reploff;
    replicationResurrectProvisionalPrimary();
    /* Wait for the accumulated buffer to be processed before reading any more replication updates */
    if (server.pending_repl_data.blocks && streamReplDataBufToDb(server.primary) == C_ERR) {
        /* Sync session aborted during repl data streaming. */
        dualChannelServerLog(LL_WARNING, "Failed to stream local replication buffer into memory");
        /* Verify sync is still in progress */
        if (server.repl_rdb_channel_state != REPL_DUAL_CHANNEL_STATE_NONE) {
            replicationAbortDualChannelSyncTransfer();
            replicationUnsetPrimary();
        }
        return;
    }
    freePendingReplDataBuf();
    dualChannelServerLog(LL_NOTICE, "Successfully streamed replication data into memory");
    /* We can resume reading from the primary connection once the local replication buffer has been loaded. */
    replicationSteadyStateInit();
    replicationSendAck(); /* Send ACK to notify primary that replica is synced */
    server.rdb_client_id = -1;
    server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_STATE_NONE;
}

/* Replication: Replica side.
 * Main channel successfully established psync with primary. Check whether the rdb channel
 * has completed its part and act accordingly. */
int dualChannelSyncHandlePsync(void) {
    serverAssert(server.repl_state == REPL_STATE_RECEIVE_PSYNC_REPLY);
    if (server.repl_rdb_channel_state < REPL_DUAL_CHANNEL_RDB_LOADED) {
        /* RDB is still loading */
        if (connSetReadHandler(server.repl_provisional_primary.conn, bufferReplData) == C_ERR) {
            dualChannelServerLog(LL_WARNING, "Error while setting readable handler: %s", strerror(errno));
            cancelReplicationHandshake(1);
            return C_ERR;
        }
        replDataBufInit();
        return C_OK;
    }
    serverAssert(server.repl_rdb_channel_state == REPL_DUAL_CHANNEL_RDB_LOADED);
    /* RDB is loaded */
    dualChannelServerLog(LL_DEBUG, "Psync established after rdb load");
    dualChannelSyncSuccess();
    return C_OK;
}

/* Replication: Replica side.
 * RDB channel done loading the RDB. Check whether the main channel has completed its part
 * and act accordingly. */
void dualChannelSyncHandleRdbLoadCompletion(void) {
    serverAssert(server.repl_rdb_channel_state == REPL_DUAL_CHANNEL_RDB_LOAD);
    if (server.repl_state < REPL_STATE_TRANSFER) {
        /* Main psync channel hasn't been established yet */
        server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_RDB_LOADED;
        return;
    }
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    connSetReadHandler(server.repl_transfer_s, NULL);
    dualChannelSyncSuccess();
    return;
}

/* Try a partial resynchronization with the primary if we are about to reconnect.
 * If there is no cached primary structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the primary replid and the primary replication
 * global offset.
 *
 * This function is designed to be called from syncWithPrimary(), so the
 * following assumptions are made:
 *
 * 1) We pass the function an already connected socket "fd".
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the server.primary client structure.
 *
 * The function is split in two halves: if read_reply is 0, the function
 * writes the PSYNC command on the socket, and a new function call is
 * needed, with read_reply set to 1, in order to read the reply of the
 * command. This is useful in order to support non blocking operations, so
 * that we write, return into the event loop, and read when there are data.
 *
 * When read_reply is 0 the function returns PSYNC_WRITE_ERR if there
 * was a write error, or PSYNC_WAIT_REPLY to signal we need another call
 * with read_reply set to 1. However even when read_reply is set to 1
 * the function may return PSYNC_WAIT_REPLY again to signal there were
 * insufficient data to read to complete its work. We should re-enter
 * into the event loop and wait in such a case.
 *
 * The function returns:
 *
 * PSYNC_CONTINUE: If the PSYNC command succeeded and we can continue.
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the primary replid and global replication
 *                   offset is saved.
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 * PSYNC_WRITE_ERROR: There was an error writing the command to the socket.
 * PSYNC_WAIT_REPLY: Call again the function with read_reply set to 1.
 * PSYNC_TRY_LATER: Primary is currently in a transient error condition.
 *
 * Notable side effects:
 *
 * 1) As a side effect of the function call the function removes the readable
 *    event handler from "fd", unless the return value is PSYNC_WAIT_REPLY.
 * 2) server.primary_initial_offset is set to the right value according
 *    to the primary reply. This will be used to populate the 'server.primary'
 *    structure replication offset.
 */

#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
#define PSYNC_FULLRESYNC_DUAL_CHANNEL 6
int replicaTryPartialResynchronization(connection *conn, int read_reply) {
    char *psync_replid;
    char psync_offset[32];
    sds reply;

    /* Writing half */
    if (!read_reply) {
        /* Initially set primary_initial_offset to -1 to mark the current
         * primary replid and offset as not valid. Later if we'll be able to do
         * a FULL resync using the PSYNC command we'll set the offset at the
         * right value, so that this information will be propagated to the
         * client structure representing the primary into server.primary. */
        server.primary_initial_offset = -1;

        if (server.repl_rdb_channel_state != REPL_DUAL_CHANNEL_STATE_NONE) {
            /* While in dual channel replication, we should use our prepared repl id and offset. */
            psync_replid = server.repl_provisional_primary.replid;
            snprintf(psync_offset, sizeof(psync_offset), "%lld", server.repl_provisional_primary.reploff + 1);
            dualChannelServerLog(LL_NOTICE,
                                 "Trying a partial resynchronization using main channel (request %s:%s).",
                                 psync_replid, psync_offset);
        } else if (server.cached_primary) {
            psync_replid = server.cached_primary->repl_data->replid;
            snprintf(psync_offset, sizeof(psync_offset), "%lld", server.cached_primary->repl_data->reploff + 1);
            serverLog(LL_NOTICE, "Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);
        } else {
            serverLog(LL_NOTICE, "Partial resynchronization not possible (no cached primary)");
            psync_replid = "?";
            memcpy(psync_offset, "-1", 3);
        }

        /* Issue the PSYNC command, if this is a primary with a failover in
         * progress then send the failover argument to the replica to cause it
         * to become a primary */
        if (server.failover_state == FAILOVER_IN_PROGRESS) {
            reply = sendCommand(conn, "PSYNC", psync_replid, psync_offset, "FAILOVER", NULL);
        } else {
            reply = sendCommand(conn, "PSYNC", psync_replid, psync_offset, NULL);
        }

        if (reply != NULL) {
            serverLog(LL_WARNING, "Unable to send PSYNC to primary: %s", reply);
            sdsfree(reply);
            connSetReadHandler(conn, NULL);
            return PSYNC_WRITE_ERROR;
        }
        return PSYNC_WAIT_REPLY;
    }

    /* Reading half */
    reply = receiveSynchronousResponse(conn);
    /* Primary did not reply to PSYNC */
    if (reply == NULL) {
        connSetReadHandler(conn, NULL);
        serverLog(LL_WARNING, "Primary did not reply to PSYNC, will try later");
        return PSYNC_TRY_LATER;
    }

    if (sdslen(reply) == 0) {
        /* The primary may send empty newlines after it receives PSYNC
         * and before to reply, just to keep the connection alive. */
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    connSetReadHandler(conn, NULL);

    if (!strncmp(reply, "+FULLRESYNC", 11)) {
        char *replid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the replid
         * and the replication offset. */
        replid = strchr(reply, ' ');
        if (replid) {
            replid++;
            offset = strchr(replid, ' ');
            if (offset) offset++;
        }
        if (!replid || !offset || (offset - replid - 1) != CONFIG_RUN_ID_SIZE) {
            serverLog(LL_WARNING, "Primary replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the primary supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the primary
             * replid to make sure next PSYNCs will fail. */
            memset(server.primary_replid, 0, CONFIG_RUN_ID_SIZE + 1);
        } else {
            memcpy(server.primary_replid, replid, offset - replid - 1);
            server.primary_replid[CONFIG_RUN_ID_SIZE] = '\0';
            server.primary_initial_offset = strtoll(offset, NULL, 10);
            serverLog(LL_NOTICE, "Full resync from primary: %s:%lld", server.primary_replid,
                      server.primary_initial_offset);
        }
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    if (!strncmp(reply, "+CONTINUE", 9)) {
        if (server.repl_rdb_channel_state != REPL_DUAL_CHANNEL_STATE_NONE) {
            /* During dual channel sync sesseion, primary struct is already initialized. */
            sdsfree(reply);
            return PSYNC_CONTINUE;
        }
        /* Partial resync was accepted. */
        serverLog(LL_NOTICE, "Successful partial resynchronization with primary.");

        /* Check the new replication ID advertised by the primary. If it
         * changed, we need to set the new ID as primary ID, and set
         * secondary ID as the old primary ID up to the current offset, so
         * that our sub-replicas will be able to PSYNC with us after a
         * disconnection. */
        char *start = reply + 10;
        char *end = reply + 9;
        while (end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end - start == CONFIG_RUN_ID_SIZE) {
            char new[CONFIG_RUN_ID_SIZE + 1];
            memcpy(new, start, CONFIG_RUN_ID_SIZE);
            new[CONFIG_RUN_ID_SIZE] = '\0';

            if (strcmp(new, server.cached_primary->repl_data->replid)) {
                /* Primary ID changed. */
                serverLog(LL_NOTICE, "Primary replication ID changed to %s", new);

                /* Set the old ID as our ID2, up to the current offset+1. */
                memcpy(server.replid2, server.cached_primary->repl_data->replid, sizeof(server.replid2));
                server.second_replid_offset = server.primary_repl_offset + 1;

                /* Update the cached primary ID and our own primary ID to the
                 * new one. */
                memcpy(server.replid, new, sizeof(server.replid));
                memcpy(server.cached_primary->repl_data->replid, new, sizeof(server.replid));

                /* Disconnect all the sub-replicas: they need to be notified. */
                disconnectReplicas();
            }
        }

        /* Setup the replication to continue. */
        sdsfree(reply);
        replicationResurrectCachedPrimary(conn);

        /* If this instance was restarted and we read the metadata to
         * PSYNC from the persistence file, our replication backlog could
         * be still not initialized. Create it. */
        if (server.repl_backlog == NULL) createReplicationBacklog();
        return PSYNC_CONTINUE;
    }

    /* If we reach this point we received either an error (since the primary does
     * not understand PSYNC or because it is in a special state and cannot
     * serve our request), or an unexpected reply from the primary.
     *
     * Return PSYNC_NOT_SUPPORTED on errors we don't understand, otherwise
     * return PSYNC_TRY_LATER if we believe this is a transient error. */

    if (!strncmp(reply, "-NOMASTERLINK", 13) || !strncmp(reply, "-LOADING", 8)) {
        serverLog(LL_NOTICE,
                  "Primary is currently unable to PSYNC "
                  "but should be in the future: %s",
                  reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    if (!strncmp(reply, "+DUALCHANNELSYNC", strlen("+DUALCHANNELSYNC"))) {
        /* A response of +DUALCHANNELSYNC from the primary implies that partial
         * synchronization is not possible and that the primary supports full
         * sync using dedicated RDB channel. Full sync will continue that way. */
        dualChannelServerLog(LL_NOTICE, "PSYNC is not possible, initialize RDB channel.");
        sdsfree(reply);
        return PSYNC_FULLRESYNC_DUAL_CHANNEL;
    }

    if (strncmp(reply, "-ERR", 4)) {
        /* If it's not an error, log the unexpected event. */
        serverLog(LL_WARNING, "Unexpected reply to PSYNC from primary: %s", reply);
    } else {
        serverLog(LL_NOTICE,
                  "Primary does not support PSYNC or is in "
                  "error state (reply: %s)",
                  reply);
    }
    sdsfree(reply);
    return PSYNC_NOT_SUPPORTED;
}


sds getTryPsyncString(int result) {
    switch (result) {
    case PSYNC_WRITE_ERROR: return sdsnew("PSYNC_WRITE_ERROR");
    case PSYNC_WAIT_REPLY: return sdsnew("PSYNC_WAIT_REPLY");
    case PSYNC_CONTINUE: return sdsnew("PSYNC_CONTINUE");
    case PSYNC_FULLRESYNC: return sdsnew("PSYNC_FULLRESYNC");
    case PSYNC_NOT_SUPPORTED: return sdsnew("PSYNC_NOT_SUPPORTED");
    case PSYNC_TRY_LATER: return sdsnew("PSYNC_TRY_LATER");
    case PSYNC_FULLRESYNC_DUAL_CHANNEL: return sdsnew("PSYNC_FULLRESYNC_DUAL_CHANNEL");
    default: return sdsnew("Unknown result");
    }
}

int dualChannelReplMainConnSendHandshake(connection *conn, sds *err) {
    char llstr[LONG_STR_SIZE];
    ull2string(llstr, sizeof(llstr), server.rdb_client_id);
    *err = sendCommand(conn, "REPLCONF", "set-rdb-client-id", llstr, NULL);
    if (*err) return C_ERR;
    return C_OK;
}

int dualChannelReplMainConnRecvCapaReply(connection *conn, sds *err) {
    *err = receiveSynchronousResponse(conn);
    if (*err == NULL) return C_ERR;
    if ((*err)[0] == '-') {
        dualChannelServerLog(LL_NOTICE, "Primary does not understand REPLCONF identify: %s", *err);
        return C_ERR;
    }
    return C_OK;
}

int dualChannelReplMainConnSendPsync(connection *conn, sds *err) {
    if (server.debug_pause_after_fork) debugPauseProcess();
    if (replicaTryPartialResynchronization(conn, 0) == PSYNC_WRITE_ERROR) {
        dualChannelServerLog(LL_WARNING, "Aborting dual channel sync. Write error.");
        *err = sdsnew(connGetLastError(conn));
        return C_ERR;
    }
    return C_OK;
}

int dualChannelReplMainConnRecvPsyncReply(connection *conn, sds *err) {
    int psync_result = replicaTryPartialResynchronization(conn, 1);
    if (psync_result == PSYNC_WAIT_REPLY) return C_OK; /* Try again later... */

    if (psync_result == PSYNC_CONTINUE) {
        dualChannelServerLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Primary accepted a Partial Resynchronization%s",
                             server.repl_rdb_transfer_s != NULL ? ", RDB load in background." : ".");
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            serverCommunicateSystemd("STATUS=PRIMARY <-> REPLICA sync: Partial Resynchronization accepted. Ready to "
                                     "accept connections in read-write mode.\n");
        }
        dualChannelSyncHandlePsync();
        return C_OK;
    }
    *err = getTryPsyncString(psync_result);
    return C_ERR;
}

/* Replication: Replica side.
 * This connection handler fires after rdb-connection was initialized. We use it
 * to adjust the replica main for loading incremental changes into the local buffer. */
void dualChannelSetupMainConnForPsync(connection *conn) {
    char *err = NULL;
    int ret;

    switch (server.repl_state) {
    case REPL_STATE_SEND_HANDSHAKE:
        ret = dualChannelReplMainConnSendHandshake(conn, &err);
        if (ret == C_OK) server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;
        break;
    case REPL_STATE_RECEIVE_CAPA_REPLY:
        ret = dualChannelReplMainConnRecvCapaReply(conn, &err);
        if (ret == C_ERR) {
            break;
        }
        if (ret == C_OK) server.repl_state = REPL_STATE_SEND_PSYNC;
        sdsfree(err);
        err = NULL;
        /* fall through */
    case REPL_STATE_SEND_PSYNC:
        ret = dualChannelReplMainConnSendPsync(conn, &err);
        if (ret == C_OK) server.repl_state = REPL_STATE_RECEIVE_PSYNC_REPLY;
        break;
    case REPL_STATE_RECEIVE_PSYNC_REPLY:
        ret = dualChannelReplMainConnRecvPsyncReply(conn, &err);
        if (ret == C_OK && server.repl_rdb_channel_state != REPL_DUAL_CHANNEL_STATE_NONE)
            server.repl_state = REPL_STATE_TRANSFER;
        /*  In case the RDB is already loaded, the repl_state will be set during establishPrimaryConnection. */
        break;
    default:
        serverPanic("Unexpected replication state: %d", server.repl_state);
    }

    if (ret == C_ERR) {
        dualChannelServerLog(LL_WARNING, "Aborting dual channel sync. Main channel psync result %d %s", ret, err ? err : "");
        cancelReplicationHandshake(1);
    }
    sdsfree(err);
}

/*
 * Dual channel for full sync
 *
 * * Motivation *
 *  - Reduce primary memory load. We do that by moving the COB tracking to the replica side. This also decrease
 *    the chance for COB overruns. Note that primary's input buffer limits at the replica side are less restricted
 *    then primary's COB as the replica plays less critical part in the replication group. While increasing the
 *    primary's COB may end up with primary reaching swap and clients suffering, at replica side we're more at
 *    ease with it. Larger COB means better chance to sync successfully.
 *  - Reduce primary main process CPU load. By opening a new, dedicated channel for the RDB transfer, child
 *    processes can have direct access to the new channel. Due to TLS connection restrictions, this was not
 *    possible using one main channel. We eliminate the need for the child process to use the primary's
 *    child-proc -> main-proc pipeline, thus freeing up the main process to process clients queries.
 *
 * * High level interface design *
 *  - Dual channel sync begins when the replica sends a REPLCONF capa dual-channel to the primary during initial
 *    handshake. This allows the replica to verify whether the primary supports dual-channel-replication and, if
 *    so, state that this is the replica's main channel, which is not used for snapshot transfer.
 *  - When replica lacks sufficient data for PSYNC, the primary will send +DUALCHANNELSYNC response instead
 *    of RDB data. As a next step, the replica creates a new channel (rdb-channel) and configures it against
 *    the primary with the appropriate capabilities and requirements. The replica then requests a sync
 *    using the RDB channel.
 *  - Prior to forking, the primary sends the replica the snapshot's end repl-offset, and attaches the replica
 *    to the replication backlog to keep repl data until the replica requests psync. The replica uses the main
 *    channel to request a PSYNC starting at the snapshot end offset.
 *  - The primary main threads sends incremental changes via the main channel, while the bgsave process
 *    sends the RDB directly to the replica via the rdb-channel. As for the replica, the incremental
 *    changes are stored on a local buffer, while the RDB is loaded into memory.
 *  - Once the replica completes loading the rdb, it drops the rdb channel and streams the accumulated incremental
 *    changes into memory. Repl steady state continues normally.
 *
 * * Replica state machine *
 * ┌───────────────────┐             Dual channel sync
 * │RECEIVE_PING_REPLY │          ┌──────────────────────────────────────────────────────────────┐
 * └────────┬──────────┘          │     RDB channel states               Main channel state      │
 *          │+PONG                │     ┌────────────────────────────┐   ┌───────────────────┐   │
 * ┌────────▼──────────┐        ┌─┼─────►DUAL_CHANNEL_SEND_HANDSHAKE │ ┌─►SEND_HANDSHAKE     │   │
 * │SEND_HANDSHAKE     │        │ │     └────┬───────────────────────┘ │ └──┬────────────────┘   │
 * └────────┬──────────┘        │ │          │                         │    │REPLCONF set-rdb-client-id
 *          │                   │ │  ┌───────▼───────────────────────┐ │ ┌──▼────────────────┐   │
 * ┌────────▼──────────┐        │ │  │DUAL_CHANNEL_RECEIVE_AUTH_REPLY│ │ │RECEIVE_CAPA_REPLY │   │
 * │RECEIVE_AUTH_REPLY │        │ │  └───────┬───────────────────────┘ │ └──┬────────────────┘   │
 * └────────┬──────────┘        │ │          │+OK                      │    │+OK                 │
 *          │+OK                │ │  ┌───────▼───────────────────────┐ │ ┌──▼────────────────┐   │
 * ┌────────▼──────────┐        │ │  │DUAL_CHANNEL_RECEIVE_REPLCONF_REPLY│SEND_PSYNC         │   │
 * │RECEIVE_PORT_REPLY │        │ │  └───────┬───────────────────────┘ │ └──┬────────────────┘   │
 * └────────┬──────────┘        │ │          │+OK                      │    │PSYNC use snapshot  │
 *          │+OK                │ │  ┌───────▼───────────────────┐     │    │end-offset provided │
 * ┌────────▼──────────┐        │ │  │DUAL_CHANNEL_RECEIVE_ENDOFF│     │    │by the primary      │
 * │RECEIVE_IP_REPLY   │        │ │  └───────┬───────────────────┘     │ ┌──▼────────────────┐   │
 * └────────┬──────────┘        │ │          │$ENDOFF                  │ │RECEIVE_PSYNC_REPLY│   │
 *          │+OK                │ │          ├─────────────────────────┘ └──┬────────────────┘   │
 * ┌────────▼──────────┐        │ │          │                              │+CONTINUE           │
 * │RECEIVE_CAPA_REPLY │        │ │  ┌───────▼───────────────┐           ┌──▼────────────────┐   │
 * └────────┬──────────┘        │ │  │DUAL_CHANNEL_RDB_LOAD  │           │TRANSFER           │   │
 *          │+OK                │ │  └───────┬───────────────┘           └─────┬─────────────┘   │
 * ┌────────▼─────────────┐     │ │          │Done loading                     │                 │
 * │RECEIVE_VERSION_REPLY │     │ │  ┌───────▼───────────────┐                 │                 │
 * └────────┬─────────────┘     │ │  │DUAL_CHANNEL_RDB_LOADED│                 │                 │
 *          │+OK                │ │  └───────┬───────────────┘                 │                 │
 * ┌────────▼───┐               │ │          │                                 │                 │
 * │SEND_PSYNC  │               │ │          │Replica loads local replication  │                 │
 * └─┬──────────┘               │ │          │buffer into memory               │                 │
 *   │PSYNC (use cached-primary)│ │          └─────────┬───────────────────────┘                 │
 * ┌─▼─────────────────┐        │ │                    │                                         │
 * │RECEIVE_PSYNC_REPLY│        │ └────────────────────┼─────────────────────────────────────────┘
 * └────────┬─┬────────┘        │                      │
 * +CONTINUE│ │+DUALCHANNELSYNC │                      │
 *   │      │ └─────────────────┘                      │
 *   │      │+FULLRESYNC                               │
 *   │    ┌─▼─────────────────┐                   ┌────▼──────────────┐
 *   │    │TRANSFER           ├───────────────────►CONNECTED          │
 *   │    └───────────────────┘                   └────▲──────────────┘
 *   │                                                 │
 *   └─────────────────────────────────────────────────┘
 */
/* This handler fires when the non blocking connect was able to
 * establish a connection with the primary. */
void syncWithPrimary(connection *conn) {
    char tmpfile[256], *err = NULL;
    int psync_result;

    /* If this event fired after the user turned the instance into a primary
     * with REPLICAOF NO ONE we must just return ASAP. */
    if (server.repl_state == REPL_STATE_NONE) {
        connClose(conn);
        return;
    }

    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state. */
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING, "Error condition on socket for SYNC: %s", connGetLastError(conn));
        goto error;
    }

    /* Send a PING to check the primary is able to reply without errors. */
    if (server.repl_state == REPL_STATE_CONNECTING) {
        serverLog(LL_NOTICE, "Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        connSetReadHandler(conn, syncWithPrimary);
        connSetWriteHandler(conn, NULL);
        server.repl_state = REPL_STATE_RECEIVE_PING_REPLY;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        err = sendCommand(conn, "PING", NULL);
        if (err) goto write_error;
        return;
    }

    /* Receive the PONG command. */
    if (server.repl_state == REPL_STATE_RECEIVE_PING_REPLY) {
        err = receiveSynchronousResponse(conn);

        /* The primary did not reply */
        if (err == NULL) goto no_response_error;

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis OSS replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        if (err[0] != '+' && strncmp(err, "-NOAUTH", 7) != 0 && strncmp(err, "-NOPERM", 7) != 0 &&
            strncmp(err, "-ERR operation not permitted", 28) != 0) {
            serverLog(LL_WARNING, "Error reply to PING from primary: '%s'", err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE, "Primary replied to PING, replication can continue...");
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_SEND_HANDSHAKE;
    }

    if (server.repl_state == REPL_STATE_SEND_HANDSHAKE) {
        /* AUTH with the primary if required. */
        if (server.primary_auth) {
            char *args[3] = {"AUTH", NULL, NULL};
            size_t lens[3] = {4, 0, 0};
            int argc = 1;
            if (server.primary_user) {
                args[argc] = server.primary_user;
                lens[argc] = strlen(server.primary_user);
                argc++;
            }
            args[argc] = server.primary_auth;
            lens[argc] = sdslen(server.primary_auth);
            argc++;
            err = sendCommandArgv(conn, argc, args, lens);
            if (err) goto write_error;
        }

        /* Set the replica port, so that primary's INFO command can list the
         * replica listening port correctly. */
        {
            sds portstr = getReplicaPortString();
            err = sendCommand(conn, "REPLCONF", "listening-port", portstr, NULL);
            sdsfree(portstr);
            if (err) goto write_error;
        }

        /* Set the replica ip, so that primary's INFO command can list the
         * replica IP address port correctly in case of port forwarding or NAT.
         * Skip REPLCONF ip-address if there is no replica-announce-ip option set. */
        if (server.replica_announce_ip) {
            err = sendCommand(conn, "REPLCONF", "ip-address", server.replica_announce_ip, NULL);
            if (err) goto write_error;
        }

        /* Inform the primary of our (replica) capabilities.
         *
         * EOF: supports EOF-style RDB transfer for diskless replication.
         * PSYNC2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
         *
         * The primary will ignore capabilities it does not understand. */
        err = sendCommand(conn, "REPLCONF", "capa", "eof", "capa", "psync2",
                          server.dual_channel_replication ? "capa" : NULL,
                          server.dual_channel_replication ? "dual-channel" : NULL, NULL);
        if (err) goto write_error;

        /* Inform the primary of our (replica) version. */
        err = sendCommand(conn, "REPLCONF", "version", VALKEY_VERSION, NULL);
        if (err) goto write_error;

        server.repl_state = REPL_STATE_RECEIVE_AUTH_REPLY;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY && !server.primary_auth)
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;

    /* Receive AUTH reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        if (err[0] == '-') {
            serverLog(LL_WARNING, "Unable to AUTH to PRIMARY: %s", err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;
        return;
    }

    /* Receive REPLCONF listening-port reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_PORT_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* Ignore the error if any, not all the Redis OSS versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,
                      "(Non critical) Primary does not understand "
                      "REPLCONF listening-port: %s",
                      err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_IP_REPLY;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY && !server.replica_announce_ip)
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;

    /* Receive REPLCONF ip-address reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* Ignore the error if any, not all the Redis OSS versions support
         * REPLCONF ip-address. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,
                      "(Non critical) Primary does not understand "
                      "REPLCONF ip-address: %s",
                      err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;
        return;
    }

    /* Receive CAPA reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_CAPA_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* Ignore the error if any, not all the Redis OSS versions support
         * REPLCONF capa. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,
                      "(Non critical) Primary does not understand "
                      "REPLCONF capa: %s",
                      err);
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_RECEIVE_VERSION_REPLY;
        return;
    }

    /* Receive VERSION reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_VERSION_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* Ignore the error if any. Valkey >= 8 supports REPLCONF VERSION. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,
                      "(Non critical) Primary does not understand "
                      "REPLCONF VERSION: %s",
                      err);
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_SEND_PSYNC;
    }

    /* Try a partial resynchronization. If we don't have a cached primary
     * replicaTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the primary replid
     * and the global offset, to try a partial resync at the next
     * reconnection attempt. */
    if (server.repl_state == REPL_STATE_SEND_PSYNC) {
        if (replicaTryPartialResynchronization(conn, 0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            abortFailover("Write error to failover target");
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_PSYNC_REPLY;
        return;
    }

    /* If reached this point, we should be in REPL_STATE_RECEIVE_PSYNC_REPLY. */
    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC_REPLY) {
        serverLog(LL_WARNING,
                  "syncWithPrimary(): state machine error, "
                  "state should be RECEIVE_PSYNC but is %d",
                  server.repl_state);
        goto error;
    }

    psync_result = replicaTryPartialResynchronization(conn, 1);
    if (psync_result == PSYNC_WAIT_REPLY) return; /* Try again later... */

    /* Check the status of the planned failover. We expect PSYNC_CONTINUE,
     * but there is nothing technically wrong with a full resync which
     * could happen in edge cases. */
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        if (psync_result == PSYNC_CONTINUE || psync_result == PSYNC_FULLRESYNC) {
            clearFailoverState();
        } else {
            abortFailover("Failover target rejected psync request");
            return;
        }
    }

    /* If the primary is in an transient error, we should try to PSYNC
     * from scratch later, so go to the error path. This happens when
     * the server is loading the dataset or is not connected with its
     * primary and so forth. */
    if (psync_result == PSYNC_TRY_LATER) goto error;

    /* Note: if PSYNC does not return WAIT_REPLY, it will take care of
     * uninstalling the read handler from the file descriptor. */

    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Primary accepted a Partial Resynchronization.");
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            serverCommunicateSystemd("STATUS=PRIMARY <-> REPLICA sync: Partial Resynchronization accepted. Ready to "
                                     "accept connections in read-write mode.\n");
        }
        return;
    }

    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the server.primary_replid and primary_initial_offset are
     * already populated. */
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE, "Retrying with SYNC...");
        if (connSyncWrite(conn, "SYNC\r\n", 6, server.repl_syncio_timeout * 1000) == -1) {
            serverLog(LL_WARNING, "I/O error writing to PRIMARY: %s", connGetLastError(conn));
            goto error;
        }
    }

    /* Prepare a suitable temp file for bulk transfer */
    if (!useDisklessLoad()) {
        int dfd = -1, maxtries = 5;
        while (maxtries--) {
            snprintf(tmpfile, 256, "temp-%d.%ld.rdb", (int)server.unixtime, (long int)getpid());
            dfd = open(tmpfile, O_CREAT | O_WRONLY | O_EXCL, 0644);
            if (dfd != -1) break;
            /* We save the errno of open to prevent some systems from modifying it after
             * the sleep call. For example, sleep in Mac will change errno to ETIMEDOUT. */
            int saved_errno = errno;
            sleep(1);
            errno = saved_errno;
        }
        if (dfd == -1) {
            serverLog(LL_WARNING, "Opening the temp file needed for PRIMARY <-> REPLICA synchronization: %s",
                      strerror(errno));
            goto error;
        }
        server.repl_transfer_tmpfile = zstrdup(tmpfile);
        server.repl_transfer_fd = dfd;
    }

    /* Using dual-channel-replication, the primary responded +DUALCHANNELSYNC. We need to
     * initialize the RDB channel. */
    if (psync_result == PSYNC_FULLRESYNC_DUAL_CHANNEL) {
        /* Create RDB connection */
        server.repl_rdb_transfer_s = connCreate(connTypeOfReplication());
        if (connConnect(server.repl_rdb_transfer_s, server.primary_host, server.primary_port, server.bind_source_addr,
                        dualChannelFullSyncWithPrimary) == C_ERR) {
            serverLog(LL_WARNING, "Unable to connect to Primary: %s", connGetLastError(server.repl_transfer_s));
            connClose(server.repl_rdb_transfer_s);
            server.repl_rdb_transfer_s = NULL;
            goto error;
        }
        if (connSetReadHandler(conn, NULL) == C_ERR) {
            char conninfo[CONN_INFO_LEN];
            dualChannelServerLog(LL_WARNING, "Can't clear main connection handler: %s (%s)", strerror(errno),
                                 connGetInfo(conn, conninfo, sizeof(conninfo)));
            goto error;
        }
        server.repl_rdb_channel_state = REPL_DUAL_CHANNEL_SEND_HANDSHAKE;
        return;
    }
    /* Setup the non blocking download of the bulk file. */
    if (connSetReadHandler(conn, readSyncBulkPayload) == C_ERR) {
        char conninfo[CONN_INFO_LEN];
        serverLog(LL_WARNING, "Can't create readable event for SYNC: %s (%s)", strerror(errno),
                  connGetInfo(conn, conninfo, sizeof(conninfo)));
        goto error;
    }

    server.repl_state = REPL_STATE_TRANSFER;
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_lastio = server.unixtime;
    return;

no_response_error: /* Handle receiveSynchronousResponse() error when primary has no reply */
    serverLog(LL_WARNING, "Primary did not respond to command during SYNC handshake");
    /* Fall through to regular error handling */

error:
    connClose(conn);
    server.repl_transfer_s = NULL;
    if (server.repl_rdb_transfer_s) {
        connClose(server.repl_rdb_transfer_s);
        server.repl_rdb_transfer_s = NULL;
    }
    if (server.repl_transfer_fd != -1) close(server.repl_transfer_fd);
    if (server.repl_transfer_tmpfile) zfree(server.repl_transfer_tmpfile);
    server.repl_transfer_tmpfile = NULL;
    server.repl_transfer_fd = -1;
    server.repl_state = REPL_STATE_CONNECT;
    return;

write_error: /* Handle sendCommand() errors. */
    serverLog(LL_WARNING, "Sending command to primary in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}

int connectWithPrimary(void) {
    server.repl_transfer_s = connCreate(connTypeOfReplication());
    if (connConnect(server.repl_transfer_s, server.primary_host, server.primary_port, server.bind_source_addr,
                    syncWithPrimary) == C_ERR) {
        serverLog(LL_WARNING, "Unable to connect to PRIMARY: %s", connGetLastError(server.repl_transfer_s));
        connClose(server.repl_transfer_s);
        server.repl_transfer_s = NULL;
        return C_ERR;
    }


    server.repl_transfer_lastio = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTING;
    serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync started");
    return C_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void undoConnectWithPrimary(void) {
    connClose(server.repl_transfer_s);
    server.repl_transfer_s = NULL;
}

/* Abort the async download of the bulk dataset while SYNC-ing with primary.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void replicationAbortSyncTransfer(void) {
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    undoConnectWithPrimary();
    if (server.repl_transfer_fd != -1) {
        close(server.repl_transfer_fd);
        bg_unlink(server.repl_transfer_tmpfile);
        zfree(server.repl_transfer_tmpfile);
        server.repl_transfer_tmpfile = NULL;
        server.repl_transfer_fd = -1;
    }
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REPL_STATE_CONNECT.
 *
 * Otherwise zero is returned and no operation is performed at all. */
int cancelReplicationHandshake(int reconnect) {
    if (server.repl_rdb_channel_state != REPL_DUAL_CHANNEL_STATE_NONE) {
        replicationAbortDualChannelSyncTransfer();
    }
    if (server.repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer();
        server.repl_state = REPL_STATE_CONNECT;
    } else if (server.repl_state == REPL_STATE_CONNECTING || replicaIsInHandshakeState()) {
        undoConnectWithPrimary();
        server.repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }

    if (!reconnect) return 1;

    /* try to re-connect without waiting for replicationCron, this is needed
     * for the "diskless loading short read" test. */
    serverLog(LL_NOTICE, "Reconnecting to PRIMARY %s:%d after failure", server.primary_host, server.primary_port);
    connectWithPrimary();

    return 1;
}

/* Set replication to the specified primary address and port. */
void replicationSetPrimary(char *ip, int port, int full_sync_required) {
    int was_primary = server.primary_host == NULL;

    sdsfree(server.primary_host);
    server.primary_host = NULL;
    if (server.primary) {
        /* When joining 'myself' to a new primary, set the dont_cache_primary flag
         * if a full sync is required. This happens when 'myself' was previously
         * part of a different shard from the new primary. Since 'myself' does not
         * have the replication history of the shard it is joining, clearing the
         * cached primary is necessary to ensure proper replication behavior. */
        server.primary->flag.dont_cache_primary = full_sync_required;
        freeClient(server.primary);
    }
    disconnectAllBlockedClients(); /* Clients blocked in primary, now replica. */

    /* Setting primary_host only after the call to freeClient since it calls
     * replicationHandlePrimaryDisconnection which can trigger a re-connect
     * directly from within that call. */
    server.primary_host = sdsnew(ip);
    server.primary_port = port;

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Here we don't disconnect with replicas, since they may hopefully be able
     * to partially resync with us. We will disconnect with replicas and force
     * them to resync with us when changing replid on partially resync with new
     * primary, or finishing transferring RDB and preparing loading DB on full
     * sync with new primary. */

    cancelReplicationHandshake(0);

    /* Before destroying our primary state, create a cached primary using
     * our own parameters, to later PSYNC with the new primary. */
    if (was_primary && !full_sync_required) {
        replicationDiscardCachedPrimary();
        replicationCachePrimaryUsingMyself();
    }

    /* Fire the role change modules event. */
    moduleFireServerEvent(VALKEYMODULE_EVENT_REPLICATION_ROLE_CHANGED, VALKEYMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA,
                          NULL);

    /* Fire the primary link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(VALKEYMODULE_EVENT_PRIMARY_LINK_CHANGE, VALKEYMODULE_SUBEVENT_PRIMARY_LINK_DOWN, NULL);

    server.repl_state = REPL_STATE_CONNECT;
    /* Allow trying dual-channel-replication with the new primary. If new primary doesn't
     * support dual-channel-replication, we will set to 0 afterwards. */
    serverLog(LL_NOTICE, "Connecting to PRIMARY %s:%d", server.primary_host, server.primary_port);
    connectWithPrimary();
}

/* Cancel replication, setting the instance as a primary itself. */
void replicationUnsetPrimary(void) {
    if (server.primary_host == NULL) return; /* Nothing to do. */

    /* Fire the primary link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(VALKEYMODULE_EVENT_PRIMARY_LINK_CHANGE, VALKEYMODULE_SUBEVENT_PRIMARY_LINK_DOWN, NULL);

    /* Clear primary_host first, since the freeClient calls
     * replicationHandlePrimaryDisconnection which can attempt to re-connect. */
    sdsfree(server.primary_host);
    server.primary_host = NULL;
    if (server.primary) freeClient(server.primary);
    replicationDiscardCachedPrimary();
    cancelReplicationHandshake(0);
    /* When a replica is turned into a primary, the current replication ID
     * (that was inherited from the primary at synchronization time) is
     * used as secondary ID up to the current offset, and a new replication
     * ID is created to continue with a new replication history. */
    shiftReplicationId();
    /* Disconnecting all the replicas is required: we need to inform replicas
     * of the replication ID change (see shiftReplicationId() call). However
     * the replicas will be able to partially resync with us, so it will be
     * a very fast reconnection. */
    disconnectReplicas();
    server.repl_state = REPL_STATE_NONE;

    /* We need to make sure the new primary will start the replication stream
     * with a SELECT statement. This is forced after a full resync, but
     * with PSYNC version 2, there is no need for full resync after a
     * primary switch. */
    server.replicas_eldb = -1;

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Once we turn from replica to primary, we consider the starting time without
     * replicas (that is used to count the replication backlog time to live) as
     * starting from now. Otherwise the backlog will be freed after a
     * failover if replicas do not connect immediately. */
    server.repl_no_replicas_since = server.unixtime;

    /* Reset down time so it'll be ready for when we turn into replica again. */
    server.repl_down_since = 0;

    /* Fire the role change modules event. */
    moduleFireServerEvent(VALKEYMODULE_EVENT_REPLICATION_ROLE_CHANGED, VALKEYMODULE_EVENT_REPLROLECHANGED_NOW_PRIMARY,
                          NULL);

    /* Restart the AOF subsystem in case we shut it down during a sync when
     * we were still a replica. */
    if (server.aof_enabled && server.aof_state == AOF_OFF) restartAOFAfterSYNC();
}

/* This function is called when the replica lose the connection with the
 * primary into an unexpected way. */
void replicationHandlePrimaryDisconnection(void) {
    /* Fire the primary link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(VALKEYMODULE_EVENT_PRIMARY_LINK_CHANGE, VALKEYMODULE_SUBEVENT_PRIMARY_LINK_DOWN, NULL);

    server.primary = NULL;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unixtime;
    /* We lost connection with our primary, don't disconnect replicas yet,
     * maybe we'll be able to PSYNC with our primary later. We'll disconnect
     * the replicas only if we'll have to do a full resync with our primary. */

    /* Try to re-connect immediately rather than wait for replicationCron
     * waiting 1 second may risk backlog being recycled. */
    if (server.primary_host) {
        serverLog(LL_NOTICE, "Reconnecting to PRIMARY %s:%d", server.primary_host, server.primary_port);
        connectWithPrimary();
    }
}

void replicaofCommand(client *c) {
    /* REPLICAOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the primary node. */
    if (server.cluster_enabled) {
        addReplyError(c, "REPLICAOF not allowed in cluster mode.");
        return;
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c, "REPLICAOF not allowed while failing over.");
        return;
    }

    /* The special host/port combination "NO" "ONE" turns the instance
     * into a primary. Otherwise the new primary address is set. */
    if (!strcasecmp(c->argv[1]->ptr, "no") && !strcasecmp(c->argv[2]->ptr, "one")) {
        if (server.primary_host) {
            replicationUnsetPrimary();
            sds client = catClientInfoShortString(sdsempty(), c, server.hide_user_data_from_log);
            serverLog(LL_NOTICE, "PRIMARY MODE enabled (user request from '%s')", client);
            sdsfree(client);
        }
    } else {
        long port;

        if (c->flag.replica) {
            /* If a client is already a replica they cannot run this command,
             * because it involves flushing all replicas (including this
             * client) */
            addReplyError(c, "Command is not valid when client is a replica.");
            return;
        }

        if (getRangeLongFromObjectOrReply(c, c->argv[2], 0, 65535, &port, "Invalid master port") != C_OK) return;

        /* Check if we are already attached to the specified primary */
        if (server.primary_host && !strcasecmp(server.primary_host, c->argv[1]->ptr) && server.primary_port == port) {
            serverLog(LL_NOTICE, "REPLICAOF would result into synchronization "
                                 "with the primary we are already connected "
                                 "with. No operation performed.");
            addReplySds(c, sdsnew("+OK Already connected to specified "
                                  "master\r\n"));
            return;
        }
        /* There was no previous primary or the user specified a different one,
         * we can continue. */
        replicationSetPrimary(c->argv[1]->ptr, port, 0);
        sds client = catClientInfoShortString(sdsempty(), c, server.hide_user_data_from_log);
        serverLog(LL_NOTICE, "REPLICAOF %s:%d enabled (user request from '%s')", server.primary_host,
                  server.primary_port, client);
        sdsfree(client);
    }
    addReply(c, shared.ok);
}

/* ROLE command: provide information about the role of the instance
 * (primary or replica) and additional information related to replication
 * in an easy to process format. */
void roleCommand(client *c) {
    if (server.sentinel_mode) {
        sentinelRoleCommand(c);
        return;
    }

    if (server.primary_host == NULL) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int replicas = 0;

        addReplyArrayLen(c, 3);
        addReplyBulkCBuffer(c, "master", 6);
        addReplyLongLong(c, server.primary_repl_offset);
        mbcount = addReplyDeferredLen(c);
        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            client *replica = ln->value;
            char ip[NET_IP_STR_LEN], *replica_addr = replica->repl_data->replica_addr;

            if (!replica_addr) {
                if (connAddrPeerName(replica->conn, ip, sizeof(ip), NULL) == -1) continue;
                replica_addr = ip;
            }
            if (replica->repl_data->repl_state != REPLICA_STATE_ONLINE) continue;
            addReplyArrayLen(c, 3);
            addReplyBulkCString(c, replica_addr);
            addReplyBulkLongLong(c, replica->repl_data->replica_listening_port);
            addReplyBulkLongLong(c, replica->repl_data->repl_ack_off);
            replicas++;
        }
        setDeferredArrayLen(c, mbcount, replicas);
    } else {
        char *replica_state = NULL;

        addReplyArrayLen(c, 5);
        addReplyBulkCBuffer(c, "slave", 5);
        addReplyBulkCString(c, server.primary_host);
        addReplyLongLong(c, server.primary_port);
        if (replicaIsInHandshakeState()) {
            replica_state = "handshake";
        } else {
            switch (server.repl_state) {
            case REPL_STATE_NONE: replica_state = "none"; break;
            case REPL_STATE_CONNECT: replica_state = "connect"; break;
            case REPL_STATE_CONNECTING: replica_state = "connecting"; break;
            case REPL_STATE_TRANSFER: replica_state = "sync"; break;
            case REPL_STATE_CONNECTED: replica_state = "connected"; break;
            default: replica_state = "unknown"; break;
            }
        }
        addReplyBulkCString(c, replica_state);
        addReplyLongLong(c, server.primary ? server.primary->repl_data->reploff : -1);
    }
}

/* Send a REPLCONF ACK command to the primary to inform it about the current
 * processed offset. If we are not connected with a primary, the command has
 * no effects. */
void replicationSendAck(void) {
    client *c = server.primary;

    if (c != NULL) {
        int send_fack = server.fsynced_reploff != -1;
        c->flag.primary_force_reply = 1;
        addReplyArrayLen(c, send_fack ? 5 : 3);
        addReplyBulkCString(c, "REPLCONF");
        addReplyBulkCString(c, "ACK");
        addReplyBulkLongLong(c, c->repl_data->reploff);
        if (send_fack) {
            addReplyBulkCString(c, "FACK");
            addReplyBulkLongLong(c, server.fsynced_reploff);
        }
        c->flag.primary_force_reply = 0;

        /* Accumulation from above replies must be reset back to 0 manually,
         * as this subroutine does not invoke resetClient(). */
        c->net_output_bytes_curr_cmd = 0;
    }
}

/* ---------------------- PRIMARY CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our primary's client structure after a transient disconnection.
 * It is cached into server.cached_primary and flushed away using the following
 * functions. */

/* This function is called by freeClient() in order to cache the primary
 * client structure instead of destroying it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached primary are:
 *
 * replicationDiscardCachedPrimary() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedPrimary() that is used after a successful PSYNC
 * handshake in order to reactivate the cached primary.
 */
void replicationCachePrimary(client *c) {
    serverAssert(server.primary != NULL && server.cached_primary == NULL);
    serverLog(LL_NOTICE, "Caching the disconnected primary state.");

    /* Wait for IO operations to be done before proceeding */
    waitForClientIO(c);
    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the primary client so that's ready to accept new commands:
     * we want to discard the non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the primary. */
    sdsclear(server.primary->querybuf);
    server.primary->qb_pos = 0;
    server.primary->repl_data->repl_applied = 0;
    server.primary->repl_data->read_reploff = server.primary->repl_data->reploff;
    if (c->flag.multi) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);
    resetClientIOState(c);

    /* Save the primary. Server.primary will be set to null later by
     * replicationHandlePrimaryDisconnection(). */
    server.cached_primary = server.primary;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }
    /* Invalidate the Sock Name cache. */
    if (c->sockname) {
        sdsfree(c->sockname);
        c->sockname = NULL;
    }

    /* Caching the primary happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.primary to NULL. */
    replicationHandlePrimaryDisconnection();
}

/* This function is called when a primary is turned into a replica, in order to
 * create from scratch a cached primary for the new client, that will allow
 * to PSYNC with the replica that was promoted as the new primary after a
 * failover.
 *
 * Assuming this instance was previously the primary instance of the new primary,
 * the new primary will accept its replication ID, and potential also the
 * current offset if no data was lost during the failover. So we use our
 * current replication ID and offset in order to synthesize a cached primary. */
void replicationCachePrimaryUsingMyself(void) {
    serverLog(LL_NOTICE, "Before turning into a replica, using my own primary parameters "
                         "to synthesize a cached primary: I may be able to synchronize with "
                         "the new primary with just a partial transfer.");

    /* This will be used to populate the field server.primary->repl_data->reploff
     * by replicationCreatePrimaryClient(). We'll later set the created
     * primary as server.cached_primary, so the replica will use such
     * offset for PSYNC. */
    server.primary_initial_offset = server.primary_repl_offset;

    /* The primary client we create can be set to any DBID, because
     * the new primary will start its replication stream with SELECT. */
    replicationCreatePrimaryClient(NULL, -1);

    /* Use our own ID / offset. */
    memcpy(server.primary->repl_data->replid, server.replid, sizeof(server.replid));

    /* Set as cached primary. */
    unlinkClient(server.primary);
    server.cached_primary = server.primary;
    server.primary = NULL;
}

/* Free a cached primary, called when there are no longer the conditions for
 * a partial resync on reconnection. */
void replicationDiscardCachedPrimary(void) {
    if (server.cached_primary == NULL) return;

    serverLog(LL_NOTICE, "Discarding previously cached primary state.");
    server.cached_primary->flag.primary = 0;
    freeClient(server.cached_primary);
    server.cached_primary = NULL;
}

/* Replication: Replica side.
 * This method performs the necessary steps to establish a connection with the primary server.
 * It sets private data, updates flags, and fires an event to notify modules about the primary link change. */
void establishPrimaryConnection(void) {
    connSetPrivateData(server.primary->conn, server.primary);
    server.primary->flag.close_after_reply = 0;
    server.primary->flag.close_asap = 0;
    server.primary->flag.authenticated = 1;
    server.primary->last_interaction = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the primary link modules event. */
    moduleFireServerEvent(VALKEYMODULE_EVENT_PRIMARY_LINK_CHANGE, VALKEYMODULE_SUBEVENT_PRIMARY_LINK_UP, NULL);
}

/* Replication: Replica side.
 * Turn the cached primary into the current primary, using the file descriptor
 * passed as argument as the socket for the new primary.
 *
 * This function is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from where this
 * primary left. */
void replicationResurrectCachedPrimary(connection *conn) {
    server.primary = server.cached_primary;
    server.cached_primary = NULL;
    server.primary->conn = conn;

    establishPrimaryConnection();
    /* Re-add to the list of clients. */
    linkClient(server.primary);
    replicationSteadyStateInit();
}

/* Replication: Replica side.
 * Prepare replica to steady state.
 * prerequisite: server.primary is already initialized and linked in client list. */
void replicationSteadyStateInit(void) {
    if (connSetReadHandler(server.primary->conn, readQueryFromClient)) {
        serverLog(LL_WARNING, "Error resurrecting the cached primary, impossible to add the readable handler: %s",
                  strerror(errno));
        freeClientAsync(server.primary); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    if (clientHasPendingReplies(server.primary)) {
        if (connSetWriteHandler(server.primary->conn, sendReplyToClient)) {
            serverLog(LL_WARNING, "Error resurrecting the cached primary, impossible to add the writable handler: %s",
                      strerror(errno));
            freeClientAsync(server.primary); /* Close ASAP. */
        }
    }
}

/* Replication: Replica side.
 * Turn the provisional primary into the current primary.
 * This function is called after dual channel sync is finished successfully. */
void replicationResurrectProvisionalPrimary(void) {
    /* Create a primary client, but do not initialize the read handler yet, as this replica still has a local buffer to
     * drain. */
    replicationCreatePrimaryClientWithHandler(server.repl_transfer_s, server.repl_provisional_primary.dbid, NULL);
    memcpy(server.primary->repl_data->replid, server.repl_provisional_primary.replid, sizeof(server.repl_provisional_primary.replid));
    server.primary->repl_data->reploff = server.repl_provisional_primary.reploff;
    server.primary->repl_data->read_reploff = server.repl_provisional_primary.read_reploff;
    server.primary_repl_offset = server.primary->repl_data->reploff;
    memcpy(server.replid, server.primary->repl_data->replid, sizeof(server.primary->repl_data->replid));
    establishPrimaryConnection();
}

/* ------------------------- MIN-REPLICAS-TO-WRITE  --------------------------- */

/* This function counts the number of replicas with lag <= min-replicas-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected replicas with the specified lag (or less). */
void refreshGoodReplicasCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    if (!server.repl_min_replicas_to_write || !server.repl_min_replicas_max_lag) return;

    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;
        time_t lag = server.unixtime - replica->repl_data->repl_ack_time;

        if (replica->repl_data->repl_state == REPLICA_STATE_ONLINE && lag <= server.repl_min_replicas_max_lag) good++;
    }
    server.repl_good_replicas_count = good;
}

/* return true if status of good replicas is OK. otherwise false */
int checkGoodReplicasStatus(void) {
    return server.primary_host ||                                                /* not a primary status should be OK */
           !server.repl_min_replicas_max_lag ||                                  /* Min replica max lag not configured */
           !server.repl_min_replicas_to_write ||                                 /* Min replica to write not configured */
           server.repl_good_replicas_count >= server.repl_min_replicas_to_write; /* check if we have enough replicas */
}

/* ----------------------- SYNCHRONOUS REPLICATION --------------------------
 * Synchronous replication design can be summarized in points:
 *
 * - Primary have a global replication offset, used by PSYNC.
 * - Primary increment the offset every time new commands are sent to replicas.
 * - Replicas ping back primary with the offset processed so far.
 *
 * So synchronous replication adds a new WAIT command in the form:
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * That returns the number of replicas that processed the query when
 * we finally have at least num_replicas, or when the timeout was
 * reached.
 *
 * The command is implemented in this way:
 *
 * - Every time a client processes a command, we remember the replication
 *   offset after sending that command to the replicas.
 * - When WAIT is called, we ask replicas to send an acknowledgement ASAP.
 *   The client is blocked at the same time (see blocked.c).
 * - Once we receive enough ACKs for a given offset or when the timeout
 *   is reached, the WAIT command is unblocked and the reply sent to the
 *   client.
 */

/* This just set a flag so that we broadcast a REPLCONF GETACK command
 * to all the replicas in the beforeSleep() function. Note that this way
 * we "group" all the clients that want to wait for synchronous replication
 * in a given event loop iteration, and send a single GETACK for them all. */
void replicationRequestAckFromReplicas(void) {
    server.get_ack_from_replicas = 1;
}

/* This function return client woff. If the script is currently running,
 * returns the actual client woff */
long long getClientWriteOffset(client *c) {
    if (scriptIsRunning()) {
        /* If a script is currently running, the client passed in is a fake
         * client, and its woff is always 0. */
        serverAssert(scriptGetClient() == c);
        c = scriptGetCaller();
    }
    return c->woff;
}

/* Return the number of replicas that already acknowledged the specified
 * replication offset. */
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;

        if (replica->repl_data->repl_state != REPLICA_STATE_ONLINE) continue;
        if (replica->repl_data->repl_ack_off >= offset) count++;
    }
    return count;
}

/* Return the number of replicas that already acknowledged the specified
 * replication offset being AOF fsynced. */
int replicationCountAOFAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;

        if (replica->repl_data->repl_state != REPLICA_STATE_ONLINE) continue;
        if (replica->repl_data->repl_aof_off >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands). */
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = getClientWriteOffset(c);

    if (server.primary_host) {
        addReplyError(
            c, "WAIT cannot be used with replica instances. Please also note that if a replica is configured to be "
               "writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }

    /* Argument parsing. */
    if (getLongFromObjectOrReply(c, c->argv[1], &numreplicas, NULL) != C_OK) return;
    if (getTimeoutFromObjectOrReply(c, c->argv[2], &timeout, UNIT_MILLISECONDS) != C_OK) return;

    /* First try without blocking at all. */
    ackreplicas = replicationCountAcksByOffset(offset);
    if (ackreplicas >= numreplicas || c->flag.deny_blocking) {
        addReplyLongLong(c, ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from replicas. */
    blockClientForReplicaAck(c, timeout, offset, numreplicas, 0);

    /* Make sure that the server will send an ACK request to all the replicas
     * before returning to the event loop. */
    replicationRequestAckFromReplicas();
}

/* WAIT for N replicas and / or local primary to acknowledge our latest
 * write command got synced to the disk. */
void waitaofCommand(client *c) {
    mstime_t timeout;
    long numreplicas, numlocal, ackreplicas, acklocal;

    /* Argument parsing. */
    if (getRangeLongFromObjectOrReply(c, c->argv[1], 0, 1, &numlocal, NULL) != C_OK) return;
    if (getPositiveLongFromObjectOrReply(c, c->argv[2], &numreplicas, NULL) != C_OK) return;
    if (getTimeoutFromObjectOrReply(c, c->argv[3], &timeout, UNIT_MILLISECONDS) != C_OK) return;

    if (server.primary_host) {
        addReplyError(c, "WAITAOF cannot be used with replica instances. Please also note that writes to replicas are "
                         "just local and are not propagated.");
        return;
    }
    if (numlocal && !server.aof_enabled) {
        addReplyError(c, "WAITAOF cannot be used when numlocal is set but appendonly is disabled.");
        return;
    }

    long long offset = getClientWriteOffset(c);

    /* First try without blocking at all. */
    ackreplicas = replicationCountAOFAcksByOffset(offset);
    acklocal = server.fsynced_reploff >= offset;
    if ((ackreplicas >= numreplicas && acklocal >= numlocal) || c->flag.deny_blocking) {
        addReplyArrayLen(c, 2);
        addReplyLongLong(c, acklocal);
        addReplyLongLong(c, ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from replicas. */
    blockClientForReplicaAck(c, timeout, offset, numreplicas, numlocal);

    /* Make sure that the server will send an ACK request to all the replicas
     * before returning to the event loop. */
    replicationRequestAckFromReplicas();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingReplicas(client *c) {
    serverAssert(c->bstate->client_waiting_acks_list_node);
    listDelNode(server.clients_waiting_acks, c->bstate->client_waiting_acks_list_node);
    c->bstate->client_waiting_acks_list_node = NULL;
    updateStatsOnUnblock(c, 0, 0, 0);
}

/* Check if there are clients blocked in WAIT, WAITAOF, or WAIT_PREREPL
 * that can be unblocked since we received enough ACKs from replicas. */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    long long last_aof_offset = 0;
    int last_numreplicas = 0;
    int last_aof_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(server.clients_waiting_acks, &li);
    while ((ln = listNext(&li))) {
        int numlocal = 0;
        int numreplicas = 0;

        client *c = ln->value;
        int is_wait_aof = c->cmd->proc == waitaofCommand;

        if (is_wait_aof && c->bstate->numlocal && !server.aof_enabled) {
            addReplyError(c, "WAITAOF cannot be used when numlocal is set but appendonly is disabled.");
            unblockClient(c, 1);
            continue;
        }

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * or calling replicationCountAOFAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        if (!is_wait_aof && last_offset && last_offset >= c->bstate->reploffset &&
            last_numreplicas >= c->bstate->numreplicas) {
            numreplicas = last_numreplicas;
        } else if (is_wait_aof && last_aof_offset && last_aof_offset >= c->bstate->reploffset &&
                   last_aof_numreplicas >= c->bstate->numreplicas) {
            numreplicas = last_aof_numreplicas;
        } else {
            numreplicas = is_wait_aof ? replicationCountAOFAcksByOffset(c->bstate->reploffset)
                                      : replicationCountAcksByOffset(c->bstate->reploffset);

            /* Check if the number of replicas is satisfied. */
            if (numreplicas < c->bstate->numreplicas) continue;

            if (is_wait_aof) {
                last_aof_offset = c->bstate->reploffset;
                last_aof_numreplicas = numreplicas;
            } else {
                last_offset = c->bstate->reploffset;
                last_numreplicas = numreplicas;
            }
        }

        /* Check if the local constraint of WAITAOF is served */
        if (is_wait_aof) {
            numlocal = server.fsynced_reploff >= c->bstate->reploffset;
            if (numlocal < c->bstate->numlocal) continue;
        }

        /* Reply before unblocking, because unblock client calls reqresAppendResponse */
        if (is_wait_aof) {
            /* WAITAOF has an array reply */
            addReplyArrayLen(c, 2);
            addReplyLongLong(c, numlocal);
            addReplyLongLong(c, numreplicas);
        } else if (c->flag.pending_command) {
            c->flag.replication_done = 1;
        } else {
            addReplyLongLong(c, numreplicas);
        }

        unblockClient(c, 1);
    }
}

/* Return the replica replication offset for this instance, that is
 * the offset for which we already processed the primary replication stream. */
long long replicationGetReplicaOffset(void) {
    long long offset = 0;

    if (server.primary_host != NULL) {
        if (server.primary) {
            offset = server.primary->repl_data->reploff;
        } else if (server.cached_primary) {
            offset = server.cached_primary->repl_data->reploff;
        }
    }
    /* offset may be -1 when the primary does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the primary, so we return a positive
     * integer. */
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

/* Replication cron function, called 1 time per second. */
void replicationCron(void) {
    static long long replication_cron_loops = 0;

    /* Check failover status first, to see if we need to start
     * handling the failover. */
    updateFailoverStatus();

    /* Non blocking connection timeout? */
    if (server.primary_host && (server.repl_state == REPL_STATE_CONNECTING || replicaIsInHandshakeState()) &&
        (time(NULL) - server.repl_transfer_lastio) > server.repl_timeout) {
        serverLog(LL_WARNING, "Timeout connecting to the PRIMARY...");
        cancelReplicationHandshake(1);
    }

    /* Bulk transfer I/O timeout? */
    if (server.primary_host && server.repl_state == REPL_STATE_TRANSFER &&
        (time(NULL) - server.repl_transfer_lastio) > server.repl_timeout) {
        serverLog(LL_WARNING, "Timeout receiving bulk data from PRIMARY... If the problem persists try to set the "
                              "'repl-timeout' parameter in valkey.conf to a larger value.");
        cancelReplicationHandshake(1);
    }

    /* Timed out primary when we are an already connected replica? */
    if (server.primary_host && server.repl_state == REPL_STATE_CONNECTED &&
        (time(NULL) - server.primary->last_interaction) > server.repl_timeout) {
        serverLog(LL_WARNING, "PRIMARY timeout: no data nor PING received...");
        freeClient(server.primary);
    }

    /* Check if we should connect to a PRIMARY */
    if (server.repl_state == REPL_STATE_CONNECT) {
        serverLog(LL_NOTICE, "Connecting to PRIMARY %s:%d", server.primary_host, server.primary_port);
        connectWithPrimary();
    }

    /* Send ACK to primary from time to time.
     * Note that we do not send periodic acks to primary that don't
     * support PSYNC and replication offsets. */
    if (server.primary_host && server.primary && !(server.primary->flag.pre_psync)) replicationSendAck();

    /* If we have attached replicas, PING them from time to time.
     * So replicas can implement an explicit timeout to primaries, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. */
    listIter li;
    listNode *ln;
    robj *ping_argv[1];

    /* First, send PING according to ping_replica_period. */
    if ((replication_cron_loops % server.repl_ping_replica_period) == 0 && listLength(server.replicas)) {
        /* Note that we don't send the PING if the clients are paused during
         * a Cluster manual failover: the PING we send will otherwise
         * alter the replication offsets of primary and replica, and will no longer
         * match the one stored into 'mf_primary_offset' state. */
        int manual_failover_in_progress =
            ((server.cluster_enabled && clusterManualFailoverTimeLimit()) || server.failover_end_time) &&
            isPausedActionsWithUpdate(PAUSE_ACTION_REPLICA);

        if (!manual_failover_in_progress) {
            ping_argv[0] = shared.ping;
            replicationFeedReplicas(-1, ping_argv, 1);
        }
    }

    /* Second, send a newline to all the replicas in pre-synchronization
     * stage, that is, replicas waiting for the primary to create the RDB file.
     *
     * Also send the a newline to all the chained replicas we have, if we lost
     * connection from our primary, to keep the replicas aware that their
     * primary is online. This is needed since sub-replicas only receive proxied
     * data from top-level primaries, so there is no explicit pinging in order
     * to avoid altering the replication offsets. This special out of band
     * pings (newlines) can be sent, they will have no effect in the offset.
     *
     * The newline will be ignored by the replica but will refresh the
     * last interaction timer preventing a timeout. In this case we ignore the
     * ping period and refresh the connection once per second since certain
     * timeouts are set at a few seconds (example: PSYNC response). */
    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;

        int is_presync =
            (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_START ||
             (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END && server.rdb_child_type != RDB_CHILD_TYPE_SOCKET));

        if (is_presync) {
            connWrite(replica->conn, "\n", 1);
        }
    }

    /* Disconnect timedout replicas. */
    if (listLength(server.replicas)) {
        listIter li;
        listNode *ln;

        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            client *replica = ln->value;

            if (replica->repl_data->repl_state == REPLICA_STATE_ONLINE) {
                if (replica->flag.pre_psync) continue;
                if ((server.unixtime - replica->repl_data->repl_ack_time) > server.repl_timeout) {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (streaming sync): %s",
                              replicationGetReplicaName(replica));
                    freeClient(replica);
                    continue;
                }
            }
            /* We consider disconnecting only diskless replicas because disk-based replicas aren't fed
             * by the fork child so if a disk-based replica is stuck it doesn't prevent the fork child
             * from terminating. */
            if (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END &&
                server.rdb_child_type == RDB_CHILD_TYPE_SOCKET) {
                if (replica->repl_data->repl_last_partial_write != 0 &&
                    (server.unixtime - replica->repl_data->repl_last_partial_write) > server.repl_timeout) {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (full sync): %s",
                              replicationGetReplicaName(replica));
                    freeClient(replica);
                    continue;
                }
            }
        }
    }

    /* If this is a primary without attached replicas and there is a replication
     * backlog active, in order to reclaim memory we can free it after some
     * (configured) time. Note that this cannot be done for replicas: replicas
     * without sub-replicas attached should still accumulate data into the
     * backlog, in order to reply to PSYNC queries if they are turned into
     * primaries after a failover. */
    if (listLength(server.replicas) == 0 && server.repl_backlog_time_limit && server.repl_backlog &&
        server.primary_host == NULL) {
        time_t idle = server.unixtime - server.repl_no_replicas_since;

        if (idle > server.repl_backlog_time_limit) {
            /* When we free the backlog, we always use a new
             * replication ID and clear the ID2. This is needed
             * because when there is no backlog, the primary_repl_offset
             * is not updated, but we would still retain our replication
             * ID, leading to the following problem:
             *
             * 1. We are a primary instance.
             * 2. Our replica is promoted to primary. It's repl-id-2 will
             *    be the same as our repl-id.
             * 3. We, yet as primary, receive some updates, that will not
             *    increment the primary_repl_offset.
             * 4. Later we are turned into a replica, connect to the new
             *    primary that will accept our PSYNC request by second
             *    replication ID, but there will be data inconsistency
             *    because we received writes. */
            changeReplicationId();
            clearReplicationId2();
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                      "Replication backlog freed after %d seconds "
                      "without connected replicas.",
                      (int)server.repl_backlog_time_limit);
        }
    }

    replicationStartPendingFork();

    /* Remove the RDB file used for replication if the server is not running
     * with any persistence. */
    removeRDBUsedToSyncReplicas();

    /* Sanity check replication buffer, the first block of replication buffer blocks
     * must be referenced by someone, since it will be freed when not referenced,
     * otherwise, server will OOM. also, its refcount must not be more than
     * replicas number + 1(replication backlog). */
    if (listLength(server.repl_buffer_blocks) > 0) {
        replBufBlock *o = listNodeValue(listFirst(server.repl_buffer_blocks));
        serverAssert(o->refcount > 0 &&
                     o->refcount <= (int)listLength(server.replicas) + 1 + (int)raxSize(server.replicas_waiting_psync));
    }

    /* Refresh the number of replicas with lag <= min-replicas-max-lag. */
    refreshGoodReplicasCount();
    replication_cron_loops++; /* Incremented with frequency 1 HZ. */
}

int shouldStartChildReplication(int *mincapa_out, int *req_out) {
    /* We should start a BGSAVE good for replication if we have replicas in
     * WAIT_BGSAVE_START state.
     *
     * In case of diskless replication, we make sure to wait the specified
     * number of seconds (according to configuration) so that other replicas
     * have the time to arrive before we start streaming. */
    if (!hasActiveChildProcess()) {
        time_t idle, max_idle = 0;
        int replicas_waiting = 0;
        int mincapa;
        int req;
        int first = 1;
        listNode *ln;
        listIter li;

        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            client *replica = ln->value;
            if (replica->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_START) {
                if (first) {
                    /* Get first replica's requirements */
                    req = replica->repl_data->replica_req;
                } else if (req != replica->repl_data->replica_req) {
                    /* Skip replicas that don't match */
                    continue;
                }
                idle = server.unixtime - replica->last_interaction;
                if (idle > max_idle) max_idle = idle;
                replicas_waiting++;
                mincapa = first ? replica->repl_data->replica_capa : (mincapa & replica->repl_data->replica_capa);
                first = 0;
            }
        }

        if (replicas_waiting && (!server.repl_diskless_sync ||
                                 (server.repl_diskless_sync_max_replicas > 0 &&
                                  replicas_waiting >= server.repl_diskless_sync_max_replicas) ||
                                 max_idle >= server.repl_diskless_sync_delay)) {
            if (mincapa_out) *mincapa_out = mincapa;
            if (req_out) *req_out = req;
            return 1;
        }
    }

    return 0;
}

void replicationStartPendingFork(void) {
    int mincapa = -1;
    int req = -1;

    if (shouldStartChildReplication(&mincapa, &req)) {
        /* Start the BGSAVE. The called function may start a
         * BGSAVE with socket target or disk target depending on the
         * configuration and replicas capabilities and requirements. */
        startBgsaveForReplication(mincapa, req);
    }
}

/* Find replica at IP:PORT from replica list */
static client *findReplica(char *host, int port) {
    listIter li;
    listNode *ln;
    client *replica;

    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        replica = ln->value;
        char ip[NET_IP_STR_LEN], *replicaip = replica->repl_data->replica_addr;

        if (!replicaip) {
            if (connAddrPeerName(replica->conn, ip, sizeof(ip), NULL) == -1) continue;
            replicaip = ip;
        }

        if (!strcasecmp(host, replicaip) && (port == replica->repl_data->replica_listening_port)) return replica;
    }

    return NULL;
}

const char *getFailoverStateString(void) {
    switch (server.failover_state) {
    case NO_FAILOVER: return "no-failover";
    case FAILOVER_IN_PROGRESS: return "failover-in-progress";
    case FAILOVER_WAIT_FOR_SYNC: return "waiting-for-sync";
    default: return "unknown";
    }
}

/* Resets the internal failover configuration, this needs
 * to be called after a failover either succeeds or fails
 * as it includes the client unpause. */
void clearFailoverState(void) {
    server.failover_end_time = 0;
    server.force_failover = 0;
    zfree(server.target_replica_host);
    server.target_replica_host = NULL;
    server.target_replica_port = 0;
    server.failover_state = NO_FAILOVER;
    unpauseActions(PAUSE_DURING_FAILOVER);
}

/* Abort an ongoing failover if one is going on. */
void abortFailover(const char *err) {
    if (server.failover_state == NO_FAILOVER) return;

    if (server.target_replica_host) {
        serverLog(LL_NOTICE, "FAILOVER to %s:%d aborted: %s", server.target_replica_host, server.target_replica_port,
                  err);
    } else {
        serverLog(LL_NOTICE, "FAILOVER to any replica aborted: %s", err);
    }
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        replicationUnsetPrimary();
    }
    clearFailoverState();
}

/*
 * FAILOVER [TO <HOST> <PORT> [FORCE]] [ABORT] [TIMEOUT <timeout>]
 *
 * This command will coordinate a failover between the primary and one
 * of its replicas. The happy path contains the following steps:
 * 1) The primary will initiate a client pause write, to stop replication
 * traffic.
 * 2) The primary will periodically check if any of its replicas has
 * consumed the entire replication stream through acks.
 * 3) Once any replica has caught up, the primary will itself become a replica.
 * 4) The primary will send a PSYNC FAILOVER request to the target replica, which
 * if accepted will cause the replica to become the new primary and start a sync.
 *
 * FAILOVER ABORT is the only way to abort a failover command, as replicaof
 * will be disabled. This may be needed if the failover is unable to progress.
 *
 * The optional arguments [TO <HOST> <IP>] allows designating a specific replica
 * to be failed over to.
 *
 * FORCE flag indicates that even if the target replica is not caught up,
 * failover to it anyway. This must be specified with a timeout and a target
 * HOST and IP.
 *
 * TIMEOUT <timeout> indicates how long should the primary wait for
 * a replica to sync up before aborting. If not specified, the failover
 * will attempt forever and must be manually aborted.
 */
void failoverCommand(client *c) {
    if (!clusterAllowFailoverCmd(c)) {
        return;
    }

    /* Handle special case for abort */
    if ((c->argc == 2) && !strcasecmp(c->argv[1]->ptr, "abort")) {
        if (server.failover_state == NO_FAILOVER) {
            addReplyError(c, "No failover in progress.");
            return;
        }

        abortFailover("Failover manually aborted");
        addReply(c, shared.ok);
        return;
    }

    long timeout_in_ms = 0;
    int force_flag = 0;
    long port = 0;
    char *host = NULL;

    /* Parse the command for syntax and arguments. */
    for (int j = 1; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr, "timeout") && (j + 1 < c->argc) && timeout_in_ms == 0) {
            if (getLongFromObjectOrReply(c, c->argv[j + 1], &timeout_in_ms, NULL) != C_OK) return;
            if (timeout_in_ms <= 0) {
                addReplyError(c, "FAILOVER timeout must be greater than 0");
                return;
            }
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr, "to") && (j + 2 < c->argc) && !host) {
            if (getLongFromObjectOrReply(c, c->argv[j + 2], &port, NULL) != C_OK) return;
            host = c->argv[j + 1]->ptr;
            j += 2;
        } else if (!strcasecmp(c->argv[j]->ptr, "force") && !force_flag) {
            force_flag = 1;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c, "FAILOVER already in progress.");
        return;
    }

    if (server.primary_host) {
        addReplyError(c, "FAILOVER is not valid when server is a replica.");
        return;
    }

    if (listLength(server.replicas) == 0) {
        addReplyError(c, "FAILOVER requires connected replicas.");
        return;
    }

    if (force_flag && (!timeout_in_ms || !host)) {
        addReplyError(c, "FAILOVER with force option requires both a timeout "
                         "and target HOST and IP.");
        return;
    }

    /* If a replica address was provided, validate that it is connected. */
    if (host) {
        client *replica = findReplica(host, port);

        if (replica == NULL) {
            addReplyError(c, "FAILOVER target HOST and PORT is not "
                             "a replica.");
            return;
        }

        /* Check if requested replica is online */
        if (replica->repl_data->repl_state != REPLICA_STATE_ONLINE) {
            addReplyError(c, "FAILOVER target replica is not online.");
            return;
        }

        server.target_replica_host = zstrdup(host);
        server.target_replica_port = port;
        serverLog(LL_NOTICE, "FAILOVER requested to %s:%ld.", host, port);
    } else {
        serverLog(LL_NOTICE, "FAILOVER requested to any replica.");
    }

    mstime_t now = commandTimeSnapshot();
    if (timeout_in_ms) {
        server.failover_end_time = now + timeout_in_ms;
    }

    server.force_failover = force_flag;
    server.failover_state = FAILOVER_WAIT_FOR_SYNC;
    /* Cluster failover will unpause eventually */
    pauseActions(PAUSE_DURING_FAILOVER, LLONG_MAX, PAUSE_ACTIONS_CLIENT_WRITE_SET);
    addReply(c, shared.ok);
}

/* Failover cron function, checks coordinated failover state.
 *
 * Implementation note: The current implementation calls replicationSetPrimary()
 * to start the failover request, this has some unintended side effects if the
 * failover doesn't work like blocked clients will be unblocked and replicas will
 * be disconnected. This could be optimized further.
 */
void updateFailoverStatus(void) {
    if (server.failover_state != FAILOVER_WAIT_FOR_SYNC) return;
    mstime_t now = server.mstime;

    /* Check if failover operation has timed out */
    if (server.failover_end_time && server.failover_end_time <= now) {
        if (server.force_failover) {
            serverLog(LL_NOTICE, "FAILOVER to %s:%d time out exceeded, failing over.", server.target_replica_host,
                      server.target_replica_port);
            server.failover_state = FAILOVER_IN_PROGRESS;
            /* If timeout has expired force a failover if requested. */
            replicationSetPrimary(server.target_replica_host, server.target_replica_port, 0);
            return;
        } else {
            /* Force was not requested, so timeout. */
            abortFailover("Replica never caught up before timeout");
            return;
        }
    }

    /* Check to see if the replica has caught up so failover can start */
    client *replica = NULL;
    if (server.target_replica_host) {
        replica = findReplica(server.target_replica_host, server.target_replica_port);
    } else {
        listIter li;
        listNode *ln;

        listRewind(server.replicas, &li);
        /* Find any replica that has matched our repl_offset */
        while ((ln = listNext(&li))) {
            replica = ln->value;
            if (replica->repl_data->repl_ack_off == server.primary_repl_offset) {
                char ip[NET_IP_STR_LEN], *replicaaddr = replica->repl_data->replica_addr;

                if (!replicaaddr) {
                    if (connAddrPeerName(replica->conn, ip, sizeof(ip), NULL) == -1) continue;
                    replicaaddr = ip;
                }

                /* We are now failing over to this specific node */
                server.target_replica_host = zstrdup(replicaaddr);
                server.target_replica_port = replica->repl_data->replica_listening_port;
                break;
            }
        }
    }

    /* We've found a replica that is caught up */
    if (replica && (replica->repl_data->repl_ack_off == server.primary_repl_offset)) {
        server.failover_state = FAILOVER_IN_PROGRESS;
        serverLog(LL_NOTICE, "Failover target %s:%d is synced, failing over.", server.target_replica_host,
                  server.target_replica_port);
        /* Designated replica is caught up, failover to it. */
        replicationSetPrimary(server.target_replica_host, server.target_replica_port, 0);
    }
}
