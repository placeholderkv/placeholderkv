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

#include "server.h"
#include "cluster.h"
#include "cluster_slot_stats.h"
#include "script.h"
#include "intset.h"
#include "sds.h"
#include "fpconv_dtoa.h"
#include "fmtargs.h"
#include "io_threads.h"
#include "module.h"
#include <strings.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>
#include <stdatomic.h>

/* This struct is used to encapsulate filtering criteria for operations on clients
 * such as identifying specific clients to kill or retrieve. Each field in the struct
 * represents a filter that can be applied based on specific attributes of a client. */
typedef struct {
    /* A set of client IDs to filter. If NULL, no ID filtering is applied. */
    intset *ids;
    /* Maximum age (in seconds) of a client connection for filtering.
     * Connections younger than this value will not match.
     * A value of 0 means no age filtering. */
    long long max_age;
    /* Address/port of the client. If NULL, no address filtering is applied. */
    char *addr;
    /* Remote address/port of the client. If NULL, no address filtering is applied. */
    char *laddr;
    /* Filtering clients by authentication user. If NULL, no user-based filtering is applied. */
    user *user;
    /* Client type to filter. If set to -1, no type filtering is applied. */
    int type;
    /* Boolean flag to determine if the current client (`me`) should be filtered. 1 means "skip me", 0 means otherwise. */
    int skipme;
} clientFilter;

static void setProtocolError(const char *errstr, client *c);
static void pauseClientsByClient(mstime_t end, int isPauseClientAll);
int postponeClientRead(client *c);
char *getClientSockname(client *c);
static int parseClientFiltersOrReply(client *c, int index, clientFilter *filter);
static int clientMatchesFilter(client *client, clientFilter client_filter);
static sds getAllFilteredClientsInfoString(clientFilter *client_filter, int hide_user_data);

int ProcessingEventsWhileBlocked = 0; /* See processEventsWhileBlocked(). */
__thread sds thread_shared_qb = NULL;

typedef enum {
    PARSE_OK = 0,
    PARSE_ERR = -1,
    PARSE_NEEDMORE = -2,
} parseResult;

/* Return the amount of memory used by the sds string at object->ptr
 * for a string object. This includes internal fragmentation. */
size_t getStringObjectSdsUsedMemory(robj *o) {
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
    if (o->encoding != OBJ_ENCODING_INT) {
        return sdsAllocSize(o->ptr);
    }
    return 0;
}

/* Return the length of a string object.
 * This does NOT include internal fragmentation or sds unused space. */
size_t getStringObjectLen(robj *o) {
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
    switch (o->encoding) {
    case OBJ_ENCODING_RAW: return sdslen(o->ptr);
    case OBJ_ENCODING_EMBSTR: return sdslen(o->ptr);
    default: return 0; /* Just integer encoding for now. */
    }
}

/* Client.reply list dup and free methods. */
void *dupClientReplyValue(void *o) {
    clientReplyBlock *old = o;
    clientReplyBlock *buf = zmalloc(sizeof(clientReplyBlock) + old->size);
    memcpy(buf, o, sizeof(clientReplyBlock) + old->size);
    return buf;
}

void freeClientReplyValue(void *o) {
    zfree(o);
}

/* This function links the client to the global linked list of clients.
 * unlinkClient() does the opposite, among other things. */
void linkClient(client *c) {
    listAddNodeTail(server.clients, c);
    /* Note that we remember the linked list node where the client is stored,
     * this way removing the client in unlinkClient() will not require
     * a linear scan, but just a constant time operation. */
    c->client_list_node = listLast(server.clients);
    uint64_t id = htonu64(c->id);
    raxInsert(server.clients_index, (unsigned char *)&id, sizeof(id), c, NULL);
}

/* Initialize client authentication state. */
static void clientSetDefaultAuth(client *c) {
    /* If the default user does not require authentication, the user is
     * directly authenticated. */
    c->user = DefaultUser;
    c->flag.authenticated = (c->user->flags & USER_FLAG_NOPASS) && !(c->user->flags & USER_FLAG_DISABLED);
}

int authRequired(client *c) {
    /* Check if the user is authenticated. This check is skipped in case
     * the default user is flagged as "nopass" and is active. */
    int auth_required = (!(DefaultUser->flags & USER_FLAG_NOPASS) || (DefaultUser->flags & USER_FLAG_DISABLED)) &&
                        !c->flag.authenticated;
    return auth_required;
}

static inline int isReplicaReadyForReplData(client *replica) {
    return (replica->repl_data->repl_state == REPLICA_STATE_ONLINE || replica->repl_data->repl_state == REPLICA_STATE_BG_RDB_LOAD) &&
           !(replica->flag.close_asap);
}

client *createClient(connection *conn) {
    client *c = zmalloc(sizeof(client));

    /* passing NULL as conn it is possible to create a non connected client.
     * This is useful since all the commands needs to be executed
     * in the context of a client. When commands are executed in other
     * contexts (for instance a Lua script) we need a non connected client. */
    if (conn) {
        connEnableTcpNoDelay(conn);
        if (server.tcpkeepalive) connKeepAlive(conn, server.tcpkeepalive);
        connSetReadHandler(conn, readQueryFromClient);
        connSetPrivateData(conn, c);
        conn->flags |= CONN_FLAG_ALLOW_ACCEPT_OFFLOAD;
    }
    c->buf = zmalloc_usable(PROTO_REPLY_CHUNK_BYTES, &c->buf_usable_size);
    selectDb(c, 0);
    uint64_t client_id = atomic_fetch_add_explicit(&server.next_client_id, 1, memory_order_relaxed);
    c->id = client_id;
#ifdef LOG_REQ_RES
    reqresReset(c, 0);
    c->resp = server.client_default_resp;
#else
    c->resp = 2;
#endif
    c->conn = conn;
    c->name = NULL;
    c->lib_name = NULL;
    c->lib_ver = NULL;
    c->bufpos = 0;
    c->buf_peak = c->buf_usable_size;
    c->buf_peak_last_reset_time = server.unixtime;
    c->qb_pos = 0;
    c->querybuf = NULL;
    c->querybuf_peak = 0;
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->argv_len = 0;
    c->argv_len_sum = 0;
    c->original_argc = 0;
    c->original_argv = NULL;
    c->nread = 0;
    c->read_flags = 0;
    c->write_flags = 0;
    c->cmd = c->lastcmd = c->realcmd = c->io_parsed_cmd = NULL;
    c->cur_script = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->raw_flag = 0;
    c->capa = 0;
    c->slot = -1;
    c->ctime = c->last_interaction = server.unixtime;
    c->duration = 0;
    clientSetDefaultAuth(c);
    c->reply = listCreate();
    c->deferred_reply_errors = NULL;
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    listSetFreeMethod(c->reply, freeClientReplyValue);
    listSetDupMethod(c->reply, dupClientReplyValue);
    c->repl_data = NULL;
    c->bstate = NULL;
    c->pubsub_data = NULL;
    c->module_data = NULL;
    c->mstate = NULL;
    c->woff = 0;
    c->peerid = NULL;
    c->sockname = NULL;
    c->client_list_node = NULL;
    c->io_read_state = CLIENT_IDLE;
    c->io_write_state = CLIENT_IDLE;
    c->nwritten = 0;
    c->last_memory_usage = 0;
    c->last_memory_type = CLIENT_TYPE_NORMAL;
    listInitNode(&c->clients_pending_write_node, c);
    listInitNode(&c->pending_read_list_node, c);
    c->mem_usage_bucket = NULL;
    c->mem_usage_bucket_node = NULL;
    if (conn) linkClient(c);
    c->net_input_bytes = 0;
    c->net_input_bytes_curr_cmd = 0;
    c->net_output_bytes = 0;
    c->net_output_bytes_curr_cmd = 0;
    c->commands_processed = 0;
    c->io_last_reply_block = NULL;
    c->io_last_bufpos = 0;
    return c;
}

void installClientWriteHandler(client *c) {
    int ae_barrier = 0;
    /* For the fsync=always policy, we want that a given FD is never
     * served for reading and writing in the same event loop iteration,
     * so that in the middle of receiving the query, and serving it
     * to the client, we'll call beforeSleep() that will do the
     * actual fsync of AOF to disk. the write barrier ensures that. */
    if (server.aof_state == AOF_ON && server.aof_fsync == AOF_FSYNC_ALWAYS) {
        ae_barrier = 1;
    }
    if (connSetWriteHandlerWithBarrier(c->conn, sendReplyToClient, ae_barrier) == C_ERR) {
        freeClientAsync(c);
    }
}

/* This function puts the client in the queue of clients that should write
 * their output buffers to the socket. Note that it does not *yet* install
 * the write handler, to start clients are put in a queue of clients that need
 * to write, so we try to do that before returning in the event loop (see the
 * handleClientsWithPendingWrites() function).
 * If we fail and there is more data to write, compared to what the socket
 * buffers can hold, then we'll really install the handler. */
void putClientInPendingWriteQueue(client *c) {
    /* Schedule the client to write the output buffers to the socket only
     * if not already done and, for replicas, if the replica can actually receive
     * writes at this stage. */
    if (!c->flag.pending_write &&
        (!c->repl_data ||
         c->repl_data->repl_state == REPL_STATE_NONE ||
         (isReplicaReadyForReplData(c) && !c->repl_data->repl_start_cmd_stream_on_ack))) {
        /* Here instead of installing the write handler, we just flag the
         * client and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */
        c->flag.pending_write = 1;
        listLinkNodeHead(server.clients_pending_write, &c->clients_pending_write_node);
    }
}

/* This function is called every time we are going to transmit new data
 * to the client. The behavior is the following:
 *
 * If the client should receive new data (normal clients will) the function
 * returns C_OK, and make sure to install the write handler in our event
 * loop so that when the socket is writable new data gets written.
 *
 * If the client should not receive new data, because it is a fake client
 * (used to load AOF in memory), a primary or because the setup of the write
 * handler failed, the function returns C_ERR.
 *
 * The function may return C_OK without actually installing the write
 * event handler in the following cases:
 *
 * 1) The event handler should already be installed since the output buffer
 *    already contains something.
 * 2) The client is a replica but not yet online, so we want to just accumulate
 *    writes in the buffer but not actually sending them yet.
 *
 * Typically gets called every time a reply is built, before adding more
 * data to the clients output buffers. If the function returns C_ERR no
 * data should be appended to the output buffers. */
int prepareClientToWrite(client *c) {
    /* If it's the Lua client we always return ok without installing any
     * handler since there is no socket at all. */
    if (c->flag.script || c->flag.module) return C_OK;

    /* If CLIENT_CLOSE_ASAP flag is set, we need not write anything. */
    if (c->flag.close_asap) return C_ERR;

    /* CLIENT REPLY OFF / SKIP handling: don't send replies.
     * CLIENT_PUSHING handling: disables the reply silencing flags. */
    if ((c->flag.reply_off || c->flag.reply_skip) && !c->flag.pushing) return C_ERR;

    /* Primaries don't receive replies, unless CLIENT_PRIMARY_FORCE_REPLY flag
     * is set. */
    if (c->flag.primary && !c->flag.primary_force_reply) return C_ERR;

    /* Skip the fake client, such as the fake client for AOF loading.
     * But CLIENT_ID_CACHED_RESPONSE is allowed since it is a fake client
     * but has a connection to cache the response. */
    if (c->flag.fake && c->id != CLIENT_ID_CACHED_RESPONSE) return C_ERR;
    serverAssert(c->conn);

    /* Schedule the client to write the output buffers to the socket, unless
     * it should already be setup to do so (it has already pending data). */
    if (!clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);

    /* Authorize the caller to queue in the output buffer of this client. */
    return C_OK;
}

/* Returns everything in the client reply linked list in a SDS format.
 * This should only be used only with a caching client. */
sds aggregateClientOutputBuffer(client *c) {
    sds cmd_response = sdsempty();
    listIter li;
    listNode *ln;
    clientReplyBlock *val_block;
    listRewind(c->reply, &li);

    /* Here, c->buf is not used, thus we confirm c->bufpos remains 0. */
    serverAssert(c->bufpos == 0);
    while ((ln = listNext(&li)) != NULL) {
        val_block = (clientReplyBlock *)listNodeValue(ln);
        cmd_response = sdscatlen(cmd_response, val_block->buf, val_block->used);
    }
    return cmd_response;
}

/* This function creates and returns a fake client for recording the command response
 * to initiate caching of any command response.
 *
 * It needs be paired with `deleteCachedResponseClient` function to stop caching. */
client *createCachedResponseClient(int resp) {
    struct client *recording_client = createClient(NULL);
    /* It is a fake client but with a connection, setting a special client id,
     * so we can identify it's a fake cached response client. */
    recording_client->id = CLIENT_ID_CACHED_RESPONSE;
    recording_client->resp = resp;
    /* Allocating the `conn` allows to prepare the caching client before adding
     * data to the clients output buffer by `prepareClientToWrite`. */
    recording_client->conn = zcalloc(sizeof(connection));
    recording_client->flag.fake = 1;
    return recording_client;
}

/* This function is used to stop caching of any command response after `createCachedResponseClient` is called.
 * It returns the command response as SDS from the recording_client's reply buffer. */
void deleteCachedResponseClient(client *recording_client) {
    zfree(recording_client->conn);
    recording_client->conn = NULL;
    freeClient(recording_client);
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

/* Attempts to add the reply to the static buffer in the client struct.
 * Returns the length of data that is added to the reply buffer.
 *
 * Sanitizer suppression: client->buf_usable_size determined by
 * zmalloc_usable_size() call. Writing beyond client->buf boundaries confuses
 * sanitizer and generates a false positive out-of-bounds error */
VALKEY_NO_SANITIZE("bounds")
size_t _addReplyToBuffer(client *c, const char *s, size_t len) {
    size_t available = c->buf_usable_size - c->bufpos;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(c->reply) > 0) return 0;

    size_t reply_len = len > available ? available : len;
    memcpy(c->buf + c->bufpos, s, reply_len);
    c->bufpos += reply_len;
    /* We update the buffer peak after appending the reply to the buffer */
    if (c->buf_peak < (size_t)c->bufpos) c->buf_peak = (size_t)c->bufpos;
    return reply_len;
}

/* Adds the reply to the reply linked list.
 * Note: some edits to this function need to be relayed to AddReplyFromClient. */
void _addReplyProtoToList(client *c, list *reply_list, const char *s, size_t len) {
    listNode *ln = listLast(reply_list);
    clientReplyBlock *tail = ln ? listNodeValue(ln) : NULL;

    /* Note that 'tail' may be NULL even if we have a tail node, because when
     * addReplyDeferredLen() is used, it sets a dummy node to NULL just
     * to fill it later, when the size of the bulk length is set. */

    /* Append to tail string when possible. */
    if (tail) {
        /* Copy the part we can fit into the tail, and leave the rest for a
         * new node */
        size_t avail = tail->size - tail->used;
        size_t copy = avail >= len ? len : avail;
        memcpy(tail->buf + tail->used, s, copy);
        tail->used += copy;
        s += copy;
        len -= copy;
    }
    if (len) {
        /* Create a new node, make sure it is allocated to at
         * least PROTO_REPLY_CHUNK_BYTES */
        size_t usable_size;
        size_t size = len < PROTO_REPLY_CHUNK_BYTES ? PROTO_REPLY_CHUNK_BYTES : len;
        tail = zmalloc_usable(size + sizeof(clientReplyBlock), &usable_size);
        /* take over the allocation's internal fragmentation */
        tail->size = usable_size - sizeof(clientReplyBlock);
        tail->used = len;
        memcpy(tail->buf, s, len);
        listAddNodeTail(reply_list, tail);
        c->reply_bytes += tail->size;

        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* The subscribe / unsubscribe command family has a push as a reply,
 * or in other words, it responds with a push (or several of them
 * depending on how many arguments it got), and has no reply. */
int cmdHasPushAsReply(struct serverCommand *cmd) {
    if (!cmd) return 0;
    return cmd->proc == subscribeCommand || cmd->proc == unsubscribeCommand || cmd->proc == psubscribeCommand ||
           cmd->proc == punsubscribeCommand || cmd->proc == ssubscribeCommand || cmd->proc == sunsubscribeCommand;
}

void _addReplyToBufferOrList(client *c, const char *s, size_t len) {
    if (c->flag.close_after_reply) return;

    /* Replicas should normally not cause any writes to the reply buffer. In case a rogue replica sent a command on the
     * replication link that caused a reply to be generated we'll simply disconnect it.
     * Note this is the simplest way to check a command added a response. Replication links are used to write data but
     * not for responses, so we should normally never get here on a replica client. */
    if (getClientType(c) == CLIENT_TYPE_REPLICA) {
        sds cmdname = c->lastcmd ? c->lastcmd->fullname : NULL;
        logInvalidUseAndFreeClientAsync(c, "Replica generated a reply to command '%s'",
                                        cmdname ? cmdname : "<unknown>");
        return;
    }

    c->net_output_bytes_curr_cmd += len;

    /* We call it here because this function may affect the reply
     * buffer offset (see function comment) */
    reqresSaveClientReplyOffset(c);

    /* If we're processing a push message into the current client (i.e. executing PUBLISH
     * to a channel which we are subscribed to, then we wanna postpone that message to be added
     * after the command's reply (specifically important during multi-exec). the exception is
     * the SUBSCRIBE command family, which (currently) have a push message instead of a proper reply.
     * The check for executing_client also avoids affecting push messages that are part of eviction.
     * Check CLIENT_PUSHING first to avoid race conditions, as it's absent in module's fake client. */
    if (c->flag.pushing && c == server.current_client && server.executing_client &&
        !cmdHasPushAsReply(server.executing_client->cmd)) {
        _addReplyProtoToList(c, server.pending_push_messages, s, len);
        return;
    }

    size_t reply_len = _addReplyToBuffer(c, s, len);
    if (len > reply_len) _addReplyProtoToList(c, c->reply, s + reply_len, len - reply_len);
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

/* Add the object 'obj' string representation to the client output buffer. */
void addReply(client *c, robj *obj) {
    if (prepareClientToWrite(c) != C_OK) return;

    if (sdsEncodedObject(obj)) {
        _addReplyToBufferOrList(c, obj->ptr, sdslen(obj->ptr));
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* For integer encoded strings we just convert it into a string
         * using our optimized function, and attach the resulting string
         * to the output buffer. */
        char buf[32];
        size_t len = ll2string(buf, sizeof(buf), (long)obj->ptr);
        _addReplyToBufferOrList(c, buf, len);
    } else {
        serverPanic("Wrong obj->encoding in addReply()");
    }
}

/* Add the SDS 's' string to the client output buffer, as a side effect
 * the SDS string is freed. */
void addReplySds(client *c, sds s) {
    if (prepareClientToWrite(c) != C_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    _addReplyToBufferOrList(c, s, sdslen(s));
    sdsfree(s);
}

/* This low level function just adds whatever protocol you send it to the
 * client buffer, trying the static buffer initially, and using the string
 * of objects if not possible.
 *
 * It is efficient because does not create an SDS object nor an Object
 * if not needed. The object will only be created by calling
 * _addReplyProtoToList() if we fail to extend the existing tail object
 * in the list of objects. */
void addReplyProto(client *c, const char *s, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyToBufferOrList(c, s, len);
}

/* Low level function called by the addReplyError...() functions.
 * It emits the protocol for an error reply, in the form:
 *
 * -ERRORCODE Error Message<CR><LF>
 *
 * If the error code is already passed in the string 's', the error
 * code provided is used, otherwise the string "-ERR " for the generic
 * error code is automatically added.
 * Note that 's' must NOT end with \r\n. */
void addReplyErrorLength(client *c, const char *s, size_t len) {
    /* If the string already starts with "-..." then the error code
     * is provided by the caller. Otherwise we use "-ERR". */
    if (!len || s[0] != '-') addReplyProto(c, "-ERR ", 5);
    addReplyProto(c, s, len);
    addReplyProto(c, "\r\n", 2);
}

/* Do some actions after an error reply was sent (Log if needed, updates stats, etc.)
 * Possible flags:
 * * ERR_REPLY_FLAG_NO_STATS_UPDATE - indicate not to update any error stats. */
void afterErrorReply(client *c, const char *s, size_t len, int flags) {
    /* Module clients fall into two categories:
     * Calls to RM_Call, in which case the error isn't being returned to a client, so should not be counted.
     * Module thread safe context calls to RM_ReplyWithError, which will be added to a real client by the main thread
     * later. */
    if (c->flag.module) {
        if (!c->deferred_reply_errors) {
            c->deferred_reply_errors = listCreate();
            listSetFreeMethod(c->deferred_reply_errors, sdsfreeVoid);
        }
        listAddNodeTail(c->deferred_reply_errors, sdsnewlen(s, len));
        return;
    }

    if (!(flags & ERR_REPLY_FLAG_NO_STATS_UPDATE)) {
        /* Increment the global error counter */
        server.stat_total_error_replies++;
        /* Increment the error stats
         * If the string already starts with "-..." then the error prefix
         * is provided by the caller (we limit the search to 32 chars). Otherwise we use "-ERR". */
        char *err_prefix = "ERR";
        size_t prefix_len = 3;
        if (s[0] == '-') {
            char *spaceloc = memchr(s, ' ', len < 32 ? len : 32);
            /* If we cannot retrieve the error prefix, use the default: "ERR". */
            if (spaceloc) {
                const size_t errEndPos = (size_t)(spaceloc - s);
                err_prefix = (char *)s + 1;
                prefix_len = errEndPos - 1;
            }
        }
        /* After the errors RAX reaches its limit, instead of tracking
         * custom errors (e.g. LUA), we track the error under `errorstat_ERRORSTATS_OVERFLOW` */
        if (flags & ERR_REPLY_FLAG_CUSTOM && raxSize(server.errors) >= ERRORSTATS_LIMIT &&
            !raxFind(server.errors, (unsigned char *)err_prefix, prefix_len, NULL)) {
            err_prefix = ERRORSTATS_OVERFLOW_ERR;
            prefix_len = strlen(ERRORSTATS_OVERFLOW_ERR);
        }
        incrementErrorCount(err_prefix, prefix_len);
    } else {
        /* stat_total_error_replies will not be updated, which means that
         * the cmd stats will not be updated as well, we still want this command
         * to be counted as failed so we update it here. We update c->realcmd in
         * case c->cmd was changed (like in GEOADD). */
        c->realcmd->failed_calls++;
    }

    /* Sometimes it could be normal that a replica replies to a primary with
     * an error and this function gets called. Actually the error will never
     * be sent because addReply*() against primary clients has no effect...
     *
     * It can happen when the versions are different and replica cannot recognize
     * the commands sent by the primary. However it is useful to log such events since
     * they are rare and may hint at errors in a script or a bug in the server. */
    int ctype = getClientType(c);
    if (ctype == CLIENT_TYPE_PRIMARY || ctype == CLIENT_TYPE_REPLICA || c->id == CLIENT_ID_AOF) {
        char *to, *from;

        if (c->id == CLIENT_ID_AOF) {
            to = "AOF-loading-client";
            from = "server";
        } else if (ctype == CLIENT_TYPE_PRIMARY) {
            to = "primary";
            from = "replica";
        } else {
            to = "replica";
            from = "primary";
        }

        if (len > 4096) len = 4096;
        sds cmdname = c->lastcmd ? c->lastcmd->fullname : NULL;
        serverLog(LL_WARNING,
                  "== CRITICAL == This %s is sending an error "
                  "to its %s: '%.*s' after processing the command "
                  "'%s'",
                  from, to, (int)len, s, cmdname ? cmdname : "<unknown>");
        if (ctype == CLIENT_TYPE_PRIMARY && server.repl_backlog && server.repl_backlog->histlen > 0) {
            showLatestBacklog();
        }
        server.stat_unexpected_error_replies++;

        /* Based off the propagation error behavior, check if we need to panic here. There
         * are currently two checked cases:
         * * If this command was from our primary and we are not a writable replica.
         * * We are reading from an AOF file. */
        int panic_in_replicas = (ctype == CLIENT_TYPE_PRIMARY && server.repl_replica_ro) &&
                                (server.propagation_error_behavior == PROPAGATION_ERR_BEHAVIOR_PANIC ||
                                 server.propagation_error_behavior == PROPAGATION_ERR_BEHAVIOR_PANIC_ON_REPLICAS);
        int panic_in_aof =
            c->id == CLIENT_ID_AOF && server.propagation_error_behavior == PROPAGATION_ERR_BEHAVIOR_PANIC;
        if (panic_in_replicas || panic_in_aof) {
            serverPanic("This %s panicked sending an error to its %s"
                        " after processing the command '%s'",
                        from, to, cmdname ? cmdname : "<unknown>");
        }
    }
}

/* The 'err' object is expected to start with -ERRORCODE and end with \r\n.
 * Unlike addReplyErrorSds and others alike which rely on addReplyErrorLength. */
void addReplyErrorObject(client *c, robj *err) {
    addReply(c, err);
    afterErrorReply(c, err->ptr, sdslen(err->ptr) - 2, 0); /* Ignore trailing \r\n */
}

/* Sends either a reply or an error reply by checking the first char.
 * If the first char is '-' the reply is considered an error.
 * In any case the given reply is sent, if the reply is also recognize
 * as an error we also perform some post reply operations such as
 * logging and stats update. */
void addReplyOrErrorObject(client *c, robj *reply) {
    serverAssert(sdsEncodedObject(reply));
    sds rep = reply->ptr;
    if (sdslen(rep) > 1 && rep[0] == '-') {
        addReplyErrorObject(c, reply);
    } else {
        addReply(c, reply);
    }
}

/* See addReplyErrorLength for expectations from the input string. */
void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c, err, strlen(err));
    afterErrorReply(c, err, strlen(err), 0);
}

/* Add error reply to the given client.
 * Supported flags:
 * * ERR_REPLY_FLAG_NO_STATS_UPDATE - indicate not to perform any error stats updates */
void addReplyErrorSdsEx(client *c, sds err, int flags) {
    addReplyErrorLength(c, err, sdslen(err));
    afterErrorReply(c, err, sdslen(err), flags);
    sdsfree(err);
}

/* See addReplyErrorLength for expectations from the input string. */
/* As a side effect the SDS string is freed. */
void addReplyErrorSds(client *c, sds err) {
    addReplyErrorSdsEx(c, err, 0);
}

/* See addReplyErrorLength for expectations from the input string. */
/* As a side effect the SDS string is freed. */
void addReplyErrorSdsSafe(client *c, sds err) {
    err = sdsmapchars(err, "\r\n", "  ", 2);
    addReplyErrorSdsEx(c, err, 0);
}

/* Internal function used by addReplyErrorFormat, addReplyErrorFormatEx and RM_ReplyWithErrorFormat.
 * Refer to afterErrorReply for more information about the flags. */
void addReplyErrorFormatInternal(client *c, int flags, const char *fmt, va_list ap) {
    va_list cpy;
    va_copy(cpy, ap);
    sds s = sdscatvprintf(sdsempty(), fmt, cpy);
    va_end(cpy);
    /* Trim any newlines at the end (ones will be added by addReplyErrorLength) */
    s = sdstrim(s, "\r\n");
    /* Make sure there are no newlines in the middle of the string, otherwise
     * invalid protocol is emitted. */
    s = sdsmapchars(s, "\r\n", "  ", 2);
    addReplyErrorLength(c, s, sdslen(s));
    afterErrorReply(c, s, sdslen(s), flags);
    sdsfree(s);
}

void addReplyErrorFormatEx(client *c, int flags, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    addReplyErrorFormatInternal(c, flags, fmt, ap);
    va_end(ap);
}

/* See addReplyErrorLength for expectations from the formatted string.
 * The formatted string is safe to contain \r and \n anywhere. */
void addReplyErrorFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    addReplyErrorFormatInternal(c, 0, fmt, ap);
    va_end(ap);
}

void addReplyErrorArity(client *c) {
    addReplyErrorFormat(c, "wrong number of arguments for '%s' command", c->cmd->fullname);
}

void addReplyErrorExpireTime(client *c) {
    addReplyErrorFormat(c, "invalid expire time in '%s' command", c->cmd->fullname);
}

void addReplyStatusLength(client *c, const char *s, size_t len) {
    addReplyProto(c, "+", 1);
    addReplyProto(c, s, len);
    addReplyProto(c, "\r\n", 2);
}

void addReplyStatus(client *c, const char *status) {
    addReplyStatusLength(c, status, strlen(status));
}

void addReplyStatusFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    sds s = sdscatvprintf(sdsempty(), fmt, ap);
    va_end(ap);
    addReplyStatusLength(c, s, sdslen(s));
    sdsfree(s);
}

/* Sometimes we are forced to create a new reply node, and we can't append to
 * the previous one, when that happens, we wanna try to trim the unused space
 * at the end of the last reply node which we won't use anymore. */
void trimReplyUnusedTailSpace(client *c) {
    listNode *ln = listLast(c->reply);
    clientReplyBlock *tail = ln ? listNodeValue(ln) : NULL;

    /* Note that 'tail' may be NULL even if we have a tail node, because when
     * addReplyDeferredLen() is used */
    if (!tail) return;

    /* We only try to trim the space is relatively high (more than a 1/4 of the
     * allocation), otherwise there's a high chance realloc will NOP.
     * Also, to avoid large memmove which happens as part of realloc, we only do
     * that if the used part is small.  */
    if (tail->size - tail->used > tail->size / 4 && tail->used < PROTO_REPLY_CHUNK_BYTES &&
        c->io_write_state != CLIENT_PENDING_IO) {
        size_t usable_size;
        size_t old_size = tail->size;
        tail = zrealloc_usable(tail, tail->used + sizeof(clientReplyBlock), &usable_size);
        /* take over the allocation's internal fragmentation (at least for
         * memory usage tracking) */
        tail->size = usable_size - sizeof(clientReplyBlock);
        c->reply_bytes = c->reply_bytes + tail->size - old_size;
        listNodeValue(ln) = tail;
    }
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *addReplyDeferredLen(client *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredAggregateLen() will be called. */
    if (prepareClientToWrite(c) != C_OK) return NULL;

    /* Replicas should normally not cause any writes to the reply buffer. In case a rogue replica sent a command on the
     * replication link that caused a reply to be generated we'll simply disconnect it.
     * Note this is the simplest way to check a command added a response. Replication links are used to write data but
     * not for responses, so we should normally never get here on a replica client. */
    if (getClientType(c) == CLIENT_TYPE_REPLICA) {
        sds cmdname = c->lastcmd ? c->lastcmd->fullname : NULL;
        logInvalidUseAndFreeClientAsync(c, "Replica generated a reply to command '%s'",
                                        cmdname ? cmdname : "<unknown>");
        return NULL;
    }

    /* We call it here because this function conceptually affects the reply
     * buffer offset (see function comment) */
    reqresSaveClientReplyOffset(c);

    trimReplyUnusedTailSpace(c);
    listAddNodeTail(c->reply, NULL); /* NULL is our placeholder. */
    return listLast(c->reply);
}

void setDeferredReply(client *c, void *node, const char *s, size_t length) {
    listNode *ln = (listNode *)node;
    clientReplyBlock *next, *prev;

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addReplyDeferredLen() */
    if (node == NULL) return;
    serverAssert(!listNodeValue(ln));

    /* Normally we fill this dummy NULL node, added by addReplyDeferredLen(),
     * with a new buffer structure containing the protocol needed to specify
     * the length of the array following. However sometimes there might be room
     * in the previous/next node so we can instead remove this NULL node, and
     * suffix/prefix our data in the node immediately before/after it, in order
     * to save a write(2) syscall later. Conditions needed to do it:
     *
     * - The prev node is non-NULL and has space in it or
     * - The next node is non-NULL,
     * - It has enough room already allocated
     * - And not too large (avoid large memmove)
     * - And the client is not in a pending I/O state */
    if (ln->prev != NULL && (prev = listNodeValue(ln->prev)) && prev->size - prev->used > 0 &&
        c->io_write_state != CLIENT_PENDING_IO) {
        size_t len_to_copy = prev->size - prev->used;
        if (len_to_copy > length) len_to_copy = length;
        memcpy(prev->buf + prev->used, s, len_to_copy);
        prev->used += len_to_copy;
        length -= len_to_copy;
        if (length == 0) {
            listDelNode(c->reply, ln);
            return;
        }
        s += len_to_copy;
    }

    if (ln->next != NULL && (next = listNodeValue(ln->next)) && next->size - next->used >= length &&
        next->used < PROTO_REPLY_CHUNK_BYTES * 4 && c->io_write_state != CLIENT_PENDING_IO) {
        memmove(next->buf + length, next->buf, next->used);
        memcpy(next->buf, s, length);
        next->used += length;
        listDelNode(c->reply, ln);
    } else {
        /* Create a new node */
        size_t usable_size;
        clientReplyBlock *buf = zmalloc_usable(length + sizeof(clientReplyBlock), &usable_size);
        /* Take over the allocation's internal fragmentation */
        buf->size = usable_size - sizeof(clientReplyBlock);
        buf->used = length;
        memcpy(buf->buf, s, length);
        listNodeValue(ln) = buf;
        c->reply_bytes += buf->size;

        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* Populate the length object and try gluing it to the next chunk. */
void setDeferredAggregateLen(client *c, void *node, long length, char prefix) {
    serverAssert(length >= 0);

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addReplyDeferredLen() */
    if (node == NULL) return;

    /* Things like *2\r\n, %3\r\n or ~4\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    const size_t hdr_len = OBJ_SHARED_HDR_STRLEN(length);
    const int opt_hdr = length < OBJ_SHARED_BULKHDR_LEN;
    if (prefix == '*' && opt_hdr) {
        setDeferredReply(c, node, shared.mbulkhdr[length]->ptr, hdr_len);
        return;
    }
    if (prefix == '%' && opt_hdr) {
        setDeferredReply(c, node, shared.maphdr[length]->ptr, hdr_len);
        return;
    }
    if (prefix == '~' && opt_hdr) {
        setDeferredReply(c, node, shared.sethdr[length]->ptr, hdr_len);
        return;
    }

    char lenstr[128];
    lenstr[0] = prefix;
    size_t lenstr_len = ll2string(lenstr + 1, sizeof(lenstr) - 1, length);
    lenstr[lenstr_len + 1] = '\r';
    lenstr[lenstr_len + 2] = '\n';
    setDeferredReply(c, node, lenstr, lenstr_len + 3);
}

void setDeferredArrayLen(client *c, void *node, long length) {
    setDeferredAggregateLen(c, node, length, '*');
}

void setDeferredMapLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    setDeferredAggregateLen(c, node, length, prefix);
}

void setDeferredSetLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    setDeferredAggregateLen(c, node, length, prefix);
}

void setDeferredAttributeLen(client *c, void *node, long length) {
    serverAssert(c->resp >= 3);
    setDeferredAggregateLen(c, node, length, '|');
}

void setDeferredPushLen(client *c, void *node, long length) {
    serverAssert(c->resp >= 3);
    setDeferredAggregateLen(c, node, length, '>');
}

/* Prepare a client for future writes. This is used so that we can
 * skip a large number of calls to prepareClientToWrite when
 * a command produces a lot of discrete elements in its output. */
writePreparedClient *prepareClientForFutureWrites(client *c) {
    if (prepareClientToWrite(c) == C_OK) {
        return (writePreparedClient *)c;
    }
    return NULL;
}

/* Add a double as a bulk reply */
void addReplyDouble(client *c, double d) {
    if (c->resp == 3) {
        char dbuf[MAX_D2STRING_CHARS + 3];
        dbuf[0] = ',';
        const int dlen = d2string(dbuf + 1, sizeof(dbuf) - 1, d);
        dbuf[dlen + 1] = '\r';
        dbuf[dlen + 2] = '\n';
        dbuf[dlen + 3] = '\0';
        addReplyProto(c, dbuf, dlen + 3);
    } else {
        char dbuf[MAX_LONG_DOUBLE_CHARS + 32];
        /* In order to prepend the string length before the formatted number,
         * but still avoid an extra memcpy of the whole number, we reserve space
         * for maximum header `$0000\r\n`, print double, add the resp header in
         * front of it, and then send the buffer with the right `start` offset. */
        const int dlen = d2string(dbuf + 7, sizeof(dbuf) - 7, d);
        int digits = digits10(dlen);
        int start = 4 - digits;
        serverAssert(start >= 0);
        dbuf[start] = '$';

        /* Convert `dlen` to string, putting it's digits after '$' and before the
         * formatted double string. */
        for (int i = digits, val = dlen; val && i > 0; --i, val /= 10) {
            dbuf[start + i] = "0123456789"[val % 10];
        }
        dbuf[5] = '\r';
        dbuf[6] = '\n';
        dbuf[dlen + 7] = '\r';
        dbuf[dlen + 8] = '\n';
        dbuf[dlen + 9] = '\0';
        addReplyProto(c, dbuf + start, dlen + 9 - start);
    }
}

void addReplyBigNum(client *c, const char *num, size_t len) {
    if (c->resp == 2) {
        addReplyBulkCBuffer(c, num, len);
    } else {
        addReplyProto(c, "(", 1);
        addReplyProto(c, num, len);
        addReplyProto(c, "\r\n", 2);
    }
}

/* Add a long double as a bulk reply, but uses a human readable formatting
 * of the double instead of exposing the crude behavior of doubles to the
 * dear user. */
void addReplyHumanLongDouble(client *c, long double d) {
    if (c->resp == 2) {
        robj *o = createStringObjectFromLongDouble(d, 1);
        addReplyBulk(c, o);
        decrRefCount(o);
    } else {
        char buf[MAX_LONG_DOUBLE_CHARS];
        int len = ld2string(buf, sizeof(buf), d, LD_STR_HUMAN);
        addReplyProto(c, ",", 1);
        addReplyProto(c, buf, len);
        addReplyProto(c, "\r\n", 2);
    }
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
static void _addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    const int opt_hdr = ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0;
    const size_t hdr_len = OBJ_SHARED_HDR_STRLEN(ll);
    if (prefix == '*' && opt_hdr) {
        _addReplyToBufferOrList(c, shared.mbulkhdr[ll]->ptr, hdr_len);
        return;
    } else if (prefix == '$' && opt_hdr) {
        _addReplyToBufferOrList(c, shared.bulkhdr[ll]->ptr, hdr_len);
        return;
    } else if (prefix == '%' && opt_hdr) {
        _addReplyToBufferOrList(c, shared.maphdr[ll]->ptr, hdr_len);
        return;
    } else if (prefix == '~' && opt_hdr) {
        _addReplyToBufferOrList(c, shared.sethdr[ll]->ptr, hdr_len);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf + 1, sizeof(buf) - 1, ll);
    buf[len + 1] = '\r';
    buf[len + 2] = '\n';
    _addReplyToBufferOrList(c, buf, len + 3);
}

void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c, shared.czero);
    else if (ll == 1)
        addReply(c, shared.cone);
    else {
        if (prepareClientToWrite(c) != C_OK) return;
        _addReplyLongLongWithPrefix(c, ll, ':');
    }
}

void addReplyAggregateLen(client *c, long length, int prefix) {
    serverAssert(length >= 0);
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyLongLongWithPrefix(c, length, prefix);
}

void addReplyArrayLen(client *c, long length) {
    addReplyAggregateLen(c, length, '*');
}

void addWritePreparedReplyArrayLen(writePreparedClient *wpc, long length) {
    client *c = (client *)wpc;
    serverAssert(length >= 0);
    _addReplyLongLongWithPrefix(c, length, '*');
}

void addReplyMapLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    addReplyAggregateLen(c, length, prefix);
}

void addWritePreparedReplyMapLen(writePreparedClient *wpc, long length) {
    client *c = (client *)wpc;
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    _addReplyLongLongWithPrefix(c, length, prefix);
}

void addReplySetLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    addReplyAggregateLen(c, length, prefix);
}

void addReplyAttributeLen(client *c, long length) {
    serverAssert(c->resp >= 3);
    addReplyAggregateLen(c, length, '|');
}

void addReplyPushLen(client *c, long length) {
    serverAssert(c->resp >= 3);
    serverAssertWithInfo(c, NULL, c->flag.pushing);
    addReplyAggregateLen(c, length, '>');
}

void addReplyNull(client *c) {
    if (c->resp == 2) {
        addReplyProto(c, "$-1\r\n", 5);
    } else {
        addReplyProto(c, "_\r\n", 3);
    }
}

void addReplyBool(client *c, int b) {
    if (c->resp == 2) {
        addReply(c, b ? shared.cone : shared.czero);
    } else {
        addReplyProto(c, b ? "#t\r\n" : "#f\r\n", 4);
    }
}

/* A null array is a concept that no longer exists in RESP3. However
 * RESP2 had it, so API-wise we have this call, that will emit the correct
 * RESP2 protocol, however for RESP3 the reply will always be just the
 * Null type "_\r\n". */
void addReplyNullArray(client *c) {
    if (c->resp == 2) {
        addReplyProto(c, "*-1\r\n", 5);
    } else {
        addReplyProto(c, "_\r\n", 3);
    }
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(client *c, robj *obj) {
    size_t len = stringObjectLen(obj);
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyLongLongWithPrefix(c, len, '$');
}

/* Add an Object as a bulk reply */
void addReplyBulk(client *c, robj *obj) {
    addReplyBulkLen(c, obj);
    addReply(c, obj);
    addReplyProto(c, "\r\n", 2);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(client *c, const void *p, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;
    _addReplyLongLongWithPrefix(c, len, '$');
    _addReplyToBufferOrList(c, p, len);
    _addReplyToBufferOrList(c, "\r\n", 2);
}

void addWritePreparedReplyBulkCBuffer(writePreparedClient *wpc, const void *p, size_t len) {
    client *c = (client *)wpc;
    _addReplyLongLongWithPrefix(c, len, '$');
    _addReplyToBufferOrList(c, p, len);
    _addReplyToBufferOrList(c, "\r\n", 2);
}

/* Add sds to reply (takes ownership of sds and frees it) */
void addReplyBulkSds(client *c, sds s) {
    if (prepareClientToWrite(c) != C_OK) {
        sdsfree(s);
        return;
    }
    _addReplyLongLongWithPrefix(c, sdslen(s), '$');
    _addReplyToBufferOrList(c, s, sdslen(s));
    sdsfree(s);
    _addReplyToBufferOrList(c, "\r\n", 2);
}

void addWritePreparedReplyBulkSds(writePreparedClient *wpc, sds s) {
    client *c = (client *)wpc;
    _addReplyLongLongWithPrefix(c, sdslen(s), '$');
    _addReplyToBufferOrList(c, s, sdslen(s));
    sdsfree(s);
    _addReplyToBufferOrList(c, "\r\n", 2);
}

/* Set sds to a deferred reply (for symmetry with addReplyBulkSds it also frees the sds) */
void setDeferredReplyBulkSds(client *c, void *node, sds s) {
    sds reply = sdscatprintf(sdsempty(), "$%d\r\n%s\r\n", (unsigned)sdslen(s), s);
    setDeferredReply(c, node, reply, sdslen(reply));
    sdsfree(reply);
    sdsfree(s);
}

/* Add a C null term string as bulk reply */
void addReplyBulkCString(client *c, const char *s) {
    if (s == NULL) {
        addReplyNull(c);
    } else {
        addReplyBulkCBuffer(c, s, strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(client *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf, 64, ll);
    addReplyBulkCBuffer(c, buf, len);
}

void addWritePreparedReplyBulkLongLong(writePreparedClient *wpc, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf, 64, ll);
    addWritePreparedReplyBulkCBuffer(wpc, buf, len);
}

/* Reply with a verbatim type having the specified extension.
 *
 * The 'ext' is the "extension" of the file, actually just a three
 * character type that describes the format of the verbatim string.
 * For instance "txt" means it should be interpreted as a text only
 * file by the receiver, "md " as markdown, and so forth. Only the
 * three first characters of the extension are used, and if the
 * provided one is shorter than that, the remaining is filled with
 * spaces. */
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext) {
    if (c->resp == 2) {
        addReplyBulkCBuffer(c, s, len);
    } else {
        char buf[32];
        size_t preflen = snprintf(buf, sizeof(buf), "=%zu\r\nxxx:", len + 4);
        char *p = buf + preflen - 4;
        for (int i = 0; i < 3; i++) {
            if (*ext == '\0') {
                p[i] = ' ';
            } else {
                p[i] = *ext++;
            }
        }
        addReplyProto(c, buf, preflen);
        addReplyProto(c, s, len);
        addReplyProto(c, "\r\n", 2);
    }
}

/* This function is similar to the addReplyHelp function but adds the
 * ability to pass in two arrays of strings. Some commands have
 * some additional subcommands based on the specific feature implementation
 * the server is compiled with (currently just clustering). This function allows
 * to pass is the common subcommands in `help` and any implementation
 * specific subcommands in `extended_help`.
 */
void addExtendedReplyHelp(client *c, const char **help, const char **extended_help) {
    sds cmd = sdsnew((char *)c->argv[0]->ptr);
    void *blenp = addReplyDeferredLen(c);
    int blen = 0;
    int idx = 0;

    sdstoupper(cmd);
    addReplyStatusFormat(c, "%s <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", cmd);
    sdsfree(cmd);

    while (help[blen]) addReplyStatus(c, help[blen++]);
    if (extended_help) {
        while (extended_help[idx]) addReplyStatus(c, extended_help[idx++]);
    }
    blen += idx;

    addReplyStatus(c, "HELP");
    addReplyStatus(c, "    Print this help.");

    blen += 1; /* Account for the header. */
    blen += 2; /* Account for the footer. */
    setDeferredArrayLen(c, blenp, blen);
}

/* Add an array of C strings as status replies with a heading.
 * This function is typically invoked by commands that support
 * subcommands in response to the 'help' subcommand. The help array
 * is terminated by NULL sentinel. */
void addReplyHelp(client *c, const char **help) {
    addExtendedReplyHelp(c, help, NULL);
}

/* Add a suggestive error reply.
 * This function is typically invoked by from commands that support
 * subcommands in response to an unknown subcommand or argument error. */
void addReplySubcommandSyntaxError(client *c) {
    sds cmd = sdsnew((char *)c->argv[0]->ptr);
    sdstoupper(cmd);
    addReplyErrorFormat(c, "unknown subcommand or wrong number of arguments for '%.128s'. Try %s HELP.",
                        (char *)c->argv[1]->ptr, cmd);
    sdsfree(cmd);
}

/* Append 'src' client output buffers into 'dst' client output buffers.
 * This function clears the output buffers of 'src' */
void AddReplyFromClient(client *dst, client *src) {
    /* If the source client contains a partial response due to client output
     * buffer limits, propagate that to the dest rather than copy a partial
     * reply. We don't wanna run the risk of copying partial response in case
     * for some reason the output limits don't reach the same decision (maybe
     * they changed) */
    if (src->flag.close_asap) {
        sds client = catClientInfoString(sdsempty(), dst, server.hide_user_data_from_log);
        freeClientAsync(dst);
        serverLog(LL_WARNING, "Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
        return;
    }

    /* First add the static buffer (either into the static buffer or reply list) */
    addReplyProto(dst, src->buf, src->bufpos);

    /* We need to check with prepareClientToWrite again (after addReplyProto)
     * since addReplyProto may have changed something (like CLIENT_CLOSE_ASAP) */
    if (prepareClientToWrite(dst) != C_OK) return;

    /* We're bypassing _addReplyProtoToList, so we need to add the pre/post
     * checks in it. */
    if (dst->flag.close_after_reply) return;

    /* Concatenate the reply list into the dest */
    if (listLength(src->reply)) listJoin(dst->reply, src->reply);
    dst->reply_bytes += src->reply_bytes;
    src->reply_bytes = 0;
    src->bufpos = 0;

    if (src->deferred_reply_errors) {
        deferredAfterErrorReply(dst, src->deferred_reply_errors);
        listRelease(src->deferred_reply_errors);
        src->deferred_reply_errors = NULL;
    }

    /* Check output buffer limits */
    closeClientOnOutputBufferLimitReached(dst, 1);
}

/* Append the listed errors to the server error statistics. the input
 * list is not modified and remains the responsibility of the caller. */
void deferredAfterErrorReply(client *c, list *errors) {
    listIter li;
    listNode *ln;
    listRewind(errors, &li);
    while ((ln = listNext(&li))) {
        sds err = ln->value;
        afterErrorReply(c, err, sdslen(err), 0);
    }
}

/* Logically copy 'src' replica client buffers info to 'dst' replica.
 * Basically increase referenced buffer block node reference count. */
void copyReplicaOutputBuffer(client *dst, client *src) {
    serverAssert(src->bufpos == 0 && listLength(src->reply) == 0);

    if (src->repl_data->ref_repl_buf_node == NULL) return;
    dst->repl_data->ref_repl_buf_node = src->repl_data->ref_repl_buf_node;
    dst->repl_data->ref_block_pos = src->repl_data->ref_block_pos;
    ((replBufBlock *)listNodeValue(dst->repl_data->ref_repl_buf_node))->refcount++;
}

/* Return true if the specified client has pending reply buffers to write to
 * the socket. */
int clientHasPendingReplies(client *c) {
    if (getClientType(c) == CLIENT_TYPE_REPLICA) {
        /* Replicas use global shared replication buffer instead of
         * private output buffer. */
        serverAssert(c->bufpos == 0 && listLength(c->reply) == 0);
        if (c->repl_data->ref_repl_buf_node == NULL) return 0;

        /* If the last replication buffer block content is totally sent,
         * we have nothing to send. */
        listNode *ln = listLast(server.repl_buffer_blocks);
        replBufBlock *tail = listNodeValue(ln);
        if (ln == c->repl_data->ref_repl_buf_node && c->repl_data->ref_block_pos == tail->used) return 0;

        return 1;
    } else {
        return c->bufpos || listLength(c->reply);
    }
}

void clientAcceptHandler(connection *conn) {
    client *c = connGetPrivateData(conn);

    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING, "Error accepting a client connection: %s (addr=%s laddr=%s)", connGetLastError(conn),
                  getClientPeerId(c), getClientSockname(c));
        freeClientAsync(c);
        return;
    }

    /* If the server is running in protected mode (the default) and there
     * is no password set, nor a specific interface is bound, we don't accept
     * requests from non loopback interfaces. Instead we try to explain the
     * user what to do to fix it if needed. */
    if (server.protected_mode && DefaultUser->flags & USER_FLAG_NOPASS) {
        if (connIsLocal(conn) != 1) {
            char *err = "-DENIED Running in protected mode because protected "
                        "mode is enabled and no password is set for the default user. "
                        "In this mode connections are only accepted from the loopback interface. "
                        "If you want to connect from external computers, you "
                        "may adopt one of the following solutions: "
                        "1) Just disable protected mode sending the command "
                        "'CONFIG SET protected-mode no' from the loopback interface "
                        "by connecting from the same host the server is "
                        "running, however MAKE SURE it's not publicly accessible "
                        "from internet if you do so. Use CONFIG REWRITE to make this "
                        "change permanent. "
                        "2) Alternatively you can just disable the protected mode by "
                        "editing the configuration file, and setting the protected "
                        "mode option to 'no', and then restarting the server. "
                        "3) If you started the server manually just for testing, restart "
                        "it with the '--protected-mode no' option. "
                        "4) Set up an authentication password for the default user. "
                        "NOTE: You only need to do one of the above things in order for "
                        "the server to start accepting connections from the outside.\r\n";
            if (connWrite(c->conn, err, strlen(err)) == -1) {
                /* Nothing to do, Just to avoid the warning... */
            }
            server.stat_rejected_conn++;
            freeClientAsync(c);
            return;
        }
    }

    server.stat_numconnections++;
    moduleFireServerEvent(VALKEYMODULE_EVENT_CLIENT_CHANGE, VALKEYMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED, c);
}

void acceptCommonHandler(connection *conn, struct ClientFlags flags, char *ip) {
    client *c;
    UNUSED(ip);

    if (connGetState(conn) != CONN_STATE_ACCEPTING) {
        char addr[NET_ADDR_STR_LEN] = {0};
        char laddr[NET_ADDR_STR_LEN] = {0};
        connFormatAddr(conn, addr, sizeof(addr), 1);
        connFormatAddr(conn, laddr, sizeof(addr), 0);
        serverLog(LL_VERBOSE, "Accepted client connection in error state: %s (addr=%s laddr=%s)",
                  connGetLastError(conn), addr, laddr);
        connClose(conn);
        return;
    }

    /* Limit the number of connections we take at the same time.
     *
     * Admission control will happen before a client is created and connAccept()
     * called, because we don't want to even start transport-level negotiation
     * if rejected. */
    if (listLength(server.clients) + getClusterConnectionsCount() >= server.maxclients) {
        char *err;
        if (server.cluster_enabled)
            err = "-ERR max number of clients + cluster "
                  "connections reached\r\n";
        else
            err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors.
         * Note that for TLS connections, no handshake was done yet so nothing
         * is written and the connection will just drop. */
        if (connWrite(conn, err, strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        server.stat_rejected_conn++;
        connClose(conn);
        return;
    }

    /* Create connection and client */
    if ((c = createClient(conn)) == NULL) {
        char addr[NET_ADDR_STR_LEN] = {0};
        char laddr[NET_ADDR_STR_LEN] = {0};
        connFormatAddr(conn, addr, sizeof(addr), 1);
        connFormatAddr(conn, laddr, sizeof(addr), 0);
        serverLog(LL_WARNING, "Error registering fd event for the new client connection: %s (addr=%s laddr=%s)",
                  connGetLastError(conn), addr, laddr);
        connClose(conn); /* May be already closed, just ignore errors */
        return;
    }

    /* Last chance to keep flags */
    if (flags.unix_socket) c->flag.unix_socket = 1;

    /* Initiate accept.
     *
     * Note that connAccept() is free to do two things here:
     * 1. Call clientAcceptHandler() immediately;
     * 2. Schedule a future call to clientAcceptHandler().
     *
     * Because of that, we must do nothing else afterwards.
     */
    if (connAccept(conn, clientAcceptHandler) == C_ERR) {
        if (connGetState(conn) == CONN_STATE_ERROR)
            serverLog(LL_WARNING, "Error accepting a client connection: %s (addr=%s laddr=%s)", connGetLastError(conn),
                      getClientPeerId(c), getClientSockname(c));
        freeClient(connGetPrivateData(conn));
        return;
    }
}

void freeClientOriginalArgv(client *c) {
    /* We didn't rewrite this client */
    if (!c->original_argv) return;

    if (tryOffloadFreeArgvToIOThreads(c, c->original_argc, c->original_argv) == C_ERR) {
        for (int j = 0; j < c->original_argc; j++) decrRefCount(c->original_argv[j]);
        zfree(c->original_argv);
    }

    c->original_argv = NULL;
    c->original_argc = 0;
}

void freeClientArgv(client *c) {
    /* If original_argv exists, 'c->argv' was allocated by the main thread,
     * so it's more efficient to free it directly here rather than offloading to IO threads */
    if (c->original_argv || tryOffloadFreeArgvToIOThreads(c, c->argc, c->argv) == C_ERR) {
        for (int j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
        zfree(c->argv);
    }
    c->argc = 0;
    c->cmd = NULL;
    c->io_parsed_cmd = NULL;
    c->argv_len_sum = 0;
    c->argv_len = 0;
    c->argv = NULL;
}

/* Close all the replicas connections. This is useful in chained replication
 * when we resync with our own primary and want to force all our replicas to
 * resync with us as well. */
void disconnectReplicas(void) {
    listIter li;
    listNode *ln;
    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        freeClient((client *)ln->value);
    }
}

/* Remove the specified client from global lists where the client could
 * be referenced, not including the Pub/Sub channels.
 * This is used by freeClient() and replicationCachePrimary(). */
void unlinkClient(client *c) {
    listNode *ln;

    /* If this is marked as current client unset it. */
    if (c->conn && server.current_client == c) server.current_client = NULL;

    /* Certain operations must be done only if the client has an active connection.
     * If the client was already unlinked or if it's a "fake client" the
     * conn is already set to NULL. */
    if (c->conn) {
        /* Remove from the list of active clients. */
        if (c->client_list_node) {
            uint64_t id = htonu64(c->id);
            raxRemove(server.clients_index, (unsigned char *)&id, sizeof(id), NULL);
            listDelNode(server.clients, c->client_list_node);
            c->client_list_node = NULL;
        }
        removeClientFromPendingCommandsBatch(c);

        /* Check if this is a replica waiting for diskless replication (rdb pipe),
         * in which case it needs to be cleaned from that list */
        if (c->repl_data && c->flag.replica && c->repl_data->repl_state == REPLICA_STATE_WAIT_BGSAVE_END && server.rdb_pipe_conns) {
            int i;
            int still_alive = 0;
            for (i = 0; i < server.rdb_pipe_numconns; i++) {
                if (server.rdb_pipe_conns[i] == c->conn) {
                    rdbPipeWriteHandlerConnRemoved(c->conn);
                    server.rdb_pipe_conns[i] = NULL;
                }
                if (server.rdb_pipe_conns[i]) still_alive++;
            }
            if (still_alive == 0) {
                serverLog(LL_NOTICE, "Diskless rdb transfer, last replica dropped, killing fork child.");
                killRDBChild();
            }
        }
        /* Only use shutdown when the fork is active and we are the parent. */
        if (server.child_type && !c->flag.repl_rdb_channel) {
            connShutdown(c->conn);
        } else if (c->flag.repl_rdb_channel) {
            shutdown(c->conn->fd, SHUT_RDWR);
        }
        connClose(c->conn);
        c->conn = NULL;
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flag.pending_write) {
        serverAssert(&c->clients_pending_write_node.next != NULL || &c->clients_pending_write_node.prev != NULL);
        if (c->io_write_state == CLIENT_IDLE) {
            listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        } else {
            listUnlinkNode(server.clients_pending_io_write, &c->clients_pending_write_node);
        }
        c->flag.pending_write = 0;
    }

    /* Remove from the list of pending reads if needed. */
    serverAssert(c->io_read_state != CLIENT_PENDING_IO && c->io_write_state != CLIENT_PENDING_IO);
    if (c->flag.pending_read) {
        listUnlinkNode(server.clients_pending_io_read, &c->pending_read_list_node);
        c->flag.pending_read = 0;
    }


    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    if (c->flag.unblocked) {
        ln = listSearchKey(server.unblocked_clients, c);
        serverAssert(ln != NULL);
        listDelNode(server.unblocked_clients, ln);
        c->flag.unblocked = 0;
    }

    /* Clear the tracking status. */
    if (c->flag.tracking) disableTracking(c);
}

/* Clear the client state to resemble a newly connected client. */
void clearClientConnectionState(client *c) {
    listNode *ln;

    /* MONITOR clients are also marked with CLIENT_REPLICA, we need to
     * distinguish between the two.
     */
    if (c->flag.monitor) {
        ln = listSearchKey(server.monitors, c);
        serverAssert(ln != NULL);
        listDelNode(server.monitors, ln);

        c->flag.monitor = 0;
        c->flag.replica = 0;
    }

    serverAssert(!(c->flag.replica || c->flag.primary));

    if (c->flag.tracking) disableTracking(c);
    selectDb(c, 0);
#ifdef LOG_REQ_RES
    c->resp = server.client_default_resp;
#else
    c->resp = 2;
#endif

    clientSetDefaultAuth(c);
    moduleNotifyUserChanged(c);
    discardTransaction(c);
    freeClientPubSubData(c);

    if (c->name) {
        decrRefCount(c->name);
        c->name = NULL;
    }

    /* Note: lib_name and lib_ver are not reset since they still
     * represent the client library behind the connection. */

    /* Selectively clear state flags not covered above */
    c->flag.asking = 0;
    c->flag.readonly = 0;
    c->flag.reply_off = 0;
    c->flag.reply_skip_next = 0;
    c->flag.no_touch = 0;
    c->flag.no_evict = 0;
}

void freeClient(client *c) {
    listNode *ln;

    /* If a client is protected, yet we need to free it right now, make sure
     * to at least use asynchronous freeing. */
    if (c->flag.protected || c->flag.protected_rdb_channel) {
        freeClientAsync(c);
        return;
    }

    /* Wait for IO operations to be done before proceeding */
    waitForClientIO(c);

    /* For connected clients, call the disconnection event of modules hooks. */
    if (c->conn) {
        moduleFireServerEvent(VALKEYMODULE_EVENT_CLIENT_CHANGE, VALKEYMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED, c);
    }

    /* Notify module system that this client auth status changed. */
    moduleNotifyUserChanged(c);
    freeClientModuleData(c);

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. Note that we need to do this here, because later
     * we may call replicationCachePrimary() and the client should already
     * be removed from the list of clients to free. */
    if (c->flag.close_asap) {
        ln = listSearchKey(server.clients_to_close, c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_to_close, ln);
    }

    /* If it is our primary that's being disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    if (server.primary && c->flag.primary) {
        serverLog(LL_NOTICE, "Connection with primary lost.");
        if (!c->flag.dont_cache_primary && !(c->flag.protocol_error || c->flag.blocked)) {
            c->flag.close_asap = 0;
            c->flag.close_after_reply = 0;
            replicationCachePrimary(c);
            return;
        }
    }

    /* Log link disconnection with replica */
    if (getClientType(c) == CLIENT_TYPE_REPLICA) {
        if (c->flag.repl_rdb_channel)
            dualChannelServerLog(LL_NOTICE, "Replica %s rdb channel disconnected.", replicationGetReplicaName(c));
        else
            serverLog(LL_NOTICE, "Connection with replica %s lost.", replicationGetReplicaName(c));
    }

    /* Free the query buffer */
    if (c->querybuf && c->querybuf == thread_shared_qb) {
        sdsclear(c->querybuf);
    } else {
        sdsfree(c->querybuf);
    }
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    /* If there is any in-flight command, we don't record their duration. */
    c->duration = 0;
    if (c->flag.blocked) unblockClient(c, 1);

    freeClientBlockingState(c);
    freeClientPubSubData(c);

    /* Free data structures. */
    listRelease(c->reply);
    c->reply = NULL;
    zfree_with_size(c->buf, c->buf_usable_size);
    c->buf = NULL;

    freeClientArgv(c);
    freeClientOriginalArgv(c);
    if (c->deferred_reply_errors) listRelease(c->deferred_reply_errors);
    c->deferred_reply_errors = NULL;
#ifdef LOG_REQ_RES
    reqresReset(c, 1);
#endif

    /* Remove the contribution that this client gave to our
     * incrementally computed memory usage. */
    if (c->conn) server.stat_clients_type_memory[c->last_memory_type] -= c->last_memory_usage;

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    unlinkClient(c);

    freeClientReplicationData(c);

    /* Remove client from memory usage buckets */
    if (c->mem_usage_bucket) {
        c->mem_usage_bucket->mem_usage_sum -= c->last_memory_usage;
        listDelNode(c->mem_usage_bucket->clients, c->mem_usage_bucket_node);
    }

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    if (c->name) decrRefCount(c->name);
    if (c->lib_name) decrRefCount(c->lib_name);
    if (c->lib_ver) decrRefCount(c->lib_ver);
    freeClientMultiState(c);
    sdsfree(c->peerid);
    sdsfree(c->sockname);
    zfree(c);
}

/* Schedule a client to free it at a safe time in the beforeSleep() function.
 * This function is useful when we need to terminate a client but we are in
 * a context where calling freeClient() is not possible, because the client
 * should be valid for the continuation of the flow of the program. */
void freeClientAsync(client *c) {
    if (c->flag.close_asap || c->flag.script) return;
    c->flag.close_asap = 1;
    debugServerAssertWithInfo(c, NULL, listSearchKey(server.clients_to_close, c) == NULL);
    listAddNodeTail(server.clients_to_close, c);
}

/* Log errors for invalid use and free the client in async way.
 * We will add additional information about the client to the message. */
void logInvalidUseAndFreeClientAsync(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    sds info = sdscatvprintf(sdsempty(), fmt, ap);
    va_end(ap);

    sds client = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);
    serverLog(LL_WARNING, "%s, disconnecting it: %s", info, client);

    sdsfree(info);
    sdsfree(client);
    freeClientAsync(c);
}

/* Resets the shared query buffer used by the given client.
 * If any data remained in the buffer, the client will take ownership of the buffer
 * and a new empty buffer will be allocated for the shared buffer. */
void resetSharedQueryBuf(client *c) {
    serverAssert(c->querybuf == thread_shared_qb);
    size_t remaining = sdslen(c->querybuf) - c->qb_pos;

    if (remaining > 0) {
        /* Let the client take ownership of the shared buffer. */
        initSharedQueryBuf();
        return;
    }

    c->querybuf = NULL;
    sdsclear(thread_shared_qb);
    c->qb_pos = 0;
}

/* Trims the client query buffer to the current position. */
void trimClientQueryBuffer(client *c) {
    if (c->querybuf == thread_shared_qb) {
        resetSharedQueryBuf(c);
    }

    if (c->querybuf == NULL) {
        return;
    }

    serverAssert(c->qb_pos <= sdslen(c->querybuf));

    if (c->qb_pos > 0) {
        sdsrange(c->querybuf, c->qb_pos, -1);
        c->qb_pos = 0;
    }
}

/* Perform processing of the client before moving on to processing the next client.
 * This is useful for performing operations that affect the global state but can't
 * wait until we're done with all clients. In other words, it can't wait until beforeSleep().
 * With IO threads enabled, this function offloads the write to the IO threads if possible. */
void beforeNextClient(client *c) {
    /* Notice, this code is also called from 'processUnblockedClients'.
     * But in case of a module blocked client (see RM_Call 'K' flag) we do not reach this code path.
     * So whenever we change the code here we need to consider if we need this change on module
     * blocked client as well */

    /* Trim the query buffer to the current position. */
    if (c->flag.primary) {
        /* If the client is a primary, trim the querybuf to repl_applied,
         * since primary client is very special, its querybuf not only
         * used to parse command, but also proxy to sub-replicas.
         *
         * Here are some scenarios we cannot trim to qb_pos:
         * 1. we don't receive complete command from primary
         * 2. primary client blocked cause of client pause
         * 3. io threads operate read, primary client flagged with CLIENT_PENDING_COMMAND
         *
         * In these scenarios, qb_pos points to the part of the current command
         * or the beginning of next command, and the current command is not applied yet,
         * so the repl_applied is not equal to qb_pos. */
        if (c->repl_data->repl_applied) {
            sdsrange(c->querybuf, c->repl_data->repl_applied, -1);
            c->qb_pos -= c->repl_data->repl_applied;
            c->repl_data->repl_applied = 0;
        }
    } else {
        trimClientQueryBuffer(c);
    }
    /* Handle async frees */
    /* Note: this doesn't make the server.clients_to_close list redundant because of
     * cases where we want an async free of a client other than myself. For example
     * in ACL modifications we disconnect clients authenticated to non-existent
     * users (see ACL LOAD). */
    if (c->flag.close_asap) {
        freeClient(c);
        return;
    }

    updateClientMemUsageAndBucket(c);
    /* If IO threads are enabled try to write immediately the reply instead of waiting to beforeSleep,
     * unless aof_fsync is set to always in which case we need to wait for beforeSleep after writing the aof buffer. */
    if (server.aof_fsync != AOF_FSYNC_ALWAYS) {
        trySendWriteToIOThreads(c);
    }
}

/* Free the clients marked as CLOSE_ASAP, return the number of clients
 * freed. */
int freeClientsInAsyncFreeQueue(void) {
    int freed = 0;
    listIter li;
    listNode *ln;

    listRewind(server.clients_to_close, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);

        if (c->flag.protected_rdb_channel) {
            /* Check if it's safe to remove RDB connection protection during synchronization
             * The primary gives a grace period before freeing this client because
             * it serves as a reference to the first required replication data block for
             * this replica */
            if (!c->repl_data->rdb_client_disconnect_time) {
                if (c->conn) connSetReadHandler(c->conn, NULL);
                c->repl_data->rdb_client_disconnect_time = server.unixtime;
                dualChannelServerLog(LL_VERBOSE, "Postpone RDB client id=%llu (%s) free for %d seconds",
                                     (unsigned long long)c->id, replicationGetReplicaName(c), server.wait_before_rdb_client_free);
            }
            if (server.unixtime - c->repl_data->rdb_client_disconnect_time <= server.wait_before_rdb_client_free) continue;
            dualChannelServerLog(
                LL_NOTICE,
                "Replica main channel failed to establish PSYNC within the grace period (%ld seconds). "
                "Freeing RDB client %llu.",
                (long int)(server.unixtime - c->repl_data->rdb_client_disconnect_time), (unsigned long long)c->id);
            c->flag.protected_rdb_channel = 0;
        }

        if (c->flag.protected) continue;

        c->flag.close_asap = 0;
        freeClient(c);
        listDelNode(server.clients_to_close, ln);
        freed++;
    }
    return freed;
}

/* Return a client by ID, or NULL if the client ID is not in the set
 * of registered clients. Note that "fake clients", created with -1 as FD,
 * are not registered clients. */
client *lookupClientByID(uint64_t id) {
    id = htonu64(id);
    void *c = NULL;
    raxFind(server.clients_index, (unsigned char *)&id, sizeof(id), &c);
    return c;
}

void writeToReplica(client *c) {
    /* Can be called from main-thread only as replica write offload is not supported yet */
    serverAssert(inMainThread());
    int nwritten = 0;
    serverAssert(c->bufpos == 0 && listLength(c->reply) == 0);
    while (clientHasPendingReplies(c)) {
        replBufBlock *o = listNodeValue(c->repl_data->ref_repl_buf_node);
        serverAssert(o->used >= c->repl_data->ref_block_pos);

        /* Send current block if it is not fully sent. */
        if (o->used > c->repl_data->ref_block_pos) {
            nwritten = connWrite(c->conn, o->buf + c->repl_data->ref_block_pos, o->used - c->repl_data->ref_block_pos);
            if (nwritten <= 0) {
                c->write_flags |= WRITE_FLAGS_WRITE_ERROR;
                return;
            }
            c->nwritten += nwritten;
            c->repl_data->ref_block_pos += nwritten;
        }

        /* If we fully sent the object on head, go to the next one. */
        listNode *next = listNextNode(c->repl_data->ref_repl_buf_node);
        if (next && c->repl_data->ref_block_pos == o->used) {
            o->refcount--;
            ((replBufBlock *)(listNodeValue(next)))->refcount++;
            c->repl_data->ref_repl_buf_node = next;
            c->repl_data->ref_block_pos = 0;
            incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
        }
    }
}

/* This function should be called from _writeToClient when the reply list is not empty,
 * it gathers the scattered buffers from reply list and sends them away with connWritev.
 * If we write successfully, it returns C_OK, otherwise, C_ERR is returned.
 * Sets the c->nwritten to the number of bytes the server wrote to the client.
 * Can be called from the main thread or an I/O thread */
static int writevToClient(client *c) {
    int iovcnt = 0;
    int iovmax = min(IOV_MAX, c->conn->iovcnt);
    struct iovec iov_arr[iovmax];
    struct iovec *iov = iov_arr;
    ssize_t bufpos, iov_bytes_len = 0;
    listNode *lastblock;

    if (inMainThread()) {
        lastblock = listLast(c->reply);
        bufpos = c->bufpos;
    } else {
        lastblock = c->io_last_reply_block;
        bufpos = lastblock ? (size_t)c->bufpos : c->io_last_bufpos;
    }

    /* If the static reply buffer is not empty,
     * add it to the iov array for writev() as well. */
    if (bufpos > 0) {
        iov[iovcnt].iov_base = c->buf + c->sentlen;
        iov[iovcnt].iov_len = bufpos - c->sentlen;
        iov_bytes_len += iov[iovcnt++].iov_len;
    }
    /* The first node of reply list might be incomplete from the last call,
     * thus it needs to be calibrated to get the actual data address and length. */
    size_t sentlen = bufpos > 0 ? 0 : c->sentlen;
    listIter iter;
    listNode *next;
    clientReplyBlock *o;
    size_t used;
    listRewind(c->reply, &iter);
    while ((next = listNext(&iter)) && iovcnt < iovmax && iov_bytes_len < NET_MAX_WRITES_PER_EVENT) {
        o = listNodeValue(next);

        used = o->used;
        /* Use c->io_last_bufpos as the currently used portion of the block.
         *  We use io_last_bufpos instead of o->used to ensure that we only access data guaranteed to be visible to the
         * current thread. Using o->used, which may have been updated by the main thread, could lead to accessing data
         * that may not yet be visible to the current thread*/
        if (!inMainThread() && next == lastblock) used = c->io_last_bufpos;

        if (used == 0) { /* empty node, skip over it. */
            if (next == lastblock) break;
            sentlen = 0;
            continue;
        }

        iov[iovcnt].iov_base = o->buf + sentlen;
        iov[iovcnt].iov_len = used - sentlen;
        iov_bytes_len += iov[iovcnt++].iov_len;

        sentlen = 0;
        if (next == lastblock) break;
    }

    serverAssert(iovcnt != 0);

    ssize_t totwritten = 0;
    while (1) {
        int nwritten = connWritev(c->conn, iov, iovcnt);
        if (nwritten <= 0) {
            c->write_flags |= WRITE_FLAGS_WRITE_ERROR;
            totwritten = totwritten > 0 ? totwritten : nwritten;
            break;
        }
        totwritten += nwritten;

        if (totwritten == iov_bytes_len) break;

        if (totwritten > NET_MAX_WRITES_PER_EVENT) {
            /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
             * bytes, Since it's a good idea to serve
             * other clients as well, even if a very large request comes from
             * super fast link that is always able to accept data (in real world
             * scenario think about 'KEYS *' against the loopback interface).
             *
             * However if we are over the maxmemory limit we ignore that and
             * just deliver as much data as it is possible to deliver. */
            int ignore_max_write_limit = server.maxmemory > 0 && zmalloc_used_memory() > server.maxmemory;
            if (!ignore_max_write_limit) {
                break;
            }
        }

        /* proceed to the unwritten blocks */
        while (nwritten > 0) {
            if ((size_t)nwritten < iov[0].iov_len) {
                iov[0].iov_base = (char *)iov[0].iov_base + nwritten;
                iov[0].iov_len -= nwritten;
                break;
            }
            nwritten -= iov[0].iov_len;
            iov++;
            iovcnt--;
        }
    }

    c->nwritten = totwritten;
    return totwritten > 0 ? C_OK : C_ERR;
}

/* This function does actual writing output buffers to non-replica client, it is called by writeToClient.
 * If we write successfully, it returns C_OK, otherwise, C_ERR is returned,
 * and 'c->nwritten' is set to the number of bytes the server wrote to the client. */
int _writeToClient(client *c) {
    listNode *lastblock;
    size_t bufpos;

    if (inMainThread()) {
        /* In the main thread, access bufpos and lastblock directly */
        lastblock = listLast(c->reply);
        bufpos = (size_t)c->bufpos;
    } else {
        /* If there is a last block, use bufpos directly; otherwise, use io_last_bufpos */
        bufpos = c->io_last_reply_block ? (size_t)c->bufpos : c->io_last_bufpos;
        lastblock = c->io_last_reply_block;
    }

    /* If the reply list is not empty, use writev to save system calls and TCP packets */
    if (lastblock) return writevToClient(c);

    ssize_t bytes_to_write = bufpos - c->sentlen;
    ssize_t tot_written = 0;

    while (tot_written < bytes_to_write) {
        int nwritten = connWrite(c->conn, c->buf + c->sentlen + tot_written, bytes_to_write - tot_written);
        if (nwritten <= 0) {
            c->write_flags |= WRITE_FLAGS_WRITE_ERROR;
            tot_written = tot_written > 0 ? tot_written : nwritten;
            break;
        }
        tot_written += nwritten;
    }

    c->nwritten = tot_written;
    return tot_written > 0 ? C_OK : C_ERR;
}

static void _postWriteToClient(client *c) {
    if (c->nwritten <= 0) return;

    listIter iter;
    listNode *next;
    clientReplyBlock *o;

    server.stat_net_output_bytes += c->nwritten;

    /* Locate the new node which has leftover data and
     * release all nodes in front of it. */
    ssize_t remaining = c->nwritten;
    if (c->bufpos > 0) { /* Deal with static reply buffer first. */
        int buf_len = c->bufpos - c->sentlen;
        c->sentlen += c->nwritten;
        /* If the buffer was sent, set bufpos to zero to continue with
         * the remainder of the reply. */
        if (c->nwritten >= buf_len) {
            c->bufpos = 0;
            c->sentlen = 0;
        }
        remaining -= buf_len;
    }
    listRewind(c->reply, &iter);
    while (remaining > 0) {
        next = listNext(&iter);
        o = listNodeValue(next);
        if (remaining < (ssize_t)(o->used - c->sentlen)) {
            c->sentlen += remaining;
            break;
        }
        remaining -= (ssize_t)(o->used - c->sentlen);
        c->reply_bytes -= o->size;
        listDelNode(c->reply, next);
        c->sentlen = 0;
    }
}

/* Updates the client's memory usage and bucket and server stats after writing.
 * If a write handler is installed , it will attempt to clear the write event.
 * If the client is no longer valid, it will return C_ERR, otherwise C_OK. */
int postWriteToClient(client *c) {
    c->io_last_reply_block = NULL;
    c->io_last_bufpos = 0;
    /* Update total number of writes on server */
    server.stat_total_writes_processed++;
    if (getClientType(c) != CLIENT_TYPE_REPLICA) {
        _postWriteToClient(c);
    } else {
        server.stat_net_repl_output_bytes += c->nwritten > 0 ? c->nwritten : 0;
    }

    if (c->write_flags & WRITE_FLAGS_WRITE_ERROR) {
        if (connGetState(c->conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_VERBOSE, "Error writing to client: %s", connGetLastError(c->conn));
            freeClientAsync(c);
            return C_ERR;
        }
    }
    if (c->nwritten > 0) {
        c->net_output_bytes += c->nwritten;
        /* For clients representing primaries we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        if (!c->flag.primary) c->last_interaction = server.unixtime;
    }
    if (!clientHasPendingReplies(c)) {
        c->sentlen = 0;
        if (connHasWriteHandler(c->conn)) {
            connSetWriteHandler(c->conn, NULL);
        }

        /* Close connection after entire reply has been sent. */
        if (c->flag.close_after_reply) {
            freeClientAsync(c);
            return C_ERR;
        }
    }
    /* Update client's memory usage after writing.*/
    updateClientMemUsageAndBucket(c);
    return C_OK;
}

/* Write data in output buffers to client. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed because of some
 * error.
 *
 * This function is called by main-thread only */
int writeToClient(client *c) {
    if (c->io_write_state != CLIENT_IDLE || c->io_read_state != CLIENT_IDLE) return C_OK;

    c->nwritten = 0;
    c->write_flags = 0;

    if (getClientType(c) == CLIENT_TYPE_REPLICA) {
        writeToReplica(c);
    } else {
        _writeToClient(c);
    }

    return postWriteToClient(c);
}

/* Write event handler. Just send data to the client. */
void sendReplyToClient(connection *conn) {
    client *c = connGetPrivateData(conn);
    if (trySendWriteToIOThreads(c) == C_OK) return;
    writeToClient(c);
}

void handleQbLimitReached(client *c) {
    sds ci = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log), bytes = sdsempty();
    bytes = sdscatrepr(bytes, c->querybuf, 64);
    serverLog(LL_WARNING, "Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci,
              bytes);
    sdsfree(ci);
    sdsfree(bytes);
    freeClientAsync(c);
    server.stat_client_qbuf_limit_disconnections++;
}

/* Handle read errors and update statistics.
 *
 * Called only from the main thread.
 * If the read was done in an I/O thread, this function is invoked after the
 * read job has completed, in the main thread context.
 *
 * Returns:
 *   - C_OK if the querybuf can be further processed.
 *   - C_ERR if not. */
int handleReadResult(client *c) {
    serverAssert(inMainThread());
    server.stat_total_reads_processed++;
    if (c->nread <= 0) {
        if (c->nread == -1) {
            if (connGetState(c->conn) != CONN_STATE_CONNECTED) {
                serverLog(LL_VERBOSE, "Reading from client: %s", connGetLastError(c->conn));
                freeClientAsync(c);
            }
        } else if (c->nread == 0) {
            if (server.verbosity <= LL_VERBOSE) {
                sds info = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);
                serverLog(LL_VERBOSE, "Client closed connection %s", info);
                sdsfree(info);
            }
            freeClientAsync(c);
        }
        return C_ERR;
    }

    c->last_interaction = server.unixtime;
    c->net_input_bytes += c->nread;
    if (c->flag.primary) {
        c->repl_data->read_reploff += c->nread;
        server.stat_net_repl_input_bytes += c->nread;
    } else {
        server.stat_net_input_bytes += c->nread;
    }

    /* Handle QB limit */
    if (c->read_flags & READ_FLAGS_QB_LIMIT_REACHED) {
        handleQbLimitReached(c);
        return C_ERR;
    }
    return C_OK;
}


void handleParseError(client *c) {
    int flags = c->read_flags;
    if (flags & READ_FLAGS_ERROR_BIG_INLINE_REQUEST) {
        addReplyError(c, "Protocol error: too big inline request");
        setProtocolError("too big inline request", c);
    } else if (flags & READ_FLAGS_ERROR_BIG_MULTIBULK) {
        addReplyError(c, "Protocol error: too big mbulk count string");
        setProtocolError("too big mbulk count string", c);
    } else if (flags & READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN) {
        addReplyError(c, "Protocol error: invalid multibulk length");
        setProtocolError("invalid mbulk count", c);
    } else if (flags & READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN) {
        addReplyError(c, "Protocol error: unauthenticated multibulk length");
        setProtocolError("unauth mbulk count", c);
    } else if (flags & READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN) {
        addReplyError(c, "Protocol error: unauthenticated bulk length");
        setProtocolError("unauth bulk length", c);
    } else if (flags & READ_FLAGS_ERROR_BIG_BULK_COUNT) {
        addReplyError(c, "Protocol error: too big bulk count string");
        setProtocolError("too big bulk count string", c);
    } else if (flags & READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER) {
        addReplyErrorFormat(c, "Protocol error: expected '$', got '%c'", c->querybuf[c->qb_pos]);
        setProtocolError("expected $ but got something else", c);
    } else if (flags & READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN) {
        addReplyError(c, "Protocol error: invalid bulk length");
        setProtocolError("invalid bulk length", c);
    } else if (flags & READ_FLAGS_ERROR_UNBALANCED_QUOTES) {
        addReplyError(c, "Protocol error: unbalanced quotes in request");
        setProtocolError("unbalanced quotes in inline request", c);
    } else if (flags & READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_PRIMARY) {
        serverLog(LL_WARNING, "WARNING: Receiving inline protocol from primary, primary stream corruption? Closing the "
                              "primary connection and discarding the cached primary.");
        setProtocolError("Master using the inline protocol. Desync?", c);
    } else {
        serverAssertWithInfo(c, NULL, "Unknown parsing error");
    }
}

int isParsingError(client *c) {
    return c->read_flags & (READ_FLAGS_ERROR_BIG_INLINE_REQUEST | READ_FLAGS_ERROR_BIG_MULTIBULK |
                            READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN | READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN |
                            READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN | READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN |
                            READ_FLAGS_ERROR_BIG_BULK_COUNT | READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER |
                            READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_PRIMARY | READ_FLAGS_ERROR_UNBALANCED_QUOTES);
}

/* This function is called after the query-buffer was parsed.
 * It is used to handle parsing errors and to update the client state.
 * The function returns C_OK if a command can be executed, otherwise C_ERR. */
parseResult handleParseResults(client *c) {
    if (isParsingError(c)) {
        handleParseError(c);
        return PARSE_ERR;
    }

    if (c->read_flags & READ_FLAGS_INLINE_ZERO_QUERY_LEN && getClientType(c) == CLIENT_TYPE_REPLICA) {
        c->repl_data->repl_ack_time = server.unixtime;
    }

    if (c->read_flags & READ_FLAGS_INLINE_ZERO_QUERY_LEN) {
        /* in case the client's query was an empty line we will ignore it and proceed to process the rest of the buffer
         * if any */
        resetClient(c);
        return PARSE_OK;
    }

    if (c->read_flags & READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN) {
        /* Multibulk processing could see a <= 0 length. */
        resetClient(c);
        return PARSE_OK;
    }

    if (c->read_flags & READ_FLAGS_PARSING_COMPLETED) {
        return PARSE_OK;
    } else {
        return PARSE_NEEDMORE;
    }
}

/* Process the completion of an IO write operation for a client.
 * This function handles various post-write tasks, including updating client state,
 * allow_async_writes - A flag indicating whether I/O threads can handle pending writes for this client.
 * returns 1 if processing completed successfully, 0 if processing is skipped. */
int processClientIOWriteDone(client *c, int allow_async_writes) {
    /* memory barrier acquire to get the latest client state */
    atomic_thread_fence(memory_order_acquire);
    /* If a client is protected, don't proceed to check the write results as it may trigger conn close. */
    if (c->flag.protected) return 0;

    listUnlinkNode(server.clients_pending_io_write, &c->clients_pending_write_node);
    c->flag.pending_write = 0;
    c->io_write_state = CLIENT_IDLE;

    /* Don't post-process-writes to clients that are going to be closed anyway. */
    if (c->flag.close_asap) return 0;

    /* Update processed count on server */
    server.stat_io_writes_processed += 1;

    connSetPostponeUpdateState(c->conn, 0);
    connUpdateState(c->conn);
    if (postWriteToClient(c) == C_ERR) {
        return 1;
    }

    if (clientHasPendingReplies(c)) {
        if (c->write_flags & WRITE_FLAGS_WRITE_ERROR) {
            /* Install the write handler if there are pending writes in some of the clients as a result of not being
             * able to write everything in one go. */
            installClientWriteHandler(c);
        } else {
            /* If we can send the client to the I/O thread, let it handle the write. */
            if (allow_async_writes && trySendWriteToIOThreads(c) == C_OK) return 1;
            /* Try again in the next eventloop */
            putClientInPendingWriteQueue(c);
        }
    }

    return 1;
}

/* This function handles the post-processing of I/O write operations that have been
 * completed for clients. It iterates through the list of clients with pending I/O
 * writes and performs necessary actions based on their current state.
 *
 * Returns The number of clients processed during this function call. */
int processIOThreadsWriteDone(void) {
    if (listLength(server.clients_pending_io_write) == 0) return 0;
    int processed = 0;
    listNode *ln;

    listNode *next = listFirst(server.clients_pending_io_write);
    while (next) {
        ln = next;
        next = listNextNode(ln);
        client *c = listNodeValue(ln);

        /* Client is still waiting for a pending I/O - skip it */
        if (c->io_write_state == CLIENT_PENDING_IO || c->io_read_state == CLIENT_PENDING_IO) continue;

        processed += processClientIOWriteDone(c, 1);
    }

    return processed;
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. */
int handleClientsWithPendingWrites(void) {
    int processed = 0;
    int pending_writes = listLength(server.clients_pending_write);
    if (pending_writes == 0) return processed; /* Return ASAP if there are no clients. */

    /* Adjust the number of I/O threads based on the number of pending writes this is required in case pending_writes >
     * poll_events (for example in pubsub) */
    adjustIOThreadsByEventLoad(pending_writes, 1);

    listIter li;
    listNode *ln;
    listRewind(server.clients_pending_write, &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        c->flag.pending_write = 0;
        listUnlinkNode(server.clients_pending_write, ln);

        /* If a client is protected, don't do anything,
         * that may trigger write error or recreate handler. */
        if (c->flag.protected) continue;

        /* Don't write to clients that are going to be closed anyway. */
        if (c->flag.close_asap) continue;

        if (!clientHasPendingReplies(c)) continue;

        /* If we can send the client to the I/O thread, let it handle the write. */
        if (trySendWriteToIOThreads(c) == C_OK) continue;

        /* We can't write to the client while IO operation is in progress. */
        if (c->io_write_state != CLIENT_IDLE || c->io_read_state != CLIENT_IDLE) continue;

        processed++;

        /* Try to write buffers to the client socket. */
        if (writeToClient(c) == C_ERR) continue;

        /* If after the synchronous writes above we still have data to
         * output to the client, we need to install the writable handler. */
        if (clientHasPendingReplies(c)) {
            installClientWriteHandler(c);
        }
    }
    return processed;
}

/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
    serverCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL;
    serverCommandProc *prevParentCmd = c->cmd && c->cmd->parent ? c->cmd->parent->proc : NULL;

    freeClientArgv(c);
    freeClientOriginalArgv(c);
    c->cur_script = NULL;
    c->reqtype = 0;
    c->multibulklen = 0;
    c->net_input_bytes_curr_cmd = 0;
    c->bulklen = -1;
    c->slot = -1;
    c->flag.executing_command = 0;
    c->flag.replication_done = 0;
    c->net_output_bytes_curr_cmd = 0;

    /* Make sure the duration has been recorded to some command. */
    serverAssert(c->duration == 0);
#ifdef LOG_REQ_RES
    reqresReset(c, 1);
#endif

    if (c->deferred_reply_errors) listRelease(c->deferred_reply_errors);
    c->deferred_reply_errors = NULL;

    /* We clear the ASKING flag as well if we are not inside a MULTI, and
     * if what we just executed is not the ASKING command itself. */
    if (!c->flag.multi && prevcmd != askingCommand) c->flag.asking = 0;

    /* We do the same for the CACHING command as well. It also affects
     * the next command or transaction executed, in a way very similar
     * to ASKING. */
    if (!c->flag.multi && prevParentCmd != clientCommand) c->flag.tracking_caching = 0;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    c->flag.reply_skip = 0;
    if (c->flag.reply_skip_next) {
        c->flag.reply_skip = 1;
        c->flag.reply_skip_next = 0;
    }
}

void resetClientIOState(client *c) {
    c->nwritten = 0;
    c->nread = 0;
    c->io_read_state = c->io_write_state = CLIENT_IDLE;
    c->io_parsed_cmd = NULL;
    c->flag.pending_command = 0;
    c->io_last_bufpos = 0;
    c->io_last_reply_block = NULL;
}

/* Initializes the shared query buffer to a new sds with the default capacity.
 * Need to ensure the initlen is not less than readlen in readToQueryBuf. */
void initSharedQueryBuf(void) {
    thread_shared_qb = sdsnewlen(NULL, PROTO_IOBUF_LEN);
    sdsclear(thread_shared_qb);
}

void freeSharedQueryBuf(void) {
    sdsfree(thread_shared_qb);
    thread_shared_qb = NULL;
}

/* This function is used when we want to re-enter the event loop but there
 * is the risk that the client we are dealing with will be freed in some
 * way. This happens for instance in:
 *
 * * DEBUG RELOAD and similar.
 * * When a Lua script is in -BUSY state.
 * * A cluster replica executing CLUSTER SETSLOT during slot migration.
 *
 * So the function will protect the client by doing two things:
 *
 * 1) It removes the file events. This way it is not possible that an
 *    error is signaled on the socket, freeing the client.
 * 2) Moreover it makes sure that if the client is freed in a different code
 *    path, it is not really released, but only marked for later release. */
void protectClient(client *c) {
    c->flag.protected = 1;
    if (c->conn) {
        connSetReadHandler(c->conn, NULL);
        connSetWriteHandler(c->conn, NULL);
    }
}

/* This will undo the client protection done by protectClient() */
void unprotectClient(client *c) {
    if (c->flag.protected) {
        c->flag.protected = 0;
        if (c->conn) {
            connSetReadHandler(c->conn, readQueryFromClient);
            if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
        }
    }
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure.
 * Sets the client read_flags to indicate the parsing outcome. */
void processInlineBuffer(client *c) {
    char *newline;
    int argc, j, linefeed_chars = 1;
    sds *argv, aux;
    size_t querylen;
    int is_primary = c->read_flags & READ_FLAGS_PRIMARY;

    /* Search for end of line */
    newline = strchr(c->querybuf + c->qb_pos, '\n');

    /* Nothing to do without a \r\n */
    if (newline == NULL) {
        if (sdslen(c->querybuf) - c->qb_pos > PROTO_INLINE_MAX_SIZE) {
            c->read_flags |= READ_FLAGS_ERROR_BIG_INLINE_REQUEST;
        }
        return;
    }

    /* Handle the \r\n case. */
    if (newline != c->querybuf + c->qb_pos && *(newline - 1) == '\r') newline--, linefeed_chars++;

    /* Split the input buffer up to the \r\n */
    querylen = newline - (c->querybuf + c->qb_pos);
    aux = sdsnewlen(c->querybuf + c->qb_pos, querylen);
    argv = sdssplitargs(aux, &argc);
    sdsfree(aux);
    if (argv == NULL) {
        c->read_flags |= READ_FLAGS_ERROR_UNBALANCED_QUOTES;
        return;
    }

    if (querylen == 0) {
        c->read_flags |= READ_FLAGS_INLINE_ZERO_QUERY_LEN;
    }

    /* Primaries should never send us inline protocol to run actual
     * commands. If this happens, it is likely due to a bug in the server where
     * we got some desynchronization in the protocol, for example
     * because of a PSYNC gone bad.
     *
     * However there is an exception: primaries may send us just a newline
     * to keep the connection active. */
    if (querylen != 0 && is_primary) {
        sdsfreesplitres(argv, argc);
        c->read_flags |= READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_PRIMARY;
        return;
    }

    /* Move querybuffer position to the next query in the buffer. */
    c->qb_pos += querylen + linefeed_chars;

    /* Setup argv array on client structure */
    if (argc) {
        if (c->argv) zfree(c->argv);
        c->argv_len = argc;
        c->argv = zmalloc(sizeof(robj *) * c->argv_len);
        c->argv_len_sum = 0;
    }

    /* Create an Object for all arguments. */
    for (c->argc = 0, j = 0; j < argc; j++) {
        /* Strings returned from sdssplitargs() may have unused capacity that we can trim. */
        argv[j] = sdsRemoveFreeSpace(argv[j], 1);
        c->argv[c->argc] = createObject(OBJ_STRING, argv[j]);
        c->argc++;
        c->argv_len_sum += sdslen(argv[j]);
    }
    zfree(argv);

    /* Per-slot network bytes-in calculation.
     *
     * We calculate and store the current command's ingress bytes under
     * c->net_input_bytes_curr_cmd, for which its per-slot aggregation is deferred
     * until c->slot is parsed later within processCommand().
     *
     * Calculation: For inline buffer, every whitespace is of length 1,
     * with the exception of the trailing '\r\n' being length 2.
     *
     * For example;
     * Command) SET key value
     * Inline) SET key value\r\n
     * */
    c->net_input_bytes_curr_cmd = (c->argv_len_sum + (c->argc - 1) + 2);
    c->read_flags |= READ_FLAGS_PARSING_COMPLETED;
}

/* Helper function. Record protocol error details in server log,
 * and set the client as CLIENT_CLOSE_AFTER_REPLY and
 * CLIENT_PROTOCOL_ERROR. */
#define PROTO_DUMP_LEN 128
static void setProtocolError(const char *errstr, client *c) {
    if (server.verbosity <= LL_VERBOSE || c->flag.primary) {
        sds client = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);

        /* Sample some protocol to given an idea about what was inside. */
        char buf[256];
        buf[0] = '\0';
        if (c->querybuf && sdslen(c->querybuf) - c->qb_pos < PROTO_DUMP_LEN) {
            snprintf(buf, sizeof(buf), "Query buffer during protocol error: '%s'", c->querybuf + c->qb_pos);
        } else if (c->querybuf) {
            snprintf(buf, sizeof(buf), "Query buffer during protocol error: '%.*s' (... more %zu bytes ...) '%.*s'",
                     PROTO_DUMP_LEN / 2, c->querybuf + c->qb_pos, sdslen(c->querybuf) - c->qb_pos - PROTO_DUMP_LEN,
                     PROTO_DUMP_LEN / 2, c->querybuf + sdslen(c->querybuf) - PROTO_DUMP_LEN / 2);
        }

        /* Remove non printable chars. */
        char *p = buf;
        while (*p != '\0') {
            if (!isprint(*p)) *p = '.';
            p++;
        }

        /* Log all the client and protocol info. */
        int loglevel = (c->flag.primary) ? LL_WARNING : LL_VERBOSE;
        serverLog(loglevel, "Protocol error (%s) from client: %s. %s", errstr, client, buf);
        sdsfree(client);
    }
    c->flag.close_after_reply = 1;
    c->flag.protocol_error = 1;
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution.
 * Sets the client's read_flags to indicate the parsing outcome.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */
void processMultibulkBuffer(client *c) {
    char *newline = NULL;
    int ok;
    long long ll;
    int is_primary = c->read_flags & READ_FLAGS_PRIMARY;
    int auth_required = c->read_flags & READ_FLAGS_AUTH_REQUIRED;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        serverAssertWithInfo(c, NULL, c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf + c->qb_pos, '\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf) - c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                c->read_flags |= READ_FLAGS_ERROR_BIG_MULTIBULK;
            }
            return;
        }

        /* Buffer should also contain \n */
        if (newline - (c->querybuf + c->qb_pos) > (ssize_t)(sdslen(c->querybuf) - c->qb_pos - 2)) return;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        serverAssertWithInfo(c, NULL, c->querybuf[c->qb_pos] == '*');
        size_t multibulklen_slen = newline - (c->querybuf + 1 + c->qb_pos);
        ok = string2ll(c->querybuf + 1 + c->qb_pos, multibulklen_slen, &ll);
        if (!ok || ll > INT_MAX) {
            c->read_flags |= READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN;
            return;
        } else if (ll > 10 && auth_required) {
            c->read_flags |= READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN;
            return;
        }

        c->qb_pos = (newline - c->querybuf) + 2;

        if (ll <= 0) {
            c->read_flags |= READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN;
            return;
        }

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv_len = min(c->multibulklen, 1024);
        c->argv = zmalloc(sizeof(robj *) * c->argv_len);
        c->argv_len_sum = 0;

        /* Per-slot network bytes-in calculation.
         *
         * We calculate and store the current command's ingress bytes under
         * c->net_input_bytes_curr_cmd, for which its per-slot aggregation is deferred
         * until c->slot is parsed later within processCommand().
         *
         * Calculation: For multi bulk buffer, we accumulate four factors, namely;
         *
         * 1) multibulklen_slen + 1
         *    Cumulative string length (and not the value of) of multibulklen,
         *    including +1 from RESP first byte.
         * 2) bulklen_slen + c->argc
         *    Cumulative string length (and not the value of) of bulklen,
         *    including +1 from RESP first byte per argument count.
         * 3) c->argv_len_sum
         *    Cumulative string length of all argument vectors.
         * 4) c->argc * 4 + 2
         *    Cumulative string length of all white-spaces, for which there exists a total of
         *    4 bytes per argument, plus 2 bytes from the leading '\r\n' from multibulklen.
         *
         * For example;
         * Command) SET key value
         * RESP) *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
         *
         * 1) String length of "*3" is 2, obtained from (multibulklen_slen + 1).
         * 2) String length of "$3" "$3" "$5" is 6, obtained from (bulklen_slen + c->argc).
         * 3) String length of "SET" "key" "value" is 11, obtained from (c->argv_len_sum).
         * 4) String length of all white-spaces "\r\n" is 14, obtained from (c->argc * 4 + 2).
         *
         * The 1st component is calculated within the below line.
         * */
        c->net_input_bytes_curr_cmd += (multibulklen_slen + 1);
    }

    serverAssertWithInfo(c, NULL, c->multibulklen > 0);
    while (c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf + c->qb_pos, '\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf) - c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                    c->read_flags |= READ_FLAGS_ERROR_BIG_BULK_COUNT;
                    return;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline - (c->querybuf + c->qb_pos) > (ssize_t)(sdslen(c->querybuf) - c->qb_pos - 2)) break;

            if (c->querybuf[c->qb_pos] != '$') {
                c->read_flags |= READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER;
                return;
            }

            size_t bulklen_slen = newline - (c->querybuf + c->qb_pos + 1);
            ok = string2ll(c->querybuf + c->qb_pos + 1, bulklen_slen, &ll);
            if (!ok || ll < 0 || (!(is_primary) && ll > server.proto_max_bulk_len)) {
                c->read_flags |= READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN;
                return;
            } else if (ll > 16384 && auth_required) {
                c->read_flags |= READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN;
                return;
            }

            c->qb_pos = newline - c->querybuf + 2;
            if (!(is_primary) && ll >= PROTO_MBULK_BIG_ARG) {
                /* When the client is not a primary client (because primary
                 * client's querybuf can only be trimmed after data applied
                 * and sent to replicas).
                 *
                 * If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data.
                 *
                 * But only when the data we have not parsed is less than
                 * or equal to ll+2. If the data length is greater than
                 * ll+2, trimming querybuf is just a waste of time, because
                 * at this time the querybuf contains not only our bulk. */
                if (sdslen(c->querybuf) - c->qb_pos <= (size_t)ll + 2) {
                    if (c->querybuf == thread_shared_qb) {
                        /* Let the client take the ownership of the shared buffer. */
                        initSharedQueryBuf();
                    }
                    sdsrange(c->querybuf, c->qb_pos, -1);
                    c->qb_pos = 0;
                    /* Hint the sds library about the amount of bytes this string is
                     * going to contain. */
                    c->querybuf = sdsMakeRoomForNonGreedy(c->querybuf, ll + 2 - sdslen(c->querybuf));
                    /* We later set the peak to the used portion of the buffer, but here we over
                     * allocated because we know what we need, make sure it'll not be shrunk before used. */
                    if (c->querybuf_peak < (size_t)ll + 2) c->querybuf_peak = ll + 2;
                }
            }
            c->bulklen = ll;
            /* Per-slot network bytes-in calculation, 2nd component.
             * c->argc portion is deferred, as it may not have been fully populated at this point. */
            c->net_input_bytes_curr_cmd += bulklen_slen;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf) - c->qb_pos < (size_t)(c->bulklen + 2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Check if we have space in argv, grow if needed */
            if (c->argc >= c->argv_len) {
                c->argv_len = min(c->argv_len < INT_MAX / 2 ? c->argv_len * 2 : INT_MAX, c->argc + c->multibulklen);
                c->argv = zrealloc(c->argv, sizeof(robj *) * c->argv_len);
            }

            /* Optimization: if a non-primary client's buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            if (!is_primary && c->qb_pos == 0 && c->bulklen >= PROTO_MBULK_BIG_ARG &&
                sdslen(c->querybuf) == (size_t)(c->bulklen + 2)) {
                c->argv[c->argc++] = createObject(OBJ_STRING, c->querybuf);
                c->argv_len_sum += c->bulklen;
                sdsIncrLen(c->querybuf, -2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                c->querybuf = sdsnewlen(SDS_NOINIT, c->bulklen + 2);
                sdsclear(c->querybuf);
            } else {
                c->argv[c->argc++] = createStringObject(c->querybuf + c->qb_pos, c->bulklen);
                c->argv_len_sum += c->bulklen;
                c->qb_pos += c->bulklen + 2;
            }
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) {
        /* Per-slot network bytes-in calculation, 3rd and 4th components.
         * Here, the deferred c->argc from 2nd component is added, resulting in c->argc * 5 instead of * 4. */
        c->net_input_bytes_curr_cmd += (c->argv_len_sum + (c->argc * 5 + 2));
        c->read_flags |= READ_FLAGS_PARSING_COMPLETED;
    }
}

/* Perform necessary tasks after a command was executed:
 *
 * 1. The client is reset unless there are reasons to avoid doing it.
 * 2. In the case of primary clients, the replication offset is updated.
 * 3. Propagate commands we got from our primary to replicas down the line. */
void commandProcessed(client *c) {
    /* If client is blocked(including paused), just return avoid reset and replicate.
     *
     * 1. Don't reset the client structure for blocked clients, so that the reply
     *    callback will still be able to access the client argv and argc fields.
     *    The client will be reset in unblockClient().
     * 2. Don't update replication offset or propagate commands to replicas,
     *    since we have not applied the command. */
    if (c->flag.blocked) return;

    reqresAppendResponse(c);
    clusterSlotStatsAddNetworkBytesInForUserClient(c);
    resetClient(c);

    if (!c->repl_data) return;

    long long prev_offset = c->repl_data->reploff;
    if (c->flag.primary && !c->flag.multi) {
        /* Update the applied replication offset of our primary. */
        c->repl_data->reploff = c->repl_data->read_reploff - sdslen(c->querybuf) + c->qb_pos;
    }

    /* If the client is a primary we need to compute the difference
     * between the applied offset before and after processing the buffer,
     * to understand how much of the replication stream was actually
     * applied to the primary state: this quantity, and its corresponding
     * part of the replication stream, will be propagated to the
     * sub-replicas and to the replication backlog. */
    if (c->flag.primary) {
        long long applied = c->repl_data->reploff - prev_offset;
        if (applied) {
            replicationFeedStreamFromPrimaryStream(c->querybuf + c->repl_data->repl_applied, applied);
            c->repl_data->repl_applied += applied;
        }
    }
}

/* This function calls processCommand(), but also performs a few sub tasks
 * for the client that are useful in that context:
 *
 * 1. It sets the current client to the client 'c'.
 * 2. calls commandProcessed() if the command was handled.
 *
 * The function returns C_ERR in case the client was freed as a side effect
 * of processing the command, otherwise C_OK is returned. */
int processCommandAndResetClient(client *c) {
    int deadclient = 0;
    client *old_client = server.current_client;
    server.current_client = c;
    if (processCommand(c) == C_OK) {
        commandProcessed(c);
        /* Update the client's memory to include output buffer growth following the
         * processed command. */
        if (c->conn) updateClientMemUsageAndBucket(c);
    }

    if (server.current_client == NULL) deadclient = 1;
    /*
     * Restore the old client, this is needed because when a script
     * times out, we will get into this code from processEventsWhileBlocked.
     * Which will cause to set the server.current_client. If not restored
     * we will return 1 to our caller which will falsely indicate the client
     * is dead and will stop reading from its buffer.
     */
    server.current_client = old_client;
    /* performEvictions may flush replica output buffers. This may
     * result in a replica, that may be the active client, to be
     * freed. */
    return deadclient ? C_ERR : C_OK;
}


/* This function will execute any fully parsed commands pending on
 * the client. Returns C_ERR if the client is no longer valid after executing
 * the command, and C_OK for all other cases. */
int processPendingCommandAndInputBuffer(client *c) {
    /* Notice, this code is also called from 'processUnblockedClients'.
     * But in case of a module blocked client (see RM_Call 'K' flag) we do not reach this code path.
     * So whenever we change the code here we need to consider if we need this change on module
     * blocked client as well */
    if (c->flag.pending_command) {
        c->flag.pending_command = 0;
        if (processCommandAndResetClient(c) == C_ERR) {
            return C_ERR;
        }
    }

    /* Now process client if it has more data in it's buffer.
     *
     * Note: when a primary client steps into this function,
     * it can always satisfy this condition, because its querybuf
     * contains data not applied. */
    if (c->querybuf && sdslen(c->querybuf) > 0) {
        return processInputBuffer(c);
    }
    return C_OK;
}

/* Parse a single command from the query buf.
 *
 * This function may be called from the main thread or from the I/O thread.
 *
 * Sets the client's read_flags to indicate the parsing outcome */
void parseCommand(client *c) {
    /* Determine request type when unknown. */
    if (!c->reqtype) {
        if (c->querybuf[c->qb_pos] == '*') {
            c->reqtype = PROTO_REQ_MULTIBULK;
        } else {
            c->reqtype = PROTO_REQ_INLINE;
        }
    }

    if (c->reqtype == PROTO_REQ_INLINE) {
        processInlineBuffer(c);
    } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
        processMultibulkBuffer(c);
    } else {
        serverPanic("Unknown request type");
    }
}

int canParseCommand(client *c) {
    if (c->cmd != NULL) return 0;

    /* Don't parse a command if the client is in the middle of something. */
    if (c->flag.blocked || c->flag.unblocked) return 0;

    /* Don't process more buffers from clients that have already pending
     * commands to execute in c->argv. */
    if (c->flag.pending_command) return 0;

    /* Don't process input from the primary while there is a busy script
     * condition on the replica. We want just to accumulate the replication
     * stream (instead of replying -BUSY like we do with other clients) and
     * later resume the processing. */
    if (isInsideYieldingLongCommand() && c->flag.primary) return 0;

    /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
     * written to the client. Make sure to not let the reply grow after
     * this flag has been set (i.e. don't process more commands).
     *
     * The same applies for clients we want to terminate ASAP. */
    if (c->flag.close_after_reply || c->flag.close_asap) return 0;

    return 1;
}

int processInputBuffer(client *c) {
    /* Parse the query buffer. */
    while (c->querybuf && c->qb_pos < sdslen(c->querybuf)) {
        if (!canParseCommand(c)) {
            break;
        }

        c->read_flags = c->flag.primary ? READ_FLAGS_PRIMARY : 0;
        c->read_flags |= authRequired(c) ? READ_FLAGS_AUTH_REQUIRED : 0;

        parseCommand(c);

        if (handleParseResults(c) != PARSE_OK) {
            break;
        }

        if (c->argc == 0) {
            /* No command to process - continue parsing the query buf. */
            continue;
        }

        if (c->querybuf == thread_shared_qb) {
            /* Before processing the command, reset the shared query buffer to its default state.
             * This avoids unintentionally modifying the shared qb during processCommand as we may use
             * the shared qb for other clients during processEventsWhileBlocked */
            resetSharedQueryBuf(c);
        }

        /* We are finally ready to execute the command. */
        if (processCommandAndResetClient(c) == C_ERR) {
            /* If the client is no longer valid, we avoid exiting this
             * loop and trimming the client buffer later. So we return
             * ASAP in that case. */
            return C_ERR;
        }
    }

    return C_OK;
}

/* This function can be called from the main-thread or from the IO-thread.
 * The function allocates query-buf for the client if required and reads to it from the network.
 * It will set c->nread to the bytes read from the network. */
void readToQueryBuf(client *c) {
    int big_arg = 0;
    size_t qblen, readlen;

    /* If the replica RDB client is marked as closed ASAP, do not try to read from it */
    if (c->flag.close_asap) return;

    int is_primary = c->read_flags & READ_FLAGS_PRIMARY;

    readlen = PROTO_IOBUF_LEN;
    qblen = c->querybuf ? sdslen(c->querybuf) : 0;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * robj representing the argument. */

    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1 && c->bulklen >= PROTO_MBULK_BIG_ARG) {
        ssize_t remaining = (size_t)(c->bulklen + 2) - (qblen - c->qb_pos);
        big_arg = 1;

        /* Note that the 'remaining' variable may be zero in some edge case,
         * for example once we resume a blocked client after CLIENT PAUSE. */
        if (remaining > 0) readlen = remaining;

        /* Primary client needs expand the readlen when meet BIG_ARG(see #9100),
         * but doesn't need align to the next arg, we can read more data. */
        if (c->flag.primary && readlen < PROTO_IOBUF_LEN) readlen = PROTO_IOBUF_LEN;
    }

    if (c->querybuf == NULL) {
        serverAssert(sdslen(thread_shared_qb) == 0);
        c->querybuf = big_arg ? sdsempty() : thread_shared_qb;
        qblen = sdslen(c->querybuf);
    }

    /* c->querybuf may be expanded. If so, the old thread_shared_qb will be released.
     * Although we have ensured that c->querybuf will not be expanded in the current
     * thread_shared_qb, we still add this check for code robustness. */
    int use_thread_shared_qb = (c->querybuf == thread_shared_qb) ? 1 : 0;
    if (!is_primary && // primary client's querybuf can grow greedy.
        (big_arg || sdsalloc(c->querybuf) < PROTO_IOBUF_LEN)) {
        /* When reading a BIG_ARG we won't be reading more than that one arg
         * into the query buffer, so we don't need to pre-allocate more than we
         * need, so using the non-greedy growing. For an initial allocation of
         * the query buffer, we also don't wanna use the greedy growth, in order
         * to avoid collision with the RESIZE_THRESHOLD mechanism. */
        c->querybuf = sdsMakeRoomForNonGreedy(c->querybuf, readlen);
        /* We later set the peak to the used portion of the buffer, but here we over
         * allocated because we know what we need, make sure it'll not be shrunk before used. */
        if (c->querybuf_peak < qblen + readlen) c->querybuf_peak = qblen + readlen;
    } else {
        c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);

        /* Read as much as possible from the socket to save read(2) system calls. */
        readlen = sdsavail(c->querybuf);
    }
    if (use_thread_shared_qb) serverAssert(c->querybuf == thread_shared_qb);

    c->nread = connRead(c->conn, c->querybuf + qblen, readlen);
    if (c->nread <= 0) {
        return;
    }

    sdsIncrLen(c->querybuf, c->nread);
    qblen = sdslen(c->querybuf);
    if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
    if (!is_primary) {
        /* The commands cached in the MULTI/EXEC queue have not been executed yet,
         * so they are also considered a part of the query buffer in a broader sense.
         *
         * For unauthenticated clients, the query buffer cannot exceed 1MB at most. */
        size_t qb_memory = sdslen(c->querybuf) + (c->mstate ? c->mstate->argv_len_sums : 0);
        if (qb_memory > server.client_max_querybuf_len ||
            (qb_memory > 1024 * 1024 && (c->read_flags & READ_FLAGS_AUTH_REQUIRED))) {
            c->read_flags |= READ_FLAGS_QB_LIMIT_REACHED;
        }
    }
}

void readQueryFromClient(connection *conn) {
    client *c = connGetPrivateData(conn);
    /* Check if we can send the client to be handled by the IO-thread */
    if (postponeClientRead(c)) return;

    if (c->io_write_state != CLIENT_IDLE || c->io_read_state != CLIENT_IDLE) return;

    readToQueryBuf(c);

    if (handleReadResult(c) == C_OK) {
        if (processInputBuffer(c) == C_ERR) return;
    }
    beforeNextClient(c);
}

/* An "Address String" is a colon separated ip:port pair.
 * For IPv4 it's in the form x.y.z.k:port, example: "127.0.0.1:1234".
 * For IPv6 addresses we use [] around the IP part, like in "[::1]:1234".
 * For Unix sockets we use path:0, like in "/tmp/redis:0".
 *
 * An Address String always fits inside a buffer of NET_ADDR_STR_LEN bytes,
 * including the null term.
 *
 * On failure the function still populates 'addr' with the "?:0" string in case
 * you want to relax error checking or need to display something anyway (see
 * anetFdToString implementation for more info). */
void genClientAddrString(client *client, char *addr, size_t addr_len, int remote) {
    if (client->flag.unix_socket) {
        /* Unix socket client. */
        snprintf(addr, addr_len, "%s:0", server.unixsocket);
    } else {
        /* TCP client. */
        connFormatAddr(client->conn, addr, addr_len, remote);
    }
}

/* This function returns the client peer id, by creating and caching it
 * if client->peerid is NULL, otherwise returning the cached value.
 * The Peer ID never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientPeerId(client *c) {
    char peerid[NET_ADDR_STR_LEN] = {0};

    if (c->peerid == NULL) {
        genClientAddrString(c, peerid, sizeof(peerid), 1);
        c->peerid = sdsnew(peerid);
    }
    return c->peerid;
}

/* This function returns the client bound socket name, by creating and caching
 * it if client->sockname is NULL, otherwise returning the cached value.
 * The Socket Name never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientSockname(client *c) {
    char sockname[NET_ADDR_STR_LEN] = {0};

    if (c->sockname == NULL) {
        genClientAddrString(c, sockname, sizeof(sockname), 0);
        c->sockname = sdsnew(sockname);
    }
    return c->sockname;
}

int isClientConnIpV6(client *c) {
    /* The cached client peer id is on the form "[IPv6]:port" for IPv6
     * addresses, so we just check for '[' here. */
    if (c->flag.fake && server.current_client) {
        /* Fake client? Use current client instead.
         * Noted that in here we are assuming server.current_client is set
         * and real (aof has already violated this in loadSingleAppendOnlyFil). */
        c = server.current_client;
    }
    return getClientPeerId(c)[0] == '[';
}

/* Concatenate a string representing the state of a client in a human
 * readable format, into the sds string 's'. */
sds catClientInfoString(sds s, client *client, int hide_user_data) {
    if (!server.crashed) waitForClientIO(client);
    char flags[17], events[3], conninfo[CONN_INFO_LEN], *p;

    p = flags;
    if (client->flag.replica) {
        if (client->flag.monitor)
            *p++ = 'O';
        else
            *p++ = 'S';
    }

    if (client->flag.primary) *p++ = 'M';
    if (client->flag.pubsub) *p++ = 'P';
    if (client->flag.multi) *p++ = 'x';
    if (client->flag.blocked) *p++ = 'b';
    if (client->flag.tracking) *p++ = 't';
    if (client->flag.tracking_broken_redir) *p++ = 'R';
    if (client->flag.tracking_bcast) *p++ = 'B';
    if (client->flag.dirty_cas) *p++ = 'd';
    if (client->flag.close_after_reply) *p++ = 'c';
    if (client->flag.unblocked) *p++ = 'u';
    if (client->flag.close_asap) *p++ = 'A';
    if (client->flag.unix_socket) *p++ = 'U';
    if (client->flag.readonly) *p++ = 'r';
    if (client->flag.no_evict) *p++ = 'e';
    if (client->flag.no_touch) *p++ = 'T';
    if (client->flag.import_source) *p++ = 'I';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    p = events;
    if (client->conn) {
        if (connHasReadHandler(client->conn)) *p++ = 'r';
        if (connHasWriteHandler(client->conn)) *p++ = 'w';
    }
    *p = '\0';

    /* Compute the total memory consumed by this client. */
    size_t obufmem, total_mem = getClientMemoryUsage(client, &obufmem);

    size_t used_blocks_of_repl_buf = 0;
    if (client->repl_data && client->repl_data->ref_repl_buf_node) {
        replBufBlock *last = listNodeValue(listLast(server.repl_buffer_blocks));
        replBufBlock *cur = listNodeValue(client->repl_data->ref_repl_buf_node);
        used_blocks_of_repl_buf = last->id - cur->id + 1;
    }
    sds ret = sdscatfmt(
        s,
        FMTARGS(
            "id=%U", (unsigned long long)client->id,
            " addr=%s", getClientPeerId(client),
            " laddr=%s", getClientSockname(client),
            " %s", connGetInfo(client->conn, conninfo, sizeof(conninfo)),
            " name=%s", hide_user_data ? "*redacted*" : (client->name ? (char *)client->name->ptr : ""),
            " age=%I", (long long)(commandTimeSnapshot() / 1000 - client->ctime),
            " idle=%I", (long long)(server.unixtime - client->last_interaction),
            " flags=%s", flags,
            " db=%i", client->db->id,
            " sub=%i", client->pubsub_data ? (int)dictSize(client->pubsub_data->pubsub_channels) : 0,
            " psub=%i", client->pubsub_data ? (int)dictSize(client->pubsub_data->pubsub_patterns) : 0,
            " ssub=%i", client->pubsub_data ? (int)dictSize(client->pubsub_data->pubsubshard_channels) : 0,
            " multi=%i", client->mstate ? client->mstate->count : -1,
            " watch=%i", client->mstate ? (int)listLength(&client->mstate->watched_keys) : 0,
            " qbuf=%U", client->querybuf ? (unsigned long long)sdslen(client->querybuf) : 0,
            " qbuf-free=%U", client->querybuf ? (unsigned long long)sdsavail(client->querybuf) : 0,
            " argv-mem=%U", (unsigned long long)client->argv_len_sum,
            " multi-mem=%U", client->mstate ? (unsigned long long)client->mstate->argv_len_sums : 0,
            " rbs=%U", (unsigned long long)client->buf_usable_size,
            " rbp=%U", (unsigned long long)client->buf_peak,
            " obl=%U", (unsigned long long)client->bufpos,
            " oll=%U", (unsigned long long)listLength(client->reply) + used_blocks_of_repl_buf,
            " omem=%U", (unsigned long long)obufmem, /* should not include client->buf since we want to see 0 for static clients. */
            " tot-mem=%U", (unsigned long long)total_mem,
            " events=%s", events,
            " cmd=%s", client->lastcmd ? client->lastcmd->fullname : "NULL",
            " user=%s", hide_user_data ? "*redacted*" : (client->user ? client->user->name : "(superuser)"),
            " redir=%I", (client->flag.tracking) ? (long long)client->pubsub_data->client_tracking_redirection : -1,
            " resp=%i", client->resp,
            " lib-name=%s", client->lib_name ? (char *)client->lib_name->ptr : "",
            " lib-ver=%s", client->lib_ver ? (char *)client->lib_ver->ptr : "",
            " tot-net-in=%U", client->net_input_bytes,
            " tot-net-out=%U", client->net_output_bytes,
            " tot-cmds=%U", client->commands_processed));
    return ret;
}

/* Concatenate a string representing the state of a client in a human
 * readable format, into the sds string 's'.
 *
 * This is a simplified and shortened version of catClientInfoString,
 * it only added some basic fields for tracking clients. */
sds catClientInfoShortString(sds s, client *client, int hide_user_data) {
    if (!server.crashed) waitForClientIO(client);
    char conninfo[CONN_INFO_LEN];

    sds ret = sdscatfmt(
        s,
        FMTARGS(
            "id=%U", (unsigned long long)client->id,
            " addr=%s", getClientPeerId(client),
            " laddr=%s", getClientSockname(client),
            " %s", connGetInfo(client->conn, conninfo, sizeof(conninfo)),
            " name=%s", hide_user_data ? "*redacted*" : (client->name ? (char *)client->name->ptr : ""),
            " user=%s", hide_user_data ? "*redacted*" : (client->user ? client->user->name : "(superuser)"),
            " lib-name=%s", client->lib_name ? (char *)client->lib_name->ptr : "",
            " lib-ver=%s", client->lib_ver ? (char *)client->lib_ver->ptr : ""));
    return ret;
}

sds getAllClientsInfoString(int type, int hide_user_data) {
    listNode *ln;
    listIter li;
    client *client;
    sds o = sdsnewlen(SDS_NOINIT, 200 * listLength(server.clients));
    sdsclear(o);
    listRewind(server.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client = listNodeValue(ln);
        if (type != -1 && getClientType(client) != type) continue;
        o = catClientInfoString(o, client, hide_user_data);
        o = sdscatlen(o, "\n", 1);
    }
    return o;
}

static sds getAllFilteredClientsInfoString(clientFilter *client_filter, int hide_user_data) {
    listNode *ln;
    listIter li;
    client *client;
    sds o = sdsempty();
    sdsclear(o);
    listRewind(server.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client = listNodeValue(ln);
        if (!clientMatchesFilter(client, *client_filter)) continue;
        o = catClientInfoString(o, client, hide_user_data);
        o = sdscatlen(o, "\n", 1);
    }
    return o;
}

/* Check validity of an attribute that's gonna be shown in CLIENT LIST. */
int validateClientAttr(const char *val) {
    /* Check if the charset is ok. We need to do this otherwise
     * CLIENT LIST format will break. You should always be able to
     * split by space to get the different fields. */
    while (*val) {
        if (*val < '!' || *val > '~') { /* ASCII is assumed. */
            return C_ERR;
        }
        val++;
    }
    return C_OK;
}

/* Returns C_OK if the name is valid. Returns C_ERR & sets `err` (when provided) otherwise. */
int validateClientName(robj *name, const char **err) {
    const char *err_msg = "Client names cannot contain spaces, newlines or special characters.";
    int len = (name != NULL) ? sdslen(name->ptr) : 0;
    /* We allow setting the client name to an empty string. */
    if (len == 0) return C_OK;
    if (validateClientAttr(name->ptr) == C_ERR) {
        if (err) *err = err_msg;
        return C_ERR;
    }
    return C_OK;
}

/* Returns C_OK if the name has been set or C_ERR if the name is invalid. */
int clientSetName(client *c, robj *name, const char **err) {
    if (validateClientName(name, err) == C_ERR) {
        return C_ERR;
    }
    int len = (name != NULL) ? sdslen(name->ptr) : 0;
    /* Setting the client name to an empty string actually removes
     * the current name. */
    if (len == 0) {
        if (c->name) decrRefCount(c->name);
        c->name = NULL;
        return C_OK;
    }
    if (c->name) decrRefCount(c->name);
    c->name = name;
    incrRefCount(name);
    return C_OK;
}

/* This function implements CLIENT SETNAME, including replying to the
 * user with an error if the charset is wrong (in that case C_ERR is
 * returned). If the function succeeded C_OK is returned, and it's up
 * to the caller to send a reply if needed.
 *
 * Setting an empty string as name has the effect of unsetting the
 * currently set name: the client will remain unnamed.
 *
 * This function is also used to implement the HELLO SETNAME option. */
int clientSetNameOrReply(client *c, robj *name) {
    const char *err = NULL;
    int result = clientSetName(c, name, &err);
    if (result == C_ERR) {
        addReplyError(c, err);
    }
    return result;
}

/* Set client or connection related info */
void clientSetinfoCommand(client *c) {
    sds attr = c->argv[2]->ptr;
    robj *valob = c->argv[3];
    sds val = valob->ptr;
    robj **destvar = NULL;
    if (!strcasecmp(attr, "lib-name")) {
        destvar = &c->lib_name;
    } else if (!strcasecmp(attr, "lib-ver")) {
        destvar = &c->lib_ver;
    } else {
        addReplyErrorFormat(c, "Unrecognized option '%s'", attr);
        return;
    }

    if (validateClientAttr(val) == C_ERR) {
        addReplyErrorFormat(c, "%s cannot contain spaces, newlines or special characters.", attr);
        return;
    }
    if (*destvar) decrRefCount(*destvar);
    if (sdslen(val)) {
        *destvar = valob;
        incrRefCount(valob);
    } else
        *destvar = NULL;
    addReply(c, shared.ok);
}

/* Reset the client state to resemble a newly connected client.
 */
void resetCommand(client *c) {
    /* MONITOR clients are also marked with CLIENT_REPLICA, we need to
     * distinguish between the two.
     */
    struct ClientFlags flags = c->flag;
    if (flags.monitor) {
        flags.monitor = 0;
        flags.replica = 0;
    }

    if (flags.replica || flags.primary || flags.module) {
        addReplyError(c, "can only reset normal client connections");
        return;
    }

    clearClientConnectionState(c);
    addReplyStatus(c, "RESET");
}

/* Disconnect the current client */
void quitCommand(client *c) {
    addReply(c, shared.ok);
    c->flag.close_after_reply = 1;
}

static int parseClientFiltersOrReply(client *c, int index, clientFilter *filter) {
    while (index < c->argc) {
        int moreargs = c->argc > index + 1;

        if (!strcasecmp(c->argv[index]->ptr, "id")) {
            if (filter->ids == NULL) {
                /* Initialize the intset for IDs */
                filter->ids = intsetNew();
            }
            index++; /* Move to the first ID after "ID" */

            /* Process all IDs until a non-numeric argument or end of args */
            while (index < c->argc) {
                long long id;
                if (!string2ll(c->argv[index]->ptr, sdslen(c->argv[index]->ptr), &id)) {
                    break; /* Stop processing IDs if a non-numeric argument is encountered */
                }
                if (id < 1) {
                    addReplyError(c, "client-id should be greater than 0");
                    return C_ERR;
                }

                uint8_t added;
                filter->ids = intsetAdd(filter->ids, id, &added);
                index++; /* Move to the next argument */
            }
        } else if (!strcasecmp(c->argv[index]->ptr, "maxage") && moreargs) {
            long long tmp;

            if (getLongLongFromObjectOrReply(c, c->argv[index + 1], &tmp,
                                             "maxage is not an integer or out of range") != C_OK)
                return C_ERR;
            if (tmp <= 0) {
                addReplyError(c, "maxage should be greater than 0");
                return C_ERR;
            }

            filter->max_age = tmp;
            index += 2;
        } else if (!strcasecmp(c->argv[index]->ptr, "type") && moreargs) {
            filter->type = getClientTypeByName(c->argv[index + 1]->ptr);
            if (filter->type == -1) {
                addReplyErrorFormat(c, "Unknown client type '%s'", (char *)c->argv[index + 1]->ptr);
                return C_ERR;
            }
            index += 2;
        } else if (!strcasecmp(c->argv[index]->ptr, "addr") && moreargs) {
            filter->addr = c->argv[index + 1]->ptr;
            index += 2;
        } else if (!strcasecmp(c->argv[index]->ptr, "laddr") && moreargs) {
            filter->laddr = c->argv[index + 1]->ptr;
            index += 2;
        } else if (!strcasecmp(c->argv[index]->ptr, "user") && moreargs) {
            filter->user = ACLGetUserByName(c->argv[index + 1]->ptr, sdslen(c->argv[index + 1]->ptr));
            if (filter->user == NULL) {
                addReplyErrorFormat(c, "No such user '%s'", (char *)c->argv[index + 1]->ptr);
                return C_ERR;
            }
            index += 2;
        } else if (!strcasecmp(c->argv[index]->ptr, "skipme") && moreargs) {
            if (!strcasecmp(c->argv[index + 1]->ptr, "yes")) {
                filter->skipme = 1;
            } else if (!strcasecmp(c->argv[index + 1]->ptr, "no")) {
                filter->skipme = 0;
            } else {
                addReplyErrorObject(c, shared.syntaxerr);
                return C_ERR;
            }
            index += 2;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return C_ERR;
        }
    }
    return C_OK;
}

static int clientMatchesFilter(client *client, clientFilter client_filter) {
    /* Check each filter condition and return false if the client does not match. */
    if (client_filter.addr && strcmp(getClientPeerId(client), client_filter.addr) != 0) return 0;
    if (client_filter.laddr && strcmp(getClientSockname(client), client_filter.laddr) != 0) return 0;
    if (client_filter.type != -1 && getClientType(client) != client_filter.type) return 0;
    if (client_filter.ids && !intsetFind(client_filter.ids, client->id)) return 0;
    if (client_filter.user && client->user != client_filter.user) return 0;
    if (client_filter.skipme && client == server.current_client) return 0;
    if (client_filter.max_age != 0 && (long long)(commandTimeSnapshot() / 1000 - client->ctime) < client_filter.max_age) return 0;

    /* If all conditions are satisfied, the client matches the filter. */
    return 1;
}

void clientHelpCommand(client *c) {
    const char *help[] = {
        "CACHING (YES|NO)",
        "    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
        "CAPA <option> [options...]",
        "    The client claims its some capability options. Options are:",
        "    * REDIRECT",
        "      The client can handle redirection during primary and replica failover in standalone mode.",
        "GETREDIR",
        "    Return the client ID we are redirecting to when tracking is enabled.",
        "GETNAME",
        "    Return the name of the current connection.",
        "ID",
        "    Return the ID of the current connection.",
        "INFO",
        "    Return information about the current client connection.",
        "KILL <ip:port>",
        "    Kill connection made from <ip:port>.",
        "KILL <option> <value> [<option> <value> [...]]",
        "    Kill connections. Options are:",
        "    * ADDR (<ip:port>|<unixsocket>:0)",
        "      Kill connections made from the specified address",
        "    * LADDR (<ip:port>|<unixsocket>:0)",
        "      Kill connections made to specified local address",
        "    * TYPE (NORMAL|PRIMARY|REPLICA|PUBSUB)",
        "      Kill connections by type.",
        "    * USER <username>",
        "      Kill connections authenticated by <username>.",
        "    * SKIPME (YES|NO)",
        "      Skip killing current connection (default: yes).",
        "    * ID <client-id> [<client-id>...]",
        "      Kill connections by client ids.",
        "    * MAXAGE <maxage>",
        "      Kill connections older than the specified age.",
        "LIST [options ...]",
        "    Return information about client connections. Options:",
        "    * TYPE (NORMAL|PRIMARY|REPLICA|PUBSUB)",
        "      Return clients of specified type.",
        "    * USER <username>",
        "      Return clients authenticated by <username>.",
        "    * ADDR <ip:port>",
        "      Return clients connected from the specified address.",
        "    * LADDR <ip:port>",
        "      Return clients connected to the specified local address.",
        "    * ID <client-id> [<client-id>...]",
        "      Return clients with the specified IDs.",
        "    * SKIPME (YES|NO)",
        "      Exclude the current client from the list (default: no).",
        "    * MAXAGE <maxage>",
        "      List connections older than the specified age.",
        "UNPAUSE",
        "    Stop the current client pause, resuming traffic.",
        "PAUSE <timeout> [WRITE|ALL]",
        "    Suspend all, or just write, clients for <timeout> milliseconds.",
        "REPLY (ON|OFF|SKIP)",
        "    Control the replies sent to the current connection.",
        "SETNAME <name>",
        "    Assign the name <name> to the current connection.",
        "SETINFO <option> <value>",
        "    Set client meta attr. Options are:",
        "    * LIB-NAME: the client lib name.",
        "    * LIB-VER: the client lib version.",
        "UNBLOCK <clientid> [TIMEOUT|ERROR]",
        "    Unblock the specified blocked client.",
        "TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
        "         [OPTIN] [OPTOUT] [NOLOOP]",
        "    Control server assisted client side caching.",
        "TRACKINGINFO",
        "    Report tracking status for the current connection.",
        "NO-EVICT (ON|OFF)",
        "    Protect current client connection from eviction.",
        "NO-TOUCH (ON|OFF)",
        "    Will not touch LRU/LFU stats when this mode is on.",
        "IMPORT-SOURCE (ON|OFF)",
        "    Mark this connection as an import source if import-mode is enabled.",
        "    Sync tools can set their connections into 'import-source' state to visit",
        "    expired keys.",
        NULL};
    addReplyHelp(c, help);
}

void clientIDCommand(client *c) {
    addReplyLongLong(c, c->id);
}

void clientInfoCommand(client *c) {
    sds info = catClientInfoString(sdsempty(), c, 0);
    info = sdscatlen(info, "\n", 1);
    addReplyVerbatim(c, info, sdslen(info), "txt");
    sdsfree(info);
}

void clientListCommand(client *c) {
    int type = -1;
    sds response = NULL;

    if (c->argc > 3) {
        clientFilter filter = {.ids = NULL, .max_age = 0, .addr = NULL, .laddr = NULL, .user = NULL, .type = -1, .skipme = 0};
        int i = 2;

        if (parseClientFiltersOrReply(c, i, &filter) != C_OK) {
            zfree(filter.ids);
            return;
        }
        response = getAllFilteredClientsInfoString(&filter, 0);
        zfree(filter.ids);
    } else if (c->argc != 2) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    if (!response) response = getAllClientsInfoString(type, 0);
    addReplyVerbatim(c, response, sdslen(response), "txt");
    sdsfree(response);
}

void clientReplyCommand(client *c) {
    /* CLIENT REPLY ON|OFF|SKIP */
    if (!strcasecmp(c->argv[2]->ptr, "on")) {
        c->flag.reply_skip = 0;
        c->flag.reply_off = 0;
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[2]->ptr, "off")) {
        c->flag.reply_off = 1;
    } else if (!strcasecmp(c->argv[2]->ptr, "skip")) {
        if (!c->flag.reply_off) c->flag.reply_skip_next = 1;
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
}

void clientNoEvictCommand(client *c) {
    /* CLIENT NO-EVICT ON|OFF */
    if (!strcasecmp(c->argv[2]->ptr, "on")) {
        c->flag.no_evict = 1;
        removeClientFromMemUsageBucket(c, 0);
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[2]->ptr, "off")) {
        c->flag.no_evict = 0;
        updateClientMemUsageAndBucket(c);
        addReply(c, shared.ok);
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
}

void clientKillCommand(client *c) {
    /* CLIENT KILL <ip:port>
     * CLIENT KILL <option> [value] ... <option> [value] */

    listNode *ln;
    listIter li;

    clientFilter client_filter = {.ids = NULL,
                                  .max_age = 0,
                                  .addr = NULL,
                                  .laddr = NULL,
                                  .user = NULL,
                                  .type = -1,
                                  .skipme = 1};

    int killed = 0, close_this_client = 0;

    if (c->argc == 3) {
        /* Old style syntax: CLIENT KILL <addr> */
        client_filter.addr = c->argv[2]->ptr;
        client_filter.skipme = 0; /* With the old form, you can kill yourself. */
    } else if (c->argc > 3) {
        int i = 2; /* Next option index. */

        /* New style syntax: parse options. */
        if (parseClientFiltersOrReply(c, i, &client_filter) != C_OK) {
            /* Free the intset on error */
            goto client_kill_done;
        }
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
        /* Free the intset on error */
        goto client_kill_done;
    }

    /* Iterate clients killing all the matching clients. */
    listRewind(server.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *client = listNodeValue(ln);
        if (!clientMatchesFilter(client, client_filter)) continue;

        /* Kill it. */
        if (c == client) {
            close_this_client = 1;
        } else {
            freeClient(client);
        }
        killed++;
    }

    /* Reply according to old/new format. */
    if (c->argc == 3) {
        if (killed == 0)
            addReplyError(c, "No such client");
        else
            addReply(c, shared.ok);
    } else {
        addReplyLongLong(c, killed);
    }

    /* If this client has to be closed, flag it as CLOSE_AFTER_REPLY
     * only after we queued the reply to its output buffers. */
    if (close_this_client) c->flag.close_after_reply = 1;
client_kill_done:
    zfree(client_filter.ids);
}


void clientUnblockCommand(client *c) {
    /* CLIENT UNBLOCK <id> [timeout|error] */
    long long id;
    int unblock_error = 0;

    if (c->argc == 4) {
        if (!strcasecmp(c->argv[3]->ptr, "timeout")) {
            unblock_error = 0;
        } else if (!strcasecmp(c->argv[3]->ptr, "error")) {
            unblock_error = 1;
        } else {
            addReplyError(c, "CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
            return;
        }
    }
    if (getLongLongFromObjectOrReply(c, c->argv[2], &id, NULL) != C_OK) return;
    struct client *target = lookupClientByID(id);
    /* Note that we never try to unblock a client blocked on a module command, which
     * doesn't have a timeout callback (even in the case of UNBLOCK ERROR).
     * The reason is that we assume that if a command doesn't expect to be timedout,
     * it also doesn't expect to be unblocked by CLIENT UNBLOCK */
    if (target && target->flag.blocked && moduleBlockedClientMayTimeout(target)) {
        if (unblock_error)
            unblockClientOnError(target, "-UNBLOCKED client unblocked via CLIENT UNBLOCK");
        else
            unblockClientOnTimeout(target);

        addReply(c, shared.cone);
    } else {
        addReply(c, shared.czero);
    }
}

void clientSetNameCommand(client *c) {
    /* CLIENT SETNAME */
    if (clientSetNameOrReply(c, c->argv[2]) == C_OK)
        addReply(c, shared.ok);
}

void clientGetNameCommand(client *c) {
    /* CLIENT GETNAME */
    if (c->name)
        addReplyBulk(c, c->name);
    else
        addReplyNull(c);
}

void clientUnpauseCommand(client *c) {
    /* CLIENT UNPAUSE */
    unpauseActions(PAUSE_BY_CLIENT_COMMAND);
    addReply(c, shared.ok);
}

void clientPauseCommand(client *c) {
    /* CLIENT PAUSE TIMEOUT [WRITE|ALL] */
    mstime_t end;
    int isPauseClientAll = 1;
    if (c->argc == 4) {
        if (!strcasecmp(c->argv[3]->ptr, "write")) {
            isPauseClientAll = 0;
        } else if (strcasecmp(c->argv[3]->ptr, "all")) {
            addReplyError(c, "CLIENT PAUSE mode must be WRITE or ALL");
            return;
        }
    }

    if (getTimeoutFromObjectOrReply(c, c->argv[2], &end, UNIT_MILLISECONDS) != C_OK) return;
    pauseClientsByClient(end, isPauseClientAll);
    addReply(c, shared.ok);
}

void clientTrackingCommand(client *c) {
    /* CLIENT TRACKING (on|off) [REDIRECT <id>] [BCAST] [PREFIX first]
     *                          [PREFIX second] [OPTIN] [OPTOUT] [NOLOOP]... */
    long long redir = 0;
    struct ClientFlags options = {0};
    robj **prefix = NULL;
    size_t numprefix = 0;
    initClientPubSubData(c);

    /* Parse the options. */
    for (int j = 3; j < c->argc; j++) {
        int moreargs = (c->argc - 1) - j;

        if (!strcasecmp(c->argv[j]->ptr, "redirect") && moreargs) {
            j++;
            if (redir != 0) {
                addReplyError(c, "A client can only redirect to a single "
                                 "other client");
                zfree(prefix);
                return;
            }

            if (getLongLongFromObjectOrReply(c, c->argv[j], &redir, NULL) != C_OK) {
                zfree(prefix);
                return;
            }
            /* We will require the client with the specified ID to exist
             * right now, even if it is possible that it gets disconnected
             * later. Still a valid sanity check. */
            if (lookupClientByID(redir) == NULL) {
                addReplyError(c, "The client ID you want redirect to "
                                 "does not exist");
                zfree(prefix);
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr, "bcast")) {
            options.tracking_bcast = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "optin")) {
            options.tracking_optin = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "optout")) {
            options.tracking_optout = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "noloop")) {
            options.tracking_noloop = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "prefix") && moreargs) {
            j++;
            prefix = zrealloc(prefix, sizeof(robj *) * (numprefix + 1));
            prefix[numprefix++] = c->argv[j];
        } else {
            zfree(prefix);
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    /* Options are ok: enable or disable the tracking for this client. */
    if (!strcasecmp(c->argv[2]->ptr, "on")) {
        /* Before enabling tracking, make sure options are compatible
         * among each other and with the current state of the client. */
        if (!(options.tracking_bcast) && numprefix) {
            addReplyError(c, "PREFIX option requires BCAST mode to be enabled");
            zfree(prefix);
            return;
        }

        if (c->flag.tracking) {
            int oldbcast = !!c->flag.tracking_bcast;
            int newbcast = !!(options.tracking_bcast);
            if (oldbcast != newbcast) {
                addReplyError(c, "You can't switch BCAST mode on/off before disabling "
                                 "tracking for this client, and then re-enabling it with "
                                 "a different mode.");
                zfree(prefix);
                return;
            }
        }

        if (options.tracking_bcast && (options.tracking_optin || options.tracking_optout)) {
            addReplyError(c, "OPTIN and OPTOUT are not compatible with BCAST");
            zfree(prefix);
            return;
        }

        if (options.tracking_optin && options.tracking_optout) {
            addReplyError(c, "You can't specify both OPTIN mode and OPTOUT mode");
            zfree(prefix);
            return;
        }

        if ((options.tracking_optin && c->flag.tracking_optout) ||
            (options.tracking_optout && c->flag.tracking_optin)) {
            addReplyError(c, "You can't switch OPTIN/OPTOUT mode before disabling "
                             "tracking for this client, and then re-enabling it with "
                             "a different mode.");
            zfree(prefix);
            return;
        }

        if (options.tracking_bcast) {
            if (!checkPrefixCollisionsOrReply(c, prefix, numprefix)) {
                zfree(prefix);
                return;
            }
        }

        enableTracking(c, redir, options, prefix, numprefix);
    } else if (!strcasecmp(c->argv[2]->ptr, "off")) {
        disableTracking(c);
    } else {
        zfree(prefix);
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    zfree(prefix);
    addReply(c, shared.ok);
}

void clientCachingCommand(client *c) {
    if (!c->flag.tracking) {
        addReplyError(c, "CLIENT CACHING can be called only when the "
                         "client is in tracking mode with OPTIN or "
                         "OPTOUT mode enabled");
        return;
    }

    char *opt = c->argv[2]->ptr;
    if (!strcasecmp(opt, "yes")) {
        if (c->flag.tracking_optin) {
            c->flag.tracking_caching = 1;
        } else {
            addReplyError(c, "CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.");
            return;
        }
    } else if (!strcasecmp(opt, "no")) {
        if (c->flag.tracking_optout) {
            c->flag.tracking_caching = 1;
        } else {
            addReplyError(c, "CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.");
            return;
        }
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* Common reply for when we succeeded. */
    addReply(c, shared.ok);
}

void clientGetredirCommand(client *c) {
    /* CLIENT GETREDIR */
    if (c->flag.tracking) {
        addReplyLongLong(c, c->pubsub_data->client_tracking_redirection);
    } else {
        addReplyLongLong(c, -1);
    }
}

void clientTrackingInfoCommand(client *c) {
    addReplyMapLen(c, 3);

    /* Flags */
    addReplyBulkCString(c, "flags");
    void *arraylen_ptr = addReplyDeferredLen(c);
    int numflags = 0;
    addReplyBulkCString(c, c->flag.tracking ? "on" : "off");
    numflags++;
    if (c->flag.tracking_bcast) {
        addReplyBulkCString(c, "bcast");
        numflags++;
    }
    if (c->flag.tracking_optin) {
        addReplyBulkCString(c, "optin");
        numflags++;
        if (c->flag.tracking_caching) {
            addReplyBulkCString(c, "caching-yes");
            numflags++;
        }
    }
    if (c->flag.tracking_optout) {
        addReplyBulkCString(c, "optout");
        numflags++;
        if (c->flag.tracking_caching) {
            addReplyBulkCString(c, "caching-no");
            numflags++;
        }
    }
    if (c->flag.tracking_noloop) {
        addReplyBulkCString(c, "noloop");
        numflags++;
    }
    if (c->flag.tracking_broken_redir) {
        addReplyBulkCString(c, "broken_redirect");
        numflags++;
    }
    setDeferredSetLen(c, arraylen_ptr, numflags);

    /* Redirect */
    addReplyBulkCString(c, "redirect");
    if (c->flag.tracking) {
        addReplyLongLong(c, c->pubsub_data->client_tracking_redirection);
    } else {
        addReplyLongLong(c, -1);
    }

    /* Prefixes */
    addReplyBulkCString(c, "prefixes");
    if (c->pubsub_data->client_tracking_prefixes) {
        addReplyArrayLen(c, raxSize(c->pubsub_data->client_tracking_prefixes));
        raxIterator ri;
        raxStart(&ri, c->pubsub_data->client_tracking_prefixes);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri)) {
            addReplyBulkCBuffer(c, ri.key, ri.key_len);
        }
        raxStop(&ri);
    } else {
        addReplyArrayLen(c, 0);
    }
}

void clientNoTouchCommand(client *c) {
    /* CLIENT NO-TOUCH ON|OFF */
    if (!strcasecmp(c->argv[2]->ptr, "on")) {
        c->flag.no_touch = 1;
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[2]->ptr, "off")) {
        c->flag.no_touch = 0;
        addReply(c, shared.ok);
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
    }
}

void clientCapaCommand(client *c) {
    for (int i = 2; i < c->argc; i++) {
        if (!strcasecmp(c->argv[i]->ptr, "redirect")) {
            c->capa |= CLIENT_CAPA_REDIRECT;
        }
    }
    addReply(c, shared.ok);
}

void clientImportSourceCommand(client *c) {
    /* CLIENT IMPORT-SOURCE ON|OFF */
    if (!server.import_mode && strcasecmp(c->argv[2]->ptr, "off")) {
        addReplyError(c, "Server is not in import mode");
        return;
    }
    if (!strcasecmp(c->argv[2]->ptr, "on")) {
        c->flag.import_source = 1;
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[2]->ptr, "off")) {
        c->flag.import_source = 0;
        addReply(c, shared.ok);
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
}

void clientCommand(client *c) {
    addReplySubcommandSyntaxError(c);
}

/* HELLO [<protocol-version> [AUTH <user> <password>] [SETNAME <name>] ] */
void helloCommand(client *c) {
    long long ver = 0;
    int next_arg = 1;

    if (c->argc >= 2) {
        if (getLongLongFromObjectOrReply(c, c->argv[next_arg++], &ver,
                                         "Protocol version is not an integer or out of range") != C_OK) {
            return;
        }

        if (ver < 2 || ver > 3) {
            addReplyError(c, "-NOPROTO unsupported protocol version");
            return;
        }
    }

    robj *username = NULL;
    robj *password = NULL;
    robj *clientname = NULL;
    for (int j = next_arg; j < c->argc; j++) {
        int moreargs = (c->argc - 1) - j;
        const char *opt = c->argv[j]->ptr;
        if (!strcasecmp(opt, "AUTH") && moreargs >= 2) {
            redactClientCommandArgument(c, j + 1);
            redactClientCommandArgument(c, j + 2);
            username = c->argv[j + 1];
            password = c->argv[j + 2];
            j += 2;
        } else if (!strcasecmp(opt, "SETNAME") && moreargs) {
            clientname = c->argv[j + 1];
            const char *err = NULL;
            if (validateClientName(clientname, &err) == C_ERR) {
                addReplyError(c, err);
                return;
            }
            j++;
        } else {
            addReplyErrorFormat(c, "Syntax error in HELLO option '%s'", opt);
            return;
        }
    }

    if (username && password) {
        robj *err = NULL;
        int auth_result = ACLAuthenticateUser(c, username, password, &err);
        if (auth_result == AUTH_ERR) {
            addAuthErrReply(c, err);
        }
        if (err) decrRefCount(err);
        /* In case of auth errors, return early since we already replied with an ERR.
         * In case of blocking module auth, we reply to the client/setname later upon unblocking. */
        if (auth_result == AUTH_ERR || auth_result == AUTH_BLOCKED) {
            return;
        }
    }

    /* At this point we need to be authenticated to continue. */
    if (!c->flag.authenticated) {
        addReplyError(c, "-NOAUTH HELLO must be called with the client already "
                         "authenticated, otherwise the HELLO <proto> AUTH <user> <pass> "
                         "option can be used to authenticate the client and "
                         "select the RESP protocol version at the same time");
        return;
    }

    /* Now that we're authenticated, set the client name. */
    if (clientname) clientSetName(c, clientname, NULL);

    /* Let's switch to the specified RESP mode. */
    if (ver) c->resp = ver;
    addReplyMapLen(c, 6 + !server.sentinel_mode + (sdslen(server.availability_zone) != 0));

    addReplyBulkCString(c, "server");
    addReplyBulkCString(c, server.extended_redis_compat ? "redis" : SERVER_NAME);

    addReplyBulkCString(c, "version");
    addReplyBulkCString(c, server.extended_redis_compat ? REDIS_VERSION : VALKEY_VERSION);

    addReplyBulkCString(c, "proto");
    addReplyLongLong(c, c->resp);

    addReplyBulkCString(c, "id");
    addReplyLongLong(c, c->id);

    addReplyBulkCString(c, "mode");
    if (server.sentinel_mode)
        addReplyBulkCString(c, "sentinel");
    else if (server.cluster_enabled)
        addReplyBulkCString(c, "cluster");
    else
        addReplyBulkCString(c, "standalone");

    if (!server.sentinel_mode) {
        addReplyBulkCString(c, "role");
        addReplyBulkCString(c, server.primary_host ? "replica" : "master");
    }

    addReplyBulkCString(c, "modules");
    addReplyLoadedModules(c);

    if (sdslen(server.availability_zone) != 0) {
        addReplyBulkCString(c, "availability_zone");
        addReplyBulkCBuffer(c, server.availability_zone, sdslen(server.availability_zone));
    }
}

/* This callback is bound to POST and "Host:" command names. Those are not
 * really commands, but are used in security attacks in order to talk to
 * instances via HTTP, with a technique called "cross protocol scripting"
 * which exploits the fact that services like this server will discard invalid
 * HTTP headers and will process what follows.
 *
 * As a protection against this attack, the server will terminate the connection
 * when a POST or "Host:" header is seen, and will log the event from
 * time to time (to avoid creating a DOS as a result of too many logs). */
void securityWarningCommand(client *c) {
    static time_t logged_time = 0;
    time_t now = time(NULL);

    if (llabs(now - logged_time) > 60) {
        char ip[NET_IP_STR_LEN];
        int port;
        if (connAddrPeerName(c->conn, ip, sizeof(ip), &port) == -1) {
            serverLog(LL_WARNING, "Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: "
                                  "commands to Redis. This is likely due to an attacker attempting to use Cross "
                                  "Protocol Scripting to compromise your Redis instance. Connection aborted.");
        } else {
            serverLog(LL_WARNING,
                      "Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: commands to "
                      "Redis. This is likely due to an attacker attempting to use Cross Protocol Scripting to "
                      "compromise your Redis instance. Connection from %s:%d aborted.",
                      ip, port);
        }
        logged_time = now;
    }
    freeClientAsync(c);
}

/* This function preserves the original command arguments for accurate commandlog recording.
 *
 * It performs the following operations:
 * - Stores the initial command vector if not already saved
 * - Manages memory allocation for command argument modifications
 *
 * new_argc - The new number of arguments to allocate space for if necessary.
 * new_argv - Optional pointer to a new argument vector. If NULL, space will be
 *                allocated for new_argc arguments, preserving the existing arguments.
 */
static void backupAndUpdateClientArgv(client *c, int new_argc, robj **new_argv) {
    robj **old_argv = c->argv;
    int old_argc = c->argc;

    /* Store original arguments if not already saved */
    if (!c->original_argv) {
        c->original_argc = old_argc;
        c->original_argv = old_argv;
    }

    /* Handle direct argv replacement */
    if (new_argv) {
        c->argv = new_argv;
    } else if (c->original_argv == old_argv || new_argc > old_argc) {
        /* Allocate new array if necessary */
        c->argv = zmalloc(sizeof(robj *) * new_argc);

        for (int i = 0; i < old_argc && i < new_argc; i++) {
            c->argv[i] = old_argv[i];
            incrRefCount(c->argv[i]);
        }

        /* Initialize new argument slots to NULL */
        for (int i = old_argc; i < new_argc; i++) {
            c->argv[i] = NULL;
        }
    }

    c->argc = new_argc;
    c->argv_len = new_argc;

    /* Clean up old argv if necessary */
    if (c->argv != old_argv && c->original_argv != old_argv) {
        for (int i = 0; i < old_argc; i++) {
            if (old_argv[i]) decrRefCount(old_argv[i]);
        }
        zfree(old_argv);
    }
}

/* Redact a given argument to prevent it from being shown
 * in the commandlog. This information is stored in the
 * original_argv array. */
void redactClientCommandArgument(client *c, int argc) {
    backupAndUpdateClientArgv(c, c->argc, NULL);
    if (c->original_argv[argc] == shared.redacted) {
        /* This argument has already been redacted */
        return;
    }
    decrRefCount(c->original_argv[argc]);
    c->original_argv[argc] = shared.redacted;
}

/* Rewrite the command vector of the client. All the new objects ref count
 * is incremented. The old command vector is freed, and the old objects
 * ref count is decremented. */
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj *) * argc);
    va_start(ap, argc);
    for (j = 0; j < argc; j++) {
        robj *a;

        a = va_arg(ap, robj *);
        argv[j] = a;
        incrRefCount(a);
    }
    replaceClientCommandVector(c, argc, argv);
    va_end(ap);
}

/* Completely replace the client command vector with the provided one. */
void replaceClientCommandVector(client *c, int argc, robj **argv) {
    int j;
    backupAndUpdateClientArgv(c, argc, argv);
    c->argv_len_sum = 0;
    for (j = 0; j < c->argc; j++)
        if (c->argv[j]) c->argv_len_sum += getStringObjectLen(c->argv[j]);
    c->cmd = lookupCommandOrOriginal(c->argv, c->argc);
    serverAssertWithInfo(c, NULL, c->cmd != NULL);
}

/* Rewrite a single item in the command vector.
 * The new val ref count is incremented, and the old decremented.
 *
 * It is possible to specify an argument over the current size of the
 * argument vector: in this case the array of objects gets reallocated
 * and c->argc set to the max value. However it's up to the caller to
 *
 * 1. Make sure there are no "holes" and all the arguments are set.
 * 2. If the original argument vector was longer than the one we
 *    want to end with, it's up to the caller to set c->argc and
 *    free the no longer used objects on c->argv. */
void rewriteClientCommandArgument(client *c, int i, robj *newval) {
    robj *oldval;
    int new_argc = (i >= c->argc) ? i + 1 : c->argc;
    backupAndUpdateClientArgv(c, new_argc, NULL);

    oldval = c->argv[i];
    if (oldval) c->argv_len_sum -= getStringObjectLen(oldval);
    if (newval) c->argv_len_sum += getStringObjectLen(newval);
    c->argv[i] = newval;
    incrRefCount(newval);
    if (oldval) decrRefCount(oldval);

    /* If this is the command name make sure to fix c->cmd. */
    if (i == 0) {
        c->cmd = lookupCommandOrOriginal(c->argv, c->argc);
        serverAssertWithInfo(c, NULL, c->cmd != NULL);
    }
}

/* This function returns the number of bytes that the server is
 * using to store the reply still not read by the client.
 *
 * Note: this function is very fast so can be called as many time as
 * the caller wishes. The main usage of this function currently is
 * enforcing the client output length limits. */
size_t getClientOutputBufferMemoryUsage(client *c) {
    if (getClientType(c) == CLIENT_TYPE_REPLICA) {
        size_t repl_buf_size = 0;
        size_t repl_node_num = 0;
        size_t repl_node_size = sizeof(listNode) + sizeof(replBufBlock);
        if (c->repl_data->ref_repl_buf_node) {
            replBufBlock *last = listNodeValue(listLast(server.repl_buffer_blocks));
            replBufBlock *cur = listNodeValue(c->repl_data->ref_repl_buf_node);
            repl_buf_size = last->repl_offset + last->size - cur->repl_offset;
            repl_node_num = last->id - cur->id + 1;
        }
        return repl_buf_size + (repl_node_size * repl_node_num);
    } else {
        size_t list_item_size = sizeof(listNode) + sizeof(clientReplyBlock);
        return c->reply_bytes + (list_item_size * listLength(c->reply));
    }
}

/* Returns the total client's memory usage.
 * Optionally, if output_buffer_mem_usage is not NULL, it fills it with
 * the client output buffer memory usage portion of the total. */
size_t getClientMemoryUsage(client *c, size_t *output_buffer_mem_usage) {
    size_t mem = getClientOutputBufferMemoryUsage(c);

    if (output_buffer_mem_usage != NULL) *output_buffer_mem_usage = mem;
    mem += c->querybuf ? sdsAllocSize(c->querybuf) : 0;
    mem += zmalloc_size(c);
    mem += c->buf_usable_size;
    /* For efficiency (less work keeping track of the argv memory), it doesn't include the used memory
     * i.e. unused sds space and internal fragmentation, just the string length. but this is enough to
     * spot problematic clients. */
    mem += c->argv_len_sum + sizeof(robj *) * c->argc;
    mem += multiStateMemOverhead(c);

    /* Add memory overhead of pubsub channels and patterns. Note: this is just the overhead of the robj pointers
     * to the strings themselves because they aren't stored per client. */
    mem += pubsubMemOverhead(c);

    /* Add memory overhead of the tracking prefixes, this is an underestimation so we don't need to traverse the entire
     * rax */
    if (c->pubsub_data && c->pubsub_data->client_tracking_prefixes)
        mem += c->pubsub_data->client_tracking_prefixes->numnodes * (sizeof(raxNode) * sizeof(raxNode *));

    return mem;
}

/* Get the class of a client, used in order to enforce limits to different
 * classes of clients.
 *
 * The function will return one of the following:
 * CLIENT_TYPE_NORMAL -> Normal client, including MONITOR
 * CLIENT_TYPE_REPLICA  -> replica
 * CLIENT_TYPE_PUBSUB -> Client subscribed to Pub/Sub channels
 * CLIENT_TYPE_PRIMARY -> The client representing our replication primary.
 */
int getClientType(client *c) {
    if (c->flag.primary) return CLIENT_TYPE_PRIMARY;
    /* Even though MONITOR clients are marked as replicas, we
     * want the expose them as normal clients. */
    if (c->flag.replica && !c->flag.monitor) return CLIENT_TYPE_REPLICA;
    if (c->flag.pubsub) return CLIENT_TYPE_PUBSUB;
    return CLIENT_TYPE_NORMAL;
}

int getClientTypeByName(char *name) {
    if (!strcasecmp(name, "normal"))
        return CLIENT_TYPE_NORMAL;
    else if (!strcasecmp(name, "slave"))
        return CLIENT_TYPE_REPLICA;
    else if (!strcasecmp(name, "replica"))
        return CLIENT_TYPE_REPLICA;
    else if (!strcasecmp(name, "pubsub"))
        return CLIENT_TYPE_PUBSUB;
    else if (!strcasecmp(name, "master") || !strcasecmp(name, "primary"))
        return CLIENT_TYPE_PRIMARY;
    else
        return -1;
}

char *getClientTypeName(int class) {
    switch (class) {
    case CLIENT_TYPE_NORMAL: return "normal";
    case CLIENT_TYPE_REPLICA: return "slave";
    case CLIENT_TYPE_PUBSUB: return "pubsub";
    case CLIENT_TYPE_PRIMARY: return "master";
    default: return NULL;
    }
}

/* The function checks if the client reached output buffer soft or hard
 * limit, and also update the state needed to check the soft limit as
 * a side effect.
 *
 * Return value: non-zero if the client reached the soft or the hard limit.
 *               Otherwise zero is returned. */
int checkClientOutputBufferLimits(client *c) {
    int soft = 0, hard = 0, class;
    unsigned long used_mem = getClientOutputBufferMemoryUsage(c);

    class = getClientType(c);
    /* For the purpose of output buffer limiting, primaries are handled
     * like normal clients. */
    if (class == CLIENT_TYPE_PRIMARY) class = CLIENT_TYPE_NORMAL;

    /* Note that it doesn't make sense to set the replica clients output buffer
     * limit lower than the repl-backlog-size config (partial sync will succeed
     * and then replica will get disconnected).
     * Such a configuration is ignored (the size of repl-backlog-size will be used).
     * This doesn't have memory consumption implications since the replica client
     * will share the backlog buffers memory. */
    size_t hard_limit_bytes = server.client_obuf_limits[class].hard_limit_bytes;
    if (class == CLIENT_TYPE_REPLICA && hard_limit_bytes && (long long)hard_limit_bytes < server.repl_backlog_size)
        hard_limit_bytes = server.repl_backlog_size;
    if (server.client_obuf_limits[class].hard_limit_bytes && used_mem >= hard_limit_bytes) hard = 1;
    if (server.client_obuf_limits[class].soft_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].soft_limit_bytes)
        soft = 1;

    /* We need to check if the soft limit is reached continuously for the
     * specified amount of seconds. */
    if (soft) {
        if (c->obuf_soft_limit_reached_time == 0) {
            c->obuf_soft_limit_reached_time = server.unixtime;
            soft = 0; /* First time we see the soft limit reached */
        } else {
            time_t elapsed = server.unixtime - c->obuf_soft_limit_reached_time;

            if (elapsed <= server.client_obuf_limits[class].soft_limit_seconds) {
                soft = 0; /* The client still did not reached the max number of
                             seconds for the soft limit to be considered
                             reached. */
            }
        }
    } else {
        c->obuf_soft_limit_reached_time = 0;
    }
    return soft || hard;
}

/* Asynchronously close a client if soft or hard limit is reached on the
 * output buffer size. The caller can check if the client will be closed
 * checking if the client CLIENT_CLOSE_ASAP flag is set.
 *
 * Note: we need to close the client asynchronously because this function is
 * called from contexts where the client can't be freed safely, i.e. from the
 * lower level functions pushing data inside the client output buffers.
 * When `async` is set to 0, we close the client immediately, this is
 * useful when called from cron.
 *
 * Returns 1 if client was (flagged) closed. */
int closeClientOnOutputBufferLimitReached(client *c, int async) {
    if (c->flag.fake) return 0; /* It is unsafe to free fake clients. */
    serverAssert(c->conn);
    serverAssert(c->reply_bytes < SIZE_MAX - (1024 * 64));
    /* Note that c->reply_bytes is irrelevant for replica clients
     * (they use the global repl buffers). */
    if ((c->reply_bytes == 0 && getClientType(c) != CLIENT_TYPE_REPLICA) ||
        (c->flag.close_asap && !(c->flag.protected_rdb_channel)))
        return 0;
    if (checkClientOutputBufferLimits(c)) {
        sds client = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);
        /* Remove RDB connection protection on COB overrun */

        if (async || c->flag.protected_rdb_channel) {
            c->flag.protected_rdb_channel = 0;
            freeClientAsync(c);
            serverLog(LL_WARNING, "Client %s scheduled to be closed ASAP for overcoming of output buffer limits.",
                      client);
        } else {
            freeClient(c);
            serverLog(LL_WARNING, "Client %s closed for overcoming of output buffer limits.", client);
        }
        sdsfree(client);
        server.stat_client_outbuf_limit_disconnections++;
        return 1;
    }
    return 0;
}

/* Helper function used by performEvictions() in order to flush replicas
 * output buffers without returning control to the event loop.
 * This is also called by SHUTDOWN for a best-effort attempt to send
 * replicas the latest writes. */
void flushReplicasOutputBuffers(void) {
    listIter li;
    listNode *ln;

    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = listNodeValue(ln);
        int can_receive_writes = connHasWriteHandler(replica->conn) || (replica->flag.pending_write);

        /* We don't want to send the pending data to the replica in a few
         * cases:
         *
         * 1. For some reason there is neither the write handler installed
         *    nor the client is flagged as to have pending writes: for some
         *    reason this replica may not be set to receive data. This is
         *    just for the sake of defensive programming.
         *
         * 2. The put_online_on_ack flag is true. To know why we don't want
         *    to send data to the replica in this case, please grep for the
         *    flag for this flag.
         *
         * 3. Obviously if the replica is not ONLINE.
         */
        if (isReplicaReadyForReplData(replica) && !(replica->flag.close_asap) && can_receive_writes &&
            !replica->repl_data->repl_start_cmd_stream_on_ack && clientHasPendingReplies(replica)) {
            writeToClient(replica);
        }
    }
}

mstime_t getPausedActionTimeout(uint32_t action) {
    mstime_t timeout = 0;
    for (int i = 0; i < NUM_PAUSE_PURPOSES; i++) {
        pause_event *p = &(server.client_pause_per_purpose[i]);
        if (p->paused_actions & action && (p->end - server.mstime) > timeout)
            timeout = p->end - server.mstime;
    }
    return timeout;
}

/* Compute current paused actions and its end time, aggregated for
 * all pause purposes. */
void updatePausedActions(void) {
    uint32_t prev_paused_actions = server.paused_actions;
    server.paused_actions = 0;

    for (int i = 0; i < NUM_PAUSE_PURPOSES; i++) {
        pause_event *p = &(server.client_pause_per_purpose[i]);
        if (p->end > server.mstime)
            server.paused_actions |= p->paused_actions;
        else {
            p->paused_actions = 0;
            p->end = 0;
        }
    }

    /* If the pause type is less restrictive than before, we unblock all clients
     * so they are reprocessed (may get re-paused). */
    uint32_t mask_cli = (PAUSE_ACTION_CLIENT_WRITE | PAUSE_ACTION_CLIENT_ALL);
    if ((server.paused_actions & mask_cli) < (prev_paused_actions & mask_cli)) {
        unblockPostponedClients();
    }
}

/* Unblock all paused clients (ones that where blocked by BLOCKED_POSTPONE (possibly in processCommand).
 * This means they'll get re-processed in beforeSleep, and may get paused again if needed. */
void unblockPostponedClients(void) {
    listNode *ln;
    listIter li;
    listRewind(server.postponed_clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);
        unblockClient(c, 1);
    }
}

/* Set pause-client end-time and restricted action. If already paused, then:
 * 1. Keep higher end-time value between configured and the new one
 * 2. Keep most restrictive action between configured and the new one */
static void pauseClientsByClient(mstime_t endTime, int isPauseClientAll) {
    uint32_t actions;
    pause_event *p = &server.client_pause_per_purpose[PAUSE_BY_CLIENT_COMMAND];

    if (isPauseClientAll)
        actions = PAUSE_ACTIONS_CLIENT_ALL_SET;
    else {
        actions = PAUSE_ACTIONS_CLIENT_WRITE_SET;
        /* If currently configured most restrictive client pause, then keep it */
        if (p->paused_actions & PAUSE_ACTION_CLIENT_ALL) actions = PAUSE_ACTIONS_CLIENT_ALL_SET;
    }

    pauseActions(PAUSE_BY_CLIENT_COMMAND, endTime, actions);
}

/* Pause actions up to the specified unixtime (in ms) for a given type of
 * purpose.
 *
 * A main use case of this function is to allow pausing replication traffic
 * so that a failover without data loss to occur. Replicas will continue to receive
 * traffic to facilitate this functionality.
 *
 * This function is also internally used by Cluster for the manual
 * failover procedure implemented by CLUSTER FAILOVER.
 *
 * The function always succeed, even if there is already a pause in progress.
 * The new paused_actions of a given 'purpose' will override the old ones and
 * end time will be updated if new end time is bigger than currently configured */
void pauseActions(pause_purpose purpose, mstime_t end, uint32_t actions) {
    /* Manage pause type and end time per pause purpose. */
    server.client_pause_per_purpose[purpose].paused_actions = actions;

    /* If currently configured end time bigger than new one, then keep it */
    if (server.client_pause_per_purpose[purpose].end < end) server.client_pause_per_purpose[purpose].end = end;

    updatePausedActions();

    /* We allow write commands that were queued
     * up before and after to execute. We need
     * to track this state so that we don't assert
     * in propagateNow(). */
    if (server.in_exec) {
        server.client_pause_in_transaction = 1;
    }
}

/* Unpause actions and queue them for reprocessing. */
void unpauseActions(pause_purpose purpose) {
    server.client_pause_per_purpose[purpose].end = 0;
    server.client_pause_per_purpose[purpose].paused_actions = 0;
    updatePausedActions();
}

/* Returns bitmask of paused actions */
uint32_t isPausedActions(uint32_t actions_bitmask) {
    return (server.paused_actions & actions_bitmask);
}

/* Returns bitmask of paused actions */
uint32_t isPausedActionsWithUpdate(uint32_t actions_bitmask) {
    if (!(server.paused_actions & actions_bitmask)) return 0;
    updatePausedActions();
    return (server.paused_actions & actions_bitmask);
}

/* This function is called by the server in order to process a few events from
 * time to time while blocked into some not interruptible operation.
 * This allows to reply to clients with the -LOADING error while loading the
 * data set at startup or after a full resynchronization with the primary
 * and so forth.
 *
 * It calls the event loop in order to process a few events. Specifically we
 * try to call the event loop 4 times as long as we receive acknowledge that
 * some event was processed, in order to go forward with the accept, read,
 * write, close sequence needed to serve a client.
 *
 * The function returns the total number of events processed. */
void processEventsWhileBlocked(void) {
    int iterations = 4; /* See the function top-comment. */

    /* Update our cached time since it is used to create and update the last
     * interaction time with clients and for other important things. */
    updateCachedTime(0);

    /* For the few commands that are allowed during busy scripts, we rather
     * provide a fresher time than the one from when the script started (they
     * still won't get it from the call due to execution_nesting. For commands
     * during loading this doesn't matter. */
    mstime_t prev_cmd_time_snapshot = server.cmd_time_snapshot;
    server.cmd_time_snapshot = server.mstime;

    /* Note: when we are processing events while blocked (for instance during
     * busy Lua scripts), we set a global flag. When such flag is set, we
     * avoid handling the read part of clients using threaded I/O.
     * See https://github.com/redis/redis/issues/6988 for more info.
     * Note that there could be cases of nested calls to this function,
     * specifically on a busy script during async_loading rdb, and scripts
     * that came from AOF. */
    ProcessingEventsWhileBlocked++;
    while (iterations--) {
        long long startval = server.events_processed_while_blocked;
        long long ae_events =
            aeProcessEvents(server.el, AE_FILE_EVENTS | AE_DONT_WAIT | AE_CALL_BEFORE_SLEEP | AE_CALL_AFTER_SLEEP);
        /* Note that server.events_processed_while_blocked will also get
         * incremented by callbacks called by the event loop handlers. */
        server.events_processed_while_blocked += ae_events;
        long long events = server.events_processed_while_blocked - startval;
        if (!events) break;
    }

    whileBlockedCron();

    ProcessingEventsWhileBlocked--;
    serverAssert(ProcessingEventsWhileBlocked >= 0);

    server.cmd_time_snapshot = prev_cmd_time_snapshot;
}

/* Return 1 if the client read is handled using threaded I/O.
 * 0 otherwise. */
int postponeClientRead(client *c) {
    if (ProcessingEventsWhileBlocked) return 0;

    return (trySendReadToIOThreads(c) == C_OK);
}

int processIOThreadsReadDone(void) {
    if (ProcessingEventsWhileBlocked) {
        /* When ProcessingEventsWhileBlocked we may call processIOThreadsReadDone recursively.
         * In this case, there may be some clients left in the batch waiting to be processed. */
        processClientsCommandsBatch();
    }

    if (listLength(server.clients_pending_io_read) == 0) return 0;
    int processed = 0;
    listNode *ln;

    listNode *next = listFirst(server.clients_pending_io_read);
    while (next) {
        ln = next;
        next = listNextNode(ln);
        client *c = listNodeValue(ln);

        /* Client is still waiting for a pending I/O - skip it */
        if (c->io_write_state == CLIENT_PENDING_IO || c->io_read_state == CLIENT_PENDING_IO) continue;
        /* If the write job is done, process it ASAP to free the buffer and handle connection errors */
        if (c->io_write_state == CLIENT_COMPLETED_IO) {
            int allow_async_writes = 0; /* Don't send writes for the client to IO threads before processing the reads */
            processClientIOWriteDone(c, allow_async_writes);
        }
        /* memory barrier acquire to get the updated client state */
        atomic_thread_fence(memory_order_acquire);

        listUnlinkNode(server.clients_pending_io_read, ln);
        c->flag.pending_read = 0;
        c->io_read_state = CLIENT_IDLE;

        /* Don't post-process-reads from clients that are going to be closed anyway. */
        if (c->flag.close_asap) continue;

        /* If a client is protected, don't do anything,
         * that may trigger read/write error or recreate handler. */
        if (c->flag.protected) continue;

        processed++;
        server.stat_io_reads_processed++;

        /* Save the current conn state, as connUpdateState may modify it */
        int in_accept_state = (connGetState(c->conn) == CONN_STATE_ACCEPTING);
        connSetPostponeUpdateState(c->conn, 0);
        connUpdateState(c->conn);

        /* In accept state, no client's data was read - stop here. */
        if (in_accept_state) continue;

        /* On read error - stop here. */
        if (handleReadResult(c) == C_ERR) {
            continue;
        }

        if (!(c->read_flags & READ_FLAGS_DONT_PARSE)) {
            parseResult res = handleParseResults(c);
            /* On parse error - stop here. */
            if (res == PARSE_ERR) {
                continue;
            } else if (res == PARSE_NEEDMORE) {
                beforeNextClient(c);
                continue;
            }
        }

        if (c->argc > 0) {
            c->flag.pending_command = 1;
        }

        size_t list_length_before_command_execute = listLength(server.clients_pending_io_read);
        /* try to add the command to the batch */
        int ret = addCommandToBatchAndProcessIfFull(c);
        /* If the command was not added to the commands batch, process it immediately */
        if (ret == C_ERR) {
            if (processPendingCommandAndInputBuffer(c) == C_OK) beforeNextClient(c);
        }
        if (list_length_before_command_execute != listLength(server.clients_pending_io_read)) {
            /* A client was unlink from the list possibly making the next node invalid */
            next = listFirst(server.clients_pending_io_read);
        }
    }

    processClientsCommandsBatch();

    return processed;
}

/* Returns the actual client eviction limit based on current configuration or
 * 0 if no limit. */
size_t getClientEvictionLimit(void) {
    size_t maxmemory_clients_actual = SIZE_MAX;

    /* Handle percentage of maxmemory*/
    if (server.maxmemory_clients < 0 && server.maxmemory > 0) {
        unsigned long long maxmemory_clients_bytes =
            (unsigned long long)((double)server.maxmemory * -(double)server.maxmemory_clients / 100);
        if (maxmemory_clients_bytes <= SIZE_MAX) maxmemory_clients_actual = maxmemory_clients_bytes;
    } else if (server.maxmemory_clients > 0)
        maxmemory_clients_actual = server.maxmemory_clients;
    else
        return 0;

    /* Don't allow a too small maxmemory-clients to avoid cases where we can't communicate
     * at all with the server because of bad configuration */
    if (maxmemory_clients_actual < 1024 * 128) maxmemory_clients_actual = 1024 * 128;

    return maxmemory_clients_actual;
}

void evictClients(void) {
    if (!server.client_mem_usage_buckets) return;
    /* Start eviction from topmost bucket (largest clients) */
    int curr_bucket = CLIENT_MEM_USAGE_BUCKETS - 1;
    listIter bucket_iter;
    listRewind(server.client_mem_usage_buckets[curr_bucket].clients, &bucket_iter);
    size_t client_eviction_limit = getClientEvictionLimit();
    if (client_eviction_limit == 0) return;
    while (server.stat_clients_type_memory[CLIENT_TYPE_NORMAL] + server.stat_clients_type_memory[CLIENT_TYPE_PUBSUB] >=
           client_eviction_limit) {
        listNode *ln = listNext(&bucket_iter);
        if (ln) {
            client *c = ln->value;
            sds ci = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);
            serverLog(LL_NOTICE, "Evicting client: %s", ci);
            freeClient(c);
            sdsfree(ci);
            server.stat_evictedclients++;
        } else {
            curr_bucket--;
            if (curr_bucket < 0) {
                serverLog(LL_WARNING, "Over client maxmemory after evicting all evictable clients");
                break;
            }
            listRewind(server.client_mem_usage_buckets[curr_bucket].clients, &bucket_iter);
        }
    }
}

/* IO threads functions */

void ioThreadReadQueryFromClient(void *data) {
    client *c = data;
    serverAssert(c->io_read_state == CLIENT_PENDING_IO);

    /* Read */
    readToQueryBuf(c);

    /* Check for read errors. */
    if (c->nread <= 0) {
        goto done;
    }

    /* Skip command parsing if the READ_FLAGS_DONT_PARSE flag is set. */
    if (c->read_flags & READ_FLAGS_DONT_PARSE) {
        goto done;
    }

    /* Handle QB limit */
    if (c->read_flags & READ_FLAGS_QB_LIMIT_REACHED) {
        goto done;
    }

    parseCommand(c);

    /* Parsing was not completed - let the main-thread handle it. */
    if (!(c->read_flags & READ_FLAGS_PARSING_COMPLETED)) {
        goto done;
    }

    /* Empty command - Multibulk processing could see a <= 0 length. */
    if (c->argc == 0) {
        goto done;
    }

    /* Lookup command offload */
    c->io_parsed_cmd = lookupCommand(c->argv, c->argc);
    if (c->io_parsed_cmd && commandCheckArity(c->io_parsed_cmd, c->argc, NULL) == 0) {
        /* The command was found, but the arity is invalid.
         * In this case, we reset the parsed_cmd and will let the main thread handle it. */
        c->io_parsed_cmd = NULL;
    }

    /* Offload slot calculations to the I/O thread to reduce main-thread load. */
    if (c->io_parsed_cmd && server.cluster_enabled) {
        getKeysResult result;
        initGetKeysResult(&result);
        int numkeys = getKeysFromCommand(c->io_parsed_cmd, c->argv, c->argc, &result);
        if (numkeys) {
            robj *first_key = c->argv[result.keys[0].pos];
            c->slot = keyHashSlot(first_key->ptr, sdslen(first_key->ptr));
        }
        getKeysFreeResult(&result);
    }

done:
    /* Only trim query buffer for non-primary clients
     * Primary client's buffer is handled by main thread using repl_applied position */
    if (!(c->read_flags & READ_FLAGS_PRIMARY)) {
        trimClientQueryBuffer(c);
    }
    atomic_thread_fence(memory_order_release);
    c->io_read_state = CLIENT_COMPLETED_IO;
}

void ioThreadWriteToClient(void *data) {
    client *c = data;
    serverAssert(c->io_write_state == CLIENT_PENDING_IO);
    c->nwritten = 0;
    _writeToClient(c);
    atomic_thread_fence(memory_order_release);
    c->io_write_state = CLIENT_COMPLETED_IO;
}
