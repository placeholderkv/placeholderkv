/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "io_threads.h"

static __thread int thread_id = 0; /* Thread local var */
static pthread_t io_threads[IO_THREADS_MAX_NUM] = {0};
static pthread_mutex_t io_threads_mutex[IO_THREADS_MAX_NUM];

/* IO jobs queue functions - Used to send jobs from the main-thread to the IO thread. */
typedef void (*job_handler)(void *);
typedef struct iojob {
    job_handler handler;
    void *data;
} iojob;

typedef struct IOJobQueue {
    iojob *ring_buffer;
    size_t size;
    _Atomic size_t head __attribute__((aligned(CACHE_LINE_SIZE))); /* Next write index for producer (main-thread) */
    _Atomic size_t tail __attribute__((aligned(CACHE_LINE_SIZE))); /* Next read index for consumer  (IO-thread) */
} IOJobQueue;
IOJobQueue io_jobs[IO_THREADS_MAX_NUM] = {0};

/* Initialize the job queue with a specified number of items. */
static void IOJobQueue_init(IOJobQueue *jq, size_t item_count) {
    debugServerAssertWithInfo(NULL, NULL, inMainThread());
    jq->ring_buffer = zcalloc(item_count * sizeof(iojob));
    jq->size = item_count; /* Total number of items */
    jq->head = 0;
    jq->tail = 0;
}

/* Clean up the job queue and free allocated memory. */
static void IOJobQueue_cleanup(IOJobQueue *jq) {
    debugServerAssertWithInfo(NULL, NULL, inMainThread());
    zfree(jq->ring_buffer);
    memset(jq, 0, sizeof(*jq));
}

static int IOJobQueue_isFull(const IOJobQueue *jq) {
    debugServerAssertWithInfo(NULL, NULL, inMainThread());
    size_t current_head = atomic_load_explicit(&jq->head, memory_order_relaxed);
    /* We don't use memory_order_acquire for the tail due to performance reasons,
     * In the worst case we will just assume wrongly the buffer is full and the main thread will do the job by itself. */
    size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);
    size_t next_head = (current_head + 1) % jq->size;
    return next_head == current_tail;
}

/* Attempt to push a new job to the queue from the main thread.
 * the caller must ensure the queue is not full before calling this function. */
static void IOJobQueue_push(IOJobQueue *jq, job_handler handler, void *data) {
    debugServerAssertWithInfo(NULL, NULL, inMainThread());
    /* Assert the queue is not full - should not happen as the caller should check for it before. */
    serverAssert(!IOJobQueue_isFull(jq));

    /* No need to use atomic acquire for the head, as the main thread is the only one that writes to the head index. */
    size_t current_head = atomic_load_explicit(&jq->head, memory_order_relaxed);
    size_t next_head = (current_head + 1) % jq->size;

    /* We store directly the job's fields to avoid allocating a new iojob structure. */
    serverAssert(jq->ring_buffer[current_head].data == NULL);
    serverAssert(jq->ring_buffer[current_head].handler == NULL);
    jq->ring_buffer[current_head].data = data;
    jq->ring_buffer[current_head].handler = handler;

    /* memory_order_release to make sure the data is visible to the consumer (the IO thread). */
    atomic_store_explicit(&jq->head, next_head, memory_order_release);
}

/* Returns the number of jobs currently available for consumption in the given job queue.
 *
 * This function  ensures memory visibility for the jobs by
 * using a memory acquire fence when there are jobs available. */
static size_t IOJobQueue_availableJobs(const IOJobQueue *jq) {
    debugServerAssertWithInfo(NULL, NULL, !inMainThread());
    /* We use memory_order_acquire to make sure the head and the job's fields are visible to the consumer (IO thread). */
    size_t current_head = atomic_load_explicit(&jq->head, memory_order_acquire);
    size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);

    if (current_head >= current_tail) {
        return current_head - current_tail;
    } else {
        return jq->size - (current_tail - current_head);
    }
}

/* Checks if the job Queue is empty.
 * returns 1 if the buffer is currently empty, 0 otherwise.
 * Called by the main-thread only.
 * This function uses relaxed memory order, so the caller need to use an acquire
 * memory fence before calling this function to be sure it has the latest index
 * from the other thread, especially when called repeatedly. */
static int IOJobQueue_isEmpty(const IOJobQueue *jq) {
    size_t current_head = atomic_load_explicit(&jq->head, memory_order_relaxed);
    size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);
    return current_head == current_tail;
}

/* Removes the next job from the given job queue by advancing the tail index.
 * Called by the IO thread.
 * The caller must ensure that the queue is not empty before calling this function.
 * This function uses relaxed memory order, so the caller need to use an release memory fence
 * after calling this function to make sure the updated tail is visible to the producer (main thread). */
static void IOJobQueue_removeJob(IOJobQueue *jq) {
    debugServerAssertWithInfo(NULL, NULL, !inMainThread());
    size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);
    jq->ring_buffer[current_tail].data = NULL;
    jq->ring_buffer[current_tail].handler = NULL;
    atomic_store_explicit(&jq->tail, (current_tail + 1) % jq->size, memory_order_relaxed);
}

/* Retrieves the next job handler and data from the job queue without removal.
 * Called by the consumer (IO thread). Caller must ensure queue is not empty.*/
static void IOJobQueue_peek(const IOJobQueue *jq, job_handler *handler, void **data) {
    debugServerAssertWithInfo(NULL, NULL, !inMainThread());
    size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);
    iojob *job = &jq->ring_buffer[current_tail];
    *handler = job->handler;
    *data = job->data;
}

/* End of IO job queue functions */

int inMainThread(void) {
    return thread_id == 0;
}

int getIOThreadID(void) {
    return thread_id;
}

/* Drains the I/O threads queue by waiting for all jobs to be processed.
 * This function must be called from the main thread. */
void drainIOThreadsQueue(void) {
    serverAssert(inMainThread());
    for (int i = 1; i < IO_THREADS_MAX_NUM; i++) { /* No need to drain thread 0, which is the main thread. */
        while (!IOJobQueue_isEmpty(&io_jobs[i])) {
            /* memory barrier acquire to get the latest job queue state */
            atomic_thread_fence(memory_order_acquire);
        }
    }
}

/* Wait until the IO-thread is done with the client */
void waitForClientIO(client *c) {
    /* No need to wait if the client was not offloaded to the IO thread. */
    if (c->io_read_state == CLIENT_IDLE && c->io_write_state == CLIENT_IDLE) return;

    /* Wait for read operation to complete if pending. */
    while (c->io_read_state == CLIENT_PENDING_IO) {
        atomic_thread_fence(memory_order_acquire);
    }

    /* Wait for write operation to complete if pending. */
    while (c->io_write_state == CLIENT_PENDING_IO) {
        atomic_thread_fence(memory_order_acquire);
    }

    /* Final memory barrier to ensure all changes are visible */
    atomic_thread_fence(memory_order_acquire);
}

/** Adjusts the number of active I/O threads based on the current event load.
 * If increase_only is non-zero, only allows increasing the number of threads.*/
void adjustIOThreadsByEventLoad(int numevents, int increase_only) {
    if (server.io_threads_num == 1) return; /* All I/O is being done by the main thread. */
    debugServerAssertWithInfo(NULL, NULL, server.io_threads_num > 1);
    /* When events_per_io_thread is set to 0, we offload all events to the IO threads.
     * This is used mainly for testing purposes. */
    int target_threads = server.events_per_io_thread == 0 ? (numevents + 1) : numevents / server.events_per_io_thread;

    target_threads = max(1, min(target_threads, server.io_threads_num));

    if (target_threads == server.active_io_threads_num) return;

    if (target_threads < server.active_io_threads_num) {
        if (increase_only) return;

        int threads_to_deactivate_num = server.active_io_threads_num - target_threads;
        for (int i = 0; i < threads_to_deactivate_num; i++) {
            int tid = server.active_io_threads_num - 1;
            IOJobQueue *jq = &io_jobs[tid];
            /* We can't lock the thread if it may have pending jobs */
            if (!IOJobQueue_isEmpty(jq)) return;
            pthread_mutex_lock(&io_threads_mutex[tid]);
            server.active_io_threads_num--;
        }
    } else {
        int threads_to_activate_num = target_threads - server.active_io_threads_num;
        for (int i = 0; i < threads_to_activate_num; i++) {
            pthread_mutex_unlock(&io_threads_mutex[server.active_io_threads_num]);
            server.active_io_threads_num++;
        }
    }
}

/* This function performs polling on the given event loop and updates the server's
 * IO fired events count and poll state. */
void IOThreadPoll(void *data) {
    aeEventLoop *el = (aeEventLoop *)data;
    struct timeval tvp = {0, 0};
    int num_events = aePoll(el, &tvp);

    server.io_ae_fired_events = num_events;
    atomic_store_explicit(&server.io_poll_state, AE_IO_STATE_DONE, memory_order_release);
}

static void *IOThreadMain(void *myid) {
    /* The ID is the thread ID number (from 1 to server.io_threads_num-1). ID 0 is the main thread. */
    long id = (long)myid;
    char thdname[32];

    serverAssert(server.io_threads_num > 0);
    serverAssert(id > 0 && id < server.io_threads_num);
    snprintf(thdname, sizeof(thdname), "io_thd_%ld", id);
    valkey_set_thread_title(thdname);
    serverSetCpuAffinity(server.server_cpulist);
    makeThreadKillable();
    initSharedQueryBuf();

    thread_id = (int)id;
    size_t jobs_to_process = 0;
    IOJobQueue *jq = &io_jobs[id];
    while (1) {
        /* Wait for jobs */
        for (int j = 0; j < 1000000; j++) {
            jobs_to_process = IOJobQueue_availableJobs(jq);
            if (jobs_to_process) break;
        }

        /* Give the main thread a chance to stop this thread. */
        if (jobs_to_process == 0) {
            pthread_mutex_lock(&io_threads_mutex[id]);
            pthread_mutex_unlock(&io_threads_mutex[id]);
            continue;
        }

        for (size_t j = 0; j < jobs_to_process; j++) {
            job_handler handler;
            void *data;
            /* We keep the job in the queue until it's processed. This ensures that if the main thread checks
             * and finds the queue empty, it can be certain that the IO thread is not currently handling any job. */
            IOJobQueue_peek(jq, &handler, &data);
            handler(data);
            /* Remove the job after it was processed */
            IOJobQueue_removeJob(jq);
        }
        /* Memory barrier to make sure the main thread sees the updated tail index.
         * We do it once per loop and not per tail-update for optimization reasons.
         * As the main-thread main concern is to check if the queue is empty, it's enough to do it once at the end. */
        atomic_thread_fence(memory_order_release);
    }
    freeSharedQueryBuf();
    return NULL;
}

#define IO_JOB_QUEUE_SIZE 2048
static void createIOThread(int id) {
    pthread_t tid;
    pthread_mutex_init(&io_threads_mutex[id], NULL);
    IOJobQueue_init(&io_jobs[id], IO_JOB_QUEUE_SIZE);
    pthread_mutex_lock(&io_threads_mutex[id]); /* Thread will be stopped. */
    if (pthread_create(&tid, NULL, IOThreadMain, (void *)(long)id) != 0) {
        serverLog(LL_WARNING, "Fatal: Can't initialize IO thread, pthread_create failed with: %s", strerror(errno));
        exit(1);
    }
    io_threads[id] = tid;
}

/* Terminates the IO thread specified by id.
 * Called on server shutdown */
static void shutdownIOThread(int id) {
    int err;
    pthread_t tid = io_threads[id];
    if (tid == pthread_self()) return;
    if (tid == 0) return;

    pthread_cancel(tid);

    if ((err = pthread_join(tid, NULL)) != 0) {
        serverLog(LL_WARNING, "IO thread(tid:%lu) can not be joined: %s", (unsigned long)tid, strerror(err));
    } else {
        serverLog(LL_NOTICE, "IO thread(tid:%lu) terminated", (unsigned long)tid);
    }

    IOJobQueue_cleanup(&io_jobs[id]);
}

void killIOThreads(void) {
    for (int j = 1; j < server.io_threads_num; j++) { /* We don't kill thread 0, which is the main thread. */
        shutdownIOThread(j);
    }
}

/* Initialize the data structures needed for I/O threads. */
void initIOThreads(void) {
    server.active_io_threads_num = 1; /* We start with threads not active. */
    server.io_poll_state = AE_IO_STATE_NONE;
    server.io_ae_fired_events = 0;

    /* Don't spawn any thread if the user selected a single thread:
     * we'll handle I/O directly from the main thread. */
    if (server.io_threads_num == 1) return;

    serverAssert(server.io_threads_num <= IO_THREADS_MAX_NUM);

    prefetchCommandsBatchInit();

    /* Spawn and initialize the I/O threads. */
    for (int i = 1; i < server.io_threads_num; i++) {
        createIOThread(i);
    }
}

int trySendReadToIOThreads(client *c) {
    if (server.active_io_threads_num <= 1) return C_ERR;
    /* If IO thread is already reading, return C_OK to make sure the main thread will not handle it. */
    if (c->io_read_state != CLIENT_IDLE) return C_OK;
    /* For simplicity, don't offload replica clients reads as read traffic from replica is negligible */
    if (getClientType(c) == CLIENT_TYPE_REPLICA) return C_ERR;
    /* With Lua debug client we may call connWrite directly in the main thread */
    if (c->flag.lua_debug) return C_ERR;
    /* For simplicity let the main-thread handle the blocked clients */
    if (c->flag.blocked || c->flag.unblocked) return C_ERR;
    if (c->flag.close_asap) return C_ERR;
    size_t tid = (c->id % (server.active_io_threads_num - 1)) + 1;

    /* Handle case where client has a pending IO write job on a different thread:
     * 1. A write job is still pending (io_write_state == CLIENT_PENDING_IO)
     * 2. The pending job is on a different thread (c->cur_tid != tid)
     *
     * This situation can occur if active_io_threads_num increased since the
     * original job assignment. In this case, we keep the job on its current
     * thread to ensure the same thread handles the client's I/O operations. */
    if (c->io_write_state == CLIENT_PENDING_IO && c->cur_tid != (uint8_t)tid) tid = c->cur_tid;

    IOJobQueue *jq = &io_jobs[tid];
    if (IOJobQueue_isFull(jq)) return C_ERR;

    c->cur_tid = tid;
    c->read_flags = canParseCommand(c) ? 0 : READ_FLAGS_DONT_PARSE;
    c->read_flags |= authRequired(c) ? READ_FLAGS_AUTH_REQUIRED : 0;
    c->read_flags |= c->flag.primary ? READ_FLAGS_PRIMARY : 0;

    c->io_read_state = CLIENT_PENDING_IO;
    connSetPostponeUpdateState(c->conn, 1);
    IOJobQueue_push(jq, ioThreadReadQueryFromClient, c);
    c->flag.pending_read = 1;
    listLinkNodeTail(server.clients_pending_io_read, &c->pending_read_list_node);
    return C_OK;
}

/* This function attempts to offload the client's write to an I/O thread.
 * Returns C_OK if the client's writes were successfully offloaded to an I/O thread,
 * or C_ERR if the client is not eligible for offloading. */
int trySendWriteToIOThreads(client *c) {
    if (server.active_io_threads_num <= 1) return C_ERR;
    /* The I/O thread is already writing for this client. */
    if (c->io_write_state != CLIENT_IDLE) return C_OK;
    /* Nothing to write */
    if (!clientHasPendingReplies(c)) return C_ERR;
    /* For simplicity, avoid offloading non-online replicas */
    if (getClientType(c) == CLIENT_TYPE_REPLICA && c->repl_data->repl_state != REPLICA_STATE_ONLINE) return C_ERR;
    /* We can't offload debugged clients as the main-thread may read at the same time  */
    if (c->flag.lua_debug) return C_ERR;

    size_t tid = (c->id % (server.active_io_threads_num - 1)) + 1;
    /* Handle case where client has a pending IO read job on a different thread:
     * 1. A read job is still pending (io_read_state == CLIENT_PENDING_IO)
     * 2. The pending job is on a different thread (c->cur_tid != tid)
     *
     * This situation can occur if active_io_threads_num increased since the
     * original job assignment. In this case, we keep the job on its current
     * thread to ensure the same thread handles the client's I/O operations. */
    if (c->io_read_state == CLIENT_PENDING_IO && c->cur_tid != (uint8_t)tid) tid = c->cur_tid;

    IOJobQueue *jq = &io_jobs[tid];
    if (IOJobQueue_isFull(jq)) return C_ERR;

    c->cur_tid = tid;
    if (c->flag.pending_write) {
        /* We move the client to the io pending write queue */
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
    } else {
        c->flag.pending_write = 1;
    }
    serverAssert(c->clients_pending_write_node.prev == NULL && c->clients_pending_write_node.next == NULL);
    listLinkNodeTail(server.clients_pending_io_write, &c->clients_pending_write_node);

    int is_replica = getClientType(c) == CLIENT_TYPE_REPLICA;
    if (is_replica) {
        c->io_last_reply_block = listLast(server.repl_buffer_blocks);
        replBufBlock *o = listNodeValue(c->io_last_reply_block);
        c->io_last_bufpos = o->used;
    } else {
        /* Save the last block of the reply list to io_last_reply_block and the used
         * position to io_last_bufpos. The I/O thread will write only up to
         * io_last_bufpos, regardless of the c->bufpos value. This is to prevent I/O
         * threads from reading data that might be invalid in their local CPU cache. */
        c->io_last_reply_block = listLast(c->reply);
        if (c->io_last_reply_block) {
            c->io_last_bufpos = ((clientReplyBlock *)listNodeValue(c->io_last_reply_block))->used;
        } else {
            c->io_last_bufpos = (size_t)c->bufpos;
        }
    }

    serverAssert(c->bufpos > 0 || c->io_last_bufpos > 0 || is_replica);

    /* The main-thread will update the client state after the I/O thread completes the write. */
    connSetPostponeUpdateState(c->conn, 1);
    c->write_flags = is_replica ? WRITE_FLAGS_IS_REPLICA : 0;
    c->io_write_state = CLIENT_PENDING_IO;

    IOJobQueue_push(jq, ioThreadWriteToClient, c);
    return C_OK;
}

/* Internal function to free the client's argv in an IO thread. */
void IOThreadFreeArgv(void *data) {
    robj **argv = (robj **)data;
    int last_arg = 0;
    for (int i = 0;; i++) {
        robj *o = argv[i];
        if (o == NULL) {
            continue;
        }

        /* The main-thread set the refcount to 0 to indicate that this is the last argument to free */
        if (o->refcount == 0) {
            last_arg = 1;
            o->refcount = 1;
        }

        decrRefCount(o);

        if (last_arg) {
            break;
        }
    }

    zfree(argv);
}

/* This function attempts to offload the client's argv to an IO thread.
 * Returns C_OK if the client's argv were successfully offloaded to an IO thread,
 * C_ERR otherwise. */
int tryOffloadFreeArgvToIOThreads(client *c, int argc, robj **argv) {
    if (server.active_io_threads_num <= 1 || argc == 0) {
        return C_ERR;
    }

    size_t tid = (c->id % (server.active_io_threads_num - 1)) + 1;

    IOJobQueue *jq = &io_jobs[tid];
    if (IOJobQueue_isFull(jq)) {
        return C_ERR;
    }

    int last_arg_to_free = -1;

    /* Prepare the argv */
    for (int j = 0; j < argc; j++) {
        if (argv[j]->refcount > 1) {
            decrRefCount(argv[j]);
            /* Set argv[j] to NULL to avoid double free */
            argv[j] = NULL;
        } else {
            last_arg_to_free = j;
        }
    }

    /* If no argv to free, free the argv array at the main thread */
    if (last_arg_to_free == -1) {
        zfree(argv);
        return C_OK;
    }

    /* We set the refcount of the last arg to free to 0 to indicate that
     * this is the last argument to free. With this approach, we don't need to
     * send the argc to the IO thread and we can send just the argv ptr. */
    argv[last_arg_to_free]->refcount = 0;

    /* Must succeed as we checked the free space before. */
    IOJobQueue_push(jq, IOThreadFreeArgv, argv);

    return C_OK;
}

/* This function attempts to offload the free of an object to an IO thread.
 * Returns C_OK if the object was successfully offloaded to an IO thread,
 * C_ERR otherwise.*/
int tryOffloadFreeObjToIOThreads(robj *obj) {
    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    if (obj->refcount > 1) return C_ERR;

    if (obj->encoding != OBJ_ENCODING_RAW || obj->type != OBJ_STRING) return C_ERR;

    /* We select the thread ID in a round-robin fashion. */
    size_t tid = (server.stat_io_freed_objects % (server.active_io_threads_num - 1)) + 1;

    IOJobQueue *jq = &io_jobs[tid];
    if (IOJobQueue_isFull(jq)) {
        return C_ERR;
    }

    /* We offload only the free of the ptr that may be allocated by the I/O thread.
     * The object itself was allocated by the main thread and will be freed by the main thread. */
    IOJobQueue_push(jq, sdsfreeVoid, obj->ptr);
    obj->ptr = NULL;
    decrRefCount(obj);

    server.stat_io_freed_objects++;
    return C_OK;
}

/* This function retrieves the results of the IO Thread poll.
 * returns the number of fired events if the IO thread has finished processing poll events, 0 otherwise. */
static int getIOThreadPollResults(aeEventLoop *eventLoop) {
    int io_state;
    io_state = atomic_load_explicit(&server.io_poll_state, memory_order_acquire);
    if (io_state == AE_IO_STATE_POLL) {
        /* IO thread is still processing poll events. */
        return 0;
    }

    /* IO thread is done processing poll events. */
    serverAssert(io_state == AE_IO_STATE_DONE);
    server.stat_poll_processed_by_io_threads++;
    server.io_poll_state = AE_IO_STATE_NONE;

    /* Remove the custom poll proc. */
    aeSetCustomPollProc(eventLoop, NULL);
    aeSetPollProtect(eventLoop, 0);
    return server.io_ae_fired_events;
}

void trySendPollJobToIOThreads(void) {
    if (server.active_io_threads_num <= 1) {
        return;
    }

    /* If there are no pending jobs, let the main thread do the poll-wait by itself. */
    if (listLength(server.clients_pending_io_write) + listLength(server.clients_pending_io_read) == 0) {
        return;
    }

    /* If the IO thread is already processing poll events, don't send another job. */
    if (server.io_poll_state != AE_IO_STATE_NONE) {
        return;
    }

    /* The poll is sent to the last thread. While a random thread could have been selected,
     * the last thread has a slightly better chance of being less loaded compared to other threads,
     * As we activate the lowest threads first. */
    int tid = server.active_io_threads_num - 1;
    IOJobQueue *jq = &io_jobs[tid];
    if (IOJobQueue_isFull(jq)) return; /* The main thread will handle the poll itself. */

    server.io_poll_state = AE_IO_STATE_POLL;
    aeSetCustomPollProc(server.el, getIOThreadPollResults);
    aeSetPollProtect(server.el, 1);
    IOJobQueue_push(jq, IOThreadPoll, server.el);
}

static void ioThreadAccept(void *data) {
    client *c = (client *)data;
    connAccept(c->conn, NULL);
    c->io_read_state = CLIENT_COMPLETED_IO;
}

/*
 * Attempts to offload an Accept operation (currently used for TLS accept) for a client
 * connection to I/O threads.
 *
 * Returns:
 *   C_OK  - If the accept operation was successfully queued for processing
 *   C_ERR - If the connection is not eligible for offloading
 *
 * Parameters:
 *   conn - The connection object to perform the accept operation on
 */
int trySendAcceptToIOThreads(connection *conn) {
    if (server.io_threads_num <= 1) {
        return C_ERR;
    }

    if (!(conn->flags & CONN_FLAG_ALLOW_ACCEPT_OFFLOAD)) {
        return C_ERR;
    }

    client *c = connGetPrivateData(conn);
    if (c->io_read_state != CLIENT_IDLE) {
        return C_OK;
    }

    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    size_t thread_id = (c->id % (server.active_io_threads_num - 1)) + 1;
    IOJobQueue *job_queue = &io_jobs[thread_id];

    if (IOJobQueue_isFull(job_queue)) {
        return C_ERR;
    }

    c->io_read_state = CLIENT_PENDING_IO;
    c->flag.pending_read = 1;
    listLinkNodeTail(server.clients_pending_io_read, &c->pending_read_list_node);
    connSetPostponeUpdateState(c->conn, 1);
    server.stat_io_accept_offloaded++;
    IOJobQueue_push(job_queue, ioThreadAccept, c);

    return C_OK;
}
