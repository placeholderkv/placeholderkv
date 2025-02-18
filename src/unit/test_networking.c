#include "../networking.c"
#include "../server.c"
#include "test_help.h"

#include <stdatomic.h>

/* Fake structures and functions */
typedef struct fakeConnection {
    connection conn;
    int error;
    char *buffer;
    size_t buf_size;
    size_t written;
} fakeConnection;

/* Fake connWrite function */
static int fake_connWrite(connection *conn, const void *data, size_t size) {
    fakeConnection *fake_conn = (fakeConnection *)conn;
    if (fake_conn->error) return -1;

    size_t to_write = size;
    if (fake_conn->written + to_write > fake_conn->buf_size) {
        to_write = fake_conn->buf_size - fake_conn->written;
    }

    memcpy(fake_conn->buffer + fake_conn->written, data, to_write);
    fake_conn->written += to_write;
    return to_write;
}

/* Fake connWritev function */
static int fake_connWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    fakeConnection *fake_conn = (fakeConnection *)conn;
    if (fake_conn->error) return -1;

    size_t total = 0;
    for (int i = 0; i < iovcnt; i++) {
        size_t to_write = iov[i].iov_len;
        if (fake_conn->written + to_write > fake_conn->buf_size) {
            to_write = fake_conn->buf_size - fake_conn->written;
        }
        if (to_write == 0) break;

        memcpy(fake_conn->buffer + fake_conn->written, iov[i].iov_base, to_write);
        fake_conn->written += to_write;
        total += to_write;
    }
    return total;
}

/* Fake connection type */
static ConnectionType CT_Fake = {
    .write = fake_connWrite,
    .writev = fake_connWritev,
};

static fakeConnection *connCreateFake(void) {
    fakeConnection *conn = zcalloc(sizeof(fakeConnection));
    conn->conn.type = &CT_Fake;
    conn->conn.fd = -1;
    conn->conn.iovcnt = IOV_MAX;
    return conn;
}

int test_writeToReplica(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    client *c = zcalloc(sizeof(client));
    initClientReplicationData(c);
    server.repl_buffer_blocks = listCreate();
    createReplicationBacklog();
    c->reply = listCreate();

    /* Test 1: Single block write */
    {
        fakeConnection *fake_conn = connCreateFake();
        fake_conn->buffer = zmalloc(1024);
        fake_conn->buf_size = 1024;
        c->conn = (connection *)fake_conn;

        /* Create replication buffer block */
        replBufBlock *block = zmalloc(sizeof(replBufBlock) + 128);
        block->size = 128;
        block->used = 64;
        memset(block->buf, 'A', 64);

        /* Setup client state */
        listAddNodeTail(server.repl_buffer_blocks, block);
        c->repl_data->ref_repl_buf_node = listFirst(server.repl_buffer_blocks);
        c->repl_data->ref_block_pos = 0;
        c->bufpos = 0;

        writeToReplica(c);

        TEST_ASSERT(c->nwritten == 64);
        TEST_ASSERT(fake_conn->written == 64);
        TEST_ASSERT(memcmp(fake_conn->buffer, block->buf, 64) == 0);
        TEST_ASSERT((c->write_flags & WRITE_FLAGS_WRITE_ERROR) == 0);

        /* Cleanup */
        zfree(fake_conn->buffer);
        zfree(fake_conn);
        zfree(block);
        listEmpty(server.repl_buffer_blocks);
    }

    /* Test 2: Multiple blocks write */
    {
        fakeConnection *fake_conn = connCreateFake();
        fake_conn->error = 0;
        fake_conn->written = 0;
        fake_conn->buffer = zmalloc(1024);
        fake_conn->buf_size = 1024;
        c->conn = (connection *)fake_conn;

        /* Create multiple replication buffer blocks */
        replBufBlock *block1 = zmalloc(sizeof(replBufBlock) + 128);
        replBufBlock *block2 = zmalloc(sizeof(replBufBlock) + 128);
        block1->size = 128;
        block1->used = 64;
        block2->size = 128;
        block2->used = 32;
        memset(block1->buf, 'A', 64);
        memset(block2->buf, 'B', 32);

        /* Setup client state */
        listAddNodeTail(server.repl_buffer_blocks, block1);
        listAddNodeTail(server.repl_buffer_blocks, block2);
        c->repl_data->ref_repl_buf_node = listFirst(server.repl_buffer_blocks);
        c->repl_data->ref_block_pos = 0;
        c->bufpos = 0;

        writeToReplica(c);

        TEST_ASSERT(c->nwritten == 96); /* 64 + 32 */
        TEST_ASSERT(fake_conn->written == 96);
        TEST_ASSERT(memcmp(fake_conn->buffer, block1->buf, 64) == 0);
        TEST_ASSERT(memcmp(fake_conn->buffer + 64, block2->buf, 32) == 0);
        TEST_ASSERT((c->write_flags & WRITE_FLAGS_WRITE_ERROR) == 0);

        /* Cleanup */
        zfree(fake_conn->buffer);
        zfree(fake_conn);
        zfree(block1);
        zfree(block2);
        listEmpty(server.repl_buffer_blocks);
    }

    /* Test 3: Write error */
    {
        fakeConnection *fake_conn = connCreateFake();
        fake_conn->error = 1; /* Simulate write error */
        fake_conn->buffer = zmalloc(1024);
        fake_conn->buf_size = 1024;
        fake_conn->written = 0;
        c->conn = (connection *)fake_conn;

        /* Create replication buffer block */
        replBufBlock *block = zmalloc(sizeof(replBufBlock) + 128);
        block->size = 128;
        block->used = 64;
        memset(block->buf, 'A', 64);

        /* Setup client state */
        listAddNodeTail(server.repl_buffer_blocks, block);
        block->refcount = 1;
        c->repl_data->ref_repl_buf_node = listFirst(server.repl_buffer_blocks);
        c->repl_data->ref_block_pos = 0;
        c->bufpos = 0;

        writeToReplica(c);

        TEST_ASSERT(c->nwritten <= 0);
        TEST_ASSERT((c->write_flags & WRITE_FLAGS_WRITE_ERROR) != 0);

        /* Cleanup */
        listEmpty(server.repl_buffer_blocks);
        zfree(fake_conn->buffer);
        zfree(fake_conn);
        zfree(block);
        c->repl_data->ref_repl_buf_node = NULL;
    }

    /* Cleanup */
    listRelease(server.repl_buffer_blocks);
    listRelease(c->reply);
    freeClientReplicationData(c);
    zfree(c);

    return 0;
}

int test_postWriteToReplica(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    client *c = zcalloc(sizeof(client));
    initClientReplicationData(c);
    server.repl_buffer_blocks = listCreate();
    c->reply = listCreate();

    /* Test 1: No write case */
    {
        c->nwritten = 0;
        server.stat_net_repl_output_bytes = 0;

        postWriteToReplica(c);

        TEST_ASSERT(server.stat_net_repl_output_bytes == 0);
    }

    /* Test 2: Single block partial write */
    {
        replBufBlock *block = zmalloc(sizeof(replBufBlock) + 128);
        block->size = 128;
        block->used = 100;
        block->refcount = 1;

        listAddNodeTail(server.repl_buffer_blocks, block);
        c->repl_data->ref_repl_buf_node = listFirst(server.repl_buffer_blocks);
        c->repl_data->ref_block_pos = 20;
        c->nwritten = 30;

        server.stat_net_repl_output_bytes = 0;

        postWriteToReplica(c);

        TEST_ASSERT(server.stat_net_repl_output_bytes == 30);
        TEST_ASSERT(c->repl_data->ref_block_pos == 50); /* 20 + 30 */
        TEST_ASSERT(c->repl_data->ref_repl_buf_node == listFirst(server.repl_buffer_blocks));
        TEST_ASSERT(block->refcount == 1);

        /* Cleanup */
        zfree(block);
        listEmpty(server.repl_buffer_blocks);
    }

    /* Test 3: Multiple blocks write */
    {
        replBufBlock *block1 = zmalloc(sizeof(replBufBlock) + 128);
        replBufBlock *block2 = zmalloc(sizeof(replBufBlock) + 128);
        block1->size = 128;
        block1->used = 64;
        block1->refcount = 1;
        block2->size = 128;
        block2->used = 100;
        block2->refcount = 0;

        listAddNodeTail(server.repl_buffer_blocks, block1);
        listAddNodeTail(server.repl_buffer_blocks, block2);
        c->repl_data->ref_repl_buf_node = listFirst(server.repl_buffer_blocks);
        c->repl_data->ref_block_pos = 30;
        c->nwritten = 50;

        server.stat_net_repl_output_bytes = 0;

        postWriteToReplica(c);

        TEST_ASSERT(server.stat_net_repl_output_bytes == 50);
        TEST_ASSERT(c->repl_data->ref_block_pos == 16); /* (30 + 50) - 64 */
        TEST_ASSERT(c->repl_data->ref_repl_buf_node == listLast(server.repl_buffer_blocks));
        TEST_ASSERT(block1->refcount == 0);
        TEST_ASSERT(block2->refcount == 1);

        /* Cleanup */
        zfree(block1);
        zfree(block2);
        listEmpty(server.repl_buffer_blocks);
    }

    /* Test 4: Write exactly to block boundary */
    {
        replBufBlock *block = zmalloc(sizeof(replBufBlock) + 128);
        block->size = 128;
        block->used = 64;
        block->refcount = 1;

        /* Setup client state */
        listAddNodeTail(server.repl_buffer_blocks, block);
        c->repl_data->ref_repl_buf_node = listFirst(server.repl_buffer_blocks);
        c->repl_data->ref_block_pos = 30;
        c->nwritten = 34; /* Should reach exactly the end of block */

        server.stat_net_repl_output_bytes = 0;

        postWriteToReplica(c);

        TEST_ASSERT(server.stat_net_repl_output_bytes == 34);
        TEST_ASSERT(c->repl_data->ref_block_pos == 64);
        TEST_ASSERT(c->repl_data->ref_repl_buf_node == listFirst(server.repl_buffer_blocks));
        TEST_ASSERT(block->refcount == 1); /* we don't free the last block even if it's fully written */

        /* Cleanup */
        zfree(block);
        c->repl_data->ref_repl_buf_node = NULL;
        listEmpty(server.repl_buffer_blocks);
    }

    /* Cleanup */
    freeClientReplicationData(c);
    raxFree(server.repl_backlog->blocks_index);
    zfree(server.repl_backlog);
    listRelease(server.repl_buffer_blocks);
    listRelease(c->reply);
    zfree(c);

    return 0;
}

int test_backupAndUpdateClientArgv(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    client *c = zmalloc(sizeof(client));

    /* Test 1: Initial backup of arguments */
    c->argc = 2;
    robj **initial_argv = zmalloc(sizeof(robj *) * 2);
    c->argv = initial_argv;
    c->argv[0] = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test"));
    c->argv[1] = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test2"));
    c->original_argv = NULL;

    backupAndUpdateClientArgv(c, 3, NULL);

    TEST_ASSERT(c->argv != initial_argv);
    TEST_ASSERT(c->original_argv == initial_argv);
    TEST_ASSERT(c->original_argc == 2);
    TEST_ASSERT(c->argc == 3);
    TEST_ASSERT(c->argv_len == 3);
    TEST_ASSERT(c->argv[0]->refcount == 2);
    TEST_ASSERT(c->argv[1]->refcount == 2);
    TEST_ASSERT(c->argv[2] == NULL);

    /* Test 2: Direct argv replacement */
    robj **new_argv = zmalloc(sizeof(robj *) * 2);
    new_argv[0] = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test"));
    new_argv[1] = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test2"));

    backupAndUpdateClientArgv(c, 2, new_argv);

    TEST_ASSERT(c->argv == new_argv);
    TEST_ASSERT(c->argc == 2);
    TEST_ASSERT(c->argv_len == 2);
    TEST_ASSERT(c->original_argv != c->argv);
    TEST_ASSERT(c->original_argv == initial_argv);
    TEST_ASSERT(c->original_argc == 2);
    TEST_ASSERT(c->original_argv[0]->refcount == 1);
    TEST_ASSERT(c->original_argv[1]->refcount == 1);

    /* Test 3: Expanding argc */
    backupAndUpdateClientArgv(c, 4, NULL);

    TEST_ASSERT(c->argc == 4);
    TEST_ASSERT(c->argv_len == 4);
    TEST_ASSERT(c->argv[0] != NULL);
    TEST_ASSERT(c->argv[1] != NULL);
    TEST_ASSERT(c->argv[2] == NULL);
    TEST_ASSERT(c->argv[3] == NULL);
    TEST_ASSERT(c->original_argv == initial_argv);

    /* Cleanup */
    for (int i = 0; i < c->original_argc; i++) {
        decrRefCount(c->original_argv[i]);
    }
    zfree(c->original_argv);

    for (int i = 0; i < c->argc; i++) {
        if (c->argv[i]) decrRefCount(c->argv[i]);
    }
    zfree(c->argv);
    zfree(c);

    return 0;
}

int test_rewriteClientCommandArgument(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    client *c = zmalloc(sizeof(client));
    c->argc = 3;
    robj **initial_argv = zmalloc(sizeof(robj *) * 3);
    c->argv = initial_argv;
    c->original_argv = NULL;
    c->argv_len_sum = 0;

    /* Initialize client with command "SET key value" */
    c->argv[0] = createStringObject("SET", 3);
    robj *original_key = createStringObject("key", 3);
    c->argv[1] = original_key;
    c->argv[2] = createStringObject("value", 5);
    c->argv_len_sum = 11; // 3 + 3 + 5

    /* Test 1: Rewrite existing argument */
    robj *newval = createStringObject("newkey", 6);
    rewriteClientCommandArgument(c, 1, newval);

    TEST_ASSERT(c->argv[1] == newval);
    TEST_ASSERT(c->argv[1]->refcount == 2);
    TEST_ASSERT(c->argv_len_sum == 14); // 3 + 6 + 5
    TEST_ASSERT(c->original_argv == initial_argv);
    TEST_ASSERT(c->original_argv[1] == original_key);
    TEST_ASSERT(c->original_argv[1]->refcount == 1);

    /* Test 3: Extend argument vector */
    robj *extraval = createStringObject("extra", 5);
    rewriteClientCommandArgument(c, 3, extraval);

    TEST_ASSERT(c->argc == 4);
    TEST_ASSERT(c->argv[3] == extraval);
    TEST_ASSERT(c->argv_len_sum == 19); // 3 + 6 + 5 + 5
    TEST_ASSERT(c->original_argv == initial_argv);

    /* Cleanup */
    for (int i = 0; i < c->argc; i++) {
        if (c->argv[i]) decrRefCount(c->argv[i]);
    }
    zfree(c->argv);

    for (int i = 0; i < c->original_argc; i++) {
        if (c->original_argv[i]) decrRefCount(c->original_argv[i]);
    }
    zfree(c->original_argv);

    decrRefCount(newval);
    decrRefCount(extraval);

    zfree(c);

    return 0;
}

static client *createTestClient(void) {
    client *c = zcalloc(sizeof(client));

    c->buf = zmalloc_usable(PROTO_REPLY_CHUNK_BYTES, &c->buf_usable_size);
    c->reply = listCreate();
    listSetFreeMethod(c->reply, freeClientReplyValue);
    listSetDupMethod(c->reply, dupClientReplyValue);
    /* dummy connection to bypass assert in closeClientOnOutputBufferLimitReached */
    c->conn = (connection *)c;

    return c;
}

static void freeReplyOffloadClient(client *c) {
    listRelease(c->reply);
    zfree(c->buf);
    zfree(c);
}

/* Each bulk offload puts 2 pointers to a reply buffer */
#define PTRS_LEN (sizeof(void *) * 2)

int test_addRepliesWithOffloadsToBuffer(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    client *c = createTestClient();

    /* Test 1:  Add bulk offloads to the buffer */
    robj *obj = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test"));
    _addBulkStrRefToBufferOrList(c, obj);

    TEST_ASSERT(obj->refcount == 2);
    TEST_ASSERT(c->bufpos == sizeof(payloadHeader) + PTRS_LEN);

    payloadHeader *header1 = c->last_header;
    TEST_ASSERT(header1->type == BULK_STR_REF);
    TEST_ASSERT(header1->len == PTRS_LEN);

    robj **ptr = (robj **)(c->buf + sizeof(payloadHeader));
    TEST_ASSERT(obj == *ptr);

    robj *obj2 = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test2"));
    _addBulkStrRefToBufferOrList(c, obj2);

    /* 2 offloads expected in c->buf */
    TEST_ASSERT(c->bufpos == sizeof(payloadHeader) + 2 * PTRS_LEN);
    TEST_ASSERT(header1->type == BULK_STR_REF);
    TEST_ASSERT(header1->len == 2 * PTRS_LEN);

    ptr = (robj **)(c->buf + sizeof(payloadHeader) + PTRS_LEN);
    TEST_ASSERT(obj2 == *ptr);

    /* Test 2:  Add plain reply to the buffer */
    const char *plain = "+OK\r\n";
    size_t plain_len = strlen(plain);
    _addReplyToBufferOrList(c, plain, plain_len);

    /* 2 offloads and plain reply expected in c->buf. So 2 headers expected as well */
    TEST_ASSERT(c->bufpos == 2 * sizeof(payloadHeader) + 2 * PTRS_LEN + plain_len);
    TEST_ASSERT(header1->type == BULK_STR_REF);
    TEST_ASSERT(header1->len == 2 * PTRS_LEN);
    payloadHeader *header2 = c->last_header;
    TEST_ASSERT(header2->type == PLAIN_REPLY);
    TEST_ASSERT(header2->len == plain_len);

    /* Add more plain replies. Check same plain reply header updated properly */
    for (int i = 0; i < 9; ++i) _addReplyToBufferOrList(c, plain, plain_len);
    TEST_ASSERT(c->bufpos == 2 * sizeof(payloadHeader) + 2 * PTRS_LEN + 10 * plain_len);
    TEST_ASSERT(header2->type == PLAIN_REPLY);
    TEST_ASSERT(header2->len == plain_len * 10);

    /* Test 3:  Add one more bulk offload to the buffer */
    _addBulkStrRefToBufferOrList(c, obj);
    TEST_ASSERT(obj->refcount == 3);
    TEST_ASSERT(c->bufpos == 3 * sizeof(payloadHeader) + 3 * PTRS_LEN + 10 * plain_len);
    payloadHeader *header3 = c->last_header;
    TEST_ASSERT(header3->type == BULK_STR_REF);
    ptr = (robj **)((char *)c->last_header + sizeof(payloadHeader));
    TEST_ASSERT(obj == *ptr);

    releaseReplyReferences(c);
    decrRefCount(obj);
    decrRefCount(obj2);

    freeReplyOffloadClient(c);

    return 0;
}

int test_addRepliesWithOffloadsToList(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Required for isCopyAvoidPreferred / isCopyAvoidIndicatedByIOThreads */
    int io_threads_num = server.io_threads_num;
    int min_io_threads_for_copy_avoid = server.min_io_threads_copy_avoid;
    server.io_threads_num = 1;
    server.min_io_threads_copy_avoid = 1;

    client *c = createTestClient();

    /* Test 1:  Add bulk offloads to the reply list */

    /* Select reply length so that there is place for 2 headers and 4 bytes only
     * 4 bytes is not enough for object pointer(s)
     * This will force bulk offload to be added to reply list
     */
    size_t reply_len = c->buf_usable_size - 2 * sizeof(payloadHeader) - 4;
    char *reply = zmalloc(reply_len);
    memset(reply, 'a', reply_len);
    _addReplyToBufferOrList(c, reply, reply_len);
    TEST_ASSERT(c->flag.buf_encoded);
    TEST_ASSERT(c->bufpos == sizeof(payloadHeader) + reply_len);
    TEST_ASSERT(listLength(c->reply) == 0);

    /* As bulk offload header+pointer can't be accommodated in c->buf
     * then one block is expected in c->reply */
    robj *obj = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test"));
    _addBulkStrRefToBufferOrList(c, obj);
    TEST_ASSERT(obj->refcount == 2);
    TEST_ASSERT(c->bufpos == sizeof(payloadHeader) + reply_len);
    TEST_ASSERT(listLength(c->reply) == 1);

    /* Check bulk offload header+pointer inside c->reply */
    listIter iter;
    listRewind(c->reply, &iter);
    listNode *next = listNext(&iter);
    clientReplyBlock *blk = listNodeValue(next);

    TEST_ASSERT(blk->used == sizeof(payloadHeader) + PTRS_LEN);
    payloadHeader *header1 = blk->last_header;
    TEST_ASSERT(header1->type == BULK_STR_REF);
    TEST_ASSERT(header1->len == PTRS_LEN);

    robj **ptr = (robj **)(blk->buf + sizeof(payloadHeader));
    TEST_ASSERT(obj == *ptr);

    /* Test 2:  Add one more bulk offload to the reply list */
    _addBulkStrRefToBufferOrList(c, obj);
    TEST_ASSERT(obj->refcount == 3);
    TEST_ASSERT(listLength(c->reply) == 1);
    TEST_ASSERT(blk->used == sizeof(payloadHeader) + 2 * PTRS_LEN);
    TEST_ASSERT(header1->type == BULK_STR_REF);
    TEST_ASSERT(header1->len == 2 * PTRS_LEN);

    /* Test 3: Add plain replies to cause reply list grow  */
    while (reply_len < blk->size - blk->used) _addReplyToBufferOrList(c, reply, reply_len);
    _addReplyToBufferOrList(c, reply, reply_len);

    TEST_ASSERT(listLength(c->reply) == 2);
    /* last header in 1st block */
    payloadHeader *header2 = blk->last_header;
    listRewind(c->reply, &iter);
    listNext(&iter);
    next = listNext(&iter);
    clientReplyBlock *blk2 = listNodeValue(next);
    /* last header in 2nd block */
    payloadHeader *header3 = blk2->last_header;
    TEST_ASSERT(header2->type == PLAIN_REPLY && header3->type == PLAIN_REPLY);
    TEST_ASSERT((header2->len + header3->len) % reply_len == 0);

    releaseReplyReferences(c);
    decrRefCount(obj);

    zfree(reply);

    freeReplyOffloadClient(c);

    /* Restore modified values */
    server.io_threads_num = io_threads_num;
    server.min_io_threads_copy_avoid = min_io_threads_for_copy_avoid;

    return 0;
}

int test_addBufferToReplyIOV(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    const char *expected_reply = "$5\r\nhello\r\n";
    ssize_t total_len = strlen(expected_reply);
    const int iovmax = 16;
    char crlf[2] = {'\r', '\n'};

    /* Test 1: 1st writevToclient invocation */
    client *c = createTestClient();
    robj *obj = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "hello"));
    _addBulkStrRefToBufferOrList(c, obj);

    struct iovec iov_arr[iovmax];
    char prefixes[iovmax / 3 + 1][LONG_STR_SIZE + 3];
    bufWriteMetadata metadata[1];

    replyIOV reply;
    initReplyIOV(c, iovmax, iov_arr, prefixes, crlf, &reply);
    addBufferToReplyIOV(c->flag.buf_encoded, c->buf, c->bufpos, &reply, &metadata[0]);

    TEST_ASSERT(reply.iov_len_total == total_len);
    TEST_ASSERT(reply.iovcnt == 3);
    const char *ptr = expected_reply;
    for (int i = 0; i < reply.iovcnt; ++i) {
        TEST_ASSERT(memcmp(ptr, reply.iov[i].iov_base, reply.iov[i].iov_len) == 0);
        ptr += reply.iov[i].iov_len;
    }

    /* Test 2: Last written buf/pos/data_len after 1st invocation */
    saveLastWrittenBuf(c, metadata, 1, reply.iov_len_total, 1); /* only 1 byte has been written */
    TEST_ASSERT(c->io_last_written_buf == c->buf);
    TEST_ASSERT(c->io_last_written_bufpos == 0); /* incomplete write */
    TEST_ASSERT(c->io_last_written_data_len == 1);

    /* Test 3: 2nd writevToclient invocation */
    struct iovec iov_arr2[iovmax];
    char prefixes2[iovmax / 3 + 1][LONG_STR_SIZE + 3];
    bufWriteMetadata metadata2[1];

    replyIOV reply2;
    initReplyIOV(c, iovmax, iov_arr2, prefixes2, crlf, &reply2);
    addBufferToReplyIOV(c->flag.buf_encoded, c->buf, c->bufpos, &reply2, &metadata2[0]);
    TEST_ASSERT(reply2.iov_len_total == total_len - 1);
    TEST_ASSERT((*(char *)reply2.iov[0].iov_base) == '5');

    /* Test 4: Last written buf/pos/data_len after 2nd invocation */
    saveLastWrittenBuf(c, metadata2, 1, reply2.iov_len_total, 4); /* 4 more bytes has been written */
    TEST_ASSERT(c->io_last_written_buf == c->buf);
    TEST_ASSERT(c->io_last_written_bufpos == 0);   /* incomplete write */
    TEST_ASSERT(c->io_last_written_data_len == 5); /* 1 + 4 */

    /* Test 5: 3rd writevToclient invocation */
    struct iovec iov_arr3[iovmax];
    char prefixes3[iovmax / 3 + 1][LONG_STR_SIZE + 3];
    bufWriteMetadata metadata3[1];

    replyIOV reply3;
    initReplyIOV(c, iovmax, iov_arr3, prefixes3, crlf, &reply3);
    addBufferToReplyIOV(c->flag.buf_encoded, c->buf, c->bufpos, &reply3, &metadata3[0]);
    TEST_ASSERT(reply3.iov_len_total == total_len - 5);
    TEST_ASSERT((*(char *)reply3.iov[0].iov_base) == 'e');

    /* Test 6: Last written buf/pos/data_len after 3rd invocation */
    saveLastWrittenBuf(c, metadata3, 1, reply3.iov_len_total, reply3.iov_len_total); /* everything has been written */
    TEST_ASSERT(c->io_last_written_buf == c->buf);
    TEST_ASSERT(c->io_last_written_bufpos == c->bufpos);
    TEST_ASSERT(c->io_last_written_data_len == (size_t)total_len);

    decrRefCount(obj);
    decrRefCount(obj);

    freeReplyOffloadClient(c);

    return 0;
}
