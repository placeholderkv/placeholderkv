#include "../networking.c"
#include "../server.c"
#include "test_help.h"

#include <stdatomic.h>

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

static client* createTestClient(void) {
    client *c = zcalloc(sizeof(client));

    c->buf = zmalloc_usable(PROTO_REPLY_CHUNK_BYTES, &c->buf_usable_size);
    c->reply = listCreate();
    listSetFreeMethod(c->reply, freeClientReplyValue);
    listSetDupMethod(c->reply, dupClientReplyValue);
    c->flag.reply_offload = 1;
    c->flag.fake = 1;

    return c;
}

static void freeReplyOffloadClient(client *c) {
    listRelease(c->reply);
    zfree(c->buf);
    zfree(c);
}

int test_addRepliesWithOffloadsToBuffer(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    client * c = createTestClient();

    /* Test 1:  Add bulk offloads to the buffer */
    robj *obj = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test"));
    _addBulkOffloadToBufferOrList(c, obj);

    TEST_ASSERT(obj->refcount == 2);
    TEST_ASSERT(c->bufpos == sizeof(payloadHeader) + sizeof(void*));

    payloadHeader *header1 = c->last_header;
    TEST_ASSERT(header1->type == CLIENT_REPLY_PAYLOAD_BULK_OFFLOAD);
    TEST_ASSERT(header1->len == sizeof(void*));

    robj **ptr = (robj **)(c->buf + sizeof(payloadHeader));
    TEST_ASSERT(obj == *ptr);

    robj *obj2 = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test2"));
    _addBulkOffloadToBufferOrList(c, obj2);

    TEST_ASSERT(c->bufpos == sizeof(payloadHeader) + 2 * sizeof(void*));
    TEST_ASSERT(header1->type == CLIENT_REPLY_PAYLOAD_BULK_OFFLOAD);
    TEST_ASSERT(header1->len == 2 * sizeof(void*));

    ptr = (robj **)(c->buf + sizeof(payloadHeader) + sizeof(void*));
    TEST_ASSERT(obj2 == *ptr);

    /* Test 2:  Add plain reply to the buffer */
    const char* plain = "+OK\r\n";
    size_t plain_len = strlen(plain);
    _addReplyToBufferOrList(c, plain, plain_len);

    TEST_ASSERT(c->bufpos == 2 * sizeof(payloadHeader) + 2 * sizeof(void*) + plain_len);
    TEST_ASSERT(header1->type == CLIENT_REPLY_PAYLOAD_BULK_OFFLOAD);
    TEST_ASSERT(header1->len == 2 * sizeof(void*));
    payloadHeader *header2 = c->last_header;
    TEST_ASSERT(header2->type == CLIENT_REPLY_PAYLOAD_DATA);
    TEST_ASSERT(header2->len == plain_len);

    for (int i = 0; i < 9; ++i) _addReplyToBufferOrList(c, plain, plain_len);
    TEST_ASSERT(c->bufpos == 2 * sizeof(payloadHeader) + 2 * sizeof(void*) + 10 * plain_len);
    TEST_ASSERT(header2->type == CLIENT_REPLY_PAYLOAD_DATA);
    TEST_ASSERT(header2->len == plain_len * 10);

    /* Test 3:  Add one more bulk offload to the buffer */
    _addBulkOffloadToBufferOrList(c, obj);
    TEST_ASSERT(obj->refcount == 3);
    TEST_ASSERT(c->bufpos == 3 * sizeof(payloadHeader) + 3 * sizeof(void*) + 10 * plain_len);
    payloadHeader *header3 = c->last_header;
    TEST_ASSERT(header3->type == CLIENT_REPLY_PAYLOAD_BULK_OFFLOAD);
    ptr = (robj **)((char*)c->last_header + sizeof(payloadHeader));
    TEST_ASSERT(obj == *ptr);

    decrRefCount(obj);
    decrRefCount(obj);
    decrRefCount(obj);

    decrRefCount(obj2);
    decrRefCount(obj2);

    freeReplyOffloadClient(c);

    return 0;
}

int test_addRepliesWithOffloadsToList(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    client *c = createTestClient();

    /* Test 1:  Add bulk offloads to the reply list */

    /* Reply len to fill the buffer almost completely */
    size_t reply_len = c->buf_usable_size - 2 * sizeof(payloadHeader) - 4;

    char *reply = zmalloc(reply_len);
    memset(reply, 'a', reply_len);
    _addReplyToBufferOrList(c, reply, reply_len);
    TEST_ASSERT(c->bufpos == sizeof(payloadHeader) + reply_len);
    TEST_ASSERT(listLength(c->reply) == 0);

    robj *obj = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "test"));
    _addBulkOffloadToBufferOrList(c, obj);

    TEST_ASSERT(obj->refcount == 2);
    TEST_ASSERT(c->bufpos == sizeof(payloadHeader) + reply_len);
    TEST_ASSERT(listLength(c->reply) == 1);

    listIter iter;
    listRewind(c->reply, &iter);
    listNode *next = listNext(&iter);
    clientReplyBlock *blk = listNodeValue(next);

    TEST_ASSERT(blk->used == sizeof(payloadHeader) + sizeof(void*));
    payloadHeader *header1 = blk->last_header;
    TEST_ASSERT(header1->type == CLIENT_REPLY_PAYLOAD_BULK_OFFLOAD);
    TEST_ASSERT(header1->len == sizeof(void*));

    robj **ptr = (robj **)(blk->buf + sizeof(payloadHeader));
    TEST_ASSERT(obj == *ptr);

    /* Test 2:  Add one more bulk offload to the reply list */
    _addBulkOffloadToBufferOrList(c, obj);
    TEST_ASSERT(obj->refcount == 3);
    TEST_ASSERT(listLength(c->reply) == 1);
    TEST_ASSERT(blk->used == sizeof(payloadHeader) + 2 * sizeof(void*));
    TEST_ASSERT(header1->type == CLIENT_REPLY_PAYLOAD_BULK_OFFLOAD);
    TEST_ASSERT(header1->len == 2 * sizeof(void*));

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
    TEST_ASSERT(header2->type == CLIENT_REPLY_PAYLOAD_DATA && header3->type == CLIENT_REPLY_PAYLOAD_DATA);
    TEST_ASSERT((header2->len + header3->len) % reply_len == 0);

    decrRefCount(obj);
    decrRefCount(obj);
    decrRefCount(obj);

    freeReplyOffloadClient(c);

    return 0;
}

int test_addBufferToReplyIOV(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    const char* expected_reply = "$5\r\nhello\r\n";
    ssize_t total_len = strlen(expected_reply);
    const int iovmax = 16;
    char crlf[2] = {'\r', '\n'};

    /* Test 1: 1st writevToclient invocation */
    client *c = createTestClient();
    robj *obj = createObject(OBJ_STRING, sdscatfmt(sdsempty(), "hello"));
    _addBulkOffloadToBufferOrList(c, obj);

    struct iovec iov_arr[iovmax];
    char prefixes[iovmax / 3 + 1][LONG_STR_SIZE + 3];
    bufWriteMetadata metadata[1];

    replyIOV reply;
    initReplyIOV(c, iovmax, iov_arr, prefixes, crlf, &reply);
    addBufferToReplyIOV(c->buf, c->bufpos, &reply, &metadata[0]);

    TEST_ASSERT(reply.iov_len_total == total_len);
    TEST_ASSERT(reply.cnt == 3);
    const char* ptr = expected_reply;
    for (int i = 0; i < reply.cnt; ++i) {
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
    addBufferToReplyIOV(c->buf, c->bufpos, &reply2, &metadata2[0]);
    TEST_ASSERT(reply2.iov_len_total == total_len - 1);
    TEST_ASSERT((*(char*)reply2.iov[0].iov_base) == '5');

    /* Test 4: Last written buf/pos/data_len after 2nd invocation */
    saveLastWrittenBuf(c, metadata2, 1, reply2.iov_len_total, 4); /* 4 more bytes has been written */
    TEST_ASSERT(c->io_last_written_buf == c->buf);
    TEST_ASSERT(c->io_last_written_bufpos == 0); /* incomplete write */
    TEST_ASSERT(c->io_last_written_data_len == 5); /* 1 + 4 */

    /* Test 5: 3rd writevToclient invocation */
    struct iovec iov_arr3[iovmax];
    char prefixes3[iovmax / 3 + 1][LONG_STR_SIZE + 3];
    bufWriteMetadata metadata3[1];

    replyIOV reply3;
    initReplyIOV(c, iovmax, iov_arr3, prefixes3, crlf, &reply3);
    addBufferToReplyIOV(c->buf, c->bufpos, &reply3, &metadata3[0]);
    TEST_ASSERT(reply3.iov_len_total == total_len - 5);
    TEST_ASSERT((*(char*)reply3.iov[0].iov_base) == 'e');

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