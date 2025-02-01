/* Module designed to test the modules subsystem.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2016, Redis Ltd.
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

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>

/* --------------------------------- Helpers -------------------------------- */

/* Return true if the reply and the C null term string matches. */
int TestMatchReply(ValkeyModuleCallReply *reply, char *str) {
    ValkeyModuleString *mystr;
    mystr = ValkeyModule_CreateStringFromCallReply(reply);
    if (!mystr) return 0;
    const char *ptr = ValkeyModule_StringPtrLen(mystr,NULL);
    return strcmp(ptr,str) == 0;
}

/* ------------------------------- Test units ------------------------------- */

/* TEST.CALL -- Test Call() API. */
int TestCall(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    ValkeyModule_Call(ctx,"DEL","c","mylist");
    ValkeyModuleString *mystr = ValkeyModule_CreateString(ctx,"foo",3);
    ValkeyModule_Call(ctx,"RPUSH","csl","mylist",mystr,(long long)1234);
    reply = ValkeyModule_Call(ctx,"LRANGE","ccc","mylist","0","-1");
    long long items = ValkeyModule_CallReplyLength(reply);
    if (items != 2) goto fail;

    ValkeyModuleCallReply *item0, *item1;

    item0 = ValkeyModule_CallReplyArrayElement(reply,0);
    item1 = ValkeyModule_CallReplyArrayElement(reply,1);
    if (!TestMatchReply(item0,"foo")) goto fail;
    if (!TestMatchReply(item1,"1234")) goto fail;

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallResp3Attribute(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    reply = ValkeyModule_Call(ctx,"DEBUG","3cc" ,"PROTOCOL", "attrib"); /* 3 stands for resp 3 reply */
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_STRING) goto fail;

    /* make sure we can not reply to resp2 client with resp3 (it might be a string but it contains attribute) */
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;

    if (!TestMatchReply(reply,"Some real reply following the attribute")) goto fail;

    reply = ValkeyModule_CallReplyAttribute(reply);
    if (!reply || ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ATTRIBUTE) goto fail;
    /* make sure we can not reply to resp2 client with resp3 attribute */
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;
    if (ValkeyModule_CallReplyLength(reply) != 1) goto fail;

    ValkeyModuleCallReply *key, *val;
    if (ValkeyModule_CallReplyAttributeElement(reply,0,&key,&val) != VALKEYMODULE_OK) goto fail;
    if (!TestMatchReply(key,"key-popularity")) goto fail;
    if (ValkeyModule_CallReplyType(val) != VALKEYMODULE_REPLY_ARRAY) goto fail;
    if (ValkeyModule_CallReplyLength(val) != 2) goto fail;
    if (!TestMatchReply(ValkeyModule_CallReplyArrayElement(val, 0),"key:123")) goto fail;
    if (!TestMatchReply(ValkeyModule_CallReplyArrayElement(val, 1),"90")) goto fail;

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestGetResp(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    int flags = ValkeyModule_GetContextFlags(ctx);

    if (flags & VALKEYMODULE_CTX_FLAGS_RESP3) {
        ValkeyModule_ReplyWithLongLong(ctx, 3);
    } else {
        ValkeyModule_ReplyWithLongLong(ctx, 2);
    }

    return VALKEYMODULE_OK;
}

int TestCallRespAutoMode(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    ValkeyModule_Call(ctx,"DEL","c","myhash");
    ValkeyModule_Call(ctx,"HSET","ccccc","myhash", "f1", "v1", "f2", "v2");
    /* 0 stands for auto mode, we will get the reply in the same format as the client */
    reply = ValkeyModule_Call(ctx,"HGETALL","0c" ,"myhash");
    ValkeyModule_ReplyWithCallReply(ctx, reply);
    return VALKEYMODULE_OK;
}

int TestCallResp3Map(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    ValkeyModule_Call(ctx,"DEL","c","myhash");
    ValkeyModule_Call(ctx,"HSET","ccccc","myhash", "f1", "v1", "f2", "v2");
    reply = ValkeyModule_Call(ctx,"HGETALL","3c" ,"myhash"); /* 3 stands for resp 3 reply */
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_MAP) goto fail;

    /* make sure we can not reply to resp2 client with resp3 map */
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;

    long long items = ValkeyModule_CallReplyLength(reply);
    if (items != 2) goto fail;

    ValkeyModuleCallReply *key0, *key1;
    ValkeyModuleCallReply *val0, *val1;
    if (ValkeyModule_CallReplyMapElement(reply,0,&key0,&val0) != VALKEYMODULE_OK) goto fail;
    if (ValkeyModule_CallReplyMapElement(reply,1,&key1,&val1) != VALKEYMODULE_OK) goto fail;
    if (!TestMatchReply(key0,"f1")) goto fail;
    if (!TestMatchReply(key1,"f2")) goto fail;
    if (!TestMatchReply(val0,"v1")) goto fail;
    if (!TestMatchReply(val1,"v2")) goto fail;

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallResp3Bool(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    reply = ValkeyModule_Call(ctx,"DEBUG","3cc" ,"PROTOCOL", "true"); /* 3 stands for resp 3 reply */
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_BOOL) goto fail;
    /* make sure we can not reply to resp2 client with resp3 bool */
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;

    if (!ValkeyModule_CallReplyBool(reply)) goto fail;
    reply = ValkeyModule_Call(ctx,"DEBUG","3cc" ,"PROTOCOL", "false"); /* 3 stands for resp 3 reply */
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_BOOL) goto fail;
    if (ValkeyModule_CallReplyBool(reply)) goto fail;

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallResp3Null(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    reply = ValkeyModule_Call(ctx,"DEBUG","3cc" ,"PROTOCOL", "null"); /* 3 stands for resp 3 reply */
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_NULL) goto fail;

    /* make sure we can not reply to resp2 client with resp3 null */
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallReplyWithNestedReply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    ValkeyModule_Call(ctx,"DEL","c","mylist");
    ValkeyModule_Call(ctx,"RPUSH","ccl","mylist","test",(long long)1234);
    reply = ValkeyModule_Call(ctx,"LRANGE","ccc","mylist","0","-1");
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ARRAY) goto fail;
    if (ValkeyModule_CallReplyLength(reply) < 1) goto fail;
    ValkeyModuleCallReply *nestedReply = ValkeyModule_CallReplyArrayElement(reply, 0);

    ValkeyModule_ReplyWithCallReply(ctx,nestedReply);
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallReplyWithArrayReply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    ValkeyModule_Call(ctx,"DEL","c","mylist");
    ValkeyModule_Call(ctx,"RPUSH","ccl","mylist","test",(long long)1234);
    reply = ValkeyModule_Call(ctx,"LRANGE","ccc","mylist","0","-1");
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ARRAY) goto fail;

    ValkeyModule_ReplyWithCallReply(ctx,reply);
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallResp3Double(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    reply = ValkeyModule_Call(ctx,"DEBUG","3cc" ,"PROTOCOL", "double"); /* 3 stands for resp 3 reply */
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_DOUBLE) goto fail;

    /* make sure we can not reply to resp2 client with resp3 double*/
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;

    double d = ValkeyModule_CallReplyDouble(reply);
    /* we compare strings, since comparing doubles directly can fail in various architectures, e.g. 32bit */
    char got[30], expected[30];
    snprintf(got, sizeof(got), "%.17g", d);
    snprintf(expected, sizeof(expected), "%.17g", 3.141);
    if (strcmp(got, expected) != 0) goto fail;
    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallResp3BigNumber(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    reply = ValkeyModule_Call(ctx,"DEBUG","3cc" ,"PROTOCOL", "bignum"); /* 3 stands for resp 3 reply */
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_BIG_NUMBER) goto fail;

    /* make sure we can not reply to resp2 client with resp3 big number */
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;

    size_t len;
    const char* big_num = ValkeyModule_CallReplyBigNumber(reply, &len);
    ValkeyModule_ReplyWithStringBuffer(ctx,big_num,len);
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallResp3Verbatim(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    reply = ValkeyModule_Call(ctx,"DEBUG","3cc" ,"PROTOCOL", "verbatim"); /* 3 stands for resp 3 reply */
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_VERBATIM_STRING) goto fail;

    /* make sure we can not reply to resp2 client with resp3 verbatim string */
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;

    const char* format;
    size_t len;
    const char* str = ValkeyModule_CallReplyVerbatim(reply, &len, &format);
    ValkeyModuleString *s = ValkeyModule_CreateStringPrintf(ctx, "%.*s:%.*s", 3, format, (int)len, str);
    ValkeyModule_ReplyWithString(ctx,s);
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

int TestCallResp3Set(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    ValkeyModule_Call(ctx,"DEL","c","myset");
    ValkeyModule_Call(ctx,"sadd","ccc","myset", "v1", "v2");
    reply = ValkeyModule_Call(ctx,"smembers","3c" ,"myset"); // N stands for resp 3 reply
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_SET) goto fail;

    /* make sure we can not reply to resp2 client with resp3 set */
    if (ValkeyModule_ReplyWithCallReply(ctx, reply) != VALKEYMODULE_ERR) goto fail;

    long long items = ValkeyModule_CallReplyLength(reply);
    if (items != 2) goto fail;

    ValkeyModuleCallReply *val0, *val1;

    val0 = ValkeyModule_CallReplySetElement(reply,0);
    val1 = ValkeyModule_CallReplySetElement(reply,1);

    /*
     * The order of elements on sets are not promised so we just
     * veridy that the reply matches one of the elements.
     */
    if (!TestMatchReply(val0,"v1") && !TestMatchReply(val0,"v2")) goto fail;
    if (!TestMatchReply(val1,"v1") && !TestMatchReply(val1,"v2")) goto fail;

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,"ERR");
    return VALKEYMODULE_OK;
}

/* TEST.STRING.APPEND -- Test appending to an existing string object. */
int TestStringAppend(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModuleString *s = ValkeyModule_CreateString(ctx,"foo",3);
    ValkeyModule_StringAppendBuffer(ctx,s,"bar",3);
    ValkeyModule_ReplyWithString(ctx,s);
    ValkeyModule_FreeString(ctx,s);
    return VALKEYMODULE_OK;
}

/* TEST.STRING.APPEND.AM -- Test append with retain when auto memory is on. */
int TestStringAppendAM(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleString *s = ValkeyModule_CreateString(ctx,"foo",3);
    ValkeyModule_RetainString(ctx,s);
    ValkeyModule_TrimStringAllocation(s);    /* Mostly NOP, but exercises the API function */
    ValkeyModule_StringAppendBuffer(ctx,s,"bar",3);
    ValkeyModule_ReplyWithString(ctx,s);
    ValkeyModule_FreeString(ctx,s);
    return VALKEYMODULE_OK;
}

/* TEST.STRING.TRIM -- Test we trim a string with free space. */
int TestTrimString(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    ValkeyModuleString *s = ValkeyModule_CreateString(ctx,"foo",3);
    char *tmp = ValkeyModule_Alloc(1024);
    ValkeyModule_StringAppendBuffer(ctx,s,tmp,1024);
    size_t string_len = ValkeyModule_MallocSizeString(s);
    ValkeyModule_TrimStringAllocation(s);
    size_t len_after_trim = ValkeyModule_MallocSizeString(s);

    /* Determine if using jemalloc memory allocator. */
    ValkeyModuleServerInfoData *info = ValkeyModule_GetServerInfo(ctx, "memory");
    const char *field = ValkeyModule_ServerInfoGetFieldC(info, "mem_allocator");
    int use_jemalloc = !strncmp(field, "jemalloc", 8);

    /* Jemalloc will reallocate `s` from 2k to 1k after ValkeyModule_TrimStringAllocation(),
     * but non-jemalloc memory allocators may keep the old size. */
    if ((use_jemalloc && len_after_trim < string_len) ||
        (!use_jemalloc && len_after_trim <= string_len))
    {
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        ValkeyModule_ReplyWithError(ctx, "String was not trimmed as expected.");
    }
    ValkeyModule_FreeServerInfo(ctx, info);
    ValkeyModule_Free(tmp);
    ValkeyModule_FreeString(ctx,s);
    return VALKEYMODULE_OK;
}

/* TEST.STRING.PRINTF -- Test string formatting. */
int TestStringPrintf(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    if (argc < 3) {
        return ValkeyModule_WrongArity(ctx);
    }
    ValkeyModuleString *s = ValkeyModule_CreateStringPrintf(ctx,
        "Got %d args. argv[1]: %s, argv[2]: %s",
        argc,
        ValkeyModule_StringPtrLen(argv[1], NULL),
        ValkeyModule_StringPtrLen(argv[2], NULL)
    );

    ValkeyModule_ReplyWithString(ctx,s);

    return VALKEYMODULE_OK;
}

int failTest(ValkeyModuleCtx *ctx, const char *msg) {
    ValkeyModule_ReplyWithError(ctx, msg);
    return VALKEYMODULE_ERR;
}

int TestUnlink(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModuleKey *k = ValkeyModule_OpenKey(ctx, ValkeyModule_CreateStringPrintf(ctx, "unlinked"), VALKEYMODULE_WRITE | VALKEYMODULE_READ);
    if (!k) return failTest(ctx, "Could not create key");

    if (VALKEYMODULE_ERR == ValkeyModule_StringSet(k, ValkeyModule_CreateStringPrintf(ctx, "Foobar"))) {
        return failTest(ctx, "Could not set string value");
    }

    ValkeyModuleCallReply *rep = ValkeyModule_Call(ctx, "EXISTS", "c", "unlinked");
    if (!rep || ValkeyModule_CallReplyInteger(rep) != 1) {
        return failTest(ctx, "Key does not exist before unlink");
    }

    if (VALKEYMODULE_ERR == ValkeyModule_UnlinkKey(k)) {
        return failTest(ctx, "Could not unlink key");
    }

    rep = ValkeyModule_Call(ctx, "EXISTS", "c", "unlinked");
    if (!rep || ValkeyModule_CallReplyInteger(rep) != 0) {
        return failTest(ctx, "Could not verify key to be unlinked");
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int TestNestedCallReplyArrayElement(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModuleString *expect_key = ValkeyModule_CreateString(ctx, "mykey", strlen("mykey"));
    ValkeyModule_SelectDb(ctx, 1);
    ValkeyModule_Call(ctx, "LPUSH", "sc", expect_key, "myvalue");

    ValkeyModuleCallReply *scan_reply = ValkeyModule_Call(ctx, "SCAN", "l", (long long)0);
    ValkeyModule_Assert(scan_reply != NULL && ValkeyModule_CallReplyType(scan_reply) == VALKEYMODULE_REPLY_ARRAY);
    ValkeyModule_Assert(ValkeyModule_CallReplyLength(scan_reply) == 2);

    long long scan_cursor;
    ValkeyModuleCallReply *cursor_reply = ValkeyModule_CallReplyArrayElement(scan_reply, 0);
    ValkeyModule_Assert(ValkeyModule_CallReplyType(cursor_reply) == VALKEYMODULE_REPLY_STRING);
    ValkeyModule_Assert(ValkeyModule_StringToLongLong(ValkeyModule_CreateStringFromCallReply(cursor_reply), &scan_cursor) == VALKEYMODULE_OK);
    ValkeyModule_Assert(scan_cursor == 0);

    ValkeyModuleCallReply *keys_reply = ValkeyModule_CallReplyArrayElement(scan_reply, 1);
    ValkeyModule_Assert(ValkeyModule_CallReplyType(keys_reply) == VALKEYMODULE_REPLY_ARRAY);
    ValkeyModule_Assert( ValkeyModule_CallReplyLength(keys_reply) == 1);
 
    ValkeyModuleCallReply *key_reply = ValkeyModule_CallReplyArrayElement(keys_reply, 0);
    ValkeyModule_Assert(ValkeyModule_CallReplyType(key_reply) == VALKEYMODULE_REPLY_STRING);
    ValkeyModuleString *key = ValkeyModule_CreateStringFromCallReply(key_reply);
    ValkeyModule_Assert(ValkeyModule_StringCompare(key, expect_key) == 0);

    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    return VALKEYMODULE_OK;
}

/* TEST.STRING.TRUNCATE -- Test truncating an existing string object. */
int TestStringTruncate(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_Call(ctx, "SET", "cc", "foo", "abcde");
    ValkeyModuleKey *k = ValkeyModule_OpenKey(ctx, ValkeyModule_CreateStringPrintf(ctx, "foo"), VALKEYMODULE_READ | VALKEYMODULE_WRITE);
    if (!k) return failTest(ctx, "Could not create key");

    size_t len = 0;
    char* s;

    /* expand from 5 to 8 and check null pad */
    if (VALKEYMODULE_ERR == ValkeyModule_StringTruncate(k, 8)) {
        return failTest(ctx, "Could not truncate string value (8)");
    }
    s = ValkeyModule_StringDMA(k, &len, VALKEYMODULE_READ);
    if (!s) {
        return failTest(ctx, "Failed to read truncated string (8)");
    } else if (len != 8) {
        return failTest(ctx, "Failed to expand string value (8)");
    } else if (0 != strncmp(s, "abcde\0\0\0", 8)) {
        return failTest(ctx, "Failed to null pad string value (8)");
    }

    /* shrink from 8 to 4 */
    if (VALKEYMODULE_ERR == ValkeyModule_StringTruncate(k, 4)) {
        return failTest(ctx, "Could not truncate string value (4)");
    }
    s = ValkeyModule_StringDMA(k, &len, VALKEYMODULE_READ);
    if (!s) {
        return failTest(ctx, "Failed to read truncated string (4)");
    } else if (len != 4) {
        return failTest(ctx, "Failed to shrink string value (4)");
    } else if (0 != strncmp(s, "abcd", 4)) {
        return failTest(ctx, "Failed to truncate string value (4)");
    }

    /* shrink to 0 */
    if (VALKEYMODULE_ERR == ValkeyModule_StringTruncate(k, 0)) {
        return failTest(ctx, "Could not truncate string value (0)");
    }
    s = ValkeyModule_StringDMA(k, &len, VALKEYMODULE_READ);
    if (!s) {
        return failTest(ctx, "Failed to read truncated string (0)");
    } else if (len != 0) {
        return failTest(ctx, "Failed to shrink string value to (0)");
    }

    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int NotifyCallback(ValkeyModuleCtx *ctx, int type, const char *event,
                   ValkeyModuleString *key) {
  ValkeyModule_AutoMemory(ctx);
  /* Increment a counter on the notifications: for each key notified we
   * increment a counter */
  ValkeyModule_Log(ctx, "notice", "Got event type %d, event %s, key %s", type,
                  event, ValkeyModule_StringPtrLen(key, NULL));

  ValkeyModule_Call(ctx, "HINCRBY", "csc", "notifications", key, "1");
  return VALKEYMODULE_OK;
}

/* TEST.NOTIFICATIONS -- Test Keyspace Notifications. */
int TestNotifications(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

#define FAIL(msg, ...)                                                                       \
    {                                                                                        \
        ValkeyModule_Log(ctx, "warning", "Failed NOTIFY Test. Reason: " #msg, ##__VA_ARGS__); \
        goto err;                                                                            \
    }
    ValkeyModule_Call(ctx, "FLUSHDB", "");

    ValkeyModule_Call(ctx, "SET", "cc", "foo", "bar");
    ValkeyModule_Call(ctx, "SET", "cc", "foo", "baz");
    ValkeyModule_Call(ctx, "SADD", "cc", "bar", "x");
    ValkeyModule_Call(ctx, "SADD", "cc", "bar", "y");

    ValkeyModule_Call(ctx, "HSET", "ccc", "baz", "x", "y");
    /* LPUSH should be ignored and not increment any counters */
    ValkeyModule_Call(ctx, "LPUSH", "cc", "l", "y");
    ValkeyModule_Call(ctx, "LPUSH", "cc", "l", "y");

    /* Miss some keys intentionally so we will get a "keymiss" notification. */
    ValkeyModule_Call(ctx, "GET", "c", "nosuchkey");
    ValkeyModule_Call(ctx, "SMEMBERS", "c", "nosuchkey");

    size_t sz;
    const char *rep;
    ValkeyModuleCallReply *r = ValkeyModule_Call(ctx, "HGET", "cc", "notifications", "foo");
    if (r == NULL || ValkeyModule_CallReplyType(r) != VALKEYMODULE_REPLY_STRING) {
        FAIL("Wrong or no reply for foo");
    } else {
        rep = ValkeyModule_CallReplyStringPtr(r, &sz);
        if (sz != 1 || *rep != '2') {
            FAIL("Got reply '%s'. expected '2'", ValkeyModule_CallReplyStringPtr(r, NULL));
        }
    }

    r = ValkeyModule_Call(ctx, "HGET", "cc", "notifications", "bar");
    if (r == NULL || ValkeyModule_CallReplyType(r) != VALKEYMODULE_REPLY_STRING) {
        FAIL("Wrong or no reply for bar");
    } else {
        rep = ValkeyModule_CallReplyStringPtr(r, &sz);
        if (sz != 1 || *rep != '2') {
            FAIL("Got reply '%s'. expected '2'", rep);
        }
    }

    r = ValkeyModule_Call(ctx, "HGET", "cc", "notifications", "baz");
    if (r == NULL || ValkeyModule_CallReplyType(r) != VALKEYMODULE_REPLY_STRING) {
        FAIL("Wrong or no reply for baz");
    } else {
        rep = ValkeyModule_CallReplyStringPtr(r, &sz);
        if (sz != 1 || *rep != '1') {
            FAIL("Got reply '%.*s'. expected '1'", (int)sz, rep);
        }
    }
    /* For l we expect nothing since we didn't subscribe to list events */
    r = ValkeyModule_Call(ctx, "HGET", "cc", "notifications", "l");
    if (r == NULL || ValkeyModule_CallReplyType(r) != VALKEYMODULE_REPLY_NULL) {
        FAIL("Wrong reply for l");
    }

    r = ValkeyModule_Call(ctx, "HGET", "cc", "notifications", "nosuchkey");
    if (r == NULL || ValkeyModule_CallReplyType(r) != VALKEYMODULE_REPLY_STRING) {
        FAIL("Wrong or no reply for nosuchkey");
    } else {
        rep = ValkeyModule_CallReplyStringPtr(r, &sz);
        if (sz != 1 || *rep != '2') {
            FAIL("Got reply '%.*s'. expected '2'", (int)sz, rep);
        }
    }

    ValkeyModule_Call(ctx, "FLUSHDB", "");

    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
err:
    ValkeyModule_Call(ctx, "FLUSHDB", "");

    return ValkeyModule_ReplyWithSimpleString(ctx, "ERR");
}

/* test.latency latency_ms */
int TestLatency(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) {
        ValkeyModule_WrongArity(ctx);
        return VALKEYMODULE_OK;
    }

    long long latency_ms;
    if (ValkeyModule_StringToLongLong(argv[1], &latency_ms) != VALKEYMODULE_OK) {
        ValkeyModule_ReplyWithError(ctx, "Invalid integer value");
        return VALKEYMODULE_OK;
    }

    ValkeyModule_LatencyAddSample("test", latency_ms);
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    return VALKEYMODULE_OK;
}

/* TEST.CTXFLAGS -- Test GetContextFlags. */
int TestCtxFlags(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argc);
    VALKEYMODULE_NOT_USED(argv);

    ValkeyModule_AutoMemory(ctx);

    int ok = 1;
    const char *errString = NULL;
#undef FAIL
#define FAIL(msg)        \
    {                    \
        ok = 0;          \
        errString = msg; \
        goto end;        \
    }

    int flags = ValkeyModule_GetContextFlags(ctx);
    if (flags == 0) {
        FAIL("Got no flags");
    }

    if (flags & VALKEYMODULE_CTX_FLAGS_LUA) FAIL("Lua flag was set");
    if (flags & VALKEYMODULE_CTX_FLAGS_MULTI) FAIL("Multi flag was set");

    if (flags & VALKEYMODULE_CTX_FLAGS_AOF) FAIL("AOF Flag was set")
    /* Enable AOF to test AOF flags */
    ValkeyModule_Call(ctx, "config", "ccc", "set", "appendonly", "yes");
    flags = ValkeyModule_GetContextFlags(ctx);
    if (!(flags & VALKEYMODULE_CTX_FLAGS_AOF)) FAIL("AOF Flag not set after config set");

    /* Disable RDB saving and test the flag. */
    ValkeyModule_Call(ctx, "config", "ccc", "set", "save", "");
    flags = ValkeyModule_GetContextFlags(ctx);
    if (flags & VALKEYMODULE_CTX_FLAGS_RDB) FAIL("RDB Flag was set");
    /* Enable RDB to test RDB flags */
    ValkeyModule_Call(ctx, "config", "ccc", "set", "save", "900 1");
    flags = ValkeyModule_GetContextFlags(ctx);
    if (!(flags & VALKEYMODULE_CTX_FLAGS_RDB)) FAIL("RDB Flag was not set after config set");

    if (!(flags & VALKEYMODULE_CTX_FLAGS_PRIMARY)) FAIL("Master flag was not set");
    if (flags & VALKEYMODULE_CTX_FLAGS_REPLICA) FAIL("Slave flag was set");
    if (flags & VALKEYMODULE_CTX_FLAGS_READONLY) FAIL("Read-only flag was set");
    if (flags & VALKEYMODULE_CTX_FLAGS_CLUSTER) FAIL("Cluster flag was set");

    /* Disable maxmemory and test the flag. (it is implicitly set in 32bit builds. */
    ValkeyModule_Call(ctx, "config", "ccc", "set", "maxmemory", "0");
    flags = ValkeyModule_GetContextFlags(ctx);
    if (flags & VALKEYMODULE_CTX_FLAGS_MAXMEMORY) FAIL("Maxmemory flag was set");

    /* Enable maxmemory and test the flag. */
    ValkeyModule_Call(ctx, "config", "ccc", "set", "maxmemory", "100000000");
    flags = ValkeyModule_GetContextFlags(ctx);
    if (!(flags & VALKEYMODULE_CTX_FLAGS_MAXMEMORY))
        FAIL("Maxmemory flag was not set after config set");

    if (flags & VALKEYMODULE_CTX_FLAGS_EVICT) FAIL("Eviction flag was set");
    ValkeyModule_Call(ctx, "config", "ccc", "set", "maxmemory-policy", "allkeys-lru");
    flags = ValkeyModule_GetContextFlags(ctx);
    if (!(flags & VALKEYMODULE_CTX_FLAGS_EVICT)) FAIL("Eviction flag was not set after config set");

end:
    /* Revert config changes */
    ValkeyModule_Call(ctx, "config", "ccc", "set", "appendonly", "no");
    ValkeyModule_Call(ctx, "config", "ccc", "set", "save", "");
    ValkeyModule_Call(ctx, "config", "ccc", "set", "maxmemory", "0");
    ValkeyModule_Call(ctx, "config", "ccc", "set", "maxmemory-policy", "noeviction");

    if (!ok) {
        ValkeyModule_Log(ctx, "warning", "Failed CTXFLAGS Test. Reason: %s", errString);
        return ValkeyModule_ReplyWithSimpleString(ctx, "ERR");
    }

    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

/* ----------------------------- Test framework ----------------------------- */

/* Return 1 if the reply matches the specified string, otherwise log errors
 * in the server log and return 0. */
int TestAssertErrorReply(ValkeyModuleCtx *ctx, ValkeyModuleCallReply *reply, char *str, size_t len) {
    ValkeyModuleString *mystr, *expected;
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ERROR) {
        return 0;
    }

    mystr = ValkeyModule_CreateStringFromCallReply(reply);
    expected = ValkeyModule_CreateString(ctx,str,len);
    if (ValkeyModule_StringCompare(mystr,expected) != 0) {
        const char *mystr_ptr = ValkeyModule_StringPtrLen(mystr,NULL);
        const char *expected_ptr = ValkeyModule_StringPtrLen(expected,NULL);
        ValkeyModule_Log(ctx,"warning",
            "Unexpected Error reply reply '%s' (instead of '%s')",
            mystr_ptr, expected_ptr);
        return 0;
    }
    return 1;
}

int TestAssertStringReply(ValkeyModuleCtx *ctx, ValkeyModuleCallReply *reply, char *str, size_t len) {
    ValkeyModuleString *mystr, *expected;

    if (ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ERROR) {
        ValkeyModule_Log(ctx,"warning","Test error reply: %s",
            ValkeyModule_CallReplyStringPtr(reply, NULL));
        return 0;
    } else if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_STRING) {
        ValkeyModule_Log(ctx,"warning","Unexpected reply type %d",
            ValkeyModule_CallReplyType(reply));
        return 0;
    }
    mystr = ValkeyModule_CreateStringFromCallReply(reply);
    expected = ValkeyModule_CreateString(ctx,str,len);
    if (ValkeyModule_StringCompare(mystr,expected) != 0) {
        const char *mystr_ptr = ValkeyModule_StringPtrLen(mystr,NULL);
        const char *expected_ptr = ValkeyModule_StringPtrLen(expected,NULL);
        ValkeyModule_Log(ctx,"warning",
            "Unexpected string reply '%s' (instead of '%s')",
            mystr_ptr, expected_ptr);
        return 0;
    }
    return 1;
}

/* Return 1 if the reply matches the specified integer, otherwise log errors
 * in the server log and return 0. */
int TestAssertIntegerReply(ValkeyModuleCtx *ctx, ValkeyModuleCallReply *reply, long long expected) {
    if (ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ERROR) {
        ValkeyModule_Log(ctx,"warning","Test error reply: %s",
            ValkeyModule_CallReplyStringPtr(reply, NULL));
        return 0;
    } else if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_INTEGER) {
        ValkeyModule_Log(ctx,"warning","Unexpected reply type %d",
            ValkeyModule_CallReplyType(reply));
        return 0;
    }
    long long val = ValkeyModule_CallReplyInteger(reply);
    if (val != expected) {
        ValkeyModule_Log(ctx,"warning",
            "Unexpected integer reply '%lld' (instead of '%lld')",
            val, expected);
        return 0;
    }
    return 1;
}

#define T(name,...) \
    do { \
        ValkeyModule_Log(ctx,"warning","Testing %s", name); \
        reply = ValkeyModule_Call(ctx,name,__VA_ARGS__); \
    } while (0)

/* TEST.BASICS -- Run all the tests.
 * Note: it is useful to run these tests from the module rather than TCL
 * since it's easier to check the reply types like that (make a distinction
 * between 0 and "0", etc. */
int TestBasics(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;

    /* Make sure the DB is empty before to proceed. */
    T("dbsize","");
    if (!TestAssertIntegerReply(ctx,reply,0)) goto fail;

    T("ping","");
    if (!TestAssertStringReply(ctx,reply,"PONG",4)) goto fail;

    T("test.call","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.callresp3map","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.callresp3set","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.callresp3double","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.callresp3bool","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.callresp3null","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.callreplywithnestedreply","");
    if (!TestAssertStringReply(ctx,reply,"test",4)) goto fail;

    T("test.callreplywithbignumberreply","");
    if (!TestAssertStringReply(ctx,reply,"1234567999999999999999999999999999999",37)) goto fail;

    T("test.callreplywithverbatimstringreply","");
    if (!TestAssertStringReply(ctx,reply,"txt:This is a verbatim\nstring",29)) goto fail;

    T("test.ctxflags","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.string.append","");
    if (!TestAssertStringReply(ctx,reply,"foobar",6)) goto fail;

    T("test.string.truncate","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.unlink","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.nestedcallreplyarray","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.string.append.am","");
    if (!TestAssertStringReply(ctx,reply,"foobar",6)) goto fail;
    
    T("test.string.trim","");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.string.printf", "cc", "foo", "bar");
    if (!TestAssertStringReply(ctx,reply,"Got 3 args. argv[1]: foo, argv[2]: bar",38)) goto fail;

    T("test.notify", "");
    if (!TestAssertStringReply(ctx,reply,"OK",2)) goto fail;

    T("test.callreplywitharrayreply", "");
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ARRAY) goto fail;
    if (ValkeyModule_CallReplyLength(reply) != 2) goto fail;
    if (!TestAssertStringReply(ctx,ValkeyModule_CallReplyArrayElement(reply, 0),"test",4)) goto fail;
    if (!TestAssertStringReply(ctx,ValkeyModule_CallReplyArrayElement(reply, 1),"1234",4)) goto fail;

    T("foo", "E");
    if (!TestAssertErrorReply(ctx,reply,"ERR unknown command 'foo', with args beginning with: ",53)) goto fail;

    T("set", "Ec", "x");
    if (!TestAssertErrorReply(ctx,reply,"ERR wrong number of arguments for 'set' command",47)) goto fail;

    T("shutdown", "SE");
    if (!TestAssertErrorReply(ctx,reply,"ERR command 'shutdown' is not allowed on script mode",52)) goto fail;

    T("set", "WEcc", "x", "1");
    if (!TestAssertErrorReply(ctx,reply,"ERR Write command 'set' was called while write is not allowed.",62)) goto fail;

    ValkeyModule_ReplyWithSimpleString(ctx,"ALL TESTS PASSED");
    return VALKEYMODULE_OK;

fail:
    ValkeyModule_ReplyWithSimpleString(ctx,
        "SOME TEST DID NOT PASS! Check server logs");
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx,"test",1,VALKEYMODULE_APIVER_1)
        == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;

    /* Perform RM_Call inside the ValkeyModule_OnLoad
     * to verify that it works as expected without crashing.
     * The tests will verify it on different configurations
     * options (cluster/no cluster). A simple ping command
     * is enough for this test. */
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "ping", "");
    if (ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_STRING) {
        ValkeyModule_FreeCallReply(reply);
        return VALKEYMODULE_ERR;
    }
    size_t len;
    const char *reply_str = ValkeyModule_CallReplyStringPtr(reply, &len);
    if (len != 4) {
        ValkeyModule_FreeCallReply(reply);
        return VALKEYMODULE_ERR;
    }
    if (memcmp(reply_str, "PONG", 4) != 0) {
        ValkeyModule_FreeCallReply(reply);
        return VALKEYMODULE_ERR;
    }
    ValkeyModule_FreeCallReply(reply);

    if (ValkeyModule_CreateCommand(ctx,"test.call",
        TestCall,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callresp3map",
        TestCallResp3Map,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callresp3attribute",
        TestCallResp3Attribute,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callresp3set",
        TestCallResp3Set,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callresp3double",
        TestCallResp3Double,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callresp3bool",
        TestCallResp3Bool,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callresp3null",
        TestCallResp3Null,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callreplywitharrayreply",
        TestCallReplyWithArrayReply,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callreplywithnestedreply",
        TestCallReplyWithNestedReply,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callreplywithbignumberreply",
        TestCallResp3BigNumber,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.callreplywithverbatimstringreply",
        TestCallResp3Verbatim,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.string.append",
        TestStringAppend,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.string.trim",
        TestTrimString,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.string.append.am",
        TestStringAppendAM,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.string.truncate",
        TestStringTruncate,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.string.printf",
        TestStringPrintf,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.ctxflags",
        TestCtxFlags,"readonly",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.unlink",
        TestUnlink,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.nestedcallreplyarray",
        TestNestedCallReplyArrayElement,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.basics",
        TestBasics,"write",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    /* the following commands are used by an external test and should not be added to TestBasics */
    if (ValkeyModule_CreateCommand(ctx,"test.rmcallautomode",
        TestCallRespAutoMode,"write",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"test.getresp",
        TestGetResp,"readonly",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    ValkeyModule_SubscribeToKeyspaceEvents(ctx,
                                            VALKEYMODULE_NOTIFY_HASH |
                                            VALKEYMODULE_NOTIFY_SET |
                                            VALKEYMODULE_NOTIFY_STRING |
                                            VALKEYMODULE_NOTIFY_KEY_MISS,
                                        NotifyCallback);
    if (ValkeyModule_CreateCommand(ctx,"test.notify",
        TestNotifications,"write deny-oom",1,1,1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "test.latency", TestLatency, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}
