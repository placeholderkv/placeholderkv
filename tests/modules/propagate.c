/* This module is used to test the propagation (replication + AOF) of
 * commands, via the ValkeyModule_Replicate() interface, in asynchronous
 * contexts, such as callbacks not implementing commands, and thread safe
 * contexts.
 *
 * We create a timer callback and a threads using a thread safe context.
 * Using both we try to propagate counters increments, and later we check
 * if the replica contains the changes as expected.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2019, Redis Ltd.
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
#include <pthread.h>
#include <errno.h>

#define UNUSED(V) ((void) V)

ValkeyModuleCtx *detached_ctx = NULL;

static int KeySpace_NotificationGeneric(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);
    VALKEYMODULE_NOT_USED(event);
    VALKEYMODULE_NOT_USED(key);

    ValkeyModuleCallReply* rep = ValkeyModule_Call(ctx, "INCR", "c!", "notifications");
    ValkeyModule_FreeCallReply(rep);

    return VALKEYMODULE_OK;
}

/* Timer callback. */
void timerHandler(ValkeyModuleCtx *ctx, void *data) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(data);

    static int times = 0;

    ValkeyModule_Replicate(ctx,"INCR","c","timer");
    times++;

    if (times < 3)
        ValkeyModule_CreateTimer(ctx,100,timerHandler,NULL);
    else
        times = 0;
}

int propagateTestTimerCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModuleTimerID timer_id =
        ValkeyModule_CreateTimer(ctx,100,timerHandler,NULL);
    VALKEYMODULE_NOT_USED(timer_id);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

/* Timer callback. */
void timerNestedHandler(ValkeyModuleCtx *ctx, void *data) {
    int repl = (long long)data;

    /* The goal is the trigger a module command that calls RM_Replicate
     * in order to test MULTI/EXEC structure */
    ValkeyModule_Replicate(ctx,"INCRBY","cc","timer-nested-start","1");
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx,"propagate-test.nested", repl? "!" : "");
    ValkeyModule_FreeCallReply(reply);
    reply = ValkeyModule_Call(ctx, "INCR", repl? "c!" : "c", "timer-nested-middle");
    ValkeyModule_FreeCallReply(reply);
    ValkeyModule_Replicate(ctx,"INCRBY","cc","timer-nested-end","1");
}

int propagateTestTimerNestedCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModuleTimerID timer_id =
        ValkeyModule_CreateTimer(ctx,100,timerNestedHandler,(void*)0);
    VALKEYMODULE_NOT_USED(timer_id);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

int propagateTestTimerNestedReplCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModuleTimerID timer_id =
        ValkeyModule_CreateTimer(ctx,100,timerNestedHandler,(void*)1);
    VALKEYMODULE_NOT_USED(timer_id);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

void timerHandlerMaxmemory(ValkeyModuleCtx *ctx, void *data) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(data);

    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx,"SETEX","ccc!","timer-maxmemory-volatile-start","100","1");
    ValkeyModule_FreeCallReply(reply);
    reply = ValkeyModule_Call(ctx, "CONFIG", "ccc!", "SET", "maxmemory", "1");
    ValkeyModule_FreeCallReply(reply);

    ValkeyModule_Replicate(ctx, "INCR", "c", "timer-maxmemory-middle");

    reply = ValkeyModule_Call(ctx,"SETEX","ccc!","timer-maxmemory-volatile-end","100","1");
    ValkeyModule_FreeCallReply(reply);
}

int propagateTestTimerMaxmemoryCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModuleTimerID timer_id =
        ValkeyModule_CreateTimer(ctx,100,timerHandlerMaxmemory,(void*)1);
    VALKEYMODULE_NOT_USED(timer_id);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

void timerHandlerEval(ValkeyModuleCtx *ctx, void *data) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(data);

    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx,"INCRBY","cc!","timer-eval-start","1");
    ValkeyModule_FreeCallReply(reply);
    reply = ValkeyModule_Call(ctx, "EVAL", "cccc!", "redis.call('set',KEYS[1],ARGV[1])", "1", "foo", "bar");
    ValkeyModule_FreeCallReply(reply);

    ValkeyModule_Replicate(ctx, "INCR", "c", "timer-eval-middle");

    reply = ValkeyModule_Call(ctx,"INCRBY","cc!","timer-eval-end","1");
    ValkeyModule_FreeCallReply(reply);
}

int propagateTestTimerEvalCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModuleTimerID timer_id =
        ValkeyModule_CreateTimer(ctx,100,timerHandlerEval,(void*)1);
    VALKEYMODULE_NOT_USED(timer_id);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

/* The thread entry point. */
void *threadMain(void *arg) {
    VALKEYMODULE_NOT_USED(arg);
    ValkeyModuleCtx *ctx = ValkeyModule_GetThreadSafeContext(NULL);
    ValkeyModule_SelectDb(ctx,9); /* Tests ran in database number 9. */
    for (int i = 0; i < 3; i++) {
        ValkeyModule_ThreadSafeContextLock(ctx);
        ValkeyModule_Replicate(ctx,"INCR","c","a-from-thread");
        ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx,"INCR","c!","thread-call");
        ValkeyModule_FreeCallReply(reply);
        ValkeyModule_Replicate(ctx,"INCR","c","b-from-thread");
        ValkeyModule_ThreadSafeContextUnlock(ctx);
    }
    ValkeyModule_FreeThreadSafeContext(ctx);
    return NULL;
}

int propagateTestThreadCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    pthread_t tid;
    if (pthread_create(&tid,NULL,threadMain,NULL) != 0)
        return ValkeyModule_ReplyWithError(ctx,"-ERR Can't start thread");
    VALKEYMODULE_NOT_USED(tid);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

/* The thread entry point. */
void *threadDetachedMain(void *arg) {
    VALKEYMODULE_NOT_USED(arg);
    ValkeyModule_SelectDb(detached_ctx,9); /* Tests ran in database number 9. */

    ValkeyModule_ThreadSafeContextLock(detached_ctx);
    ValkeyModule_Replicate(detached_ctx,"INCR","c","thread-detached-before");
    ValkeyModuleCallReply *reply = ValkeyModule_Call(detached_ctx,"INCR","c!","thread-detached-1");
    ValkeyModule_FreeCallReply(reply);
    reply = ValkeyModule_Call(detached_ctx,"INCR","c!","thread-detached-2");
    ValkeyModule_FreeCallReply(reply);
    ValkeyModule_Replicate(detached_ctx,"INCR","c","thread-detached-after");
    ValkeyModule_ThreadSafeContextUnlock(detached_ctx);

    return NULL;
}

int propagateTestDetachedThreadCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    pthread_t tid;
    if (pthread_create(&tid,NULL,threadDetachedMain,NULL) != 0)
        return ValkeyModule_ReplyWithError(ctx,"-ERR Can't start thread");
    VALKEYMODULE_NOT_USED(tid);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

int propagateTestSimpleCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    /* Replicate two commands to test MULTI/EXEC wrapping. */
    ValkeyModule_Replicate(ctx,"INCR","c","counter-1");
    ValkeyModule_Replicate(ctx,"INCR","c","counter-2");
    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

int propagateTestMixedCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    ValkeyModuleCallReply *reply;

    /* This test mixes multiple propagation systems. */
    reply = ValkeyModule_Call(ctx, "INCR", "c!", "using-call");
    ValkeyModule_FreeCallReply(reply);

    ValkeyModule_Replicate(ctx,"INCR","c","counter-1");
    ValkeyModule_Replicate(ctx,"INCR","c","counter-2");

    reply = ValkeyModule_Call(ctx, "INCR", "c!", "after-call");
    ValkeyModule_FreeCallReply(reply);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

int propagateTestNestedCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    ValkeyModuleCallReply *reply;

    /* This test mixes multiple propagation systems. */
    reply = ValkeyModule_Call(ctx, "INCR", "c!", "using-call");
    ValkeyModule_FreeCallReply(reply);

    reply = ValkeyModule_Call(ctx,"propagate-test.simple", "!");
    ValkeyModule_FreeCallReply(reply);

    ValkeyModule_Replicate(ctx,"INCR","c","counter-3");
    ValkeyModule_Replicate(ctx,"INCR","c","counter-4");

    reply = ValkeyModule_Call(ctx, "INCR", "c!", "after-call");
    ValkeyModule_FreeCallReply(reply);

    reply = ValkeyModule_Call(ctx, "INCR", "c!", "before-call-2");
    ValkeyModule_FreeCallReply(reply);

    reply = ValkeyModule_Call(ctx, "keyspace.incr_case1", "c!", "asdf"); /* Propagates INCR */
    ValkeyModule_FreeCallReply(reply);

    reply = ValkeyModule_Call(ctx, "keyspace.del_key_copy", "c!", "asdf"); /* Propagates DEL */
    ValkeyModule_FreeCallReply(reply);

    reply = ValkeyModule_Call(ctx, "INCR", "c!", "after-call-2");
    ValkeyModule_FreeCallReply(reply);

    ValkeyModule_ReplyWithSimpleString(ctx,"OK");
    return VALKEYMODULE_OK;
}

/* Counter to track "propagate-test.incr" commands which were obeyed (due to being replicated or processed from AOF). */
static long long obeyed_cmds = 0;

/* Handles the "propagate-test.obeyed" command to return the `obeyed_cmds` count. */
int propagateTestObeyed(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    ValkeyModule_ReplyWithLongLong(ctx, obeyed_cmds);
    return VALKEYMODULE_OK;
}

int propagateTestIncr(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
{
    VALKEYMODULE_NOT_USED(argc);
    ValkeyModuleCallReply *reply;

    /* Track the number of commands which are "obeyed". */
    if (ValkeyModule_MustObeyClient(ctx)) {
        obeyed_cmds += 1;
    }
    /* This test propagates the module command, not the INCR it executes. */
    reply = ValkeyModule_Call(ctx, "INCR", "s", argv[1]);
    ValkeyModule_ReplyWithCallReply(ctx,reply);
    ValkeyModule_FreeCallReply(reply);
    ValkeyModule_ReplicateVerbatim(ctx);
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx,"propagate-test",1,VALKEYMODULE_APIVER_1)
            == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;

    detached_ctx = ValkeyModule_GetDetachedThreadSafeContext(ctx);

    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_ALL, KeySpace_NotificationGeneric) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.timer",
                propagateTestTimerCommand,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.timer-nested",
                propagateTestTimerNestedCommand,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.timer-nested-repl",
                propagateTestTimerNestedReplCommand,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.timer-maxmemory",
                propagateTestTimerMaxmemoryCommand,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.timer-eval",
                propagateTestTimerEvalCommand,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.thread",
                propagateTestThreadCommand,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.detached-thread",
                propagateTestDetachedThreadCommand,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.simple",
                propagateTestSimpleCommand,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.mixed",
                propagateTestMixedCommand,
                "write",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.nested",
                propagateTestNestedCommand,
                "write",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.incr",
                propagateTestIncr,
                "write",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx,"propagate-test.obeyed",
                propagateTestObeyed,
                "",1,1,1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    UNUSED(ctx);

    if (detached_ctx)
        ValkeyModule_FreeThreadSafeContext(detached_ctx);

    return VALKEYMODULE_OK;
}
