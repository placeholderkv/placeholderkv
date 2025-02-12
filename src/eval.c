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

/*
 * This file initializes the global LUA object and registers functions to call Valkey API from within the LUA language.
 * It heavily invokes LUA's C API documented at https://www.lua.org/pil/24.html. There are 2 entrypoint functions in
 * this file:
 * 1. evalCommand() - Gets invoked every time a user runs LUA script via eval command on Valkey.
 * 2. scriptingInit() - initServer() function from server.c invokes this to initialize LUA at startup.
 *                      It is also invoked between 2 eval invocations to reset Lua.
 */

#include "eval.h"
#include "server.h"
#include "sha1.h"
#include "rand.h"
#include "cluster.h"
#include "monotonic.h"
#include "resp_parser.h"
#include "script.h"
#include "lua/debug_lua.h"
#include "scripting_engine.h"
#include "sds.h"


void evalGenericCommandWithDebugging(client *c, int evalsha);

typedef struct evalScript {
    compiledFunction *script;
    scriptingEngine *engine;
    robj *body;
    uint64_t flags;
    listNode *node; /* list node in scripts_lru_list list. */
} evalScript;

static void dictScriptDestructor(void *val) {
    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    evalScript *es = (evalScript *)val;
    scriptingEngineCallFreeFunction(es->engine, VMSE_EVAL, es->script);
    decrRefCount(es->body);
    zfree(es);
}

static uint64_t dictStrCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char *)key, strlen((char *)key));
}

/* evalCtx.scripts sha (as sds string) -> scripts (as evalScript) cache. */
dictType shaScriptObjectDictType = {
    dictStrCaseHash,       /* hash function */
    NULL,                  /* key dup */
    dictSdsKeyCaseCompare, /* key compare */
    dictSdsDestructor,     /* key destructor */
    dictScriptDestructor,  /* val destructor */
    NULL                   /* allow to expand */
};

/* Eval context */
struct evalCtx {
    dict *scripts;                  /* A dictionary of SHA1 -> evalScript */
    list *scripts_lru_list;         /* A list of SHA1, first in first out LRU eviction. */
    unsigned long long scripts_mem; /* Cached scripts' memory + oh */
} evalCtx;

/* Initialize the scripting environment.
 *
 * This function is called the first time at server startup.
 *
 */
void evalInit(void) {
    /* Initialize a dictionary we use to map SHAs to scripts.
     *
     * Initialize a list we use for script evictions.
     * Note that we duplicate the sha when adding to the lru list due to defrag,
     * and we need to free them respectively. */
    evalCtx.scripts = dictCreate(&shaScriptObjectDictType);
    evalCtx.scripts_lru_list = listCreate();
    listSetFreeMethod(evalCtx.scripts_lru_list, sdsfreeVoid);
    evalCtx.scripts_mem = 0;
}

/* ---------------------------------------------------------------------------
 * Utility functions.
 * ------------------------------------------------------------------------- */

/* Perform the SHA1 of the input string. We use this both for hashing script
 * bodies in order to obtain the Lua function name, and in the implementation
 * of server.sha1().
 *
 * 'digest' should point to a 41 bytes buffer: 40 for SHA1 converted into an
 * hexadecimal number, plus 1 byte for null term. */
void sha1hex(char *digest, char *script, size_t len) {
    SHA1_CTX ctx;
    unsigned char hash[20];
    char *cset = "0123456789abcdef";
    int j;

    SHA1Init(&ctx);
    SHA1Update(&ctx, (unsigned char *)script, len);
    SHA1Final(hash, &ctx);

    for (j = 0; j < 20; j++) {
        digest[j * 2] = cset[((hash[j] & 0xF0) >> 4)];
        digest[j * 2 + 1] = cset[(hash[j] & 0xF)];
    }
    digest[40] = '\0';
}

/* Free lua_scripts dict and close lua interpreter. */
void freeEvalScripts(dict *scripts, list *scripts_lru_list, list *engine_callbacks) {
    dictRelease(scripts);

    listRelease(scripts_lru_list);
    if (engine_callbacks) {
        listIter *iter = listGetIterator(engine_callbacks, 0);
        listNode *node = NULL;
        while ((node = listNext(iter)) != NULL) {
            callableLazyEvalReset *callback = listNodeValue(node);
            if (callback != NULL) {
                callback->engineLazyEvalResetCallback(callback->context);
                zfree(callback);
            }
        }
        listReleaseIterator(iter);
        listRelease(engine_callbacks);
    }
}

static void resetEngineEvalEnvCallback(scriptingEngine *engine, void *context) {
    int async = context != NULL;
    callableLazyEvalReset *callback = scriptingEngineCallResetEvalEnvFunc(engine, async);

    if (async) {
        list *callbacks = context;
        listAddNodeTail(callbacks, callback);
    }
}

/* Release resources related to Lua scripting.
 * This function is used in order to reset the scripting environment. */
void evalRelease(int async) {
    if (async) {
        list *engine_callbacks = listCreate();
        scriptingEngineManagerForEachEngine(resetEngineEvalEnvCallback, engine_callbacks);
        freeEvalScriptsAsync(evalCtx.scripts, evalCtx.scripts_lru_list, engine_callbacks);

    } else {
        freeEvalScripts(evalCtx.scripts, evalCtx.scripts_lru_list, NULL);
        scriptingEngineManagerForEachEngine(resetEngineEvalEnvCallback, NULL);
    }
}


void evalReset(int async) {
    evalRelease(async);
    evalInit();
}

/* ---------------------------------------------------------------------------
 * EVAL and SCRIPT commands implementation
 * ------------------------------------------------------------------------- */

static void evalCalcScriptHash(int evalsha, sds script, char *out_sha) {
    /* We obtain the script SHA1, then check if this function is already
     * defined into the Lua state */
    if (!evalsha) {
        /* Hash the code if this is an EVAL call */
        sha1hex(out_sha, script, sdslen(script));
    } else {
        /* We already have the SHA if it is an EVALSHA */
        int j;
        char *sha = script;

        /* Convert to lowercase. We don't use tolower since the function
         * managed to always show up in the profiler output consuming
         * a non trivial amount of time. */
        for (j = 0; j < 40; j++) out_sha[j] = (sha[j] >= 'A' && sha[j] <= 'Z') ? sha[j] + ('a' - 'A') : sha[j];
        out_sha[40] = '\0';
    }
}

/* Helper function to try and extract shebang flags from the script body.
 * If no shebang is found, return with success and COMPAT mode flag.
 * The err arg is optional, can be used to get a detailed error string.
 * The out_shebang_len arg is optional, can be used to trim the shebang from the script.
 * Returns C_OK on success, and C_ERR on error. */
int evalExtractShebangFlags(sds body,
                            char **out_engine,
                            uint64_t *out_flags,
                            ssize_t *out_shebang_len,
                            sds *err) {
    serverAssert(out_flags != NULL);

    ssize_t shebang_len = 0;
    uint64_t script_flags = SCRIPT_FLAG_EVAL_COMPAT_MODE;
    if (!strncmp(body, "#!", 2)) {
        int numparts, j;
        char *shebang_end = strchr(body, '\n');
        if (shebang_end == NULL) {
            if (err) *err = sdsnew("Invalid script shebang");
            return C_ERR;
        }
        shebang_len = shebang_end - body;
        sds shebang = sdsnewlen(body, shebang_len);
        sds *parts = sdssplitargs(shebang, &numparts);
        sdsfree(shebang);
        if (!parts || numparts == 0) {
            if (err) *err = sdsnew("Invalid engine in script shebang");
            sdsfreesplitres(parts, numparts);
            return C_ERR;
        }

        if (out_engine) {
            uint32_t engine_name_len = sdslen(parts[0]) - 2;
            *out_engine = zcalloc(engine_name_len + 1);
            valkey_strlcpy(*out_engine, parts[0] + 2, engine_name_len + 1);
        }

        script_flags &= ~SCRIPT_FLAG_EVAL_COMPAT_MODE;
        for (j = 1; j < numparts; j++) {
            if (!strncmp(parts[j], "flags=", 6)) {
                sdsrange(parts[j], 6, -1);
                int numflags, jj;
                sds *flags = sdssplitlen(parts[j], sdslen(parts[j]), ",", 1, &numflags);
                for (jj = 0; jj < numflags; jj++) {
                    scriptFlag *sf;
                    for (sf = scripts_flags_def; sf->flag; sf++) {
                        if (!strcmp(flags[jj], sf->str)) break;
                    }
                    if (!sf->flag) {
                        if (err) *err = sdscatfmt(sdsempty(), "Unexpected flag in script shebang: %s", flags[jj]);
                        sdsfreesplitres(flags, numflags);
                        sdsfreesplitres(parts, numparts);
                        return C_ERR;
                    }
                    script_flags |= sf->flag;
                }
                sdsfreesplitres(flags, numflags);
            } else {
                /* We only support function flags options for lua scripts */
                if (err) *err = sdscatfmt(sdsempty(), "Unknown lua shebang option: %s", parts[j]);
                sdsfreesplitres(parts, numparts);
                return C_ERR;
            }
        }
        sdsfreesplitres(parts, numparts);
    } else {
        // When no shebang is declared, assume the engine is LUA.
        if (out_engine) {
            *out_engine = zstrdup("lua");
        }
    }
    if (out_shebang_len) *out_shebang_len = shebang_len;
    *out_flags = script_flags;
    return C_OK;
}

/* Try to extract command flags if we can, returns the modified flags.
 * Note that it does not guarantee the command arguments are right. */
uint64_t evalGetCommandFlags(client *c, uint64_t cmd_flags) {
    char sha[41];
    int evalsha = c->cmd->proc == evalShaCommand || c->cmd->proc == evalShaRoCommand;
    if (evalsha && sdslen(c->argv[1]->ptr) != 40) return cmd_flags;
    uint64_t script_flags;
    evalCalcScriptHash(evalsha, c->argv[1]->ptr, sha);
    c->cur_script = dictFind(evalCtx.scripts, sha);
    if (!c->cur_script) {
        if (evalsha) return cmd_flags;
        if (evalExtractShebangFlags(c->argv[1]->ptr, NULL, &script_flags, NULL, NULL) == C_ERR) return cmd_flags;
    } else {
        evalScript *es = dictGetVal(c->cur_script);
        script_flags = es->flags;
    }
    if (script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) return cmd_flags;
    return scriptFlagsToCmdFlags(cmd_flags, script_flags);
}

/* Delete an eval script with the specified sha.
 *
 * This will delete the script from the scripting engine and delete the script
 * from server. */
static void evalDeleteScript(client *c, sds sha) {
    /* Delete the script from server. */
    dictEntry *de = dictUnlink(evalCtx.scripts, sha);
    serverAssertWithInfo(c, NULL, de);
    evalScript *es = dictGetVal(de);
    evalCtx.scripts_mem -= sdsAllocSize(sha) + getStringObjectSdsUsedMemory(es->body);
    dictFreeUnlinkedEntry(evalCtx.scripts, de);
}

/* Users who abuse EVAL will generate a new lua script on each call, which can
 * consume large amounts of memory over time. Since EVAL is mostly the one that
 * abuses the lua cache, and these won't have pipeline issues (scripts won't
 * disappear when EVALSHA needs it and cause failure), we implement script eviction
 * only for these (not for one loaded with SCRIPT LOAD). Considering that we don't
 * have many scripts, then unlike keys, we don't need to worry about the memory
 * usage of keeping a true sorted LRU linked list.
 *
 * Returns the corresponding node added, which is used to save it in scriptHolder
 * and use it for quick removal and re-insertion into an LRU list each time the
 * script is used. */
#define LRU_LIST_LENGTH 500
static listNode *scriptsLRUAdd(client *c, sds sha) {
    /* Evict oldest. */
    while (listLength(evalCtx.scripts_lru_list) >= LRU_LIST_LENGTH) {
        listNode *ln = listFirst(evalCtx.scripts_lru_list);
        sds oldest = listNodeValue(ln);
        evalDeleteScript(c, oldest);
        listDelNode(evalCtx.scripts_lru_list, ln);
        server.stat_evictedscripts++;
    }

    /* Add current. */
    listAddNodeTail(evalCtx.scripts_lru_list, sdsdup(sha));
    return listLast(evalCtx.scripts_lru_list);
}

static int evalRegisterNewScript(client *c, robj *body, char **sha) {
    serverAssert(sha != NULL);

    /* When `*sha` is `NULL`, it's because we're coming from the SCRIPT LOAD
     * code path, and therefore we need to compute the hash of the script. */
    int is_script_load = *sha == NULL;

    if (is_script_load) {
        *sha = (char *)zcalloc(41);
        evalCalcScriptHash(0, body->ptr, *sha);

        /* If the script was previously added via EVAL, we promote it to
         * SCRIPT LOAD, prevent it from being evicted later. */
        dictEntry *entry = dictFind(evalCtx.scripts, *sha);
        if (entry != NULL) {
            evalScript *es = dictGetVal(entry);
            if (es->node) {
                listDelNode(evalCtx.scripts_lru_list, es->node);
                es->node = NULL;
            }

            return C_OK;
        }
    }

    /* Handle shebang header in script code */
    ssize_t shebang_len = 0;
    uint64_t script_flags;
    sds err = NULL;
    char *engine_name = NULL;
    if (evalExtractShebangFlags(body->ptr, &engine_name, &script_flags, &shebang_len, &err) == C_ERR) {
        if (c != NULL) {
            addReplyErrorSds(c, err);
        }
        if (engine_name) {
            zfree(engine_name);
        }
        if (is_script_load) {
            zfree(*sha);
            *sha = NULL;
        }
        return C_ERR;
    }

    serverAssert(engine_name != NULL);
    scriptingEngine *engine = scriptingEngineManagerFind(engine_name);
    if (!engine) {
        if (c != NULL) {
            addReplyErrorFormat(c, "Could not find scripting engine '%s'", engine_name);
        }
        zfree(engine_name);
        if (is_script_load) {
            zfree(*sha);
            *sha = NULL;
        }
        return C_ERR;
    }
    zfree(engine_name);

    robj *_err = NULL;
    size_t num_compiled_functions = 0;
    compiledFunction **functions =
        scriptingEngineCallCompileCode(engine,
                                       VMSE_EVAL,
                                       (sds)body->ptr + shebang_len,
                                       0,
                                       &num_compiled_functions,
                                       &_err);
    if (functions == NULL) {
        serverAssert(_err != NULL);
        if (c != NULL) {
            addReplyErrorFormat(c, "%s", (char *)_err->ptr);
        }
        decrRefCount(_err);
        if (is_script_load) {
            zfree(*sha);
            *sha = NULL;
        }
        return C_ERR;
    }

    serverAssert(num_compiled_functions == 1);

    /* We also save a SHA1 -> Original script map in a dictionary
     * so that we can replicate / write in the AOF all the
     * EVALSHA commands as EVAL using the original script. */
    evalScript *es = zcalloc(sizeof(evalScript));
    es->script = functions[0];
    es->engine = engine;
    es->flags = script_flags;
    sds _sha = sdsnew(*sha);
    if (!is_script_load) {
        /* Script eviction only applies to EVAL, not SCRIPT LOAD. */
        es->node = scriptsLRUAdd(c, _sha);
    }
    es->body = body;
    int retval = dictAdd(evalCtx.scripts, _sha, es);
    serverAssertWithInfo(c ? c : scriptingEngineGetClient(engine), NULL, retval == DICT_OK);
    evalCtx.scripts_mem += sdsAllocSize(_sha) + getStringObjectSdsUsedMemory(body);
    incrRefCount(body);
    zfree(functions);

    return C_OK;
}

static void evalGenericCommand(client *c, int evalsha) {
    char sha[41];
    long long numkeys;

    /* Get the number of arguments that are keys */
    if (getLongLongFromObjectOrReply(c, c->argv[2], &numkeys, NULL) != C_OK) return;
    if (numkeys > (c->argc - 3)) {
        addReplyError(c, "Number of keys can't be greater than number of args");
        return;
    } else if (numkeys < 0) {
        addReplyError(c, "Number of keys can't be negative");
        return;
    }

    if (c->cur_script) {
        memcpy(sha, dictGetKey(c->cur_script), 40);
        sha[40] = '\0';
    } else {
        evalCalcScriptHash(evalsha, c->argv[1]->ptr, sha);
    }

    dictEntry *entry = dictFind(evalCtx.scripts, sha);

    if (evalsha && entry == NULL) {
        /* Calling EVALSHA using an hash that was never added to the scripts
         * cache. */
        addReplyErrorObject(c, shared.noscripterr);
        return;
    }

    if (entry == NULL) {
        robj *body = c->argv[1];
        char *_sha = sha;
        if (evalRegisterNewScript(c, body, &_sha) != C_OK) {
            return;
        }
        entry = dictFind(evalCtx.scripts, sha);
        serverAssert(entry != NULL);
    }

    evalScript *es = dictGetVal(entry);
    int ro = c->cmd->proc == evalRoCommand || c->cmd->proc == evalShaRoCommand;

    scriptRunCtx rctx;
    if (scriptPrepareForRun(&rctx, scriptingEngineGetClient(es->engine), c, sha, es->flags, ro) != C_OK) {
        return;
    }
    rctx.flags |= SCRIPT_EVAL_MODE; /* mark the current run as EVAL (as opposed to FCALL) so we'll
                                      get appropriate error messages and logs */

    scriptingEngineCallFunction(es->engine,
                                &rctx,
                                c,
                                es->script,
                                VMSE_EVAL,
                                c->argv + 3,
                                numkeys,
                                c->argv + 3 + numkeys,
                                c->argc - 3 - numkeys);
    scriptResetRun(&rctx);

    if (es->node) {
        /* Quick removal and re-insertion after the script is called to
         * maintain the LRU list. */
        listUnlinkNode(evalCtx.scripts_lru_list, es->node);
        listLinkNodeTail(evalCtx.scripts_lru_list, es->node);
    }
}

void evalCommand(client *c) {
    /* Explicitly feed monitor here so that lua commands appear after their
     * script command. */
    replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc);
    if (!c->flag.lua_debug)
        evalGenericCommand(c, 0);
    else
        evalGenericCommandWithDebugging(c, 0);
}

void evalRoCommand(client *c) {
    evalCommand(c);
}

void evalShaCommand(client *c) {
    /* Explicitly feed monitor here so that lua commands appear after their
     * script command. */
    replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc);
    if (sdslen(c->argv[1]->ptr) != 40) {
        /* We know that a match is not possible if the provided SHA is
         * not the right length. So we return an error ASAP, this way
         * evalGenericCommand() can be implemented without string length
         * sanity check */
        addReplyErrorObject(c, shared.noscripterr);
        return;
    }
    if (!c->flag.lua_debug)
        evalGenericCommand(c, 1);
    else {
        addReplyError(c, "Please use EVAL instead of EVALSHA for debugging");
        return;
    }
}

void evalShaRoCommand(client *c) {
    evalShaCommand(c);
}

void scriptCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "help")) {
        const char *help[] = {
            "DEBUG (YES|SYNC|NO)",
            "    Set the debug mode for subsequent scripts executed.",
            "EXISTS <sha1> [<sha1> ...]",
            "    Return information about the existence of the scripts in the script cache.",
            "FLUSH [ASYNC|SYNC]",
            "    Flush the Lua scripts cache. Very dangerous on replicas.",
            "    When called without the optional mode argument, the behavior is determined",
            "     by the lazyfree-lazy-user-flush configuration directive. Valid modes are:",
            "    * ASYNC: Asynchronously flush the scripts cache.",
            "    * SYNC: Synchronously flush the scripts cache.",
            "KILL",
            "    Kill the currently executing Lua script.",
            "LOAD <script>",
            "    Load a script into the scripts cache without executing it.",
            "SHOW <sha1>",
            "    Show a script from the scripts cache.",
            NULL,
        };
        addReplyHelp(c, help);
    } else if (c->argc >= 2 && !strcasecmp(c->argv[1]->ptr, "flush")) {
        int async = 0;
        if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr, "sync")) {
            async = 0;
        } else if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr, "async")) {
            async = 1;
        } else if (c->argc == 2) {
            async = server.lazyfree_lazy_user_flush ? 1 : 0;
        } else {
            addReplyError(c, "SCRIPT FLUSH only support SYNC|ASYNC option");
            return;
        }
        evalReset(async);
        addReply(c, shared.ok);
    } else if (c->argc >= 2 && !strcasecmp(c->argv[1]->ptr, "exists")) {
        int j;

        addReplyArrayLen(c, c->argc - 2);
        for (j = 2; j < c->argc; j++) {
            if (dictFind(evalCtx.scripts, c->argv[j]->ptr))
                addReply(c, shared.cone);
            else
                addReply(c, shared.czero);
        }
    } else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "load")) {
        char *sha = NULL;
        if (evalRegisterNewScript(c, c->argv[2], &sha) != C_OK) {
            serverAssert(sha == NULL);
            return;
        }
        addReplyBulkCBuffer(c, sha, 40);
        zfree(sha);
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "kill")) {
        scriptKill(c, 1);
    } else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "debug")) {
        if (clientHasPendingReplies(c)) {
            addReplyError(c, "SCRIPT DEBUG must be called outside a pipeline");
            return;
        }
        if (!strcasecmp(c->argv[2]->ptr, "no")) {
            ldbDisable(c);
            addReply(c, shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr, "yes")) {
            ldbEnable(c);
            addReply(c, shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr, "sync")) {
            ldbEnable(c);
            addReply(c, shared.ok);
            c->flag.lua_debug_sync = 1;
        } else {
            addReplyError(c, "Use SCRIPT DEBUG YES/SYNC/NO");
            return;
        }
    } else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "show")) {
        dictEntry *de;
        evalScript *es;

        if (sdslen(c->argv[2]->ptr) == 40 && (de = dictFind(evalCtx.scripts, c->argv[2]->ptr))) {
            es = dictGetVal(de);
            addReplyBulk(c, es->body);
        } else {
            addReplyErrorObject(c, shared.noscripterr);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

static void getEngineUsedMemory(scriptingEngine *engine, void *context) {
    size_t *sum = (size_t *)context;
    engineMemoryInfo mem_info = scriptingEngineCallGetMemoryInfo(engine, VMSE_EVAL);
    *sum += mem_info.used_memory;
}

unsigned long evalMemory(void) {
    size_t memory = 0;
    scriptingEngineManagerForEachEngine(getEngineUsedMemory, &memory);
    return memory;
}

dict *evalScriptsDict(void) {
    return evalCtx.scripts;
}

unsigned long evalScriptsMemory(void) {
    return evalCtx.scripts_mem +
           dictMemUsage(evalCtx.scripts) +
           dictSize(evalCtx.scripts) * sizeof(evalScript) +
           listLength(evalCtx.scripts_lru_list) * sizeof(listNode);
}

/* Wrapper for EVAL / EVALSHA that enables debugging, and makes sure
 * that when EVAL returns, whatever happened, the session is ended. */
void evalGenericCommandWithDebugging(client *c, int evalsha) {
    if (ldbStartSession(c)) {
        evalGenericCommand(c, evalsha);
        ldbEndSession(c);
    } else {
        ldbDisable(c);
    }
}

/* Defrag helper for EVAL scripts
 *
 * returns NULL in case the allocation wasn't moved.
 * when it returns a non-null value, the old pointer was already released
 * and should NOT be accessed. */
void *evalActiveDefragScript(void *ptr) {
    evalScript *es = ptr;
    void *ret = NULL;

    compiledFunction *func = es->script;
    if ((func = activeDefragAlloc(func))) {
        es->script = func;
    }

    /* try to defrag script struct */
    if ((ret = activeDefragAlloc(es))) {
        es = ret;
    }

    /* try to defrag actual script object */
    robj *ob = activeDefragStringOb(es->body);
    if (ob) es->body = ob;

    return ret;
}
