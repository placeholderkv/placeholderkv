/*
 * Copyright (c) 2021, Redis Ltd.
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

#include "functions.h"
#include "sds.h"
#include "dict.h"
#include "adlist.h"

#define LOAD_TIMEOUT_MS 500

typedef enum {
    restorePolicy_Flush,
    restorePolicy_Append,
    restorePolicy_Replace
} restorePolicy;

/* Forward declaration */
static void engineFunctionDispose(void *obj);
static void engineStatsDispose(void *obj);
static void engineLibraryDispose(void *obj);
static int functionsVerifyName(sds name);

typedef struct functionsLibEngineStats {
    size_t n_lib;
    size_t n_functions;
} functionsLibEngineStats;

struct functionsLibCtx {
    dict *libraries;     /* Library name -> Library object */
    dict *functions;     /* Function name -> Function object that can be used to run the function */
    size_t cache_memory; /* Overhead memory (structs, dictionaries, ..) used by all the functions */
    dict *engines_stats; /* Per engine statistics */
};

typedef struct functionsLibMetaData {
    sds engine;
    sds name;
    sds code;
} functionsLibMetaData;

dictType functionDictType = {
    dictSdsCaseHash,       /* hash function */
    dictSdsDup,            /* key dup */
    dictSdsKeyCaseCompare, /* key compare */
    dictSdsDestructor,     /* key destructor */
    NULL,                  /* val destructor */
    NULL                   /* allow to expand */
};

dictType engineStatsDictType = {
    dictSdsCaseHash,       /* hash function */
    dictSdsDup,            /* key dup */
    dictSdsKeyCaseCompare, /* key compare */
    dictSdsDestructor,     /* key destructor */
    engineStatsDispose,    /* val destructor */
    NULL                   /* allow to expand */
};

dictType libraryFunctionDictType = {
    dictSdsHash,           /* hash function */
    dictSdsDup,            /* key dup */
    dictSdsKeyCompare,     /* key compare */
    dictSdsDestructor,     /* key destructor */
    engineFunctionDispose, /* val destructor */
    NULL                   /* allow to expand */
};

dictType librariesDictType = {
    dictSdsHash,          /* hash function */
    dictSdsDup,           /* key dup */
    dictSdsKeyCompare,    /* key compare */
    dictSdsDestructor,    /* key destructor */
    engineLibraryDispose, /* val destructor */
    NULL                  /* allow to expand */
};

/* Libraries Ctx. */
static functionsLibCtx *curr_functions_lib_ctx = NULL;

static size_t functionMallocSize(functionInfo *fi) {
    return zmalloc_size(fi) +
           sdsAllocSize(fi->name) +
           (fi->desc ? sdsAllocSize(fi->desc) : 0) +
           scriptingEngineCallGetFunctionMemoryOverhead(fi->li->engine, fi->function);
}

static size_t libraryMallocSize(functionLibInfo *li) {
    return zmalloc_size(li) + sdsAllocSize(li->name) + sdsAllocSize(li->code);
}

static void engineStatsDispose(void *obj) {
    functionsLibEngineStats *stats = obj;
    zfree(stats);
}

/* Dispose function memory */
static void engineFunctionDispose(void *obj) {
    if (!obj) {
        return;
    }
    functionInfo *fi = obj;
    sdsfree(fi->name);
    if (fi->desc) {
        sdsfree(fi->desc);
    }

    scriptingEngineCallFreeFunction(fi->li->engine, fi->function);
    zfree(fi);
}

static void engineLibraryFree(functionLibInfo *li) {
    if (!li) {
        return;
    }
    dictRelease(li->functions);
    sdsfree(li->name);
    sdsfree(li->code);
    zfree(li);
}

static void engineLibraryDispose(void *obj) {
    engineLibraryFree(obj);
}

/* Clear all the functions from the given library ctx */
void functionsLibCtxClear(functionsLibCtx *lib_ctx, void(callback)(dict *)) {
    dictEmpty(lib_ctx->functions, callback);
    dictEmpty(lib_ctx->libraries, callback);
    dictIterator *iter = dictGetIterator(lib_ctx->engines_stats);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionsLibEngineStats *stats = dictGetVal(entry);
        stats->n_functions = 0;
        stats->n_lib = 0;
    }
    dictReleaseIterator(iter);
    lib_ctx->cache_memory = 0;
}

void functionsLibCtxClearCurrent(int async, void(callback)(dict *)) {
    if (async) {
        functionsLibCtx *old_l_ctx = curr_functions_lib_ctx;
        curr_functions_lib_ctx = functionsLibCtxCreate();
        freeFunctionsAsync(old_l_ctx);
    } else {
        functionsLibCtxClear(curr_functions_lib_ctx, callback);
    }
}

/* Free the given functions ctx */
static void functionsLibCtxFreeGeneric(functionsLibCtx *functions_lib_ctx, int async) {
    if (async) {
        freeFunctionsAsync(functions_lib_ctx);
    } else {
        functionsLibCtxFree(functions_lib_ctx);
    }
}

/* Free the given functions ctx */
void functionsLibCtxFree(functionsLibCtx *functions_lib_ctx) {
    functionsLibCtxClear(functions_lib_ctx, NULL);
    dictRelease(functions_lib_ctx->functions);
    dictRelease(functions_lib_ctx->libraries);
    dictRelease(functions_lib_ctx->engines_stats);
    zfree(functions_lib_ctx);
}

/* Swap the current functions ctx with the given one.
 * Free the old functions ctx. */
void functionsLibCtxSwapWithCurrent(functionsLibCtx *new_lib_ctx, int async) {
    functionsLibCtxFreeGeneric(curr_functions_lib_ctx, async);
    curr_functions_lib_ctx = new_lib_ctx;
}

/* return the current functions ctx */
functionsLibCtx *functionsLibCtxGetCurrent(void) {
    return curr_functions_lib_ctx;
}

static void initializeFunctionsLibEngineStats(scriptingEngine *engine,
                                              void *context) {
    functionsLibCtx *lib_ctx = (functionsLibCtx *)context;
    functionsLibEngineStats *stats = zcalloc(sizeof(*stats));
    dictAdd(lib_ctx->engines_stats, scriptingEngineGetName(engine), stats);
}

/* Create a new functions ctx */
functionsLibCtx *functionsLibCtxCreate(void) {
    functionsLibCtx *ret = zmalloc(sizeof(functionsLibCtx));
    ret->libraries = dictCreate(&librariesDictType);
    ret->functions = dictCreate(&functionDictType);
    ret->engines_stats = dictCreate(&engineStatsDictType);
    scriptingEngineManagerForEachEngine(initializeFunctionsLibEngineStats, ret);
    ret->cache_memory = 0;
    return ret;
}

void functionsAddEngineStats(sds engine_name) {
    serverAssert(curr_functions_lib_ctx != NULL);
    dictEntry *entry = dictFind(curr_functions_lib_ctx->engines_stats, engine_name);
    if (entry == NULL) {
        functionsLibEngineStats *stats = zcalloc(sizeof(*stats));
        dictAdd(curr_functions_lib_ctx->engines_stats, engine_name, stats);
    }
}

/*
 * Creating a function inside the given library.
 * On success, return C_OK.
 * On error, return C_ERR and set err output parameter with a relevant error message.
 *
 * Note: the code assumes 'name' is NULL terminated but not require it to be binary safe.
 *       the function will verify that the given name is following the naming format
 *       and return an error if its not.
 */
static int functionLibCreateFunction(robj *name,
                                     void *function,
                                     functionLibInfo *li,
                                     robj *desc,
                                     uint64_t f_flags,
                                     sds *err) {
    serverAssert(name->type == OBJ_STRING);
    serverAssert(desc == NULL || desc->type == OBJ_STRING);

    if (functionsVerifyName(name->ptr) != C_OK) {
        *err = sdsnew("Function names can only contain letters, numbers, or "
                      "underscores(_) and must be at least one character long");
        return C_ERR;
    }

    sds name_sds = sdsdup(name->ptr);
    if (dictFetchValue(li->functions, name_sds)) {
        *err = sdsnew("Function already exists in the library");
        sdsfree(name_sds);
        return C_ERR;
    }

    functionInfo *fi = zmalloc(sizeof(*fi));
    *fi = (functionInfo){
        .name = name_sds,
        .function = function,
        .li = li,
        .desc = desc ? sdsdup(desc->ptr) : NULL,
        .f_flags = f_flags,
    };

    int res = dictAdd(li->functions, fi->name, fi);
    serverAssert(res == DICT_OK);

    return C_OK;
}

static functionLibInfo *engineLibraryCreate(sds name, scriptingEngine *e, sds code) {
    functionLibInfo *li = zmalloc(sizeof(*li));
    *li = (functionLibInfo){
        .name = sdsdup(name),
        .functions = dictCreate(&libraryFunctionDictType),
        .engine = e,
        .code = sdsdup(code),
    };
    return li;
}

static void libraryUnlink(functionsLibCtx *lib_ctx, functionLibInfo *li) {
    dictIterator *iter = dictGetIterator(li->functions);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionInfo *fi = dictGetVal(entry);
        int ret = dictDelete(lib_ctx->functions, fi->name);
        serverAssert(ret == DICT_OK);
        lib_ctx->cache_memory -= functionMallocSize(fi);
    }
    dictReleaseIterator(iter);
    entry = dictUnlink(lib_ctx->libraries, li->name);
    dictSetVal(lib_ctx->libraries, entry, NULL);
    dictFreeUnlinkedEntry(lib_ctx->libraries, entry);
    lib_ctx->cache_memory -= libraryMallocSize(li);

    /* update stats */
    functionsLibEngineStats *stats = dictFetchValue(lib_ctx->engines_stats, scriptingEngineGetName(li->engine));
    serverAssert(stats);
    stats->n_lib--;
    stats->n_functions -= dictSize(li->functions);
}

static void libraryLink(functionsLibCtx *lib_ctx, functionLibInfo *li) {
    dictIterator *iter = dictGetIterator(li->functions);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionInfo *fi = dictGetVal(entry);
        dictAdd(lib_ctx->functions, fi->name, fi);
        lib_ctx->cache_memory += functionMallocSize(fi);
    }
    dictReleaseIterator(iter);

    dictAdd(lib_ctx->libraries, li->name, li);
    lib_ctx->cache_memory += libraryMallocSize(li);

    /* update stats */
    functionsLibEngineStats *stats = dictFetchValue(lib_ctx->engines_stats, scriptingEngineGetName(li->engine));
    serverAssert(stats);
    stats->n_lib++;
    stats->n_functions += dictSize(li->functions);
}

/* Takes all libraries from lib_ctx_src and add to lib_ctx_dst.
 * On collision, if 'replace' argument is true, replace the existing library with the new one.
 * Otherwise abort and leave 'lib_ctx_dst' and 'lib_ctx_src' untouched.
 * Return C_OK on success and C_ERR if aborted. If C_ERR is returned, set a relevant
 * error message on the 'err' out parameter.
 *  */
static int
libraryJoin(functionsLibCtx *functions_lib_ctx_dst, functionsLibCtx *functions_lib_ctx_src, int replace, sds *err) {
    int ret = C_ERR;
    dictIterator *iter = NULL;
    /* Stores the libraries we need to replace in case a revert is required.
     * Only initialized when needed */
    list *old_libraries_list = NULL;
    dictEntry *entry = NULL;
    iter = dictGetIterator(functions_lib_ctx_src->libraries);
    while ((entry = dictNext(iter))) {
        functionLibInfo *li = dictGetVal(entry);
        functionLibInfo *old_li = dictFetchValue(functions_lib_ctx_dst->libraries, li->name);
        if (old_li) {
            if (!replace) {
                /* library already exists, failed the restore. */
                *err = sdscatfmt(sdsempty(), "Library %s already exists", li->name);
                goto done;
            } else {
                if (!old_libraries_list) {
                    old_libraries_list = listCreate();
                    listSetFreeMethod(old_libraries_list, engineLibraryDispose);
                }
                libraryUnlink(functions_lib_ctx_dst, old_li);
                listAddNodeTail(old_libraries_list, old_li);
            }
        }
    }
    dictReleaseIterator(iter);
    iter = NULL;

    /* Make sure no functions collision */
    iter = dictGetIterator(functions_lib_ctx_src->functions);
    while ((entry = dictNext(iter))) {
        functionInfo *fi = dictGetVal(entry);
        if (dictFetchValue(functions_lib_ctx_dst->functions, fi->name)) {
            *err = sdscatfmt(sdsempty(), "Function %s already exists", fi->name);
            goto done;
        }
    }
    dictReleaseIterator(iter);
    iter = NULL;

    /* No collision, it is safe to link all the new libraries. */
    iter = dictGetIterator(functions_lib_ctx_src->libraries);
    while ((entry = dictNext(iter))) {
        functionLibInfo *li = dictGetVal(entry);
        libraryLink(functions_lib_ctx_dst, li);
        dictSetVal(functions_lib_ctx_src->libraries, entry, NULL);
    }
    dictReleaseIterator(iter);
    iter = NULL;

    functionsLibCtxClear(functions_lib_ctx_src, NULL);
    if (old_libraries_list) {
        listRelease(old_libraries_list);
        old_libraries_list = NULL;
    }
    ret = C_OK;

done:
    if (iter) dictReleaseIterator(iter);
    if (old_libraries_list) {
        /* Link back all libraries on tmp_l_ctx */
        while (listLength(old_libraries_list) > 0) {
            listNode *head = listFirst(old_libraries_list);
            functionLibInfo *li = listNodeValue(head);
            listNodeValue(head) = NULL;
            libraryLink(functions_lib_ctx_dst, li);
            listDelNode(old_libraries_list, head);
        }
        listRelease(old_libraries_list);
    }
    return ret;
}

static void replyEngineStats(scriptingEngine *engine, void *context) {
    client *c = (client *)context;
    addReplyBulkCString(c, scriptingEngineGetName(engine));
    addReplyMapLen(c, 2);
    functionsLibEngineStats *e_stats =
        dictFetchValue(curr_functions_lib_ctx->engines_stats, scriptingEngineGetName(engine));
    addReplyBulkCString(c, "libraries_count");
    addReplyLongLong(c, e_stats ? e_stats->n_lib : 0);
    addReplyBulkCString(c, "functions_count");
    addReplyLongLong(c, e_stats ? e_stats->n_functions : 0);
}

void functionsRemoveLibFromEngine(scriptingEngine *engine) {
    dictIterator *iter = dictGetSafeIterator(curr_functions_lib_ctx->libraries);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionLibInfo *li = dictGetVal(entry);
        if (li->engine == engine) {
            libraryUnlink(curr_functions_lib_ctx, li);
            engineLibraryFree(li);
        }
    }
    dictReleaseIterator(iter);
}

/*
 * FUNCTION STATS
 */
void functionStatsCommand(client *c) {
    if (scriptIsRunning() && scriptIsEval()) {
        addReplyErrorObject(c, shared.slowevalerr);
        return;
    }

    addReplyMapLen(c, 2);

    addReplyBulkCString(c, "running_script");
    if (!scriptIsRunning()) {
        addReplyNull(c);
    } else {
        addReplyMapLen(c, 3);
        addReplyBulkCString(c, "name");
        addReplyBulkCString(c, scriptCurrFunction());
        addReplyBulkCString(c, "command");
        client *script_client = scriptGetCaller();
        addReplyArrayLen(c, script_client->argc);
        for (int i = 0; i < script_client->argc; ++i) {
            addReplyBulkCBuffer(c, script_client->argv[i]->ptr, sdslen(script_client->argv[i]->ptr));
        }
        addReplyBulkCString(c, "duration_ms");
        addReplyLongLong(c, scriptRunDuration());
    }

    addReplyBulkCString(c, "engines");
    addReplyMapLen(c, scriptingEngineManagerGetNumEngines());
    scriptingEngineManagerForEachEngine(replyEngineStats, c);
}

static void functionListReplyFlags(client *c, functionInfo *fi) {
    /* First count the number of flags we have */
    int flagcount = 0;
    for (scriptFlag *flag = scripts_flags_def; flag->str; ++flag) {
        if (fi->f_flags & flag->flag) {
            ++flagcount;
        }
    }

    addReplySetLen(c, flagcount);

    for (scriptFlag *flag = scripts_flags_def; flag->str; ++flag) {
        if (fi->f_flags & flag->flag) {
            addReplyStatus(c, flag->str);
        }
    }
}

/*
 * FUNCTION LIST [LIBRARYNAME PATTERN] [WITHCODE]
 *
 * Return general information about all the libraries:
 * * Library name
 * * The engine used to run the Library
 * * Functions list
 * * Library code (if WITHCODE is given)
 *
 * It is also possible to given library name pattern using
 * LIBRARYNAME argument, if given, return only libraries
 * that matches the given pattern.
 */
void functionListCommand(client *c) {
    int with_code = 0;
    sds library_name = NULL;
    for (int i = 2; i < c->argc; ++i) {
        robj *next_arg = c->argv[i];
        if (!with_code && !strcasecmp(next_arg->ptr, "withcode")) {
            with_code = 1;
            continue;
        }
        if (!library_name && !strcasecmp(next_arg->ptr, "libraryname")) {
            if (i >= c->argc - 1) {
                addReplyError(c, "library name argument was not given");
                return;
            }
            library_name = c->argv[++i]->ptr;
            continue;
        }
        addReplyErrorSds(c, sdscatfmt(sdsempty(), "Unknown argument %s", next_arg->ptr));
        return;
    }
    size_t reply_len = 0;
    void *len_ptr = NULL;
    if (library_name) {
        len_ptr = addReplyDeferredLen(c);
    } else {
        /* If no pattern is asked we know the reply len and we can just set it */
        addReplyArrayLen(c, dictSize(curr_functions_lib_ctx->libraries));
    }
    dictIterator *iter = dictGetIterator(curr_functions_lib_ctx->libraries);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionLibInfo *li = dictGetVal(entry);
        if (library_name) {
            if (!stringmatchlen(library_name, sdslen(library_name), li->name, sdslen(li->name), 1)) {
                continue;
            }
        }
        ++reply_len;
        addReplyMapLen(c, with_code ? 4 : 3);
        addReplyBulkCString(c, "library_name");
        addReplyBulkCBuffer(c, li->name, sdslen(li->name));
        addReplyBulkCString(c, "engine");
        sds engine_name = scriptingEngineGetName(li->engine);
        addReplyBulkCBuffer(c, engine_name, sdslen(engine_name));

        addReplyBulkCString(c, "functions");
        addReplyArrayLen(c, dictSize(li->functions));
        dictIterator *functions_iter = dictGetIterator(li->functions);
        dictEntry *function_entry = NULL;
        while ((function_entry = dictNext(functions_iter))) {
            functionInfo *fi = dictGetVal(function_entry);
            addReplyMapLen(c, 3);
            addReplyBulkCString(c, "name");
            addReplyBulkCBuffer(c, fi->name, sdslen(fi->name));
            addReplyBulkCString(c, "description");
            if (fi->desc) {
                addReplyBulkCBuffer(c, fi->desc, sdslen(fi->desc));
            } else {
                addReplyNull(c);
            }
            addReplyBulkCString(c, "flags");
            functionListReplyFlags(c, fi);
        }
        dictReleaseIterator(functions_iter);

        if (with_code) {
            addReplyBulkCString(c, "library_code");
            addReplyBulkCBuffer(c, li->code, sdslen(li->code));
        }
    }
    dictReleaseIterator(iter);
    if (len_ptr) {
        setDeferredArrayLen(c, len_ptr, reply_len);
    }
}

/*
 * FUNCTION DELETE <LIBRARY NAME>
 */
void functionDeleteCommand(client *c) {
    robj *function_name = c->argv[2];
    functionLibInfo *li = dictFetchValue(curr_functions_lib_ctx->libraries, function_name->ptr);
    if (!li) {
        addReplyError(c, "Library not found");
        return;
    }

    libraryUnlink(curr_functions_lib_ctx, li);
    engineLibraryFree(li);
    /* Indicate that the command changed the data so it will be replicated and
     * counted as a data change (for persistence configuration) */
    server.dirty++;
    addReply(c, shared.ok);
}

/* FUNCTION KILL */
void functionKillCommand(client *c) {
    scriptKill(c, 0);
}

/* Try to extract command flags if we can, returns the modified flags.
 * Note that it does not guarantee the command arguments are right. */
uint64_t fcallGetCommandFlags(client *c, uint64_t cmd_flags) {
    robj *function_name = c->argv[1];
    c->cur_script = dictFind(curr_functions_lib_ctx->functions, function_name->ptr);
    if (!c->cur_script) return cmd_flags;
    functionInfo *fi = dictGetVal(c->cur_script);
    uint64_t script_flags = fi->f_flags;
    return scriptFlagsToCmdFlags(cmd_flags, script_flags);
}

static void fcallCommandGeneric(client *c, int ro) {
    /* Functions need to be fed to monitors before the commands they execute. */
    replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc);

    robj *function_name = c->argv[1];
    dictEntry *de = c->cur_script;
    if (!de) de = dictFind(curr_functions_lib_ctx->functions, function_name->ptr);
    if (!de) {
        addReplyError(c, "Function not found");
        return;
    }
    functionInfo *fi = dictGetVal(de);
    scriptingEngine *engine = fi->li->engine;

    long long numkeys;
    /* Get the number of arguments that are keys */
    if (getLongLongFromObject(c->argv[2], &numkeys) != C_OK) {
        addReplyError(c, "Bad number of keys provided");
        return;
    }
    if (numkeys > (c->argc - 3)) {
        addReplyError(c, "Number of keys can't be greater than number of args");
        return;
    } else if (numkeys < 0) {
        addReplyError(c, "Number of keys can't be negative");
        return;
    }

    scriptRunCtx run_ctx;
    if (scriptPrepareForRun(&run_ctx, scriptingEngineGetClient(engine), c, fi->name, fi->f_flags, ro) != C_OK) return;

    scriptingEngineCallFunction(engine,
                                &run_ctx,
                                run_ctx.original_client,
                                fi->function,
                                c->argv + 3,
                                numkeys,
                                c->argv + 3 + numkeys,
                                c->argc - 3 - numkeys);
    scriptResetRun(&run_ctx);
}

/*
 * FCALL <FUNCTION NAME> nkeys <key1 .. keyn> <arg1 .. argn>
 */
void fcallCommand(client *c) {
    fcallCommandGeneric(c, 0);
}

/*
 * FCALL_RO <FUNCTION NAME> nkeys <key1 .. keyn> <arg1 .. argn>
 */
void fcallroCommand(client *c) {
    fcallCommandGeneric(c, 1);
}

/*
 * FUNCTION DUMP
 *
 * Returns a binary payload representing all the libraries.
 * Can be loaded using FUNCTION RESTORE
 *
 * The payload structure is the same as on RDB. Each library
 * is saved separately with the following information:
 * * Library name
 * * Engine name
 * * Library code
 * RDB_OPCODE_FUNCTION2 is saved before each library to present
 * that the payload is a library.
 * RDB version and crc64 is saved at the end of the payload.
 * The RDB version is saved for backward compatibility.
 * crc64 is saved so we can verify the payload content.
 */
void functionDumpCommand(client *c) {
    unsigned char buf[2];
    uint64_t crc;
    rio payload;
    rioInitWithBuffer(&payload, sdsempty());

    rdbSaveFunctions(&payload);

    /* RDB version */
    buf[0] = RDB_VERSION & 0xff;
    buf[1] = (RDB_VERSION >> 8) & 0xff;
    payload.io.buffer.ptr = sdscatlen(payload.io.buffer.ptr, buf, 2);

    /* CRC64 */
    crc = crc64(0, (unsigned char *)payload.io.buffer.ptr, sdslen(payload.io.buffer.ptr));
    memrev64ifbe(&crc);
    payload.io.buffer.ptr = sdscatlen(payload.io.buffer.ptr, &crc, 8);

    addReplyBulkSds(c, payload.io.buffer.ptr);
}

/*
 * FUNCTION RESTORE <payload> [FLUSH|APPEND|REPLACE]
 *
 * Restore the libraries represented by the give payload.
 * Restore policy to can be given to control how to handle existing libraries (default APPEND):
 * * FLUSH: delete all existing libraries.
 * * APPEND: appends the restored libraries to the existing libraries. On collision, abort.
 * * REPLACE: appends the restored libraries to the existing libraries.
 *   On collision, replace the old libraries with the new libraries.
 */
void functionRestoreCommand(client *c) {
    if (c->argc > 4) {
        addReplySubcommandSyntaxError(c);
        return;
    }

    restorePolicy restore_replicy = restorePolicy_Append; /* default policy: APPEND */
    sds data = c->argv[2]->ptr;
    size_t data_len = sdslen(data);
    rio payload;
    sds err = NULL;

    if (c->argc == 4) {
        const char *restore_policy_str = c->argv[3]->ptr;
        if (!strcasecmp(restore_policy_str, "append")) {
            restore_replicy = restorePolicy_Append;
        } else if (!strcasecmp(restore_policy_str, "replace")) {
            restore_replicy = restorePolicy_Replace;
        } else if (!strcasecmp(restore_policy_str, "flush")) {
            restore_replicy = restorePolicy_Flush;
        } else {
            addReplyError(c, "Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.");
            return;
        }
    }

    uint16_t rdbver;
    if (verifyDumpPayload((unsigned char *)data, data_len, &rdbver) != C_OK) {
        addReplyError(c, "DUMP payload version or checksum are wrong");
        return;
    }

    functionsLibCtx *functions_lib_ctx = functionsLibCtxCreate();
    rioInitWithBuffer(&payload, data);

    /* Read until reaching last 10 bytes that should contain RDB version and checksum. */
    while (data_len - payload.io.buffer.pos > 10) {
        int type;
        if ((type = rdbLoadType(&payload)) == -1) {
            err = sdsnew("can not read data type");
            goto load_error;
        }
        if (type == RDB_OPCODE_FUNCTION_PRE_GA) {
            err = sdsnew("Pre-GA function format not supported");
            goto load_error;
        }
        if (type != RDB_OPCODE_FUNCTION2) {
            err = sdsnew("given type is not a function");
            goto load_error;
        }
        if (rdbFunctionLoad(&payload, rdbver, functions_lib_ctx, RDBFLAGS_NONE, &err) != C_OK) {
            if (!err) {
                err = sdsnew("failed loading the given functions payload");
            }
            goto load_error;
        }
    }

    if (restore_replicy == restorePolicy_Flush) {
        functionsLibCtxSwapWithCurrent(functions_lib_ctx, server.lazyfree_lazy_user_flush);
        functions_lib_ctx = NULL; /* avoid releasing the f_ctx in the end */
    } else {
        if (libraryJoin(curr_functions_lib_ctx, functions_lib_ctx, restore_replicy == restorePolicy_Replace, &err) !=
            C_OK) {
            goto load_error;
        }
    }

    /* Indicate that the command changed the data so it will be replicated and
     * counted as a data change (for persistence configuration) */
    server.dirty++;

load_error:
    if (err) {
        addReplyErrorSds(c, err);
    } else {
        addReply(c, shared.ok);
    }
    if (functions_lib_ctx) {
        functionsLibCtxFreeGeneric(functions_lib_ctx, server.lazyfree_lazy_user_flush);
    }
}

/* FUNCTION FLUSH [ASYNC | SYNC] */
void functionFlushCommand(client *c) {
    if (c->argc > 3) {
        addReplySubcommandSyntaxError(c);
        return;
    }
    int async = 0;
    if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr, "sync")) {
        async = 0;
    } else if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr, "async")) {
        async = 1;
    } else if (c->argc == 2) {
        async = server.lazyfree_lazy_user_flush ? 1 : 0;
    } else {
        addReplyError(c, "FUNCTION FLUSH only supports SYNC|ASYNC option");
        return;
    }

    functionsLibCtxClearCurrent(async, NULL);

    /* Indicate that the command changed the data so it will be replicated and
     * counted as a data change (for persistence configuration) */
    server.dirty++;
    addReply(c, shared.ok);
}

/* FUNCTION HELP */
void functionHelpCommand(client *c) {
    const char *help[] = {
        "LOAD [REPLACE] <FUNCTION CODE>",
        "    Create a new library with the given library name and code.",
        "DELETE <LIBRARY NAME>",
        "    Delete the given library.",
        "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
        "    Return general information on all the libraries:",
        "    * Library name",
        "    * The engine used to run the Library",
        "    * Functions list",
        "    * Library code (if WITHCODE is given)",
        "    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
        "STATS",
        "    Return information about the current function running:",
        "    * Function name",
        "    * Command used to run the function",
        "    * Duration in MS that the function is running",
        "    If no function is running, return nil",
        "    In addition, returns a list of available engines.",
        "KILL",
        "    Kill the current running function.",
        "FLUSH [ASYNC|SYNC]",
        "    Delete all the libraries.",
        "    When called without the optional mode argument, the behavior is determined by the",
        "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
        "    * ASYNC: Asynchronously flush the libraries.",
        "    * SYNC: Synchronously flush the libraries.",
        "DUMP",
        "    Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command",
        "RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
        "    Restore the libraries represented by the given payload, it is possible to give a restore policy to",
        "    control how to handle existing libraries (default APPEND):",
        "    * FLUSH: delete all existing libraries.",
        "    * APPEND: appends the restored libraries to the existing libraries. On collision, abort.",
        "    * REPLACE: appends the restored libraries to the existing libraries, On collision, replace the old",
        "      libraries with the new libraries (notice that even on this option there is a chance of failure",
        "      in case of functions name collision with another library).",
        NULL,
    };
    addReplyHelp(c, help);
}

/* Verify that the function name is of the format: [a-zA-Z0-9_][a-zA-Z0-9_]? */
static int functionsVerifyName(sds name) {
    if (sdslen(name) == 0) {
        return C_ERR;
    }
    for (size_t i = 0; i < sdslen(name); ++i) {
        char curr_char = name[i];
        if ((curr_char >= 'a' && curr_char <= 'z') || (curr_char >= 'A' && curr_char <= 'Z') ||
            (curr_char >= '0' && curr_char <= '9') || (curr_char == '_')) {
            continue;
        }
        return C_ERR;
    }
    return C_OK;
}

int functionExtractLibMetaData(sds payload, functionsLibMetaData *md, sds *err) {
    sds name = NULL;
    sds engine = NULL;
    if (strncmp(payload, "#!", 2) != 0) {
        *err = sdsnew("Missing library metadata");
        return C_ERR;
    }
    char *shebang_end = strchr(payload, '\n');
    if (shebang_end == NULL) {
        *err = sdsnew("Invalid library metadata");
        return C_ERR;
    }
    size_t shebang_len = shebang_end - payload;
    sds shebang = sdsnewlen(payload, shebang_len);
    int numparts;
    sds *parts = sdssplitargs(shebang, &numparts);
    sdsfree(shebang);
    if (!parts || numparts == 0) {
        *err = sdsnew("Invalid library metadata");
        sdsfreesplitres(parts, numparts);
        return C_ERR;
    }
    engine = sdsdup(parts[0]);
    sdsrange(engine, 2, -1);
    for (int i = 1; i < numparts; ++i) {
        sds part = parts[i];
        if (strncasecmp(part, "name=", 5) == 0) {
            if (name) {
                *err = sdscatfmt(sdsempty(), "Invalid metadata value, name argument was given multiple times");
                goto error;
            }
            name = sdsdup(part);
            sdsrange(name, 5, -1);
            continue;
        }
        *err = sdscatfmt(sdsempty(), "Invalid metadata value given: %s", part);
        goto error;
    }

    if (!name) {
        *err = sdsnew("Library name was not given");
        goto error;
    }

    sdsfreesplitres(parts, numparts);

    md->name = name;
    md->code = sdsnewlen(shebang_end, sdslen(payload) - shebang_len);
    md->engine = engine;

    return C_OK;

error:
    if (name) sdsfree(name);
    if (engine) sdsfree(engine);
    sdsfreesplitres(parts, numparts);
    return C_ERR;
}

void functionFreeLibMetaData(functionsLibMetaData *md) {
    if (md->code) sdsfree(md->code);
    if (md->name) sdsfree(md->name);
    if (md->engine) sdsfree(md->engine);
}

static void freeCompiledFunctions(scriptingEngine *engine,
                                  compiledFunction **compiled_functions,
                                  size_t num_compiled_functions,
                                  size_t free_function_from_idx) {
    for (size_t i = 0; i < num_compiled_functions; i++) {
        compiledFunction *func = compiled_functions[i];
        decrRefCount(func->name);
        if (func->desc) {
            decrRefCount(func->desc);
        }
        if (i >= free_function_from_idx) {
            scriptingEngineCallFreeFunction(engine, func->function);
        }
        zfree(func);
    }

    zfree(compiled_functions);
}

/* Compile and save the given library, return the loaded library name on success
 * and NULL on failure. In case on failure the err out param is set with relevant error message */
sds functionsCreateWithLibraryCtx(sds code, int replace, sds *err, functionsLibCtx *lib_ctx, size_t timeout) {
    dictIterator *iter = NULL;
    dictEntry *entry = NULL;
    functionLibInfo *old_li = NULL;
    functionsLibMetaData md = {0};
    functionLibInfo *new_li = NULL;

    if (functionExtractLibMetaData(code, &md, err) != C_OK) {
        return NULL;
    }

    if (functionsVerifyName(md.name)) {
        *err = sdsnew("Library names can only contain letters, numbers, or underscores(_) and must be at least one "
                      "character long");
        goto error;
    }

    scriptingEngine *engine = scriptingEngineManagerFind(md.engine);
    if (!engine) {
        *err = sdscatfmt(sdsempty(), "Engine '%S' not found", md.engine);
        goto error;
    }

    functionsAddEngineStats(md.engine);

    old_li = dictFetchValue(lib_ctx->libraries, md.name);
    if (old_li && !replace) {
        old_li = NULL;
        *err = sdscatfmt(sdsempty(), "Library '%S' already exists", md.name);
        goto error;
    }

    if (old_li) {
        libraryUnlink(lib_ctx, old_li);
    }

    new_li = engineLibraryCreate(md.name, engine, code);
    size_t num_compiled_functions = 0;
    robj *compile_error = NULL;
    compiledFunction **compiled_functions =
        scriptingEngineCallCreateFunctionsLibrary(engine,
                                                  md.code,
                                                  timeout,
                                                  &num_compiled_functions,
                                                  &compile_error);
    if (compiled_functions == NULL) {
        serverAssert(num_compiled_functions == 0);
        serverAssert(compile_error != NULL);
        *err = sdsdup(compile_error->ptr);
        decrRefCount(compile_error);
        goto error;
    }

    serverAssert(compile_error == NULL);

    for (size_t i = 0; i < num_compiled_functions; i++) {
        compiledFunction *func = compiled_functions[i];
        int ret = functionLibCreateFunction(func->name,
                                            func->function,
                                            new_li,
                                            func->desc,
                                            func->f_flags,
                                            err);
        if (ret == C_ERR) {
            freeCompiledFunctions(engine,
                                  compiled_functions,
                                  num_compiled_functions,
                                  i);
            goto error;
        }
    }

    freeCompiledFunctions(engine,
                          compiled_functions,
                          num_compiled_functions,
                          num_compiled_functions);

    if (dictSize(new_li->functions) == 0) {
        *err = sdsnew("No functions registered");
        goto error;
    }

    /* Verify no duplicate functions */
    iter = dictGetIterator(new_li->functions);
    while ((entry = dictNext(iter))) {
        functionInfo *fi = dictGetVal(entry);
        if (dictFetchValue(lib_ctx->functions, fi->name)) {
            /* functions name collision, abort. */
            *err = sdscatfmt(sdsempty(), "Function %s already exists", fi->name);
            goto error;
        }
    }
    dictReleaseIterator(iter);
    iter = NULL;

    libraryLink(lib_ctx, new_li);

    if (old_li) {
        engineLibraryFree(old_li);
    }

    sds loaded_lib_name = md.name;
    md.name = NULL;
    functionFreeLibMetaData(&md);

    return loaded_lib_name;

error:
    if (iter) dictReleaseIterator(iter);
    if (new_li) engineLibraryFree(new_li);
    if (old_li) libraryLink(lib_ctx, old_li);
    functionFreeLibMetaData(&md);
    return NULL;
}

/*
 * FUNCTION LOAD [REPLACE] <LIBRARY CODE>
 * REPLACE         - optional, replace existing library
 * LIBRARY CODE    - library code to pass to the engine
 */
void functionLoadCommand(client *c) {
    int replace = 0;
    int argc_pos = 2;
    while (argc_pos < c->argc - 1) {
        robj *next_arg = c->argv[argc_pos++];
        if (!strcasecmp(next_arg->ptr, "replace")) {
            replace = 1;
            continue;
        }
        addReplyErrorFormat(c, "Unknown option given: %s", (char *)next_arg->ptr);
        return;
    }

    if (argc_pos >= c->argc) {
        addReplyError(c, "Function code is missing");
        return;
    }

    robj *code = c->argv[argc_pos];
    sds err = NULL;
    sds library_name = NULL;
    size_t timeout = LOAD_TIMEOUT_MS;
    if (mustObeyClient(c)) {
        timeout = 0;
    }
    if (!(library_name = functionsCreateWithLibraryCtx(code->ptr, replace, &err, curr_functions_lib_ctx, timeout))) {
        serverAssert(err != NULL);
        addReplyErrorSds(c, err);
        return;
    }
    /* Indicate that the command changed the data so it will be replicated and
     * counted as a data change (for persistence configuration) */
    server.dirty++;
    addReplyBulkSds(c, library_name);
}

static void getEngineUsedMemory(scriptingEngine *engine, void *context) {
    size_t *engines_memory = (size_t *)context;
    engineMemoryInfo mem_info = scriptingEngineCallGetMemoryInfo(engine);
    *engines_memory += mem_info.used_memory;
}

/* Return memory usage of all the engines combine */
unsigned long functionsMemory(void) {
    size_t engines_memory = 0;
    scriptingEngineManagerForEachEngine(getEngineUsedMemory, &engines_memory);
    return engines_memory;
}

/* Return memory overhead of all the engines combine */
unsigned long functionsMemoryOverhead(void) {
    size_t memory_overhead = scriptingEngineManagerGetMemoryUsage();
    memory_overhead += dictMemUsage(curr_functions_lib_ctx->functions);
    memory_overhead += sizeof(functionsLibCtx);
    memory_overhead += curr_functions_lib_ctx->cache_memory;
    memory_overhead += scriptingEngineManagerGetTotalMemoryOverhead();

    return memory_overhead;
}

/* Returns the number of functions */
unsigned long functionsNum(void) {
    return dictSize(curr_functions_lib_ctx->functions);
}

unsigned long functionsLibNum(void) {
    return dictSize(curr_functions_lib_ctx->libraries);
}

dict *functionsLibGet(void) {
    return curr_functions_lib_ctx->libraries;
}

size_t functionsLibCtxFunctionsLen(functionsLibCtx *functions_ctx) {
    return dictSize(functions_ctx->functions);
}

/* Initialize engine data structures.
 * Should be called once on server initialization */
int functionsInit(void) {
    curr_functions_lib_ctx = functionsLibCtxCreate();

    if (luaEngineInitEngine() != C_OK) {
        return C_ERR;
    }

    return C_OK;
}
