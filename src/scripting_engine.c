/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "scripting_engine.h"
#include "dict.h"
#include "functions.h"
#include "module.h"

typedef struct scriptingEngineImpl {
    /* Engine specific context */
    engineCtx *ctx;

    /* Callback functions implemented by the scripting engine module */
    engineMethods methods;
} scriptingEngineImpl;

typedef struct scriptingEngine {
    sds name;                    /* Name of the engine */
    ValkeyModule *module;        /* the module that implements the scripting engine */
    scriptingEngineImpl impl;    /* engine context and callbacks to interact with the engine */
    client *client;              /* Client that is used to run commands */
    ValkeyModuleCtx *module_ctx; /* Cache of the module context object */
} scriptingEngine;


typedef struct engineManger {
    dict *engines;                /* engines dictionary */
    size_t total_memory_overhead; /* the sum of the memory overhead of all registered scripting engines */
} engineManager;


static engineManager engineMgr = {
    .engines = NULL,
    .total_memory_overhead = 0,
};

static uint64_t dictStrCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char *)key, strlen((char *)key));
}

dictType engineDictType = {
    dictStrCaseHash,       /* hash function */
    NULL,                  /* key dup */
    dictSdsKeyCaseCompare, /* key compare */
    NULL,                  /* key destructor */
    NULL,                  /* val destructor */
    NULL                   /* allow to expand */
};

/* Initializes the scripting engine manager.
 * The engine manager is responsible for managing the several scripting engines
 * that are loaded in the server and implemented by Valkey Modules.
 *
 * Returns C_ERR if some error occurs during the initialization.
 */
int scriptingEngineManagerInit(void) {
    engineMgr.engines = dictCreate(&engineDictType);
    scriptingEngineDebuggerInit();
    return C_OK;
}

/* Returns the amount of memory overhead consumed by all registered scripting
   engines. */
size_t scriptingEngineManagerGetTotalMemoryOverhead(void) {
    return engineMgr.total_memory_overhead;
}

size_t scriptingEngineManagerGetNumEngines(void) {
    return dictSize(engineMgr.engines);
}

size_t scriptingEngineManagerGetMemoryUsage(void) {
    return dictMemUsage(engineMgr.engines) + sizeof(engineMgr);
}

/* Registers a new scripting engine in the engine manager.
 *
 * - `engine_name`: the name of the scripting engine. This name will match
 * against the engine name specified in the script header using a shebang.
 *
 * - `ctx`: engine specific context pointer.
 *
 * - engine_methods - the struct with the scripting engine callback functions
 * pointers.
 *
 * Returns C_ERR in case of an error during registration.
 */
int scriptingEngineManagerRegister(const char *engine_name,
                                   ValkeyModule *engine_module,
                                   engineCtx *engine_ctx,
                                   engineMethods *engine_methods) {
    sds engine_name_sds = sdsnew(engine_name);

    if (dictFetchValue(engineMgr.engines, engine_name_sds)) {
        serverLog(LL_WARNING, "Scripting engine '%s' is already registered in the server", engine_name_sds);
        sdsfree(engine_name_sds);
        return C_ERR;
    }

    client *c = createClient(NULL);
    c->flag.deny_blocking = 1;
    c->flag.script = 1;
    c->flag.fake = 1;

    scriptingEngine *e = zmalloc(sizeof(*e));
    *e = (scriptingEngine){
        .name = engine_name_sds,
        .module = engine_module,
        .impl = {
            .ctx = engine_ctx,
            .methods = *engine_methods,
        },
        .client = c,
        .module_ctx = engine_module ? moduleAllocateContext() : NULL,
    };

    dictAdd(engineMgr.engines, engine_name_sds, e);

    engineMemoryInfo mem_info = scriptingEngineCallGetMemoryInfo(e, VMSE_ALL);
    engineMgr.total_memory_overhead += zmalloc_size(e) +
                                       sdsAllocSize(e->name) +
                                       mem_info.engine_memory_overhead;

    return C_OK;
}

/* Removes a scripting engine from the engine manager.
 *
 * - `engine_name`: name of the engine to remove
 */
int scriptingEngineManagerUnregister(const char *engine_name) {
    dictEntry *entry = dictUnlink(engineMgr.engines, engine_name);
    if (entry == NULL) {
        serverLog(LL_WARNING, "There's no engine registered with name %s", engine_name);
        return C_ERR;
    }

    scriptingEngine *e = dictGetVal(entry);

    functionsRemoveLibFromEngine(e);

    engineMemoryInfo mem_info = scriptingEngineCallGetMemoryInfo(e, VMSE_ALL);
    engineMgr.total_memory_overhead -= zmalloc_size(e) +
                                       sdsAllocSize(e->name) +
                                       mem_info.engine_memory_overhead;

    sdsfree(e->name);
    freeClient(e->client);
    if (e->module_ctx) {
        serverAssert(e->module != NULL);
        zfree(e->module_ctx);
    }
    zfree(e);

    dictFreeUnlinkedEntry(engineMgr.engines, entry);

    return C_OK;
}

/*
 * Lookups the engine with `engine_name` in the engine manager and returns it if
 * it exists. Otherwise returns `NULL`.
 */
scriptingEngine *scriptingEngineManagerFind(const char *engine_name) {
    dictEntry *entry = dictFind(engineMgr.engines, engine_name);
    if (entry) {
        return dictGetVal(entry);
    }
    return NULL;
}

sds scriptingEngineGetName(scriptingEngine *engine) {
    return engine->name;
}

client *scriptingEngineGetClient(scriptingEngine *engine) {
    return engine->client;
}

ValkeyModule *scriptingEngineGetModule(scriptingEngine *engine) {
    return engine->module;
}

/*
 * Iterates the list of engines registered in the engine manager and calls the
 * callback function with each engine.
 *
 * The `context` pointer is also passed in each callback call.
 */
void scriptingEngineManagerForEachEngine(engineIterCallback callback,
                                         void *context) {
    dictIterator *iter = dictGetIterator(engineMgr.engines);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        scriptingEngine *e = dictGetVal(entry);
        callback(e, context);
    }
    dictReleaseIterator(iter);
}

static void engineSetupModuleCtx(scriptingEngine *e, client *c) {
    if (e->module != NULL) {
        serverAssert(e->module_ctx != NULL);
        moduleScriptingEngineInitContext(e->module_ctx, e->module, c);
    }
}

static void engineTeardownModuleCtx(scriptingEngine *e) {
    if (e->module != NULL) {
        serverAssert(e->module_ctx != NULL);
        moduleFreeContext(e->module_ctx);
    }
}

compiledFunction **scriptingEngineCallCompileCode(scriptingEngine *engine,
                                                  subsystemType type,
                                                  const char *code,
                                                  size_t timeout,
                                                  size_t *out_num_compiled_functions,
                                                  robj **err) {
    serverAssert(type == VMSE_EVAL || type == VMSE_FUNCTION);

    engineSetupModuleCtx(engine, NULL);

    compiledFunction **functions = engine->impl.methods.compile_code(
        engine->module_ctx,
        engine->impl.ctx,
        type,
        code,
        timeout,
        out_num_compiled_functions,
        err);

    engineTeardownModuleCtx(engine);

    return functions;
}

void scriptingEngineCallFreeFunction(scriptingEngine *engine,
                                     subsystemType type,
                                     compiledFunction *compiled_func) {
    serverAssert(type == VMSE_EVAL || type == VMSE_FUNCTION);
    engineSetupModuleCtx(engine, NULL);
    engine->impl.methods.free_function(
        engine->module_ctx,
        engine->impl.ctx,
        type,
        compiled_func);
    engineTeardownModuleCtx(engine);
}

void scriptingEngineCallFunction(scriptingEngine *engine,
                                 serverRuntimeCtx *server_ctx,
                                 client *caller,
                                 compiledFunction *compiled_function,
                                 subsystemType type,
                                 robj **keys,
                                 size_t nkeys,
                                 robj **args,
                                 size_t nargs) {
    serverAssert(type == VMSE_EVAL || type == VMSE_FUNCTION);

    engineSetupModuleCtx(engine, caller);

    engine->impl.methods.call_function(
        engine->module_ctx,
        engine->impl.ctx,
        server_ctx,
        compiled_function,
        type,
        keys,
        nkeys,
        args,
        nargs);

    engineTeardownModuleCtx(engine);
}

size_t scriptingEngineCallGetFunctionMemoryOverhead(scriptingEngine *engine,
                                                    compiledFunction *compiled_function) {
    engineSetupModuleCtx(engine, NULL);
    size_t mem = engine->impl.methods.get_function_memory_overhead(
        engine->module_ctx,
        compiled_function);
    engineTeardownModuleCtx(engine);
    return mem;
}

callableLazyEvalReset *scriptingEngineCallResetEvalEnvFunc(scriptingEngine *engine,
                                                           int async) {
    engineSetupModuleCtx(engine, NULL);
    callableLazyEvalReset *callback = engine->impl.methods.reset_eval_env(
        engine->module_ctx,
        engine->impl.ctx,
        async);
    engineTeardownModuleCtx(engine);
    return callback;
}

engineMemoryInfo scriptingEngineCallGetMemoryInfo(scriptingEngine *engine,
                                                  subsystemType type) {
    engineSetupModuleCtx(engine, NULL);
    engineMemoryInfo mem_info = engine->impl.methods.get_memory_info(
        engine->module_ctx,
        engine->impl.ctx,
        type);
    engineTeardownModuleCtx(engine);
    return mem_info;
}

debuggerEnableRet scriptingEngineCallDebuggerEnable(scriptingEngine *engine,
                                                    subsystemType type,
                                                    const debuggerCommand **commands,
                                                    size_t *commands_len) {
    if (engine->impl.methods.debugger_enable == NULL ||
        engine->impl.methods.debugger_disable == NULL ||
        engine->impl.methods.debugger_start == NULL ||
        engine->impl.methods.debugger_end == NULL) {
        return VMSE_DEBUG_NOT_SUPPORTED;
    }

    engineSetupModuleCtx(engine, NULL);
    debuggerEnableRet ret = engine->impl.methods.debugger_enable(
        engine->module_ctx,
        engine->impl.ctx,
        type,
        commands,
        commands_len);
    engineTeardownModuleCtx(engine);
    return ret;
}

void scriptingEngineCallDebuggerDisable(scriptingEngine *engine,
                                        subsystemType type) {
    engineSetupModuleCtx(engine, NULL);
    engine->impl.methods.debugger_disable(
        engine->module_ctx,
        engine->impl.ctx,
        type);
    engineTeardownModuleCtx(engine);
}

void scriptingEngineCallDebuggerStart(scriptingEngine *engine,
                                      subsystemType type,
                                      robj *source) {
    engineSetupModuleCtx(engine, NULL);
    engine->impl.methods.debugger_start(
        engine->module_ctx,
        engine->impl.ctx,
        type,
        source);
    engineTeardownModuleCtx(engine);
}

void scriptingEngineCallDebuggerEnd(scriptingEngine *engine,
                                    subsystemType type) {
    engineSetupModuleCtx(engine, NULL);
    engine->impl.methods.debugger_end(
        engine->module_ctx,
        engine->impl.ctx,
        type);
    engineTeardownModuleCtx(engine);
}

#define DS_MAX_LEN_DEFAULT 256 /* Default len limit for replies / var dumps. */

typedef struct debugState {
    scriptingEngine *engine;         /* The scripting engine. */
    const debuggerCommand *commands; /* The array of debugger commands exported by the scripting engine. */
    size_t commands_len;             /* The length of the commands array. */
    connection *conn;                /* Connection of the debugging client. */
    int active;                      /* Are we debugging EVAL right now? */
    int forked;                      /* Is this a fork()ed debugging session? */
    list *logs;                      /* List of messages to send to the client. */
    list *traces;                    /* Messages about commands executed since last stop.*/
    list *children;                  /* All forked debugging sessions pids. */
    sds cbuf;                        /* Debugger client command buffer. */
    size_t maxlen;                   /* Max var dump / reply length. */
    int maxlen_hint_sent;            /* Did we already hint about "set maxlen"? */
} debugState;

static debugState ds;

/* Initialize script debugger data structures. */
void scriptingEngineDebuggerInit(void) {
    ds.engine = NULL;
    ds.conn = NULL;
    ds.active = 0;
    ds.logs = listCreate();
    listSetFreeMethod(ds.logs, (void (*)(void *))sdsfree);
    ds.children = listCreate();
    ds.cbuf = sdsempty();
}

/* Remove all the pending messages in the specified list. */
void debugScriptFlushLog(list *log) {
    listNode *ln;

    while ((ln = listFirst(log)) != NULL) listDelNode(log, ln);
}

/* Enable debug mode of scripts for this client. */
int scriptingEngineDebuggerEnable(client *c, scriptingEngine *engine, sds *err) {
    debuggerEnableRet ret = scriptingEngineCallDebuggerEnable(
        engine,
        VMSE_EVAL,
        &ds.commands,
        &ds.commands_len);

    if (ret == VMSE_DEBUG_NOT_SUPPORTED) {
        *err = sdscatfmt(sdsempty(),
                         "The scripting engine '%s' does not support interactive script debugging",
                         scriptingEngineGetName(engine));
        return C_ERR;
    } else if (ret == VMSE_DEBUG_ENABLE_FAIL) {
        *err = sdscatfmt(sdsempty(),
                         "The scripting engine '%s' failed to initialize interactive script debugging",
                         scriptingEngineGetName(engine));
        return C_ERR;
    }
    ds.engine = engine;
    c->flag.lua_debug = 1;
    debugScriptFlushLog(ds.logs);
    ds.conn = c->conn;
    sdsfree(ds.cbuf);
    ds.cbuf = sdsempty();
    ds.maxlen = DS_MAX_LEN_DEFAULT;
    ds.maxlen_hint_sent = 0;
    return C_OK;
}

/* Exit debugging mode from the POV of client. This function is not enough
 * to properly shut down a client debugging session, see scriptingEngineDebuggerEndSession()
 * for more information. */
void scriptingEngineDebuggerDisable(client *c) {
    ds.commands = NULL;
    ds.commands_len = 0;
    c->flag.lua_debug = 0;
    c->flag.lua_debug_sync = 0;
    scriptingEngineCallDebuggerDisable(ds.engine, VMSE_EVAL);
}

/* Append a log entry to the specified debug state log. */
void scriptingEngineDebuggerLog(sds entry) {
    listAddNodeTail(ds.logs, entry);
}

/* A version of scriptingEngineDebuggerLog() which prevents producing logs greater than
 * ds.maxlen. The first time the limit is reached a hint is generated
 * to inform the user that reply trimming can be disabled using the
 * debugger "maxlen" command. */
void scriptingEngineDebuggerLogWithMaxLen(sds entry) {
    int trimmed = 0;
    if (ds.maxlen && sdslen(entry) > ds.maxlen) {
        sdsrange(entry, 0, ds.maxlen - 1);
        entry = sdscatlen(entry, " ...", 4);
        trimmed = 1;
    }
    scriptingEngineDebuggerLog(entry);
    if (trimmed && ds.maxlen_hint_sent == 0) {
        ds.maxlen_hint_sent = 1;
        scriptingEngineDebuggerLog(sdsnew("<hint> The above reply was trimmed. Use 'maxlen 0' to disable trimming."));
    }
}

/* Implements the debugger "maxlen" command. It just queries or sets the
 * ldb.maxlen variable. */
void scriptingEngineDebuggerSetMaxlen(size_t max) {
    size_t newval = max;
    ds.maxlen_hint_sent = 1; /* User knows about this command. */
    if (newval != 0 && newval <= 60) newval = 60;
    ds.maxlen = newval;
}

size_t scriptingEngineDebuggerGetMaxlen(void) {
    return ds.maxlen;
}

/* Send ds.logs to the debugging client as a multi-bulk reply
 * consisting of simple strings. Log entries which include newlines have them
 * replaced with spaces. The entries sent are also consumed. */
void scriptingEngineDebuggerFlushLogs(void) {
    sds proto = sdsempty();
    proto = sdscatfmt(proto, "*%i\r\n", (int)listLength(ds.logs));
    while (listLength(ds.logs)) {
        listNode *ln = listFirst(ds.logs);
        proto = sdscatlen(proto, "+", 1);
        sdsmapchars(ln->value, "\r\n", "  ", 2);
        proto = sdscatsds(proto, ln->value);
        proto = sdscatlen(proto, "\r\n", 2);
        listDelNode(ds.logs, ln);
    }
    if (connWrite(ds.conn, proto, sdslen(proto)) == -1) {
        /* Avoid warning. We don't check the return value of write()
         * since the next read() will catch the I/O error and will
         * close the debugging session. */
    }
    sdsfree(proto);
}

/* Start a debugging session before calling EVAL implementation.
 * The technique we use is to capture the client socket file descriptor,
 * in order to perform direct I/O with it from within the scripting engine
 * hooks. This way we don't have to re-enter the server in order to handle I/O.
 *
 * The function returns 1 if the caller should proceed to call EVAL,
 * and 0 if instead the caller should abort the operation (this happens
 * for the parent in a forked session, since it's up to the children
 * to continue, or when fork returned an error).
 *
 * The caller should call scriptingEngineDebuggerEndSession() only if
 * scriptDebugStartSession() returned 1. */
int scriptingEngineDebuggerStartSession(client *c) {
    ds.forked = !c->flag.lua_debug_sync;
    if (ds.forked) {
        pid_t cp = serverFork(CHILD_TYPE_LDB);
        if (cp == -1) {
            addReplyErrorFormat(c, "Fork() failed: can't run EVAL in debugging mode: %s", strerror(errno));
            return 0;
        } else if (cp == 0) {
            /* Child. Let's ignore important signals handled by the parent. */
            struct sigaction act;
            sigemptyset(&act.sa_mask);
            act.sa_flags = 0;
            act.sa_handler = SIG_IGN;
            sigaction(SIGTERM, &act, NULL);
            sigaction(SIGINT, &act, NULL);

            /* Log the creation of the child and close the listening
             * socket to make sure if the parent crashes a reset is sent
             * to the clients. */
            serverLog(LL_NOTICE, "%s forked for debugging eval", SERVER_TITLE);
        } else {
            /* Parent */
            listAddNodeTail(ds.children, (void *)(unsigned long)cp);
            freeClientAsync(c); /* Close the client in the parent side. */
            return 0;
        }
    } else {
        serverLog(LL_NOTICE, "%s synchronous debugging eval session started", SERVER_TITLE);
    }

    /* Setup our debugging session. */
    connBlock(ds.conn);
    connSendTimeout(ds.conn, 5000);
    ds.active = 1;

    scriptingEngineCallDebuggerStart(ds.engine, VMSE_EVAL, c->argv[1]);
    return 1;
}

/* End a debugging session after the EVAL call with debugging enabled
 * returned. */
void scriptingEngineDebuggerEndSession(client *c) {
    /* Emit the remaining logs and an <endsession> mark. */
    scriptingEngineDebuggerLog(sdsnew("<endsession>"));
    scriptingEngineDebuggerFlushLogs();

    /* If it's a fork()ed session, we just exit. */
    if (ds.forked) {
        writeToClient(c);
        serverLog(LL_NOTICE, "Lua debugging session child exiting");
        exitFromChild(0);
    } else {
        serverLog(LL_NOTICE, "%s synchronous debugging eval session ended", SERVER_TITLE);
    }

    /* Otherwise let's restore client's state. */
    connNonBlock(ds.conn);
    connSendTimeout(ds.conn, 0);

    /* Close the client connection after sending the final EVAL reply
     * in order to signal the end of the debugging session. */
    c->flag.close_after_reply = 1;

    scriptingEngineCallDebuggerEnd(ds.engine, VMSE_EVAL);
}

/* If the specified pid is among the list of children spawned for
 * forked debugging sessions, it is removed from the children list.
 * If the pid was found non-zero is returned. */
int scriptingEngineDebuggerRemoveChild(int pid) {
    listNode *ln = listSearchKey(ds.children, (void *)(unsigned long)pid);
    if (ln) {
        listDelNode(ds.children, ln);
        return 1;
    }
    return 0;
}

/* Return the number of children we still did not receive termination
 * acknowledge via wait() in the parent process. */
int scriptingEngineDebuggerPendingChildren(void) {
    return listLength(ds.children);
}

/* Kill all the forked sessions. */
void scriptingEngineDebuggerKillForkedSessions(void) {
    listIter li;
    listNode *ln;

    listRewind(ds.children, &li);
    while ((ln = listNext(&li))) {
        pid_t pid = (unsigned long)ln->value;
        serverLog(LL_NOTICE, "Killing debugging session %ld", (long)pid);
        kill(pid, SIGKILL);
    }
    listRelease(ds.children);
    ds.children = listCreate();
}

/* Expect a valid multi-bulk command in the debugging client query buffer.
 * On success the command is parsed and returned as an array of object strings,
 * otherwise NULL is returned and there is to read more buffer. */
static robj **readReadCommandInternal(size_t *argc, robj **err) {
    static const char *protocol_error = "protocol error";
    serverAssert(err != NULL && *err == NULL);
    serverAssert(argc != NULL && *argc == 0);
    robj **argv = NULL;
    size_t largc = 0;
    if (sdslen(ds.cbuf) == 0) return NULL;

    /* Working on a copy is simpler in this case. We can modify it freely
     * for the sake of simpler parsing. */
    sds copy = sdsdup(ds.cbuf);
    char *p = copy;

    /* This RESP parser is a joke... just the simplest thing that
     * works in this context. It is also very forgiving regarding broken
     * protocol. */

    /* Seek and parse *<count>\r\n. */
    p = strchr(p, '*');
    if (!p) goto protoerr;
    char *plen = p + 1; /* Multi bulk len pointer. */
    p = strstr(p, "\r\n");
    if (!p) goto keep_reading;
    *p = '\0';
    p += 2;
    *argc = atoi(plen);
    if (*argc <= 0 || *argc > 1024) goto protoerr;

    /* Parse each argument. */
    argv = zmalloc(sizeof(robj *) * (*argc));
    largc = 0;
    while (largc < *argc) {
        /* reached the end but there should be more data to read */
        if (*p == '\0') goto keep_reading;

        if (*p != '$') goto protoerr;
        plen = p + 1; /* Bulk string len pointer. */
        p = strstr(p, "\r\n");
        if (!p) goto keep_reading;
        *p = '\0';
        p += 2;
        int slen = atoi(plen); /* Length of this arg. */
        if (slen <= 0 || slen > 1024) goto protoerr;
        if ((size_t)(p + slen + 2 - copy) > sdslen(copy)) goto keep_reading;
        argv[largc++] = createStringObject(p, slen);
        p += slen; /* Skip the already parsed argument. */
        if (p[0] != '\r' || p[1] != '\n') goto protoerr;
        p += 2; /* Skip \r\n. */
    }
    sdsfree(copy);
    return argv;

protoerr:
    *err = createStringObject(protocol_error, strlen(protocol_error));
keep_reading:
    for (size_t i = 0; i < largc; i++) {
        decrRefCount(argv[i]);
    }
    zfree(argv);
    sdsfree(copy);
    return NULL;
}

static sds *wrapText(const char *text, size_t max_len, size_t *count) {
    sds *lines = NULL;
    *count = 0;

    const char *p = text;
    size_t text_len = strlen(p);

    while ((size_t)(p - text) < text_len) {
        size_t len = strlen(p);
        char *line = zmalloc(sizeof(char) * (max_len + 1));
        line[max_len] = 0;

        strncpy(line, p, max_len);

        if (len > max_len) {
            char *lastspace = strrchr(line, ' ');
            if (lastspace != NULL) {
                *lastspace = 0;
            }

            p += (lastspace - line) + 1;
        } else {
            p += len;
        }

        lines = zrealloc(lines, sizeof(sds) * (*count + 1));
        lines[*count] = sdsnew(line);
        zfree(line);
        (*count)++;
    }

    return lines;
}

static void printCommandHelp(const debuggerCommand *command,
                             int name_width,
                             int line_width) {
    sds msg = sdsempty();

    /* Format the command name according to the prefix length. */
    if (command->prefix_len > 0 && command->prefix_len < strlen(command->name)) {
        sds prefix = sdsnewlen(command->name, command->prefix_len);
        msg = sdscatfmt(msg, "[%S]%s", prefix, command->name + command->prefix_len);
    } else {
        msg = sdscatfmt(msg, "%s", command->name);
    }

    /* Format the command parameters. */
    for (size_t i = 0; i < command->params_len; i++) {
        if (command->params[i].optional) {
            msg = sdscatfmt(msg, " [%s]", command->params[i].name);
        } else {
            msg = sdscatfmt(msg, " <%s>", command->params[i].name);
        }
    }

    msg = sdscatprintf(msg, "%*s ", -(name_width - (int)sdslen(msg) - 1), "");

    /* If the command name plus the parameters don't fit in the respective
     * space slot, then start the description of the command in the next line.*/
    int breakline = (int)sdslen(msg) > name_width;
    if (breakline) {
        scriptingEngineDebuggerLog(msg);
    }

    size_t count = 0;
    sds *lines = wrapText(command->desc, line_width - name_width, &count);
    for (size_t i = 0; i < count; i++) {
        if (i == 0 && !breakline) {
            msg = sdscatsds(msg, lines[i]);
        } else {
            msg = sdscatprintf(sdsempty(), "%*s%s", name_width, "", lines[i]);
        }
        scriptingEngineDebuggerLog(msg);
        sdsfree(lines[i]);
    }
    zfree(lines);
}

#define HELP_LINE_WIDTH 70
#define HELP_CMD_NAME_WIDTH 21

#define CONTINUE_SCRIPT_EXECUTION 0
#define CONTINUE_READ_NEXT_COMMAND 1

static int printHelpMessage(robj **argv, size_t argc, void *context);

static debuggerCommand helpCommand = {
    .name = "help",
    .prefix_len = 1,
    .desc = "Show this help.",
    .handler = printHelpMessage,
};

static int printHelpMessage(robj **argv, size_t argc, void *context) {
    UNUSED(argv);
    UNUSED(argc);
    UNUSED(context);

    scriptingEngineDebuggerLog(sdscatfmt(sdsempty(), "%s debugger help:", scriptingEngineGetName(ds.engine)));

    printCommandHelp(&helpCommand, HELP_CMD_NAME_WIDTH, HELP_LINE_WIDTH);

    for (size_t i = 0; i < ds.commands_len; i++) {
        if (!ds.commands[i].invisible) {
            printCommandHelp(&ds.commands[i], HELP_CMD_NAME_WIDTH, HELP_LINE_WIDTH);
        }
    }

    scriptingEngineDebuggerFlushLogs();

    return CONTINUE_READ_NEXT_COMMAND;
}

static int checkCommandParameters(const debuggerCommand *cmd, size_t argc) {
    size_t args_count = argc - 1;
    size_t mandatory_params_count = 0;
    int has_variadic_param = 0;

    for (size_t i = 0; i < cmd->params_len; i++) {
        if (!cmd->params[i].optional) {
            mandatory_params_count++;
        }
        if (cmd->params[i].variadic) {
            has_variadic_param = 1;
        }
    }

    if (has_variadic_param && args_count > 0) {
        /* If command has a variadic parameter then we just require at least
         * one argument present. */
        return 1;
    }

    if (args_count < mandatory_params_count) {
        /* Reject command because there is not enough arguments passed. */
        return 0;
    }

    if (args_count > cmd->params_len) {
        /* Reject command because there are more arguments than parameters. */
        return 0;
    }

    return 1;
}

static const debuggerCommand *findCommand(robj **argv, size_t argc) {
    if ((sdslen(argv[0]->ptr) == helpCommand.prefix_len &&
         strncasecmp(helpCommand.name, argv[0]->ptr, helpCommand.prefix_len) == 0) ||
        strcasecmp(helpCommand.name, argv[0]->ptr) == 0) {
        return &helpCommand;
    }

    for (size_t i = 0; i < ds.commands_len; i++) {
        const debuggerCommand *cmd = &ds.commands[i];
        if ((sdslen(argv[0]->ptr) == cmd->prefix_len &&
             strncasecmp(cmd->name, argv[0]->ptr, cmd->prefix_len) == 0) ||
            strcasecmp(cmd->name, argv[0]->ptr) == 0) {
            if (checkCommandParameters(cmd, argc)) {
                return cmd;
            }
        }
    }
    return NULL;
}

static int findAndExecuteCommand(robj **argv, size_t argc) {
    const debuggerCommand *cmd = findCommand(argv, argc);
    if (cmd == NULL) {
        scriptingEngineDebuggerLog(sdsnew("<error> Unknown debugger command or "
                                          "wrong number of arguments."));
        scriptingEngineDebuggerFlushLogs();
        return CONTINUE_READ_NEXT_COMMAND;
    }

    return cmd->handler(argv, argc, cmd->context);
}

void scriptingEngineDebuggerProcessCommands(int *client_disconnected, robj **err) {
    static const char *max_buffer_error = "max client buffer reached";

    serverAssert(err != NULL);
    robj **argv = NULL;
    *client_disconnected = 0;
    *err = NULL;

    while (1) {
        size_t argc = 0;
        while ((argv = readReadCommandInternal(&argc, err)) == NULL) {
            if (*err) {
                break;
            }

            char buf[1024];
            int nread = connRead(ds.conn, buf, sizeof(buf));
            if (nread <= 0) {
                *client_disconnected = 1;
                break;
            }

            ds.cbuf = sdscatlen(ds.cbuf, buf, nread);
            /* after 1M we will exit with an error
             * so that the client will not blow the memory
             */
            if (sdslen(ds.cbuf) > 1 << 20) {
                *err = createStringObject(max_buffer_error, strlen(max_buffer_error));
                return;
            }
        }

        serverAssert(argv != NULL || *err || *client_disconnected);

        sdsfree(ds.cbuf);
        ds.cbuf = sdsempty();

        if (*err || *client_disconnected) {
            return;
        }

        if (findAndExecuteCommand(argv, argc) != CONTINUE_READ_NEXT_COMMAND) {
            return;
        }

        /* Free the command vector. */
        for (size_t i = 0; i < argc; i++) {
            decrRefCount(argv[i]);
        }
        zfree(argv);
    }
}

static const char *debugScriptRespToHuman_Int(sds *o, const char *reply);
static const char *debugScriptRespToHuman_Bulk(sds *o, const char *reply);
static const char *debugScriptRespToHuman_Status(sds *o, const char *reply);
static const char *debugScriptRespToHuman_MultiBulk(sds *o, const char *reply);
static const char *debugScriptRespToHuman_Set(sds *o, const char *reply);
static const char *debugScriptRespToHuman_Map(sds *o, const char *reply);
static const char *debugScriptRespToHuman_Null(sds *o, const char *reply);
static const char *debugScriptRespToHuman_Bool(sds *o, const char *reply);
static const char *debugScriptRespToHuman_Double(sds *o, const char *reply);

/* Get RESP from 'reply' and appends it in human readable form to
 * the passed SDS string 'o'.
 *
 * Note that the SDS string is passed by reference (pointer of pointer to
 * char*) so that we can return a modified pointer, as for SDS semantics. */
static const char *debugScriptRespToHuman(sds *o, const char *reply) {
    const char *p = reply;
    switch (*p) {
    case ':': p = debugScriptRespToHuman_Int(o, reply); break;
    case '$': p = debugScriptRespToHuman_Bulk(o, reply); break;
    case '+': p = debugScriptRespToHuman_Status(o, reply); break;
    case '-': p = debugScriptRespToHuman_Status(o, reply); break;
    case '*': p = debugScriptRespToHuman_MultiBulk(o, reply); break;
    case '~': p = debugScriptRespToHuman_Set(o, reply); break;
    case '%': p = debugScriptRespToHuman_Map(o, reply); break;
    case '_': p = debugScriptRespToHuman_Null(o, reply); break;
    case '#': p = debugScriptRespToHuman_Bool(o, reply); break;
    case ',': p = debugScriptRespToHuman_Double(o, reply); break;
    }
    return p;
}

/* The following functions are helpers for debugScriptRespToHuman(), each
 * take care of a given RESP return type. */

static const char *debugScriptRespToHuman_Int(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');
    *o = sdscatlen(*o, reply + 1, p - reply - 1);
    return p + 2;
}

static const char *debugScriptRespToHuman_Bulk(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');
    long long bulklen;

    string2ll(reply + 1, p - reply - 1, &bulklen);
    if (bulklen == -1) {
        *o = sdscatlen(*o, "NULL", 4);
        return p + 2;
    } else {
        *o = sdscatrepr(*o, p + 2, bulklen);
        return p + 2 + bulklen + 2;
    }
}

static const char *debugScriptRespToHuman_Status(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');

    *o = sdscatrepr(*o, reply, p - reply);
    return p + 2;
}

static const char *debugScriptRespToHuman_MultiBulk(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply + 1, p - reply - 1, &mbulklen);
    p += 2;
    if (mbulklen == -1) {
        *o = sdscatlen(*o, "NULL", 4);
        return p;
    }
    *o = sdscatlen(*o, "[", 1);
    for (j = 0; j < mbulklen; j++) {
        p = debugScriptRespToHuman(o, p);
        if (j != mbulklen - 1) *o = sdscatlen(*o, ",", 1);
    }
    *o = sdscatlen(*o, "]", 1);
    return p;
}

static const char *debugScriptRespToHuman_Set(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply + 1, p - reply - 1, &mbulklen);
    p += 2;
    *o = sdscatlen(*o, "~(", 2);
    for (j = 0; j < mbulklen; j++) {
        p = debugScriptRespToHuman(o, p);
        if (j != mbulklen - 1) *o = sdscatlen(*o, ",", 1);
    }
    *o = sdscatlen(*o, ")", 1);
    return p;
}

static const char *debugScriptRespToHuman_Map(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply + 1, p - reply - 1, &mbulklen);
    p += 2;
    *o = sdscatlen(*o, "{", 1);
    for (j = 0; j < mbulklen; j++) {
        p = debugScriptRespToHuman(o, p);
        *o = sdscatlen(*o, " => ", 4);
        p = debugScriptRespToHuman(o, p);
        if (j != mbulklen - 1) *o = sdscatlen(*o, ",", 1);
    }
    *o = sdscatlen(*o, "}", 1);
    return p;
}

static const char *debugScriptRespToHuman_Null(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');
    *o = sdscatlen(*o, "(null)", 6);
    return p + 2;
}

static const char *debugScriptRespToHuman_Bool(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');
    if (reply[1] == 't')
        *o = sdscatlen(*o, "#true", 5);
    else
        *o = sdscatlen(*o, "#false", 6);
    return p + 2;
}

static const char *debugScriptRespToHuman_Double(sds *o, const char *reply) {
    const char *p = strchr(reply + 1, '\r');
    *o = sdscatlen(*o, "(double) ", 9);
    *o = sdscatlen(*o, reply + 1, p - reply - 1);
    return p + 2;
}

/* Log a RESP reply C string as debugger output, in a human readable format.
 * If the resulting string is longer than 'len' plus a few more chars used as
 * prefix, it gets truncated. */
void scriptingEngineDebuggerLogRespReplyStr(const char *reply) {
    sds log = sdsnew("<reply> ");
    debugScriptRespToHuman(&log, reply);
    scriptingEngineDebuggerLogWithMaxLen(log);
}
