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
    client *c;                   /* Client that is used to run commands */
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
            .methods = {
                .create_functions_library = engine_methods->create_functions_library,
                .call_function = engine_methods->call_function,
                .free_function = engine_methods->free_function,
                .get_function_memory_overhead = engine_methods->get_function_memory_overhead,
                .get_memory_info = engine_methods->get_memory_info,
            },
        },
        .c = c,
        .module_ctx = engine_module ? moduleAllocateContext() : NULL,
    };

    dictAdd(engineMgr.engines, engine_name_sds, e);

    engineMemoryInfo mem_info = scriptingEngineCallGetMemoryInfo(e);
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

    engineMemoryInfo mem_info = scriptingEngineCallGetMemoryInfo(e);
    engineMgr.total_memory_overhead -= zmalloc_size(e) +
                                       sdsAllocSize(e->name) +
                                       mem_info.engine_memory_overhead;

    sdsfree(e->name);
    freeClient(e->c);
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
scriptingEngine *scriptingEngineManagerFind(sds engine_name) {
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
    return engine->c;
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

compiledFunction **scriptingEngineCallCreateFunctionsLibrary(scriptingEngine *engine,
                                                             const char *code,
                                                             size_t timeout,
                                                             size_t *out_num_compiled_functions,
                                                             robj **err) {
    engineSetupModuleCtx(engine, NULL);

    compiledFunction **functions = engine->impl.methods.create_functions_library(
        engine->module_ctx,
        engine->impl.ctx,
        code,
        timeout,
        out_num_compiled_functions,
        err);

    engineTeardownModuleCtx(engine);

    return functions;
}

void scriptingEngineCallFunction(scriptingEngine *engine,
                                 functionCtx *func_ctx,
                                 client *caller,
                                 void *compiled_function,
                                 robj **keys,
                                 size_t nkeys,
                                 robj **args,
                                 size_t nargs) {
    engineSetupModuleCtx(engine, caller);

    engine->impl.methods.call_function(
        engine->module_ctx,
        engine->impl.ctx,
        func_ctx,
        compiled_function,
        keys,
        nkeys,
        args,
        nargs);

    engineTeardownModuleCtx(engine);
}

void scriptingEngineCallFreeFunction(scriptingEngine *engine,
                                     void *compiled_func) {
    engineSetupModuleCtx(engine, NULL);
    engine->impl.methods.free_function(engine->module_ctx,
                                       engine->impl.ctx,
                                       compiled_func);
    engineTeardownModuleCtx(engine);
}

size_t scriptingEngineCallGetFunctionMemoryOverhead(scriptingEngine *engine,
                                                    void *compiled_function) {
    engineSetupModuleCtx(engine, NULL);
    size_t mem = engine->impl.methods.get_function_memory_overhead(
        engine->module_ctx, compiled_function);
    engineTeardownModuleCtx(engine);
    return mem;
}

engineMemoryInfo scriptingEngineCallGetMemoryInfo(scriptingEngine *engine) {
    engineSetupModuleCtx(engine, NULL);
    engineMemoryInfo mem_info = engine->impl.methods.get_memory_info(
        engine->module_ctx, engine->impl.ctx);
    engineTeardownModuleCtx(engine);
    return mem_info;
}
