/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include "engine_lua.h"
#include "function_lua.h"
#include "script_lua.h"
#include "debug_lua.h"

#include "../dict.h"
#include "../adlist.h"

#define LUA_ENGINE_NAME "LUA"
#define REGISTRY_ERROR_HANDLER_NAME "__ERROR_HANDLER__"

typedef struct luaFunction {
    lua_State *lua;   /* Pointer to the lua context where this function was created. Only used in EVAL context. */
    int function_ref; /* Special ID that allows getting the Lua function object from the Lua registry */
} luaFunction;

typedef struct luaEngineCtx {
    lua_State *eval_lua;     /* The Lua interpreter for EVAL commands. We use just one for all EVAL calls */
    lua_State *function_lua; /* The Lua interpreter for FCALL commands. We use just one for all FCALL calls */
} luaEngineCtx;

/* Adds server.debug() function used by lua debugger
 *
 * Log a string message into the output console.
 * Can take multiple arguments that will be separated by commas.
 * Nothing is returned to the caller. */
static int luaServerDebugCommand(lua_State *lua) {
    if (!ldbIsActive()) return 0;
    int argc = lua_gettop(lua);
    sds log = sdscatprintf(sdsempty(), "<debug> line %d: ", ldbGetCurrentLine());
    while (argc--) {
        log = ldbCatStackValue(log, lua, -1 - argc);
        if (argc != 0) log = sdscatlen(log, ", ", 2);
    }
    ldbLog(log);
    return 0;
}

/* Adds server.breakpoint() function used by lua debugger.
 *
 * Allows to stop execution during a debugging session from within
 * the Lua code implementation, like if a breakpoint was set in the code
 * immediately after the function. */
static int luaServerBreakpointCommand(lua_State *lua) {
    if (ldbIsActive()) {
        ldbSetBreakpointOnNextLine(1);
        lua_pushboolean(lua, 1);
    } else {
        lua_pushboolean(lua, 0);
    }
    return 1;
}


/* Adds server.replicate_commands()
 *
 * DEPRECATED: Now do nothing and always return true.
 * Turn on single commands replication if the script never called
 * a write command so far, and returns true. Otherwise if the script
 * already started to write, returns false and stick to whole scripts
 * replication, which is our default. */
int luaServerReplicateCommandsCommand(lua_State *lua) {
    lua_pushboolean(lua, 1);
    return 1;
}

static void luaStateInstallErrorHandler(lua_State *lua) {
    /* Add a helper function we use for pcall error reporting.
     * Note that when the error is in the C function we want to report the
     * information about the caller, that's what makes sense from the point
     * of view of the user debugging a script. */
    lua_pushstring(lua, REGISTRY_ERROR_HANDLER_NAME);
    char *errh_func = "local dbg = debug\n"
                      "debug = nil\n"
                      "local error_handler = function (err)\n"
                      "  local i = dbg.getinfo(2,'nSl')\n"
                      "  if i and i.what == 'C' then\n"
                      "    i = dbg.getinfo(3,'nSl')\n"
                      "  end\n"
                      "  if type(err) ~= 'table' then\n"
                      "    err = {err='ERR ' .. tostring(err)}"
                      "  end"
                      "  if i then\n"
                      "    err['source'] = i.source\n"
                      "    err['line'] = i.currentline\n"
                      "  end"
                      "  return err\n"
                      "end\n"
                      "return error_handler";
    luaL_loadbuffer(lua, errh_func, strlen(errh_func), "@err_handler_def");
    lua_pcall(lua, 0, 1, 0);
    lua_settable(lua, LUA_REGISTRYINDEX);
}

static void luaStateLockGlobalTable(lua_State *lua) {
    /* Lock the global table from any changes */
    lua_pushvalue(lua, LUA_GLOBALSINDEX);
    luaSetErrorMetatable(lua);
    /* Recursively lock all tables that can be reached from the global table */
    luaSetTableProtectionRecursively(lua);
    lua_pop(lua, 1);
}


static void initializeEvalLuaState(lua_State *lua) {
    /* register debug commands. we only need to add it under 'server' as 'redis'
     * is effectively aliased to 'server' table at this point. */
    lua_getglobal(lua, "server");

    /* server.breakpoint */
    lua_pushstring(lua, "breakpoint");
    lua_pushcfunction(lua, luaServerBreakpointCommand);
    lua_settable(lua, -3);

    /* server.debug */
    lua_pushstring(lua, "debug");
    lua_pushcfunction(lua, luaServerDebugCommand);
    lua_settable(lua, -3);

    /* server.replicate_commands */
    lua_pushstring(lua, "replicate_commands");
    lua_pushcfunction(lua, luaServerReplicateCommandsCommand);
    lua_settable(lua, -3);

    lua_setglobal(lua, "server");

    /* Duplicate the function with __server__err__hanler and
     * __redis__err_handler name for backwards compatibility. */
    lua_pushstring(lua, REGISTRY_ERROR_HANDLER_NAME);
    lua_gettable(lua, LUA_REGISTRYINDEX);
    lua_setglobal(lua, "__server__err__handler");
    lua_getglobal(lua, "__server__err__handler");
    lua_setglobal(lua, "__redis__err__handler");
}

static void initializeLuaState(luaEngineCtx *lua_engine_ctx,
                               subsystemType type) {
    lua_State *lua = lua_open();

    if (type == VMSE_EVAL) {
        lua_engine_ctx->eval_lua = lua;
    } else {
        serverAssert(type == VMSE_FUNCTION);
        lua_engine_ctx->function_lua = lua;
    }

    luaRegisterServerAPI(lua);
    luaStateInstallErrorHandler(lua);

    if (type == VMSE_EVAL) {
        initializeEvalLuaState(lua);
        luaStateLockGlobalTable(lua);
    } else {
        luaStateLockGlobalTable(lua);
        luaFunctionInitializeLuaState(lua);
    }
}

static struct luaEngineCtx *createEngineContext(void) {
    luaEngineCtx *lua_engine_ctx = zmalloc(sizeof(*lua_engine_ctx));

    initializeLuaState(lua_engine_ctx, VMSE_EVAL);
    initializeLuaState(lua_engine_ctx, VMSE_FUNCTION);

    return lua_engine_ctx;
}

static engineMemoryInfo luaEngineGetMemoryInfo(ValkeyModuleCtx *module_ctx,
                                               engineCtx *engine_ctx,
                                               subsystemType type) {
    /* The lua engine is implemented in the core, and not in a Valkey Module */
    serverAssert(module_ctx == NULL);

    luaEngineCtx *lua_engine_ctx = engine_ctx;
    engineMemoryInfo mem_info = {0};

    if (type == VMSE_EVAL || type == VMSE_ALL) {
        mem_info.used_memory += luaMemory(lua_engine_ctx->eval_lua);
    }
    if (type == VMSE_FUNCTION || type == VMSE_ALL) {
        mem_info.used_memory += luaMemory(lua_engine_ctx->function_lua);
    }

    mem_info.engine_memory_overhead = zmalloc_size(engine_ctx);

    return mem_info;
}

static compiledFunction **luaEngineCompileCode(ValkeyModuleCtx *module_ctx,
                                               engineCtx *engine_ctx,
                                               subsystemType type,
                                               const char *code,
                                               size_t timeout,
                                               size_t *out_num_compiled_functions,
                                               robj **err) {
    /* The lua engine is implemented in the core, and not in a Valkey Module */
    serverAssert(module_ctx == NULL);

    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)engine_ctx;
    compiledFunction **functions = NULL;

    if (type == VMSE_EVAL) {
        lua_State *lua = lua_engine_ctx->eval_lua;

        if (luaL_loadbuffer(lua, code, strlen(code), "@user_script")) {
            sds error = sdscatfmt(sdsempty(), "Error compiling script (new function): %s", lua_tostring(lua, -1));
            *err = createObject(OBJ_STRING, error);
            lua_pop(lua, 1);
            return functions;
        }

        serverAssert(lua_isfunction(lua, -1));
        int function_ref = luaL_ref(lua, LUA_REGISTRYINDEX);

        luaFunction *script = zcalloc(sizeof(luaFunction));
        *script = (luaFunction){
            .lua = lua,
            .function_ref = function_ref,
        };

        compiledFunction *func = zcalloc(sizeof(*func));
        *func = (compiledFunction){
            .name = NULL,
            .function = script,
            .desc = NULL,
            .f_flags = 0};

        *out_num_compiled_functions = 1;
        functions = zcalloc(sizeof(compiledFunction *));
        *functions = func;
    } else {
        functions = luaFunctionLibraryCreate(lua_engine_ctx->function_lua,
                                             code,
                                             timeout,
                                             out_num_compiled_functions,
                                             err);
    }

    return functions;
}

static void luaEngineFunctionCall(ValkeyModuleCtx *module_ctx,
                                  engineCtx *engine_ctx,
                                  serverRuntimeCtx *server_ctx,
                                  compiledFunction *compiled_function,
                                  subsystemType type,
                                  robj **keys,
                                  size_t nkeys,
                                  robj **args,
                                  size_t nargs) {
    /* The lua engine is implemented in the core, and not in a Valkey Module */
    serverAssert(module_ctx == NULL);

    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)engine_ctx;
    lua_State *lua = NULL;
    int lua_function_ref = -1;

    if (type == VMSE_EVAL) {
        lua = lua_engine_ctx->eval_lua;
        luaFunction *script = compiled_function->function;
        lua_function_ref = script->function_ref;
    } else {
        lua = lua_engine_ctx->function_lua;
        lua_function_ref = luaFunctionGetLuaFunctionRef(compiled_function);
    }

    /* Push the pcall error handler function on the stack. */
    lua_pushstring(lua, REGISTRY_ERROR_HANDLER_NAME);
    lua_gettable(lua, LUA_REGISTRYINDEX);

    lua_rawgeti(lua, LUA_REGISTRYINDEX, lua_function_ref);
    serverAssert(!lua_isnil(lua, -1));

    luaCallFunction(server_ctx,
                    lua,
                    keys,
                    nkeys,
                    args,
                    nargs,
                    type == VMSE_EVAL ? ldbIsActive() : 0);

    lua_pop(lua, 1); /* Remove the error handler. */
}

static void resetEvalContext(void *context) {
    lua_State *eval_lua = context;
    lua_gc(eval_lua, LUA_GCCOLLECT, 0);
    lua_close(eval_lua);

#if !defined(USE_LIBC)
    /* The lua interpreter may hold a lot of memory internally, and lua is
     * using libc. libc may take a bit longer to return the memory to the OS,
     * so after lua_close, we call malloc_trim try to purge it earlier.
     *
     * We do that only when the server itself does not use libc. When Lua and the server
     * use different allocators, one won't use the fragmentation holes of the
     * other, and released memory can take a long time until it is returned to
     * the OS. */
    zlibc_trim();
#endif
}

static callableLazyEvalReset *luaEngineResetEvalEnv(ValkeyModuleCtx *module_ctx,
                                                    engineCtx *engine_ctx,
                                                    int async) {
    /* The lua engine is implemented in the core, and not in a Valkey Module */
    serverAssert(module_ctx == NULL);

    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)engine_ctx;
    serverAssert(lua_engine_ctx->eval_lua);
    callableLazyEvalReset *callback = NULL;

    if (async) {
        callback = zcalloc(sizeof(*callback));
        *callback = (callableLazyEvalReset){
            .context = lua_engine_ctx->eval_lua,
            .engineLazyEvalResetCallback = resetEvalContext,
        };
    } else {
        resetEvalContext(lua_engine_ctx->eval_lua);
    }

    initializeLuaState(lua_engine_ctx, VMSE_EVAL);

    return callback;
}

static size_t luaEngineFunctionMemoryOverhead(ValkeyModuleCtx *module_ctx,
                                              compiledFunction *compiled_function) {
    /* The lua engine is implemented in the core, and not in a Valkey Module */
    serverAssert(module_ctx == NULL);

    return zmalloc_size(compiled_function->function) +
           (compiled_function->name ? zmalloc_size(compiled_function->name) : 0) +
           (compiled_function->desc ? zmalloc_size(compiled_function->desc) : 0) +
           zmalloc_size(compiled_function);
}

static void luaEngineFreeFunction(ValkeyModuleCtx *module_ctx,
                                  engineCtx *engine_ctx,
                                  subsystemType type,
                                  compiledFunction *compiled_function) {
    /* The lua engine is implemented in the core, and not in a Valkey Module */
    serverAssert(module_ctx == NULL);

    luaEngineCtx *lua_engine_ctx = engine_ctx;
    if (type == VMSE_EVAL) {
        luaFunction *script = (luaFunction *)compiled_function->function;
        if (lua_engine_ctx->eval_lua == script->lua) {
            /* The lua context is still the same, which means that we're not
             * resetting the whole eval context, and therefore, we need to
             * delete the function from the lua context.
             */
            lua_unref(lua_engine_ctx->eval_lua, script->function_ref);
        }
        zfree(script);
    } else {
        luaFunctionFreeFunction(lua_engine_ctx->function_lua, compiled_function->function);
    }

    if (compiled_function->name) {
        decrRefCount(compiled_function->name);
    }
    if (compiled_function->desc) {
        decrRefCount(compiled_function->desc);
    }
    zfree(compiled_function);
}

int luaEngineInitEngine(void) {
    ldbInit();

    engineMethods methods = {
        .compile_code = luaEngineCompileCode,
        .free_function = luaEngineFreeFunction,
        .call_function = luaEngineFunctionCall,
        .get_function_memory_overhead = luaEngineFunctionMemoryOverhead,
        .reset_eval_env = luaEngineResetEvalEnv,
        .get_memory_info = luaEngineGetMemoryInfo,
    };

    return scriptingEngineManagerRegister(LUA_ENGINE_NAME,
                                          NULL,
                                          createEngineContext(),
                                          &methods);
}
