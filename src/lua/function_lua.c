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

/*
 * function_lua.c unit provides the Lua engine functionality.
 * Including registering the engine and implementing the engine
 * callbacks:
 * * Create a function from blob (usually text)
 * * Invoke a function
 * * Free function memory
 * * Get memory usage
 *
 * Uses script_lua.c to run the Lua code.
 */

#include "function_lua.h"
#include "script_lua.h"

#include "../script.h"
#include "../adlist.h"
#include "../monotonic.h"
#include "../server.h"

#include <lauxlib.h>
#include <lualib.h>

#define REGISTRY_LOAD_CTX_NAME "__LIBRARY_CTX__"
#define LIBRARY_API_NAME "__LIBRARY_API__"
#define GLOBALS_API_NAME "__GLOBALS_API__"

/* Lua function ctx */
typedef struct luaFunctionCtx {
    /* Special ID that allows getting the Lua function object from the Lua registry */
    int lua_function_ref;
} luaFunctionCtx;

typedef struct loadCtx {
    list *functions;
    monotime start_time;
    size_t timeout;
} loadCtx;

/* Hook for FUNCTION LOAD execution.
 * Used to cancel the execution in case of a timeout (500ms).
 * This execution should be fast and should only register
 * functions so 500ms should be more than enough. */
static void luaEngineLoadHook(lua_State *lua, lua_Debug *ar) {
    UNUSED(ar);
    loadCtx *load_ctx = luaGetFromRegistry(lua, REGISTRY_LOAD_CTX_NAME);
    serverAssert(load_ctx); /* Only supported inside script invocation */
    uint64_t duration = elapsedMs(load_ctx->start_time);
    if (load_ctx->timeout > 0 && duration > load_ctx->timeout) {
        lua_sethook(lua, luaEngineLoadHook, LUA_MASKLINE, 0);

        luaPushError(lua, "FUNCTION LOAD timeout");
        luaError(lua);
    }
}

static void freeCompiledFunc(lua_State *lua,
                             compiledFunction *compiled_func) {
    decrRefCount(compiled_func->name);
    if (compiled_func->desc) {
        decrRefCount(compiled_func->desc);
    }
    luaFunctionFreeFunction(lua, compiled_func->function);
    zfree(compiled_func);
}

/*
 * Compile a given script code by generating a set of compiled functions. These
 * functions are also saved into the the registry of the Lua environment.
 *
 * Returns an array of compiled functions. The `compileFunction` struct stores a
 * Lua ref that allows to later retrieve the function from the registry.
 * In the `out_num_compiled_functions` parameter is returned the size of the
 * array.
 *
 * Return NULL on compilation error and set the error to the err variable
 */
compiledFunction **luaFunctionLibraryCreate(lua_State *lua,
                                            const char *code,
                                            size_t timeout,
                                            size_t *out_num_compiled_functions,
                                            robj **err) {
    compiledFunction **compiled_functions = NULL;

    /* set load library globals */
    lua_getmetatable(lua, LUA_GLOBALSINDEX);
    lua_enablereadonlytable(lua, -1, 0); /* disable global protection */
    lua_getfield(lua, LUA_REGISTRYINDEX, LIBRARY_API_NAME);
    lua_setfield(lua, -2, "__index");
    lua_enablereadonlytable(lua, LUA_GLOBALSINDEX, 1); /* enable global protection */
    lua_pop(lua, 1);                                   /* pop the metatable */

    /* compile the code */
    if (luaL_loadbuffer(lua, code, strlen(code), "@user_function")) {
        sds error = sdscatfmt(sdsempty(), "Error compiling function: %s", lua_tostring(lua, -1));
        *err = createObject(OBJ_STRING, error);
        lua_pop(lua, 1); /* pops the error */
        goto done;
    }
    serverAssert(lua_isfunction(lua, -1));

    loadCtx load_ctx = {
        .functions = listCreate(),
        .start_time = getMonotonicUs(),
        .timeout = timeout,
    };
    luaSaveOnRegistry(lua, REGISTRY_LOAD_CTX_NAME, &load_ctx);

    lua_sethook(lua, luaEngineLoadHook, LUA_MASKCOUNT, 100000);
    /* Run the compiled code to allow it to register functions */
    if (lua_pcall(lua, 0, 0, 0)) {
        errorInfo err_info = {0};
        luaExtractErrorInformation(lua, &err_info);
        sds error = sdscatfmt(sdsempty(), "Error registering functions: %s", err_info.msg);
        *err = createObject(OBJ_STRING, error);
        lua_pop(lua, 1); /* pops the error */
        luaErrorInformationDiscard(&err_info);

        listIter *iter = listGetIterator(load_ctx.functions, AL_START_HEAD);
        listNode *node = NULL;
        while ((node = listNext(iter)) != NULL) {
            freeCompiledFunc(lua, listNodeValue(node));
        }
        listReleaseIterator(iter);
        listRelease(load_ctx.functions);

        goto done;
    }

    compiled_functions =
        zcalloc(sizeof(compiledFunction *) * listLength(load_ctx.functions));
    listIter *iter = listGetIterator(load_ctx.functions, AL_START_HEAD);
    listNode *node = NULL;
    *out_num_compiled_functions = 0;
    while ((node = listNext(iter)) != NULL) {
        compiledFunction *func = listNodeValue(node);
        compiled_functions[*out_num_compiled_functions] = func;
        (*out_num_compiled_functions)++;
    }
    listReleaseIterator(iter);
    listRelease(load_ctx.functions);

done:
    /* restore original globals */
    lua_getmetatable(lua, LUA_GLOBALSINDEX);
    lua_enablereadonlytable(lua, -1, 0); /* disable global protection */
    lua_getfield(lua, LUA_REGISTRYINDEX, GLOBALS_API_NAME);
    lua_setfield(lua, -2, "__index");
    lua_enablereadonlytable(lua, LUA_GLOBALSINDEX, 1); /* enable global protection */
    lua_pop(lua, 1);                                   /* pop the metatable */

    lua_sethook(lua, NULL, 0, 0); /* Disable hook */
    luaSaveOnRegistry(lua, REGISTRY_LOAD_CTX_NAME, NULL);
    return compiled_functions;
}

int luaFunctionGetLuaFunctionRef(compiledFunction *compiled_function) {
    luaFunctionCtx *f = compiled_function->function;
    return f->lua_function_ref;
}

static void luaRegisterFunctionArgsInitialize(compiledFunction *func,
                                              robj *name,
                                              robj *desc,
                                              luaFunctionCtx *lua_f_ctx,
                                              uint64_t flags) {
    *func = (compiledFunction){
        .name = name,
        .desc = desc,
        .function = lua_f_ctx,
        .f_flags = flags,
    };
}

/* Read function flags located on the top of the Lua stack.
 * On success, return C_OK and set the flags to 'flags' out parameter
 * Return C_ERR if encounter an unknown flag. */
static int luaRegisterFunctionReadFlags(lua_State *lua, uint64_t *flags) {
    int j = 1;
    int ret = C_ERR;
    int f_flags = 0;
    while (1) {
        lua_pushnumber(lua, j++);
        lua_gettable(lua, -2);
        int t = lua_type(lua, -1);
        if (t == LUA_TNIL) {
            lua_pop(lua, 1);
            break;
        }
        if (!lua_isstring(lua, -1)) {
            lua_pop(lua, 1);
            goto done;
        }

        const char *flag_str = lua_tostring(lua, -1);
        int found = 0;
        for (scriptFlag *flag = scripts_flags_def; flag->str; ++flag) {
            if (!strcasecmp(flag->str, flag_str)) {
                f_flags |= flag->flag;
                found = 1;
                break;
            }
        }
        /* pops the value to continue the iteration */
        lua_pop(lua, 1);
        if (!found) {
            /* flag not found */
            goto done;
        }
    }

    *flags = f_flags;
    ret = C_OK;

done:
    return ret;
}

static int luaRegisterFunctionReadNamedArgs(lua_State *lua,
                                            compiledFunction *func) {
    char *err = NULL;
    robj *name = NULL;
    robj *desc = NULL;
    luaFunctionCtx *lua_f_ctx = NULL;
    uint64_t flags = 0;
    if (!lua_istable(lua, 1)) {
        err = "calling server.register_function with a single argument is only applicable to Lua table (representing "
              "named arguments).";
        goto error;
    }

    /* Iterating on all the named arguments */
    lua_pushnil(lua);
    while (lua_next(lua, -2)) {
        /* Stack now: table, key, value */
        if (!lua_isstring(lua, -2)) {
            err = "named argument key given to server.register_function is not a string";
            goto error;
        }

        const char *key = lua_tostring(lua, -2);
        if (!strcasecmp(key, "function_name")) {
            if (!(name = luaGetStringObject(lua, -1))) {
                err = "function_name argument given to server.register_function must be a string";
                goto error;
            }
        } else if (!strcasecmp(key, "description")) {
            if (!(desc = luaGetStringObject(lua, -1))) {
                err = "description argument given to server.register_function must be a string";
                goto error;
            }
        } else if (!strcasecmp(key, "callback")) {
            if (!lua_isfunction(lua, -1)) {
                err = "callback argument given to server.register_function must be a function";
                goto error;
            }
            int lua_function_ref = luaL_ref(lua, LUA_REGISTRYINDEX);

            lua_f_ctx = zmalloc(sizeof(*lua_f_ctx));
            lua_f_ctx->lua_function_ref = lua_function_ref;
            continue; /* value was already popped, so no need to pop it out. */
        } else if (!strcasecmp(key, "flags")) {
            if (!lua_istable(lua, -1)) {
                err = "flags argument to server.register_function must be a table representing function flags";
                goto error;
            }
            if (luaRegisterFunctionReadFlags(lua, &flags) != C_OK) {
                err = "unknown flag given";
                goto error;
            }
        } else {
            /* unknown argument was given, raise an error */
            err = "unknown argument given to server.register_function";
            goto error;
        }
        lua_pop(lua, 1); /* pop the value to continue the iteration */
    }

    if (!name) {
        err = "server.register_function must get a function name argument";
        goto error;
    }

    if (!lua_f_ctx) {
        err = "server.register_function must get a callback argument";
        goto error;
    }

    luaRegisterFunctionArgsInitialize(func,
                                      name,
                                      desc,
                                      lua_f_ctx,
                                      flags);

    return C_OK;

error:
    if (name) decrRefCount(name);
    if (desc) decrRefCount(desc);
    if (lua_f_ctx) {
        lua_unref(lua, lua_f_ctx->lua_function_ref);
        zfree(lua_f_ctx);
    }
    luaPushError(lua, err);
    return C_ERR;
}

static int luaRegisterFunctionReadPositionalArgs(lua_State *lua,
                                                 compiledFunction *func) {
    char *err = NULL;
    robj *name = NULL;
    luaFunctionCtx *lua_f_ctx = NULL;
    if (!(name = luaGetStringObject(lua, 1))) {
        err = "first argument to server.register_function must be a string";
        goto error;
    }

    if (!lua_isfunction(lua, 2)) {
        err = "second argument to server.register_function must be a function";
        goto error;
    }

    int lua_function_ref = luaL_ref(lua, LUA_REGISTRYINDEX);

    lua_f_ctx = zmalloc(sizeof(*lua_f_ctx));
    lua_f_ctx->lua_function_ref = lua_function_ref;

    luaRegisterFunctionArgsInitialize(func, name, NULL, lua_f_ctx, 0);

    return C_OK;

error:
    if (name) decrRefCount(name);
    luaPushError(lua, err);
    return C_ERR;
}

static int luaRegisterFunctionReadArgs(lua_State *lua, compiledFunction *func) {
    int argc = lua_gettop(lua);
    if (argc < 1 || argc > 2) {
        luaPushError(lua, "wrong number of arguments to server.register_function");
        return C_ERR;
    }

    if (argc == 1) {
        return luaRegisterFunctionReadNamedArgs(lua, func);
    } else {
        return luaRegisterFunctionReadPositionalArgs(lua, func);
    }
}

static int luaFunctionRegisterFunction(lua_State *lua) {
    loadCtx *load_ctx = luaGetFromRegistry(lua, REGISTRY_LOAD_CTX_NAME);
    if (!load_ctx) {
        luaPushError(lua, "server.register_function can only be called on FUNCTION LOAD command");
        return luaError(lua);
    }

    compiledFunction *func = zcalloc(sizeof(*func));

    if (luaRegisterFunctionReadArgs(lua, func) != C_OK) {
        zfree(func);
        return luaError(lua);
    }

    listAddNodeTail(load_ctx->functions, func);

    return 0;
}

void luaFunctionInitializeLuaState(lua_State *lua) {
    /* Register the library commands table and fields and store it to registry */
    lua_newtable(lua); /* load library globals */
    lua_newtable(lua); /* load library `server` table */

    lua_pushstring(lua, "register_function");
    lua_pushcfunction(lua, luaFunctionRegisterFunction);
    lua_settable(lua, -3);

    luaRegisterLogFunction(lua);
    luaRegisterVersion(lua);

    luaSetErrorMetatable(lua);
    lua_setfield(lua, -2, SERVER_API_NAME);

    /* Get the server object and also set it to the Redis API
     * compatibility namespace. */
    lua_getfield(lua, -1, SERVER_API_NAME);
    lua_setfield(lua, -2, REDIS_API_NAME);

    luaSetErrorMetatable(lua);
    luaSetTableProtectionRecursively(lua); /* protect load library globals */
    lua_setfield(lua, LUA_REGISTRYINDEX, LIBRARY_API_NAME);

    /* Save default globals to registry */
    lua_pushvalue(lua, LUA_GLOBALSINDEX);
    lua_setfield(lua, LUA_REGISTRYINDEX, GLOBALS_API_NAME);

    /* Create new empty table to be the new globals, we will be able to control the real globals
     * using metatable */
    lua_newtable(lua); /* new globals */
    lua_newtable(lua); /* new globals metatable */
    lua_pushvalue(lua, LUA_GLOBALSINDEX);
    lua_setfield(lua, -2, "__index");
    lua_enablereadonlytable(lua, -1, 1); /* protect the metatable */
    lua_setmetatable(lua, -2);
    lua_enablereadonlytable(lua, -1, 1); /* protect the new global table */
    lua_replace(lua, LUA_GLOBALSINDEX);  /* set new global table as the new globals */
}

void luaFunctionFreeFunction(lua_State *lua, void *function) {
    luaFunctionCtx *funcCtx = function;
    lua_unref(lua, funcCtx->lua_function_ref);
    zfree(function);
}
