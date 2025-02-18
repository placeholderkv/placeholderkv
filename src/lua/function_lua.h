#ifndef _FUNCTION_LUA_H_
#define _FUNCTION_LUA_H_

#include "engine_lua.h"

void luaFunctionInitializeLuaState(lua_State *lua);

compiledFunction **luaFunctionLibraryCreate(lua_State *lua,
                                            const char *code,
                                            size_t timeout,
                                            size_t *out_num_compiled_functions,
                                            robj **err);

int luaFunctionGetLuaFunctionRef(compiledFunction *compiled_function);

void luaFunctionFreeFunction(lua_State *lua, void *function);

#endif /* _FUNCTION_LUA_H_ */
