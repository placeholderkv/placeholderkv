#ifndef _SCRIPTING_ENGINE_H_
#define _SCRIPTING_ENGINE_H_

#include "server.h"

// Forward declaration of the engine structure.
typedef struct scriptingEngine scriptingEngine;

/* ValkeyModule type aliases for scripting engine structs and types. */
typedef struct ValkeyModule ValkeyModule;
typedef ValkeyModuleScriptingEngineCtx engineCtx;
typedef ValkeyModuleScriptingEngineFunctionCtx functionCtx;
typedef ValkeyModuleScriptingEngineCompiledFunction compiledFunction;
typedef ValkeyModuleScriptingEngineMemoryInfo engineMemoryInfo;
typedef ValkeyModuleScriptingEngineMethods engineMethods;

/*
 * Callback function used to iterate the list of engines registered in the
 * engine manager.
 *
 * - `engine`: the scripting engine in the current iteration.
 *
 * - `context`: a generic pointer to a context object.
 *
 */
typedef void (*engineIterCallback)(scriptingEngine *engine, void *context);

/*
 * Engine manager API functions.
 */
int scriptingEngineManagerInit(void);
size_t scriptingEngineManagerGetTotalMemoryOverhead(void);
size_t scriptingEngineManagerGetNumEngines(void);
size_t scriptingEngineManagerGetMemoryUsage(void);
int scriptingEngineManagerRegister(const char *engine_name,
                                   ValkeyModule *engine_module,
                                   engineCtx *engine_ctx,
                                   engineMethods *engine_methods);
int scriptingEngineManagerUnregister(const char *engine_name);
scriptingEngine *scriptingEngineManagerFind(sds engine_name);
void scriptingEngineManagerForEachEngine(engineIterCallback callback,
                                         void *context);

/*
 * Engine API functions.
 */
sds scriptingEngineGetName(scriptingEngine *engine);
client *scriptingEngineGetClient(scriptingEngine *engine);
ValkeyModule *scriptingEngineGetModule(scriptingEngine *engine);

/*
 * API to call engine callback functions.
 */
compiledFunction **scriptingEngineCallCreateFunctionsLibrary(scriptingEngine *engine,
                                                             const char *code,
                                                             size_t timeout,
                                                             size_t *out_num_compiled_functions,
                                                             robj **err);
void scriptingEngineCallFunction(scriptingEngine *engine,
                                 functionCtx *func_ctx,
                                 client *caller,
                                 void *compiled_function,
                                 robj **keys,
                                 size_t nkeys,
                                 robj **args,
                                 size_t nargs);
void scriptingEngineCallFreeFunction(scriptingEngine *engine,
                                     void *compiled_func);
size_t scriptingEngineCallGetFunctionMemoryOverhead(scriptingEngine *engine,
                                                    void *compiled_function);
engineMemoryInfo scriptingEngineCallGetMemoryInfo(scriptingEngine *engine);

#endif /* _SCRIPTING_ENGINE_H_ */
