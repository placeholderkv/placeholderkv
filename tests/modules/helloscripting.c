#include "valkeymodule.h"

#include <ctype.h>
#include <errno.h>
#include <string.h>

/*
 * This module implements a very simple stack based scripting language.
 * It's purpose is only to test the valkey module API to implement scripting
 * engines.
 *
 * The language is called HELLO, and a program in this language is formed by
 * a list of function definitions.
 * The language only supports 32-bit integer, and it only allows to return an
 * integer constant, or return the value passed as the first argument to the
 * function.
 *
 * Example of a program:
 *
 * ```
 * FUNCTION foo  # declaration of function 'foo'
 * ARGS 0        # pushes the value in the first argument to the top of the
 *               # stack
 * RETURN        # returns the current value on the top of the stack and marks
 *               # the end of the function declaration
 *
 * FUNCTION bar  # declaration of function 'bar'
 * CONSTI 432    # pushes the value 432 to the top of the stack
 * RETURN        # returns the current value on the top of the stack and marks
 *               # the end of the function declaration.
 * ```
 */

/*
 * List of instructions of the HELLO language.
 */
typedef enum HelloInstKind {
    FUNCTION = 0,
    CONSTI,
    ARGS,
    RETURN,
    _NUM_INSTRUCTIONS, // Not a real instruction.
} HelloInstKind;

/*
 * String representations of the instructions above.
 */
const char *HelloInstKindStr[] = {
    "FUNCTION",
    "CONSTI",
    "ARGS",
    "RETURN",
};

/*
 * Struct that represents an instance of an instruction.
 * Instructions may have at most one parameter.
 */
typedef struct HelloInst {
    HelloInstKind kind;
    union {
        uint32_t integer;
        const char *string;
    } param;
} HelloInst;

/*
 * Struct that represents an instance of a function.
 * A function is just a list of instruction instances.
 */
typedef struct HelloFunc {
    char *name;
    HelloInst instructions[256];
    uint32_t num_instructions;
    uint32_t index;
} HelloFunc;

/*
 * Struct that represents an instance of an HELLO program.
 * A program is just a list of function instances.
 */
typedef struct HelloProgram {
    HelloFunc *functions[16];
    uint32_t num_functions;
} HelloProgram;

/*
 * Struct that represents the runtime context of an HELLO program.
 */
typedef struct HelloLangCtx {
    HelloProgram *program;
} HelloLangCtx;


static HelloLangCtx *hello_ctx = NULL;


static uint32_t str2int(const char *str) {
    char *end;
    errno = 0;
    uint32_t val = (uint32_t)strtoul(str, &end, 10);
    ValkeyModule_Assert(errno == 0);
    return val;
}

/*
 * Parses the kind of instruction that the current token points to.
 */
static HelloInstKind helloLangParseInstruction(const char *token) {
    for (HelloInstKind i = 0; i < _NUM_INSTRUCTIONS; i++) {
        if (strcmp(HelloInstKindStr[i], token) == 0) {
            return i;
        }
    }
    return _NUM_INSTRUCTIONS;
}

/*
 * Parses the function param.
 */
static void helloLangParseFunction(HelloFunc *func) {
    char *token = strtok(NULL, " \n");
    ValkeyModule_Assert(token != NULL);
    func->name = ValkeyModule_Alloc(sizeof(char) * strlen(token) + 1);
    strcpy(func->name, token);
}

/*
 * Parses an integer parameter.
 */
static void helloLangParseIntegerParam(HelloFunc *func) {
    char *token = strtok(NULL, " \n");
    func->instructions[func->num_instructions].param.integer = str2int(token);
}

/*
 * Parses the CONSTI instruction parameter.
 */
static void helloLangParseConstI(HelloFunc *func) {
    helloLangParseIntegerParam(func);
    func->num_instructions++;
}

/*
 * Parses the ARGS instruction parameter.
 */
static void helloLangParseArgs(HelloFunc *func) {
    helloLangParseIntegerParam(func);
    func->num_instructions++;
}

/*
 * Parses an HELLO program source code.
 */
static int helloLangParseCode(const char *code,
                              HelloProgram *program,
                              ValkeyModuleString **err) {
    char *_code = ValkeyModule_Alloc(sizeof(char) * strlen(code) + 1);
    strcpy(_code, code);

    HelloFunc *currentFunc = NULL;

    char *token = strtok(_code, " \n");
    while (token != NULL) {
        HelloInstKind kind = helloLangParseInstruction(token);

        if (currentFunc != NULL) {
            currentFunc->instructions[currentFunc->num_instructions].kind = kind;
        }

        switch (kind) {
        case FUNCTION:
            ValkeyModule_Assert(currentFunc == NULL);
            currentFunc = ValkeyModule_Alloc(sizeof(HelloFunc));
            memset(currentFunc, 0, sizeof(HelloFunc));
            currentFunc->index = program->num_functions;
            program->functions[program->num_functions++] = currentFunc;
            helloLangParseFunction(currentFunc);
            break;
        case CONSTI:
            ValkeyModule_Assert(currentFunc != NULL);
            helloLangParseConstI(currentFunc);
            break;
        case ARGS:
            ValkeyModule_Assert(currentFunc != NULL);
            helloLangParseArgs(currentFunc);
            break;
        case RETURN:
            ValkeyModule_Assert(currentFunc != NULL);
            currentFunc->num_instructions++;
            currentFunc = NULL;
            break;
        default:
            *err = ValkeyModule_CreateStringPrintf(NULL, "Failed to parse instruction: '%s'", token);
            ValkeyModule_Free(_code);
            return -1;
        }

        token = strtok(NULL, " \n");
    }

    ValkeyModule_Free(_code);

    return 0;
}

/*
 * Executes an HELLO function.
 */
static uint32_t executeHelloLangFunction(HelloFunc *func,
                                         ValkeyModuleString **args, int nargs) {
    uint32_t stack[64];
    int sp = 0;

    for (uint32_t pc = 0; pc < func->num_instructions; pc++) {
        HelloInst instr = func->instructions[pc];
        switch (instr.kind) {
        case CONSTI:
            stack[sp++] = instr.param.integer;
            break;
        case ARGS: {
            uint32_t idx = instr.param.integer;
            ValkeyModule_Assert(idx < (uint32_t)nargs);
            size_t len;
            const char *argStr = ValkeyModule_StringPtrLen(args[idx], &len);
            uint32_t arg = str2int(argStr);
            stack[sp++] = arg;
            break;
        }
        case RETURN: {
            ValkeyModule_Assert(sp > 0);
            uint32_t val = stack[--sp];
            ValkeyModule_Assert(sp == 0);
            return val;
        }
        case FUNCTION:
        default:
            ValkeyModule_Assert(0);
        }
    }

    ValkeyModule_Assert(0);
    return 0;
}

static ValkeyModuleScriptingEngineMemoryInfo engineGetMemoryInfo(ValkeyModuleCtx *module_ctx,
                                                                 ValkeyModuleScriptingEngineCtx *engine_ctx) {
    VALKEYMODULE_NOT_USED(module_ctx);
    HelloLangCtx *ctx = (HelloLangCtx *)engine_ctx;
    ValkeyModuleScriptingEngineMemoryInfo mem_info = {0};

    if (ctx->program != NULL) {
        mem_info.used_memory += ValkeyModule_MallocSize(ctx->program);

        for (uint32_t i = 0; i < ctx->program->num_functions; i++) {
            HelloFunc *func = ctx->program->functions[i];
            if (func != NULL) {
                mem_info.used_memory += ValkeyModule_MallocSize(func);
                mem_info.used_memory += ValkeyModule_MallocSize(func->name);
            }
        }
    }

    mem_info.engine_memory_overhead = ValkeyModule_MallocSize(ctx);
    if (ctx->program != NULL) {
        mem_info.engine_memory_overhead += ValkeyModule_MallocSize(ctx->program);
    }

    return mem_info;
}

static size_t engineFunctionMemoryOverhead(ValkeyModuleCtx *module_ctx,
                                           void *compiled_function) {
    VALKEYMODULE_NOT_USED(module_ctx);
    HelloFunc *func = (HelloFunc *)compiled_function;
    return ValkeyModule_MallocSize(func->name);
}

static void engineFreeFunction(ValkeyModuleCtx *module_ctx,
                               ValkeyModuleScriptingEngineCtx *engine_ctx,
                               void *compiled_function) {
    VALKEYMODULE_NOT_USED(module_ctx);
    VALKEYMODULE_NOT_USED(engine_ctx);
    HelloLangCtx *ctx = (HelloLangCtx *)engine_ctx;
    HelloFunc *func = (HelloFunc *)compiled_function;
    ctx->program->functions[func->index] = NULL;
    ValkeyModule_Free(func->name);
    func->name = NULL;
    ValkeyModule_Free(func);
}

static ValkeyModuleScriptingEngineCompiledFunction **createHelloLangEngine(ValkeyModuleCtx *module_ctx,
                                                                           ValkeyModuleScriptingEngineCtx *engine_ctx,
                                                                           const char *code,
                                                                           size_t timeout,
                                                                           size_t *out_num_compiled_functions,
                                                                           ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(module_ctx);
    VALKEYMODULE_NOT_USED(timeout);
    VALKEYMODULE_NOT_USED(err);

    HelloLangCtx *ctx = (HelloLangCtx *)engine_ctx;

    if (ctx->program == NULL) {
        ctx->program = ValkeyModule_Alloc(sizeof(HelloProgram));
        memset(ctx->program, 0, sizeof(HelloProgram));
    } else {
        ctx->program->num_functions = 0;
    }

    int ret = helloLangParseCode(code, ctx->program, err);
    if (ret < 0) {
        for (uint32_t i = 0; i < ctx->program->num_functions; i++) {
            HelloFunc *func = ctx->program->functions[i];
            ValkeyModule_Free(func->name);
            ValkeyModule_Free(func);
            ctx->program->functions[i] = NULL;
        }
        ctx->program->num_functions = 0;
        return NULL;
    }

    ValkeyModuleScriptingEngineCompiledFunction **compiled_functions =
        ValkeyModule_Alloc(sizeof(ValkeyModuleScriptingEngineCompiledFunction *) * ctx->program->num_functions);

    for (uint32_t i = 0; i < ctx->program->num_functions; i++) {
        HelloFunc *func = ctx->program->functions[i];

        ValkeyModuleScriptingEngineCompiledFunction *cfunc =
            ValkeyModule_Alloc(sizeof(ValkeyModuleScriptingEngineCompiledFunction));
        *cfunc = (ValkeyModuleScriptingEngineCompiledFunction) {
            .name = ValkeyModule_CreateString(NULL, func->name, strlen(func->name)),
            .function = func,
            .desc = NULL,
            .f_flags = 0,
        };

        compiled_functions[i] = cfunc;
    }

    *out_num_compiled_functions = ctx->program->num_functions;

    return compiled_functions;
}

static void
callHelloLangFunction(ValkeyModuleCtx *module_ctx,
                      ValkeyModuleScriptingEngineCtx *engine_ctx,
                      ValkeyModuleScriptingEngineFunctionCtx *func_ctx,
                      void *compiled_function,
                      ValkeyModuleString **keys, size_t nkeys,
                      ValkeyModuleString **args, size_t nargs) {
    VALKEYMODULE_NOT_USED(engine_ctx);
    VALKEYMODULE_NOT_USED(func_ctx);
    VALKEYMODULE_NOT_USED(keys);
    VALKEYMODULE_NOT_USED(nkeys);

    HelloFunc *func = (HelloFunc *)compiled_function;
    uint32_t result = executeHelloLangFunction(func, args, nargs);

    ValkeyModule_ReplyWithLongLong(module_ctx, result);
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx,
                        ValkeyModuleString **argv,
                        int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "helloengine", 1, VALKEYMODULE_APIVER_1) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    hello_ctx = ValkeyModule_Alloc(sizeof(HelloLangCtx));
    hello_ctx->program = NULL;

    ValkeyModuleScriptingEngineMethods methods = {
        .version = VALKEYMODULE_SCRIPTING_ENGINE_ABI_VERSION,
        .create_functions_library = createHelloLangEngine,
        .call_function = callHelloLangFunction,
        .get_function_memory_overhead = engineFunctionMemoryOverhead,
        .free_function = engineFreeFunction,
        .get_memory_info = engineGetMemoryInfo,
    };

    ValkeyModule_RegisterScriptingEngine(ctx,
                                         "HELLO",
                                         hello_ctx,
                                         &methods);

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_UnregisterScriptingEngine(ctx, "HELLO") != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "error", "Failed to unregister engine");
        return VALKEYMODULE_ERR;
    }

    ValkeyModule_Free(hello_ctx->program);
    hello_ctx->program = NULL;
    ValkeyModule_Free(hello_ctx);
    hello_ctx = NULL;

    return VALKEYMODULE_OK;
}
