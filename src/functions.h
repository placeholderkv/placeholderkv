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

#ifndef __FUNCTIONS_H_
#define __FUNCTIONS_H_

/*
 * functions.c unit provides the Functions API:
 * * FUNCTION LOAD
 * * FUNCTION LIST
 * * FUNCTION CALL (FCALL and FCALL_RO)
 * * FUNCTION DELETE
 * * FUNCTION STATS
 * * FUNCTION KILL
 * * FUNCTION FLUSH
 * * FUNCTION DUMP
 * * FUNCTION RESTORE
 * * FUNCTION HELP
 *
 * Also contains implementation for:
 * * Save/Load function from rdb
 * * Register engines
 */

#include "server.h"
#include "scripting_engine.h"
#include "script.h"
#include "valkeymodule.h"

typedef struct functionLibInfo functionLibInfo;

/* Hold information about an engine.
 * Used on rdb.c so it must be declared here. */
typedef struct engineInfo {
    sds name;                    /* Name of the engine */
    ValkeyModule *engineModule;  /* the module that implements the scripting engine */
    ValkeyModuleCtx *module_ctx; /* Scripting engine module context */
    scriptingEngine *engine;     /* engine callbacks that allows to interact with the engine */
    client *c;                   /* Client that is used to run commands */
} engineInfo;

/* Hold information about the specific function.
 * Used on rdb.c so it must be declared here. */
typedef struct functionInfo {
    compiledFunction *compiled_function; /* Compiled function structure */
    functionLibInfo *li;                 /* Pointer to the library created the function */
} functionInfo;

/* Hold information about the specific library.
 * Used on rdb.c so it must be declared here. */
struct functionLibInfo {
    sds name;                /* Library name */
    dict *functions;         /* Functions dictionary */
    scriptingEngine *engine; /* Pointer to the scripting engine */
    sds code;                /* Library code */
};

sds functionsCreateWithLibraryCtx(sds code, int replace, sds *err, functionsLibCtx *lib_ctx, size_t timeout);
unsigned long functionsMemory(void);
unsigned long functionsMemoryOverhead(void);
unsigned long functionsNum(void);
unsigned long functionsLibNum(void);
dict *functionsLibGet(void);
size_t functionsLibCtxFunctionsLen(functionsLibCtx *functions_ctx);
functionsLibCtx *functionsLibCtxGetCurrent(void);
functionsLibCtx *functionsLibCtxCreate(void);
void functionsLibCtxClearCurrent(int async, void(callback)(dict *));
void functionsLibCtxFree(functionsLibCtx *functions_lib_ctx);
void functionsLibCtxClear(functionsLibCtx *lib_ctx, void(callback)(dict *));
void functionsLibCtxSwapWithCurrent(functionsLibCtx *new_lib_ctx, int async);

void functionsRemoveLibFromEngine(scriptingEngine *engine);

int functionsInit(void);

#endif /* __FUNCTIONS_H_ */
