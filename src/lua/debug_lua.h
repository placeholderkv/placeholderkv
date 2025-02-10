#ifndef _LUA_DEBUG_H_
#define _LUA_DEBUG_H_

#include "../scripting_engine.h"

typedef char *sds;
typedef struct serverObject robj;
typedef struct lua_State lua_State;
typedef struct client client;

void ldbInit(void);
int ldbIsEnabled(void);
void ldbDisable(void);
void ldbEnable(void);
int ldbIsActive(void);
void ldbStart(robj *source);
void ldbEnd(void);
void ldbLog(sds entry);
void ldbSendLogs(void);
void ldbLogRespReply(char *reply);

int ldbGetCurrentLine(void);
void ldbSetCurrentLine(int line);
void ldbLogSourceLine(int lnum);
sds ldbCatStackValue(sds s, lua_State *lua, int idx);
void ldbSetBreakpointOnNextLine(int enable);
int ldbIsBreakpointOnNextLineEnabled(void);
int ldbShouldBreak(void);
int ldbIsStepEnabled(void);
void ldbSetStepMode(int enable);

int ldbRepl(lua_State *lua);
void ldbGenerateDebuggerCommandsArray(lua_State *lua,
                                      const debuggerCommand **commands,
                                      size_t *commands_len);

#endif /* _LUA_DEBUG_H_ */
