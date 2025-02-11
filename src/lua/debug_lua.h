#ifndef _LUA_DEBUG_H_
#define _LUA_DEBUG_H_

typedef char *sds;
typedef struct lua_State lua_State;
typedef struct client client;

void ldbInit(void);
int ldbIsEnabled(void);
void ldbDisable(client *c);
void ldbEnable(client *c);
int ldbStartSession(client *c);
void ldbEndSession(client *c);
int ldbIsActive(void);
int ldbGetCurrentLine(void);
void ldbSetCurrentLine(int line);
void ldbSetBreakpointOnNextLine(int enable);
int ldbIsBreakpointOnNextLineEnabled(void);
int ldbShouldBreak(void);
int ldbIsStepEnabled(void);
void ldbSetStepMode(int enable);
void ldbLogSourceLine(int lnum);
void ldbLog(sds entry);
void ldbLogRespReply(char *reply);
int ldbRepl(lua_State *lua);
void ldbSendLogs(void);
sds ldbCatStackValue(sds s, lua_State *lua, int idx);
int ldbRemoveChild(int pid);
int ldbPendingChildren(void);
void ldbKillForkedSessions(void);

#endif /* _LUA_DEBUG_H_ */
