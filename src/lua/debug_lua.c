/*
 * Copyright (c) 2009-2012, Redis Ltd.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "debug_lua.h"
#include "script_lua.h"

#include "../connection.h"
#include "../adlist.h"
#include "../server.h"
#include "../scripting_engine.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <signal.h>

/* ---------------------------------------------------------------------------
 * LDB: Lua debugging facilities
 * ------------------------------------------------------------------------- */

/* Debugger shared state is stored inside this global structure. */
#define LDB_BREAKPOINTS_MAX 64 /* Max number of breakpoints. */
struct ldbState {
    int active;                  /* Are we debugging EVAL right now? */
    int bp[LDB_BREAKPOINTS_MAX]; /* An array of breakpoints line numbers. */
    int bpcount;                 /* Number of valid entries inside bp. */
    int step;                    /* Stop at next line regardless of breakpoints. */
    int luabp;                   /* Stop at next line because server.breakpoint() was called. */
    sds *src;                    /* Lua script source code split by line. */
    int lines;                   /* Number of lines in 'src'. */
    int currentline;             /* Current line number. */
} ldb;

/* Initialize Lua debugger data structures. */
void ldbInit(void) {
    ldb.active = 0;
    ldb.bpcount = 0;
    ldb.step = 0;
    ldb.luabp = 0;
    ldb.src = NULL;
    ldb.lines = 0;
    ldb.currentline = -1;
}

int ldbIsEnabled(void) {
    return ldb.active && ldb.step;
}

/* Enable debug mode of Lua scripts for this client. */
void ldbEnable(void) {
    ldb.active = 1;
    ldb.step = 1;
    ldb.bpcount = 0;
    ldb.luabp = 0;
}

/* Exit debugging mode from the POV of client. This function is not enough
 * to properly shut down a client debugging session, see ldbEndSession()
 * for more information. */
void ldbDisable(void) {
    ldb.step = 0;
    ldb.active = 0;
}

void ldbStart(robj *source) {
    ldb.active = 1;

    /* First argument of EVAL is the script itself. We split it into different
     * lines since this is the way the debugger accesses the source code. */
    sds srcstring = sdsdup(source->ptr);
    size_t srclen = sdslen(srcstring);
    while (srclen && (srcstring[srclen - 1] == '\n' || srcstring[srclen - 1] == '\r')) {
        srcstring[--srclen] = '\0';
    }
    sdssetlen(srcstring, srclen);
    ldb.src = sdssplitlen(srcstring, sdslen(srcstring), "\n", 1, &ldb.lines);
    sdsfree(srcstring);
}

void ldbEnd(void) {
    sdsfreesplitres(ldb.src, ldb.lines);
    ldb.lines = 0;
    ldb.active = 0;
}

void ldbLog(sds entry) {
    scriptingEngineDebuggerLog(entry);
}

void ldbSendLogs(void) {
    scriptingEngineDebuggerFlushLogs();
}

/* Return a pointer to ldb.src source code line, considering line to be
 * one-based, and returning a special string for out of range lines. */
static char *ldbGetSourceLine(int line) {
    int idx = line - 1;
    if (idx < 0 || idx >= ldb.lines) return "<out of range source code line>";
    return ldb.src[idx];
}

/* Return true if there is a breakpoint in the specified line. */
static int ldbIsBreakpoint(int line) {
    int j;

    for (j = 0; j < ldb.bpcount; j++)
        if (ldb.bp[j] == line) return 1;
    return 0;
}

/* Add the specified breakpoint. Ignore it if we already reached the max.
 * Returns 1 if the breakpoint was added (or was already set). 0 if there is
 * no space for the breakpoint or if the line is invalid. */
static int ldbAddBreakpoint(int line) {
    if (line <= 0 || line > ldb.lines) return 0;
    if (!ldbIsBreakpoint(line) && ldb.bpcount != LDB_BREAKPOINTS_MAX) {
        ldb.bp[ldb.bpcount++] = line;
        return 1;
    }
    return 0;
}

/* Remove the specified breakpoint, returning 1 if the operation was
 * performed or 0 if there was no such breakpoint. */
static int ldbDelBreakpoint(int line) {
    int j;

    for (j = 0; j < ldb.bpcount; j++) {
        if (ldb.bp[j] == line) {
            ldb.bpcount--;
            memmove(ldb.bp + j, ldb.bp + j + 1, ldb.bpcount - j);
            return 1;
        }
    }
    return 0;
}

/* Log the specified line in the Lua debugger output. */
void ldbLogSourceLine(int lnum) {
    char *line = ldbGetSourceLine(lnum);
    char *prefix;
    int bp = ldbIsBreakpoint(lnum);
    int current = ldb.currentline == lnum;

    if (current && bp)
        prefix = "->#";
    else if (current)
        prefix = "-> ";
    else if (bp)
        prefix = "  #";
    else
        prefix = "   ";
    sds thisline = sdscatprintf(sdsempty(), "%s%-3d %s", prefix, lnum, line);
    ldbLog(thisline);
}

/* Implement the "list" command of the Lua debugger. If around is 0
 * the whole file is listed, otherwise only a small portion of the file
 * around the specified line is shown. When a line number is specified
 * the amount of context (lines before/after) is specified via the
 * 'context' argument. */
static void ldbList(int around, int context) {
    int j;

    for (j = 1; j <= ldb.lines; j++) {
        if (around != 0 && abs(around - j) > context) continue;
        ldbLogSourceLine(j);
    }
}

/* Append a human readable representation of the Lua value at position 'idx'
 * on the stack of the 'lua' state, to the SDS string passed as argument.
 * The new SDS string with the represented value attached is returned.
 * Used in order to implement ldbLogStackValue().
 *
 * The element is not automatically removed from the stack, nor it is
 * converted to a different type. */
#define LDB_MAX_VALUES_DEPTH (LUA_MINSTACK / 2)
static sds ldbCatStackValueRec(sds s, lua_State *lua, int idx, int level) {
    int t = lua_type(lua, idx);

    if (level++ == LDB_MAX_VALUES_DEPTH) return sdscat(s, "<max recursion level reached! Nested table?>");

    switch (t) {
    case LUA_TSTRING: {
        size_t strl;
        char *strp = (char *)lua_tolstring(lua, idx, &strl);
        s = sdscatrepr(s, strp, strl);
    } break;
    case LUA_TBOOLEAN: s = sdscat(s, lua_toboolean(lua, idx) ? "true" : "false"); break;
    case LUA_TNUMBER: s = sdscatprintf(s, "%g", (double)lua_tonumber(lua, idx)); break;
    case LUA_TNIL: s = sdscatlen(s, "nil", 3); break;
    case LUA_TTABLE: {
        int expected_index = 1; /* First index we expect in an array. */
        int is_array = 1;       /* Will be set to null if check fails. */
        /* Note: we create two representations at the same time, one
         * assuming the table is an array, one assuming it is not. At the
         * end we know what is true and select the right one. */
        sds repr1 = sdsempty();
        sds repr2 = sdsempty();
        lua_pushnil(lua); /* The first key to start the iteration is nil. */
        while (lua_next(lua, idx - 1)) {
            /* Test if so far the table looks like an array. */
            if (is_array && (lua_type(lua, -2) != LUA_TNUMBER || lua_tonumber(lua, -2) != expected_index)) is_array = 0;
            /* Stack now: table, key, value */
            /* Array repr. */
            repr1 = ldbCatStackValueRec(repr1, lua, -1, level);
            repr1 = sdscatlen(repr1, "; ", 2);
            /* Full repr. */
            repr2 = sdscatlen(repr2, "[", 1);
            repr2 = ldbCatStackValueRec(repr2, lua, -2, level);
            repr2 = sdscatlen(repr2, "]=", 2);
            repr2 = ldbCatStackValueRec(repr2, lua, -1, level);
            repr2 = sdscatlen(repr2, "; ", 2);
            lua_pop(lua, 1); /* Stack: table, key. Ready for next iteration. */
            expected_index++;
        }
        /* Strip the last " ;" from both the representations. */
        if (sdslen(repr1)) sdsrange(repr1, 0, -3);
        if (sdslen(repr2)) sdsrange(repr2, 0, -3);
        /* Select the right one and discard the other. */
        s = sdscatlen(s, "{", 1);
        s = sdscatsds(s, is_array ? repr1 : repr2);
        s = sdscatlen(s, "}", 1);
        sdsfree(repr1);
        sdsfree(repr2);
    } break;
    case LUA_TFUNCTION:
    case LUA_TUSERDATA:
    case LUA_TTHREAD:
    case LUA_TLIGHTUSERDATA: {
        const void *p = lua_topointer(lua, idx);
        char *typename = "unknown";
        if (t == LUA_TFUNCTION)
            typename = "function";
        else if (t == LUA_TUSERDATA)
            typename = "userdata";
        else if (t == LUA_TTHREAD)
            typename = "thread";
        else if (t == LUA_TLIGHTUSERDATA)
            typename = "light-userdata";
        s = sdscatprintf(s, "\"%s@%p\"", typename, p);
    } break;
    default: s = sdscat(s, "\"<unknown-lua-type>\""); break;
    }
    return s;
}

/* Higher level wrapper for ldbCatStackValueRec() that just uses an initial
 * recursion level of '0'. */
sds ldbCatStackValue(sds s, lua_State *lua, int idx) {
    return ldbCatStackValueRec(s, lua, idx, 0);
}

/* Produce a debugger log entry representing the value of the Lua object
 * currently on the top of the stack. The element is not popped nor modified.
 * Check ldbCatStackValue() for the actual implementation. */
static void ldbLogStackValue(lua_State *lua, char *prefix) {
    sds s = sdsnew(prefix);
    s = ldbCatStackValue(s, lua, -1);
    scriptingEngineDebuggerLogWithMaxLen(s);
}

/* Log a RESP reply as debugger output, in a human readable format.
 * If the resulting string is longer than 'len' plus a few more chars
 * used as prefix, it gets truncated. */
void ldbLogRespReply(char *reply) {
    scriptingEngineDebuggerLogRespReplyStr(reply);
}

/* Implements the "print <var>" command of the Lua debugger. It scans for Lua
 * var "varname" starting from the current stack frame up to the top stack
 * frame. The first matching variable is printed. */
static void ldbPrint(lua_State *lua, char *varname) {
    lua_Debug ar;

    int l = 0; /* Stack level. */
    while (lua_getstack(lua, l, &ar) != 0) {
        l++;
        const char *name;
        int i = 1; /* Variable index. */
        while ((name = lua_getlocal(lua, &ar, i)) != NULL) {
            i++;
            if (strcmp(varname, name) == 0) {
                ldbLogStackValue(lua, "<value> ");
                lua_pop(lua, 1);
                return;
            } else {
                lua_pop(lua, 1); /* Discard the var name on the stack. */
            }
        }
    }

    /* Let's try with global vars in two selected cases */
    if (!strcmp(varname, "ARGV") || !strcmp(varname, "KEYS")) {
        lua_getglobal(lua, varname);
        ldbLogStackValue(lua, "<value> ");
        lua_pop(lua, 1);
    } else {
        ldbLog(sdsnew("No such variable."));
    }
}

/* Implements the "print" command (without arguments) of the Lua debugger.
 * Prints all the variables in the current stack frame. */
static void ldbPrintAll(lua_State *lua) {
    lua_Debug ar;
    int vars = 0;

    if (lua_getstack(lua, 0, &ar) != 0) {
        const char *name;
        int i = 1; /* Variable index. */
        while ((name = lua_getlocal(lua, &ar, i)) != NULL) {
            i++;
            if (!strstr(name, "(*temporary)")) {
                sds prefix = sdscatprintf(sdsempty(), "<value> %s = ", name);
                ldbLogStackValue(lua, prefix);
                sdsfree(prefix);
                vars++;
            }
            lua_pop(lua, 1);
        }
    }

    if (vars == 0) {
        ldbLog(sdsnew("No local variables in the current context."));
    }
}

/* Implements the break command to list, add and remove breakpoints. */
static void ldbBreak(robj **argv, int argc) {
    if (argc == 1) {
        if (ldb.bpcount == 0) {
            ldbLog(sdsnew("No breakpoints set. Use 'b <line>' to add one."));
            return;
        } else {
            ldbLog(sdscatfmt(sdsempty(), "%i breakpoints set:", ldb.bpcount));
            int j;
            for (j = 0; j < ldb.bpcount; j++) ldbLogSourceLine(ldb.bp[j]);
        }
    } else {
        int j;
        for (j = 1; j < argc; j++) {
            char *arg = argv[j]->ptr;
            long line;
            if (!string2l(arg, sdslen(arg), &line)) {
                ldbLog(sdscatfmt(sdsempty(), "Invalid argument:'%s'", arg));
            } else {
                if (line == 0) {
                    ldb.bpcount = 0;
                    ldbLog(sdsnew("All breakpoints removed."));
                } else if (line > 0) {
                    if (ldb.bpcount == LDB_BREAKPOINTS_MAX) {
                        ldbLog(sdsnew("Too many breakpoints set."));
                    } else if (ldbAddBreakpoint(line)) {
                        ldbList(line, 1);
                    } else {
                        ldbLog(sdsnew("Wrong line number."));
                    }
                } else if (line < 0) {
                    if (ldbDelBreakpoint(-line))
                        ldbLog(sdsnew("Breakpoint removed."));
                    else
                        ldbLog(sdsnew("No breakpoint in the specified line."));
                }
            }
        }
    }
}

/* Implements the Lua debugger "eval" command. It just compiles the user
 * passed fragment of code and executes it, showing the result left on
 * the stack. */
static void ldbEval(lua_State *lua, robj **argv, int argc) {
    /* Glue the script together if it is composed of multiple arguments. */
    sds code = sdsempty();
    for (int j = 1; j < argc; j++) {
        code = sdscatsds(code, argv[j]->ptr);
        if (j != argc - 1) code = sdscatlen(code, " ", 1);
    }
    sds expr = sdscatsds(sdsnew("return "), code);

    /* Try to compile it as an expression, prepending "return ". */
    if (luaL_loadbuffer(lua, expr, sdslen(expr), "@ldb_eval")) {
        lua_pop(lua, 1);
        /* Failed? Try as a statement. */
        if (luaL_loadbuffer(lua, code, sdslen(code), "@ldb_eval")) {
            ldbLog(sdscatfmt(sdsempty(), "<error> %s", lua_tostring(lua, -1)));
            lua_pop(lua, 1);
            sdsfree(code);
            sdsfree(expr);
            return;
        }
    }

    /* Call it. */
    sdsfree(code);
    sdsfree(expr);
    if (lua_pcall(lua, 0, 1, 0)) {
        ldbLog(sdscatfmt(sdsempty(), "<error> %s", lua_tostring(lua, -1)));
        lua_pop(lua, 1);
        return;
    }
    ldbLogStackValue(lua, "<retval> ");
    lua_pop(lua, 1);
}

/* Implement the debugger "server" command. We use a trick in order to make
 * the implementation very simple: we just call the Lua server.call() command
 * implementation, with ldb.step enabled, so as a side effect the command
 * and its reply are logged. */
static void ldbServer(lua_State *lua, robj **argv, int argc) {
    int j;

    if (!lua_checkstack(lua, argc + 1)) {
        /* Increase the Lua stack if needed to make sure there is enough room
         * to push 'argc + 1' elements to the stack. On failure, return error.
         * Notice that we need, in worst case, 'argc + 1' elements because we push all the arguments
         * given by the user (without the first argument) and we also push the 'server' global table and
         * 'server.call' function so:
         * (1 (server table)) + (1 (server.call function)) + (argc - 1 (all arguments without the first)) = argc + 1*/
        ldbLogRespReply("max lua stack reached");
        return;
    }

    lua_getglobal(lua, "server");
    lua_pushstring(lua, "call");
    lua_gettable(lua, -2); /* Stack: server, server.call */
    for (j = 1; j < argc; j++)
        lua_pushlstring(lua, argv[j]->ptr, sdslen(argv[j]->ptr));
    ldb.step = 1;                   /* Force server.call() to log. */
    lua_pcall(lua, argc - 1, 1, 0); /* Stack: server, result */
    ldb.step = 0;                   /* Disable logging. */
    lua_pop(lua, 2);                /* Discard the result and clean the stack. */
}

/* Implements "trace" command of the Lua debugger. It just prints a backtrace
 * querying Lua starting from the current callframe back to the outer one. */
static void ldbTrace(lua_State *lua) {
    lua_Debug ar;
    int level = 0;

    while (lua_getstack(lua, level, &ar)) {
        lua_getinfo(lua, "Snl", &ar);
        if (strstr(ar.short_src, "user_script") != NULL) {
            ldbLog(sdscatprintf(sdsempty(), "%s %s:", (level == 0) ? "In" : "From", ar.name ? ar.name : "top level"));
            ldbLogSourceLine(ar.currentline);
        }
        level++;
    }
    if (level == 0) {
        ldbLog(sdsnew("<error> Can't retrieve Lua stack."));
    }
}

/* Implements the debugger "maxlen" command. It just queries or sets the
 * ldb.maxlen variable. */
static void ldbMaxlen(robj **argv, int argc) {
    if (argc == 2) {
        int newval = atoi(argv[1]->ptr);
        scriptingEngineDebuggerSetMaxlen(newval);
    }
    size_t maxlen = scriptingEngineDebuggerGetMaxlen();
    if (maxlen) {
        ldbLog(sdscatprintf(sdsempty(), "<value> replies are truncated at %u bytes.", (uint32_t)maxlen));
    } else {
        ldbLog(sdscatprintf(sdsempty(), "<value> replies are unlimited."));
    }
}

/* Read debugging commands from client.
 * Return C_OK if the debugging session is continuing, otherwise
 * C_ERR if the client closed the connection or is timing out. */
int ldbRepl(lua_State *lua) {
    robj **argv;
    size_t argc;
    int client_disconnected = 0;
    robj *err = NULL;

    /* We continue processing commands until a command that should return
     * to the Lua interpreter is found. */
    while (1) {
        while ((argv = scriptingEngineDebuggerReadCommand(&argc, &client_disconnected, &err)) == NULL) {
            if (err) {
                luaPushError(lua, err->ptr);
                decrRefCount(err);
                luaError(lua);
            } else if (client_disconnected) {
                /* Make sure the script runs without user input since the
                 * client is no longer connected. */
                ldb.step = 0;
                ldb.bpcount = 0;
                return C_ERR;
            }
        }

        serverAssert(argv != NULL);

        /* Execute the command. */
        if (!strcasecmp(argv[0]->ptr, "h") || !strcasecmp(argv[0]->ptr, "help")) {
            ldbLog(sdsnew("Lua debugger help:"));
            ldbLog(sdsnew("[h]elp               Show this help."));
            ldbLog(sdsnew("[s]tep               Run current line and stop again."));
            ldbLog(sdsnew("[n]ext               Alias for step."));
            ldbLog(sdsnew("[c]ontinue           Run till next breakpoint."));
            ldbLog(sdsnew("[l]ist               List source code around current line."));
            ldbLog(sdsnew("[l]ist [line]        List source code around [line]."));
            ldbLog(sdsnew("                     line = 0 means: current position."));
            ldbLog(sdsnew("[l]ist [line] [ctx]  In this form [ctx] specifies how many lines"));
            ldbLog(sdsnew("                     to show before/after [line]."));
            ldbLog(sdsnew("[w]hole              List all source code. Alias for 'list 1 1000000'."));
            ldbLog(sdsnew("[p]rint              Show all the local variables."));
            ldbLog(sdsnew("[p]rint <var>        Show the value of the specified variable."));
            ldbLog(sdsnew("                     Can also show global vars KEYS and ARGV."));
            ldbLog(sdsnew("[b]reak              Show all breakpoints."));
            ldbLog(sdsnew("[b]reak <line>       Add a breakpoint to the specified line."));
            ldbLog(sdsnew("[b]reak -<line>      Remove breakpoint from the specified line."));
            ldbLog(sdsnew("[b]reak 0            Remove all breakpoints."));
            ldbLog(sdsnew("[t]race              Show a backtrace."));
            ldbLog(sdsnew("[e]val <code>        Execute some Lua code (in a different callframe)."));
            ldbLog(sdsnew("[v]alkey <cmd>       Execute a command."));
            ldbLog(sdsnew("[m]axlen [len]       Trim logged replies and Lua var dumps to len."));
            ldbLog(sdsnew("                     Specifying zero as <len> means unlimited."));
            ldbLog(sdsnew("[a]bort              Stop the execution of the script. In sync"));
            ldbLog(sdsnew("                     mode dataset changes will be retained."));
            ldbLog(sdsnew(""));
            ldbLog(sdsnew("Debugger functions you can call from Lua scripts:"));
            ldbLog(sdsnew("server.debug()       Produce logs in the debugger console."));
            ldbLog(sdsnew("server.breakpoint()  Stop execution like if there was a breakpoint in the"));
            ldbLog(sdsnew("                     next line of code."));
            scriptingEngineDebuggerFlushLogs();
        } else if (!strcasecmp(argv[0]->ptr, "s") || !strcasecmp(argv[0]->ptr, "step") || !strcasecmp(argv[0]->ptr, "n") ||
                   !strcasecmp(argv[0]->ptr, "next")) {
            ldb.step = 1;
            break;
        } else if (!strcasecmp(argv[0]->ptr, "c") || !strcasecmp(argv[0]->ptr, "continue")) {
            break;
        } else if (!strcasecmp(argv[0]->ptr, "t") || !strcasecmp(argv[0]->ptr, "trace")) {
            ldbTrace(lua);
            scriptingEngineDebuggerFlushLogs();
        } else if (!strcasecmp(argv[0]->ptr, "m") || !strcasecmp(argv[0]->ptr, "maxlen")) {
            ldbMaxlen(argv, argc);
            scriptingEngineDebuggerFlushLogs();
        } else if (!strcasecmp(argv[0]->ptr, "b") || !strcasecmp(argv[0]->ptr, "break")) {
            ldbBreak(argv, argc);
            scriptingEngineDebuggerFlushLogs();
        } else if (!strcasecmp(argv[0]->ptr, "e") || !strcasecmp(argv[0]->ptr, "eval")) {
            ldbEval(lua, argv, argc);
            scriptingEngineDebuggerFlushLogs();
        } else if (!strcasecmp(argv[0]->ptr, "a") || !strcasecmp(argv[0]->ptr, "abort")) {
            luaPushError(lua, "script aborted for user request");
            luaError(lua);
        } else if (argc > 1 && ((!strcasecmp(argv[0]->ptr, "r") || !strcasecmp(argv[0]->ptr, "redis")) ||
                                (!strcasecmp(argv[0]->ptr, "v") || !strcasecmp(argv[0]->ptr, "valkey")) ||
                                !strcasecmp(argv[0]->ptr, SERVER_API_NAME))) {
            /* [r]redis or [v]alkey calls a command. We accept "server" too, but
             * not "s" because that's "step". Neither can we use [c]all because
             * "c" is continue. */
            ldbServer(lua, argv, argc);
            scriptingEngineDebuggerFlushLogs();
        } else if ((!strcasecmp(argv[0]->ptr, "p") || !strcasecmp(argv[0]->ptr, "print"))) {
            if (argc == 2)
                ldbPrint(lua, argv[1]->ptr);
            else
                ldbPrintAll(lua);
            scriptingEngineDebuggerFlushLogs();
        } else if (!strcasecmp(argv[0]->ptr, "l") || !strcasecmp(argv[0]->ptr, "list")) {
            int around = ldb.currentline, ctx = 5;
            if (argc > 1) {
                int num = atoi(argv[1]->ptr);
                if (num > 0) around = num;
            }
            if (argc > 2) ctx = atoi(argv[2]->ptr);
            ldbList(around, ctx);
            scriptingEngineDebuggerFlushLogs();
        } else if (!strcasecmp(argv[0]->ptr, "w") || !strcasecmp(argv[0]->ptr, "whole")) {
            ldbList(1, 1000000);
            scriptingEngineDebuggerFlushLogs();
        } else {
            ldbLog(sdsnew("<error> Unknown Lua debugger command or "
                          "wrong number of arguments."));
            scriptingEngineDebuggerFlushLogs();
        }

        /* Free the command vector. */
        for (size_t i = 0; i < argc; i++) {
            decrRefCount(argv[i]);
        }
        zfree(argv);
    }

    /* Free the current command argv if we break inside the while loop. */
    for (size_t i = 0; i < argc; i++) {
        decrRefCount(argv[i]);
    }
    zfree(argv);

    return C_OK;
}

int ldbIsActive(void) {
    return ldb.active;
}

int ldbGetCurrentLine(void) {
    return ldb.currentline;
}

void ldbSetCurrentLine(int line) {
    ldb.currentline = line;
}

void ldbSetBreakpointOnNextLine(int enable) {
    ldb.luabp = enable;
}

int ldbIsBreakpointOnNextLineEnabled(void) {
    return ldb.luabp;
}

int ldbShouldBreak(void) {
    return ldbIsBreakpoint(ldb.currentline) || ldb.luabp;
}

int ldbIsStepEnabled(void) {
    return ldb.step;
}

void ldbSetStepMode(int enable) {
    ldb.step = enable;
}
