#include "valkeymodule.h"
#include <strings.h>
#include <limits.h>
int mutable_bool_val;
int immutable_bool_val;
long long longval;
long long memval;
unsigned long long mutable_ull_val;
unsigned long long immutable_ull_val;
ValkeyModuleString *strval = NULL;
int enumval;
int flagsval;

/* Series of get and set callbacks for each type of config, these rely on the privdata ptr
 * to point to the config, and they register the configs as such. Note that one could also just
 * use names if they wanted, and store anything in privdata. */
int getBoolConfigCommand(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    return (*(int *)privdata);
}

int setBoolConfigCommand(const char *name, int new, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    *(int *)privdata = new;
    return VALKEYMODULE_OK;
}

long long getNumericConfigCommand(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    return (*(long long *) privdata);
}

unsigned long long getUnsignedNumericConfigCommand(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    return (*(unsigned long long *) privdata);
}

int setNumericConfigCommand(const char *name, long long new, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    *(long long *)privdata = new;
    return VALKEYMODULE_OK;
}

int setUnsignedNumericConfigCommand(const char *name, unsigned long long new, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    *(unsigned long long *)privdata = new;
    return VALKEYMODULE_OK;
}

ValkeyModuleString *getStringConfigCommand(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return strval;
}
int setStringConfigCommand(const char *name, ValkeyModuleString *new, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    VALKEYMODULE_NOT_USED(privdata);
    size_t len;
    if (!strcasecmp(ValkeyModule_StringPtrLen(new, &len), "rejectisfreed")) {
        *err = ValkeyModule_CreateString(NULL, "Cannot set string to 'rejectisfreed'", 36);
        return VALKEYMODULE_ERR;
    }
    if (strval) ValkeyModule_FreeString(NULL, strval);
    ValkeyModule_RetainString(NULL, new);
    strval = new;
    return VALKEYMODULE_OK;
}

int getEnumConfigCommand(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return enumval;
}

int setEnumConfigCommand(const char *name, int val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    VALKEYMODULE_NOT_USED(privdata);
    enumval = val;
    return VALKEYMODULE_OK;
}

int getFlagsConfigCommand(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return flagsval;
}

int setFlagsConfigCommand(const char *name, int val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    VALKEYMODULE_NOT_USED(privdata);
    flagsval = val;
    return VALKEYMODULE_OK;
}

int boolApplyFunc(ValkeyModuleCtx *ctx, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(privdata);
    if (mutable_bool_val && immutable_bool_val) {
        *err = ValkeyModule_CreateString(NULL, "Bool configs cannot both be yes.", 32);
        return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

int longlongApplyFunc(ValkeyModuleCtx *ctx, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(privdata);
    if (longval == memval) {
        *err = ValkeyModule_CreateString(NULL, "These configs cannot equal each other.", 38);
        return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

int unsignedlonglongApplyFunc(ValkeyModuleCtx *ctx, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(privdata);
    if (mutable_ull_val == immutable_ull_val) {
        *err = ValkeyModule_CreateString(NULL, "These configs cannot equal each other.", 38);
        return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

int registerBlockCheck(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    int response_ok = 0;
    int result = ValkeyModule_RegisterBoolConfig(ctx, "mutable_bool", 1, VALKEYMODULE_CONFIG_DEFAULT, getBoolConfigCommand, setBoolConfigCommand, boolApplyFunc, &mutable_bool_val);
    response_ok |= (result == VALKEYMODULE_OK);

    result = ValkeyModule_RegisterStringConfig(ctx, "string", "secret password", VALKEYMODULE_CONFIG_DEFAULT, getStringConfigCommand, setStringConfigCommand, NULL, NULL);
    response_ok |= (result == VALKEYMODULE_OK);

    const char *enum_vals[] = {"none", "five", "one", "two", "four"};
    const int int_vals[] = {0, 5, 1, 2, 4};
    result = ValkeyModule_RegisterEnumConfig(ctx, "enum", 1, VALKEYMODULE_CONFIG_DEFAULT, enum_vals, int_vals, 5, getEnumConfigCommand, setEnumConfigCommand, NULL, NULL);
    response_ok |= (result == VALKEYMODULE_OK);

    result = ValkeyModule_RegisterNumericConfig(ctx, "numeric", -1, VALKEYMODULE_CONFIG_DEFAULT, -5, 2000, getNumericConfigCommand, setNumericConfigCommand, longlongApplyFunc, &longval);
    response_ok |= (result == VALKEYMODULE_OK);
    
    result = ValkeyModule_RegisterUnsignedNumericConfig(ctx, "unsigned_numeric", 1, VALKEYMODULE_CONFIG_DEFAULT | VALKEYMODULE_CONFIG_UNSIGNED, 0, LLONG_MAX * 1ULL + 1000, getUnsignedNumericConfigCommand, setUnsignedNumericConfigCommand, unsignedlonglongApplyFunc, &mutable_ull_val);
    response_ok |= (result == VALKEYMODULE_OK);

    result = ValkeyModule_LoadConfigs(ctx);
    response_ok |= (result == VALKEYMODULE_OK);
    
    /* This validates that it's not possible to register/load configs outside OnLoad,
     * thus returns an error if they succeed. */
    if (response_ok) {
        ValkeyModule_ReplyWithError(ctx, "UNEXPECTEDOK");
    } else {
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    }
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "moduleconfigs", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;

    if (ValkeyModule_RegisterBoolConfig(ctx, "mutable_bool", 1, VALKEYMODULE_CONFIG_DEFAULT, getBoolConfigCommand, setBoolConfigCommand, boolApplyFunc, &mutable_bool_val) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    /* Immutable config here. */
    if (ValkeyModule_RegisterBoolConfig(ctx, "immutable_bool", 0, VALKEYMODULE_CONFIG_IMMUTABLE, getBoolConfigCommand, setBoolConfigCommand, boolApplyFunc, &immutable_bool_val) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_RegisterStringConfig(ctx, "string", "secret password", VALKEYMODULE_CONFIG_DEFAULT, getStringConfigCommand, setStringConfigCommand, NULL, NULL) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* On the stack to make sure we're copying them. */
    const char *enum_vals[] = {"none", "five", "one", "two", "four"};
    const int int_vals[] = {0, 5, 1, 2, 4};

    if (ValkeyModule_RegisterEnumConfig(ctx, "enum", 1, VALKEYMODULE_CONFIG_DEFAULT, enum_vals, int_vals, 5, getEnumConfigCommand, setEnumConfigCommand, NULL, NULL) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_RegisterEnumConfig(ctx, "flags", 3, VALKEYMODULE_CONFIG_DEFAULT | VALKEYMODULE_CONFIG_BITFLAGS, enum_vals, int_vals, 5, getFlagsConfigCommand, setFlagsConfigCommand, NULL, NULL) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    /* Memory config here. */
    if (ValkeyModule_RegisterNumericConfig(ctx, "memory_numeric", 1024, VALKEYMODULE_CONFIG_DEFAULT | VALKEYMODULE_CONFIG_MEMORY, 0, 3000000, getNumericConfigCommand, setNumericConfigCommand, longlongApplyFunc, &memval) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_RegisterNumericConfig(ctx, "numeric", -1, VALKEYMODULE_CONFIG_DEFAULT, -5, 2000, getNumericConfigCommand, setNumericConfigCommand, longlongApplyFunc, &longval) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_RegisterUnsignedNumericConfig(ctx, "unsigned_numeric", 1, VALKEYMODULE_CONFIG_DEFAULT | VALKEYMODULE_CONFIG_UNSIGNED, 0, LLONG_MAX * 1ULL + 1000, getUnsignedNumericConfigCommand, setUnsignedNumericConfigCommand, NULL, &mutable_ull_val) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    size_t len;
    if (argc && !strcasecmp(ValkeyModule_StringPtrLen(argv[0], &len), "noload")) {
        return VALKEYMODULE_OK;
    } else if (ValkeyModule_LoadConfigs(ctx) == VALKEYMODULE_ERR) {
        if (strval) {
            ValkeyModule_FreeString(ctx, strval);
            strval = NULL;
        }
        return VALKEYMODULE_ERR;
    }
    /* Creates a command which registers configs outside OnLoad() function. */
    if (ValkeyModule_CreateCommand(ctx,"block.register.configs.outside.onload", registerBlockCheck, "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
  
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    VALKEYMODULE_NOT_USED(ctx);
    if (strval) {
        ValkeyModule_FreeString(ctx, strval);
        strval = NULL;
    }
    return VALKEYMODULE_OK;
}
