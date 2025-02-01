#ifndef _MODULE_H_
#define _MODULE_H_

/* This header file exposes a set of functions defined in module.c that are
 * not part of the module API, but are used by the core to interact with modules
 */

/* Extract encver / signature from a module type ID. */
#define VALKEYMODULE_TYPE_ENCVER_BITS 10
#define VALKEYMODULE_TYPE_ENCVER_MASK ((1 << VALKEYMODULE_TYPE_ENCVER_BITS) - 1)
#define VALKEYMODULE_TYPE_ENCVER(id) ((id) & VALKEYMODULE_TYPE_ENCVER_MASK)
#define VALKEYMODULE_TYPE_SIGN(id) \
    (((id) & ~((uint64_t)VALKEYMODULE_TYPE_ENCVER_MASK)) >> VALKEYMODULE_TYPE_ENCVER_BITS)

/* Bit flags for moduleTypeAuxSaveFunc */
#define VALKEYMODULE_AUX_BEFORE_RDB (1 << 0)
#define VALKEYMODULE_AUX_AFTER_RDB (1 << 1)

struct ValkeyModule;
struct ValkeyModuleIO;
struct ValkeyModuleDigest;
struct ValkeyModuleCtx;
struct moduleLoadQueueEntry;
struct ValkeyModuleKeyOptCtx;
struct ValkeyModuleCommand;
struct clusterState;

/* Each module type implementation should export a set of methods in order
 * to serialize and deserialize the value in the RDB file, rewrite the AOF
 * log, create the digest for "DEBUG DIGEST", and free the value when a key
 * is deleted. */
typedef void *(*moduleTypeLoadFunc)(struct ValkeyModuleIO *io, int encver);
typedef void (*moduleTypeSaveFunc)(struct ValkeyModuleIO *io, void *value);
typedef int (*moduleTypeAuxLoadFunc)(struct ValkeyModuleIO *rdb, int encver, int when);
typedef void (*moduleTypeAuxSaveFunc)(struct ValkeyModuleIO *rdb, int when);
typedef void (*moduleTypeRewriteFunc)(struct ValkeyModuleIO *io, struct serverObject *key, void *value);
typedef void (*moduleTypeDigestFunc)(struct ValkeyModuleDigest *digest, void *value);
typedef size_t (*moduleTypeMemUsageFunc)(const void *value);
typedef void (*moduleTypeFreeFunc)(void *value);
typedef size_t (*moduleTypeFreeEffortFunc)(struct serverObject *key, const void *value);
typedef void (*moduleTypeUnlinkFunc)(struct serverObject *key, void *value);
typedef void *(*moduleTypeCopyFunc)(struct serverObject *fromkey, struct serverObject *tokey, const void *value);
typedef int (*moduleTypeDefragFunc)(struct ValkeyModuleDefragCtx *ctx, struct serverObject *key, void **value);
typedef size_t (*moduleTypeMemUsageFunc2)(struct ValkeyModuleKeyOptCtx *ctx, const void *value, size_t sample_size);
typedef void (*moduleTypeFreeFunc2)(struct ValkeyModuleKeyOptCtx *ctx, void *value);
typedef size_t (*moduleTypeFreeEffortFunc2)(struct ValkeyModuleKeyOptCtx *ctx, const void *value);
typedef void (*moduleTypeUnlinkFunc2)(struct ValkeyModuleKeyOptCtx *ctx, void *value);
typedef void *(*moduleTypeCopyFunc2)(struct ValkeyModuleKeyOptCtx *ctx, const void *value);
typedef int (*moduleTypeAuthCallback)(struct ValkeyModuleCtx *ctx, void *username, void *password, const char **err);


/* The module type, which is referenced in each value of a given type, defines
 * the methods and links to the module exporting the type. */
typedef struct ValkeyModuleType {
    uint64_t id; /* Higher 54 bits of type ID + 10 lower bits of encoding ver. */
    struct ValkeyModule *module;
    moduleTypeLoadFunc rdb_load;
    moduleTypeSaveFunc rdb_save;
    moduleTypeRewriteFunc aof_rewrite;
    moduleTypeMemUsageFunc mem_usage;
    moduleTypeDigestFunc digest;
    moduleTypeFreeFunc free;
    moduleTypeFreeEffortFunc free_effort;
    moduleTypeUnlinkFunc unlink;
    moduleTypeCopyFunc copy;
    moduleTypeDefragFunc defrag;
    moduleTypeAuxLoadFunc aux_load;
    moduleTypeAuxSaveFunc aux_save;
    moduleTypeMemUsageFunc2 mem_usage2;
    moduleTypeFreeEffortFunc2 free_effort2;
    moduleTypeUnlinkFunc2 unlink2;
    moduleTypeCopyFunc2 copy2;
    moduleTypeAuxSaveFunc aux_save2;
    int aux_save_triggers;
    char name[10]; /* 9 bytes name + null term. Charset: A-Z a-z 0-9 _- */
} moduleType;

/* In Object 'robj' structures of type OBJ_MODULE, the value pointer
 * is set to the following structure, referencing the moduleType structure
 * in order to work with the value, and at the same time providing a raw
 * pointer to the value, as created by the module commands operating with
 * the module type.
 *
 * So for example in order to free such a value, it is possible to use
 * the following code:
 *
 *  if (robj->type == OBJ_MODULE) {
 *      moduleValue *mt = robj->ptr;
 *      mt->type->free(mt->value);
 *      zfree(mt); // We need to release this in-the-middle struct as well.
 *  }
 */
typedef struct moduleValue {
    moduleType *type;
    void *value;
} moduleValue;

/* This structure represents a module inside the system. */
typedef struct ValkeyModule {
    void *handle;                         /* Module dlopen() handle. */
    char *name;                           /* Module name. */
    int ver;                              /* Module version. We use just progressive integers. */
    int apiver;                           /* Module API version as requested during initialization.*/
    list *types;                          /* Module data types. */
    list *usedby;                         /* List of modules using APIs from this one. */
    list *using;                          /* List of modules we use some APIs of. */
    list *filters;                        /* List of filters the module has registered. */
    list *module_configs;                 /* List of configurations the module has registered */
    int configs_initialized;              /* Have the module configurations been initialized? */
    int in_call;                          /* RM_Call() nesting level */
    int in_hook;                          /* Hooks callback nesting level for this module (0 or 1). */
    int options;                          /* Module options and capabilities. */
    int blocked_clients;                  /* Count of ValkeyModuleBlockedClient in this module. */
    ValkeyModuleInfoFunc info_cb;         /* Callback for module to add INFO fields. */
    ValkeyModuleDefragFunc defrag_cb;     /* Callback for global data defrag. */
    struct moduleLoadQueueEntry *loadmod; /* Module load arguments for config rewrite. */
    int num_commands_with_acl_categories; /* Number of commands in this module included in acl categories */
    int onload;                           /* Flag to identify if the call is being made from Onload (0 or 1) */
    size_t num_acl_categories_added;      /* Number of acl categories added by this module. */
} ValkeyModule;

/* This is a wrapper for the 'rio' streams used inside rdb.c in the server, so that
 * the user does not have to take the total count of the written bytes nor
 * to care about error conditions. */
typedef struct ValkeyModuleIO {
    size_t bytes;         /* Bytes read / written so far. */
    rio *rio;             /* Rio stream. */
    moduleType *type;     /* Module type doing the operation. */
    int error;            /* True if error condition happened. */
    ValkeyModuleCtx *ctx; /* Optional context, see RM_GetContextFromIO()*/
    robj *key;            /* Optional name of key processed */
    int dbid;             /* The dbid of the key being processed, -1 when unknown. */
    sds pre_flush_buffer; /* A buffer that should be flushed before next write operation
                           * See rdbSaveSingleModuleAux for more details */
} ValkeyModuleIO;

/* Macro to initialize an IO context. Note that the 'ver' field is populated
 * inside rdb.c according to the version of the value to load. */
static inline void moduleInitIOContext(ValkeyModuleIO *iovar,
                                       moduleType *mtype,
                                       rio *rioptr,
                                       robj *keyptr,
                                       int db) {
    iovar->rio = rioptr;
    iovar->type = mtype;
    iovar->bytes = 0;
    iovar->error = 0;
    iovar->key = keyptr;
    iovar->dbid = db;
    iovar->ctx = NULL;
    iovar->pre_flush_buffer = NULL;
}

/* This is a structure used to export DEBUG DIGEST capabilities to
 * modules. We want to capture both the ordered and unordered elements of
 * a data structure, so that a digest can be created in a way that correctly
 * reflects the values. See the DEBUG DIGEST command implementation for more
 * background. */
typedef struct ValkeyModuleDigest {
    unsigned char o[20]; /* Ordered elements. */
    unsigned char x[20]; /* Xored elements. */
    robj *key;           /* Optional name of key processed */
    int dbid;            /* The dbid of the key being processed */
} ValkeyModuleDigest;

/* Just start with a digest composed of all zero bytes. */
static inline void moduleInitDigestContext(ValkeyModuleDigest *mdvar) {
    memset(mdvar->o, 0, sizeof(mdvar->o));
    memset(mdvar->x, 0, sizeof(mdvar->x));
}

void moduleEnqueueLoadModule(sds path, sds *argv, int argc);
sds moduleLoadQueueEntryToLoadmoduleOptionStr(ValkeyModule *module,
                                              const char *config_option_str);
ValkeyModuleCtx *moduleAllocateContext(void);
void moduleScriptingEngineInitContext(ValkeyModuleCtx *out_ctx,
                                      ValkeyModule *module,
                                      client *client);
void moduleFreeContext(ValkeyModuleCtx *ctx);
void moduleInitModulesSystem(void);
void moduleInitModulesSystemLast(void);
void modulesCron(void);
int moduleLoad(const char *path, void **argv, int argc, int is_loadex);
int moduleUnload(sds name, const char **errmsg);
void moduleLoadFromQueue(void);
int moduleGetCommandKeysViaAPI(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
int moduleGetCommandChannelsViaAPI(struct serverCommand *cmd, robj **argv, int argc, getKeysResult *result);
moduleType *moduleTypeLookupModuleByID(uint64_t id);
moduleType *moduleTypeLookupModuleByName(const char *name);
moduleType *moduleTypeLookupModuleByNameIgnoreCase(const char *name);
void moduleTypeNameByID(char *name, uint64_t moduleid);
const char *moduleTypeModuleName(moduleType *mt);
const char *moduleNameFromCommand(struct serverCommand *cmd);
void moduleFreeContext(ValkeyModuleCtx *ctx);
void moduleCallCommandUnblockedHandler(client *c);
int isModuleClientUnblocked(client *c);
void unblockClientFromModule(client *c);
void moduleHandleBlockedClients(void);
void moduleBlockedClientTimedOut(client *c, int from_module);
void modulePipeReadable(aeEventLoop *el, int fd, void *privdata, int mask);
size_t moduleCount(void);
void moduleAcquireGIL(void);
int moduleTryAcquireGIL(void);
void moduleReleaseGIL(void);
void moduleNotifyKeyspaceEvent(int type, const char *event, robj *key, int dbid);
void firePostExecutionUnitJobs(void);
void moduleCallCommandFilters(client *c);
void modulePostExecutionUnitOperations(void);
void ModuleForkDoneHandler(int exitcode, int bysignal);
int TerminateModuleForkChild(int child_pid, int wait);
ssize_t rdbSaveModulesAux(rio *rdb, int when);
int moduleAllDatatypesHandleErrors(void);
int moduleAllModulesHandleReplAsyncLoad(void);
sds modulesCollectInfo(sds info, dict *sections_dict, int for_crash_report, int sections);
void moduleFireServerEvent(uint64_t eid, int subid, void *data);
void processModuleLoadingProgressEvent(int is_aof);
int moduleTryServeClientBlockedOnKey(client *c, robj *key);
void moduleUnblockClient(client *c);
int moduleBlockedClientMayTimeout(client *c);
int moduleClientIsBlockedOnKeys(client *c);
void moduleNotifyUserChanged(client *c);
void moduleNotifyKeyUnlink(robj *key, robj *val, int dbid, int flags);
size_t moduleGetFreeEffort(robj *key, robj *val, int dbid);
size_t moduleGetMemUsage(robj *key, robj *val, size_t sample_size, int dbid);
robj *moduleTypeDupOrReply(client *c, robj *fromkey, robj *tokey, int todb, robj *value);
int moduleDefragValue(robj *key, robj *obj, int dbid);
int moduleLateDefrag(robj *key, robj *value, unsigned long *cursor, monotime endtime, int dbid);
void moduleDefragGlobals(void);
void *moduleGetHandleByName(char *modulename);
int moduleIsModuleCommand(void *module_handle, struct serverCommand *cmd);
void freeClientModuleData(client *c);

#endif /* _MODULE_H_ */
