/* This module is used to test a use case of a module that stores information
 * about keys in global memory, and relies on the enhanced data type callbacks to
 * get key name and dbid on various operations.
 *
 * it simulates a simple memory allocator. The smallest allocation unit of 
 * the allocator is a mem block with a size of 4KB. Multiple mem blocks are combined 
 * using a linked list. These linked lists are placed in a global dict named 'mem_pool'.
 * Each db has a 'mem_pool'. You can use the 'mem.alloc' command to allocate a specified 
 * number of mem blocks, and use 'mem.free' to release the memory. Use 'mem.write', 'mem.read'
 * to write and read the specified mem block (note that each mem block can only be written once).
 * Use 'mem.usage' to get the memory usage under different dbs, and it will return the size 
 * mem blocks and used mem blocks under the db.
 * The specific structure diagram is as follows:
 * 
 * 
 * Global variables of the module:
 * 
 *                                           mem blocks link
 *                          ┌─────┬─────┐
 *                          │     │     │    ┌───┐    ┌───┐    ┌───┐
 *                          │ k1  │  ───┼───►│4KB├───►│4KB├───►│4KB│
 *                          │     │     │    └───┘    └───┘    └───┘
 *                          ├─────┼─────┤
 *    ┌───────┐      ┌────► │     │     │    ┌───┐    ┌───┐
 *    │       │      │      │ k2  │  ───┼───►│4KB├───►│4KB│
 *    │ db0   ├──────┘      │     │     │    └───┘    └───┘
 *    │       │             ├─────┼─────┤
 *    ├───────┤             │     │     │    ┌───┐    ┌───┐    ┌───┐
 *    │       │             │ k3  │  ───┼───►│4KB├───►│4KB├───►│4KB│
 *    │ db1   ├──►null      │     │     │    └───┘    └───┘    └───┘
 *    │       │             └─────┴─────┘
 *    ├───────┤                  dict
 *    │       │
 *    │ db2   ├─────────┐
 *    │       │         │
 *    ├───────┤         │   ┌─────┬─────┐
 *    │       │         │   │     │     │    ┌───┐    ┌───┐    ┌───┐
 *    │ db3   ├──►null  │   │ k1  │  ───┼───►│4KB├───►│4KB├───►│4KB│
 *    │       │         │   │     │     │    └───┘    └───┘    └───┘
 *    └───────┘         │   ├─────┼─────┤
 * mem_pool[MAX_DB]     │   │     │     │    ┌───┐    ┌───┐
 *                      └──►│ k2  │  ───┼───►│4KB├───►│4KB│
 *                          │     │     │    └───┘    └───┘
 *                          └─────┴─────┘
 *                               dict
 * 
 * 
 * Keys in server database:
 * 
 *                                ┌───────┐
 *                                │ size  │
 *                   ┌───────────►│ used  │
 *                   │            │ mask  │
 *     ┌─────┬─────┐ │            └───────┘                                   ┌───────┐
 *     │     │     │ │          MemAllocObject                                │ size  │
 *     │ k1  │  ───┼─┘                                           ┌───────────►│ used  │
 *     │     │     │                                             │            │ mask  │
 *     ├─────┼─────┤              ┌───────┐        ┌─────┬─────┐ │            └───────┘
 *     │     │     │              │ size  │        │     │     │ │          MemAllocObject
 *     │ k2  │  ───┼─────────────►│ used  │        │ k1  │  ───┼─┘
 *     │     │     │              │ mask  │        │     │     │
 *     ├─────┼─────┤              └───────┘        ├─────┼─────┤
 *     │     │     │            MemAllocObject     │     │     │
 *     │ k3  │  ───┼─┐                             │ k2  │  ───┼─┐
 *     │     │     │ │                             │     │     │ │
 *     └─────┴─────┘ │            ┌───────┐        └─────┴─────┘ │            ┌───────┐
 *     server db[0]  │            │ size  │         server db[1] │            │ size  │
 *                   └───────────►│ used  │                      └───────────►│ used  │
 *                                │ mask  │                                   │ mask  │
 *                                └───────┘                                   └───────┘
 *                              MemAllocObject                              MemAllocObject
 *
 **/

#include "valkeymodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdint.h>

static ValkeyModuleType *MemAllocType;

#define MAX_DB 16
ValkeyModuleDict *mem_pool[MAX_DB];
typedef struct MemAllocObject {
    long long size;
    long long used;
    uint64_t mask;
} MemAllocObject;

MemAllocObject *createMemAllocObject(void) {
    MemAllocObject *o = ValkeyModule_Calloc(1, sizeof(*o));
    return o;
}

/*---------------------------- mem block apis ------------------------------------*/
#define BLOCK_SIZE 4096
struct MemBlock {
    char block[BLOCK_SIZE];
    struct MemBlock *next;
};

void MemBlockFree(struct MemBlock *head) {
    if (head) {
        struct MemBlock *block = head->next, *next;
        ValkeyModule_Free(head);
        while (block) {
            next = block->next;
            ValkeyModule_Free(block);
            block = next;
        }
    }
}
struct MemBlock *MemBlockCreate(long long num) {
    if (num <= 0) {
        return NULL;
    }

    struct MemBlock *head = ValkeyModule_Calloc(1, sizeof(struct MemBlock));
    struct MemBlock *block = head;
    while (--num) {
        block->next = ValkeyModule_Calloc(1, sizeof(struct MemBlock));
        block = block->next;
    }

    return head;
}

long long MemBlockNum(const struct MemBlock *head) {
    long long num = 0;
    const struct MemBlock *block = head;
    while (block) {
        num++;
        block = block->next;
    }

    return num;
}

size_t MemBlockWrite(struct MemBlock *head, long long block_index, const char *data, size_t size) {
    size_t w_size = 0;
    struct MemBlock *block = head;
    while (block_index-- && block) {
        block = block->next;
    }

    if (block) {
        size = size > BLOCK_SIZE ? BLOCK_SIZE:size;
        memcpy(block->block, data, size);
        w_size += size;
    }

    return w_size;
}

int MemBlockRead(struct MemBlock *head, long long block_index, char *data, size_t size) {
    size_t r_size = 0;
    struct MemBlock *block = head;
    while (block_index-- && block) {
        block = block->next;
    }

    if (block) {
        size = size > BLOCK_SIZE ? BLOCK_SIZE:size;
        memcpy(data, block->block, size);
        r_size += size;
    }

    return r_size;
}

void MemPoolFreeDb(ValkeyModuleCtx *ctx, int dbid) {
    ValkeyModuleString *key;
    void *tdata;
    ValkeyModuleDictIter *iter = ValkeyModule_DictIteratorStartC(mem_pool[dbid], "^", NULL, 0);
    while((key = ValkeyModule_DictNext(ctx, iter, &tdata)) != NULL) {
        MemBlockFree((struct MemBlock *)tdata);
    }
    ValkeyModule_DictIteratorStop(iter);
    ValkeyModule_FreeDict(NULL, mem_pool[dbid]);
    mem_pool[dbid] = ValkeyModule_CreateDict(NULL);
}

struct MemBlock *MemBlockClone(const struct MemBlock *head) {
    struct MemBlock *newhead = NULL;
    if (head) {
        newhead = ValkeyModule_Calloc(1, sizeof(struct MemBlock));
        memcpy(newhead->block, head->block, BLOCK_SIZE);
        struct MemBlock *newblock = newhead;
        const struct MemBlock *oldblock = head->next;
        while (oldblock) {
            newblock->next = ValkeyModule_Calloc(1, sizeof(struct MemBlock));
            newblock = newblock->next;
            memcpy(newblock->block, oldblock->block, BLOCK_SIZE);
            oldblock = oldblock->next;
        }
    }

    return newhead;
}

/*---------------------------- event handler ------------------------------------*/
void swapDbCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent e, uint64_t sub, void *data) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(e);
    VALKEYMODULE_NOT_USED(sub);

    ValkeyModuleSwapDbInfo *ei = data;

    // swap
    ValkeyModuleDict *tmp = mem_pool[ei->dbnum_first];
    mem_pool[ei->dbnum_first] = mem_pool[ei->dbnum_second];
    mem_pool[ei->dbnum_second] = tmp;
}

void flushdbCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent e, uint64_t sub, void *data) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(e);
    int i;
    ValkeyModuleFlushInfo *fi = data;

    ValkeyModule_AutoMemory(ctx);

    if (sub == VALKEYMODULE_SUBEVENT_FLUSHDB_START) {
        if (fi->dbnum != -1) {
           MemPoolFreeDb(ctx, fi->dbnum);
        } else {
            for (i = 0; i < MAX_DB; i++) {
                MemPoolFreeDb(ctx, i);
            }
        }
    }
}

/*---------------------------- command implementation ------------------------------------*/

/* MEM.ALLOC key block_num */
int MemAlloc_RedisCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);  

    if (argc != 3) {
        return ValkeyModule_WrongArity(ctx);
    }

    long long block_num;
    if ((ValkeyModule_StringToLongLong(argv[2], &block_num) != VALKEYMODULE_OK) || block_num <= 0) {
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid block_num: must be a value greater than 0");
    }

    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, argv[1], VALKEYMODULE_READ | VALKEYMODULE_WRITE);
    int type = ValkeyModule_KeyType(key);
    if (type != VALKEYMODULE_KEYTYPE_EMPTY && ValkeyModule_ModuleTypeGetType(key) != MemAllocType) {
        return ValkeyModule_ReplyWithError(ctx, VALKEYMODULE_ERRORMSG_WRONGTYPE);
    }

    MemAllocObject *o;
    if (type == VALKEYMODULE_KEYTYPE_EMPTY) {
        o = createMemAllocObject();
        ValkeyModule_ModuleTypeSetValue(key, MemAllocType, o);
    } else {
        o = ValkeyModule_ModuleTypeGetValue(key);
    }

    struct MemBlock *mem = MemBlockCreate(block_num);
    ValkeyModule_Assert(mem != NULL);
    ValkeyModule_DictSet(mem_pool[ValkeyModule_GetSelectedDb(ctx)], argv[1], mem);
    o->size = block_num;
    o->used = 0;
    o->mask = 0;

    ValkeyModule_ReplyWithLongLong(ctx, block_num);
    ValkeyModule_ReplicateVerbatim(ctx);
    return VALKEYMODULE_OK;
}

/* MEM.FREE key */
int MemFree_RedisCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);  

    if (argc != 2) {
        return ValkeyModule_WrongArity(ctx);
    }

    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, argv[1], VALKEYMODULE_READ);
    int type = ValkeyModule_KeyType(key);
    if (type != VALKEYMODULE_KEYTYPE_EMPTY && ValkeyModule_ModuleTypeGetType(key) != MemAllocType) {
        return ValkeyModule_ReplyWithError(ctx, VALKEYMODULE_ERRORMSG_WRONGTYPE);
    }

    int ret = 0;
    MemAllocObject *o;
    if (type == VALKEYMODULE_KEYTYPE_EMPTY) {
        ValkeyModule_ReplyWithLongLong(ctx, ret);
        return VALKEYMODULE_OK;
    } else {
        o = ValkeyModule_ModuleTypeGetValue(key);
    }

    int nokey;
    struct MemBlock *mem = (struct MemBlock *)ValkeyModule_DictGet(mem_pool[ValkeyModule_GetSelectedDb(ctx)], argv[1], &nokey);
    if (!nokey && mem) {
        ValkeyModule_DictDel(mem_pool[ValkeyModule_GetSelectedDb(ctx)], argv[1], NULL);
        MemBlockFree(mem);
        o->used = 0;
        o->size = 0;
        o->mask = 0;
        ret = 1;
    }

    ValkeyModule_ReplyWithLongLong(ctx, ret);
    ValkeyModule_ReplicateVerbatim(ctx);
    return VALKEYMODULE_OK;
}

/* MEM.WRITE key block_index data */
int MemWrite_RedisCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);  

    if (argc != 4) {
        return ValkeyModule_WrongArity(ctx);
    }

    long long block_index;
    if ((ValkeyModule_StringToLongLong(argv[2], &block_index) != VALKEYMODULE_OK) || block_index < 0) {
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid block_index: must be a value greater than 0");
    }

    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, argv[1], VALKEYMODULE_READ | VALKEYMODULE_WRITE);
    int type = ValkeyModule_KeyType(key);
    if (type != VALKEYMODULE_KEYTYPE_EMPTY && ValkeyModule_ModuleTypeGetType(key) != MemAllocType) {
        return ValkeyModule_ReplyWithError(ctx, VALKEYMODULE_ERRORMSG_WRONGTYPE);
    }

    MemAllocObject *o;
    if (type == VALKEYMODULE_KEYTYPE_EMPTY) {
        return ValkeyModule_ReplyWithError(ctx, "ERR Memory has not been allocated");
    } else {
        o = ValkeyModule_ModuleTypeGetValue(key);
    }

    if (o->mask & (1UL << block_index)) {
        return ValkeyModule_ReplyWithError(ctx, "ERR block is busy");
    }

    int ret = 0;
    int nokey;
    struct MemBlock *mem = (struct MemBlock *)ValkeyModule_DictGet(mem_pool[ValkeyModule_GetSelectedDb(ctx)], argv[1], &nokey);
    if (!nokey && mem) {
        size_t len;
        const char *buf = ValkeyModule_StringPtrLen(argv[3], &len);
        ret = MemBlockWrite(mem, block_index, buf, len);
        o->mask |= (1UL << block_index);
        o->used++;
    }

    ValkeyModule_ReplyWithLongLong(ctx, ret);
    ValkeyModule_ReplicateVerbatim(ctx);
    return VALKEYMODULE_OK;
}

/* MEM.READ key block_index */
int MemRead_RedisCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);  

    if (argc != 3) {
        return ValkeyModule_WrongArity(ctx);
    }

    long long block_index;
    if ((ValkeyModule_StringToLongLong(argv[2], &block_index) != VALKEYMODULE_OK) || block_index < 0) {
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid block_index: must be a value greater than 0");
    }

    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, argv[1], VALKEYMODULE_READ);
    int type = ValkeyModule_KeyType(key);
    if (type != VALKEYMODULE_KEYTYPE_EMPTY && ValkeyModule_ModuleTypeGetType(key) != MemAllocType) {
        return ValkeyModule_ReplyWithError(ctx, VALKEYMODULE_ERRORMSG_WRONGTYPE);
    }

    MemAllocObject *o;
    if (type == VALKEYMODULE_KEYTYPE_EMPTY) {
        return ValkeyModule_ReplyWithError(ctx, "ERR Memory has not been allocated");
    } else {
        o = ValkeyModule_ModuleTypeGetValue(key);
    }

    if (!(o->mask & (1UL << block_index))) {
        return ValkeyModule_ReplyWithNull(ctx);
    }

    int nokey;
    struct MemBlock *mem = (struct MemBlock *)ValkeyModule_DictGet(mem_pool[ValkeyModule_GetSelectedDb(ctx)], argv[1], &nokey);
    ValkeyModule_Assert(nokey == 0 && mem != NULL);
     
    char buf[BLOCK_SIZE];
    MemBlockRead(mem, block_index, buf, sizeof(buf));
    
    /* Assuming that the contents are all c-style strings */
    ValkeyModule_ReplyWithStringBuffer(ctx, buf, strlen(buf));
    return VALKEYMODULE_OK;
}

/* MEM.USAGE dbid */
int MemUsage_RedisCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);  

    if (argc != 2) {
        return ValkeyModule_WrongArity(ctx);
    }

    long long dbid;
    if ((ValkeyModule_StringToLongLong(argv[1], (long long *)&dbid) != VALKEYMODULE_OK)) {
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid value: must be a integer");
    }

    if (dbid < 0 || dbid >= MAX_DB) {
        return ValkeyModule_ReplyWithError(ctx, "ERR dbid out of range");
    }


    long long size = 0, used = 0;

    void *data;
    ValkeyModuleString *key;
    ValkeyModuleDictIter *iter = ValkeyModule_DictIteratorStartC(mem_pool[dbid], "^", NULL, 0);
    while((key = ValkeyModule_DictNext(ctx, iter, &data)) != NULL) {
        int dbbackup = ValkeyModule_GetSelectedDb(ctx);
        ValkeyModule_SelectDb(ctx, dbid);
        ValkeyModuleKey *openkey = ValkeyModule_OpenKey(ctx, key, VALKEYMODULE_READ);
        int type = ValkeyModule_KeyType(openkey);
        ValkeyModule_Assert(type != VALKEYMODULE_KEYTYPE_EMPTY && ValkeyModule_ModuleTypeGetType(openkey) == MemAllocType);
        MemAllocObject *o = ValkeyModule_ModuleTypeGetValue(openkey);
        used += o->used;
        size += o->size;
        ValkeyModule_CloseKey(openkey);
        ValkeyModule_SelectDb(ctx, dbbackup);
    }
    ValkeyModule_DictIteratorStop(iter);

    ValkeyModule_ReplyWithArray(ctx, 4);
    ValkeyModule_ReplyWithSimpleString(ctx, "total");
    ValkeyModule_ReplyWithLongLong(ctx, size);
    ValkeyModule_ReplyWithSimpleString(ctx, "used");
    ValkeyModule_ReplyWithLongLong(ctx, used);
    return VALKEYMODULE_OK;
}

/* MEM.ALLOCANDWRITE key block_num block_index data block_index data ... */
int MemAllocAndWrite_RedisCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);  

    if (argc < 3) {
        return ValkeyModule_WrongArity(ctx);
    }

    long long block_num;
    if ((ValkeyModule_StringToLongLong(argv[2], &block_num) != VALKEYMODULE_OK) || block_num <= 0) {
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid block_num: must be a value greater than 0");
    }

    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, argv[1], VALKEYMODULE_READ | VALKEYMODULE_WRITE);
    int type = ValkeyModule_KeyType(key);
    if (type != VALKEYMODULE_KEYTYPE_EMPTY && ValkeyModule_ModuleTypeGetType(key) != MemAllocType) {
        return ValkeyModule_ReplyWithError(ctx, VALKEYMODULE_ERRORMSG_WRONGTYPE);
    }

    MemAllocObject *o;
    if (type == VALKEYMODULE_KEYTYPE_EMPTY) {
        o = createMemAllocObject();
        ValkeyModule_ModuleTypeSetValue(key, MemAllocType, o);
    } else {
        o = ValkeyModule_ModuleTypeGetValue(key);
    }

    struct MemBlock *mem = MemBlockCreate(block_num);
    ValkeyModule_Assert(mem != NULL);
    ValkeyModule_DictSet(mem_pool[ValkeyModule_GetSelectedDb(ctx)], argv[1], mem);
    o->used = 0;
    o->mask = 0;
    o->size = block_num;

    int i = 3;
    long long block_index;
    for (; i < argc; i++) {
        /* Security is guaranteed internally, so no security check. */
        ValkeyModule_StringToLongLong(argv[i], &block_index);
        size_t len;
        const char * buf = ValkeyModule_StringPtrLen(argv[i + 1], &len);
        MemBlockWrite(mem, block_index, buf, len);
        o->used++;
        o->mask |= (1UL << block_index);
    }

    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    ValkeyModule_ReplicateVerbatim(ctx);
    return VALKEYMODULE_OK;
}

/*---------------------------- type callbacks ------------------------------------*/

void *MemAllocRdbLoad(ValkeyModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }

    MemAllocObject *o = createMemAllocObject();
    o->size = ValkeyModule_LoadSigned(rdb);
    o->used = ValkeyModule_LoadSigned(rdb);
    o->mask = ValkeyModule_LoadUnsigned(rdb);

    const ValkeyModuleString *key = ValkeyModule_GetKeyNameFromIO(rdb);
    int dbid = ValkeyModule_GetDbIdFromIO(rdb);

    if (o->size) {
        size_t size;
        char *tmpbuf;
        long long num = o->size;
        struct MemBlock *head = ValkeyModule_Calloc(1, sizeof(struct MemBlock));
        tmpbuf = ValkeyModule_LoadStringBuffer(rdb, &size);
        memcpy(head->block, tmpbuf, size > BLOCK_SIZE ? BLOCK_SIZE:size);
        ValkeyModule_Free(tmpbuf);
        struct MemBlock *block = head;
        while (--num) {
            block->next = ValkeyModule_Calloc(1, sizeof(struct MemBlock));
            block = block->next;

            tmpbuf = ValkeyModule_LoadStringBuffer(rdb, &size);
            memcpy(block->block, tmpbuf, size > BLOCK_SIZE ? BLOCK_SIZE:size);
            ValkeyModule_Free(tmpbuf);
        }

        ValkeyModule_DictSet(mem_pool[dbid], (ValkeyModuleString *)key, head);
    }
     
    return o;
}

void MemAllocRdbSave(ValkeyModuleIO *rdb, void *value) {
    MemAllocObject *o = value;
    ValkeyModule_SaveSigned(rdb, o->size);
    ValkeyModule_SaveSigned(rdb, o->used);
    ValkeyModule_SaveUnsigned(rdb, o->mask);

    const ValkeyModuleString *key = ValkeyModule_GetKeyNameFromIO(rdb);
    int dbid = ValkeyModule_GetDbIdFromIO(rdb);

    if (o->size) {
        int nokey;
        struct MemBlock *mem = (struct MemBlock *)ValkeyModule_DictGet(mem_pool[dbid], (ValkeyModuleString *)key, &nokey);
        ValkeyModule_Assert(nokey == 0 && mem != NULL);

        struct MemBlock *block = mem; 
        while (block) {
            ValkeyModule_SaveStringBuffer(rdb, block->block, BLOCK_SIZE);
            block = block->next;
        }
    }
}

void MemAllocAofRewrite(ValkeyModuleIO *aof, ValkeyModuleString *key, void *value) {
    MemAllocObject *o = (MemAllocObject *)value;
    if (o->size) {
        int dbid = ValkeyModule_GetDbIdFromIO(aof);
        int nokey;
        size_t i = 0, j = 0;
        struct MemBlock *mem = (struct MemBlock *)ValkeyModule_DictGet(mem_pool[dbid], (ValkeyModuleString *)key, &nokey);
        ValkeyModule_Assert(nokey == 0 && mem != NULL);
        size_t array_size = o->size * 2;
        ValkeyModuleString ** string_array = ValkeyModule_Calloc(array_size, sizeof(ValkeyModuleString *));
        while (mem) {
            string_array[i] = ValkeyModule_CreateStringFromLongLong(NULL, j);
            string_array[i + 1] = ValkeyModule_CreateString(NULL, mem->block, BLOCK_SIZE);
            mem = mem->next;
            i += 2;
            j++;
        }
        ValkeyModule_EmitAOF(aof, "mem.allocandwrite", "slv", key, o->size, string_array, array_size);
        for (i = 0; i < array_size; i++) {
            ValkeyModule_FreeString(NULL, string_array[i]);
        }
        ValkeyModule_Free(string_array);
    } else {
        ValkeyModule_EmitAOF(aof, "mem.allocandwrite", "sl", key, o->size);
    }
}

void MemAllocFree(void *value) {
    ValkeyModule_Free(value);
}

void MemAllocUnlink(ValkeyModuleString *key, const void *value) {
    VALKEYMODULE_NOT_USED(key);
    VALKEYMODULE_NOT_USED(value);

    /* When unlink and unlink2 exist at the same time, we will only call unlink2. */
    ValkeyModule_Assert(0);
}

void MemAllocUnlink2(ValkeyModuleKeyOptCtx *ctx, const void *value) {
    MemAllocObject *o = (MemAllocObject *)value;

    const ValkeyModuleString *key = ValkeyModule_GetKeyNameFromOptCtx(ctx);
    int dbid = ValkeyModule_GetDbIdFromOptCtx(ctx);
    
    if (o->size) {
        void *oldval;
        ValkeyModule_DictDel(mem_pool[dbid], (ValkeyModuleString *)key, &oldval);
        ValkeyModule_Assert(oldval != NULL);
        MemBlockFree((struct MemBlock *)oldval);
    }
}

void MemAllocDigest(ValkeyModuleDigest *md, void *value) {
    MemAllocObject *o = (MemAllocObject *)value;
    ValkeyModule_DigestAddLongLong(md, o->size);
    ValkeyModule_DigestAddLongLong(md, o->used);
    ValkeyModule_DigestAddLongLong(md, o->mask);

    int dbid = ValkeyModule_GetDbIdFromDigest(md);
    const ValkeyModuleString *key = ValkeyModule_GetKeyNameFromDigest(md);
    
    if (o->size) {
        int nokey;
        struct MemBlock *mem = (struct MemBlock *)ValkeyModule_DictGet(mem_pool[dbid], (ValkeyModuleString *)key, &nokey);
        ValkeyModule_Assert(nokey == 0 && mem != NULL);

        struct MemBlock *block = mem;
        while (block) {
            ValkeyModule_DigestAddStringBuffer(md, (const char *)block->block, BLOCK_SIZE);
            block = block->next;
        }
    }
}

void *MemAllocCopy2(ValkeyModuleKeyOptCtx *ctx, const void *value) {
    const MemAllocObject *old = value;
    MemAllocObject *new = createMemAllocObject();
    new->size = old->size;
    new->used = old->used;
    new->mask = old->mask;

    int from_dbid = ValkeyModule_GetDbIdFromOptCtx(ctx);
    int to_dbid = ValkeyModule_GetToDbIdFromOptCtx(ctx);
    const ValkeyModuleString *fromkey = ValkeyModule_GetKeyNameFromOptCtx(ctx);
    const ValkeyModuleString *tokey = ValkeyModule_GetToKeyNameFromOptCtx(ctx);

    if (old->size) {
        int nokey;
        struct MemBlock *oldmem = (struct MemBlock *)ValkeyModule_DictGet(mem_pool[from_dbid], (ValkeyModuleString *)fromkey, &nokey);
        ValkeyModule_Assert(nokey == 0 && oldmem != NULL);
        struct MemBlock *newmem = MemBlockClone(oldmem);
        ValkeyModule_Assert(newmem != NULL);
        ValkeyModule_DictSet(mem_pool[to_dbid], (ValkeyModuleString *)tokey, newmem);
    }   

    return new;
}

size_t MemAllocMemUsage2(ValkeyModuleKeyOptCtx *ctx, const void *value, size_t sample_size) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(sample_size);
    uint64_t size = 0;
    MemAllocObject *o = (MemAllocObject *)value;

    size += sizeof(*o);
    size += o->size * sizeof(struct MemBlock);

    return size;
}

size_t MemAllocMemFreeEffort2(ValkeyModuleKeyOptCtx *ctx, const void *value) {
    VALKEYMODULE_NOT_USED(ctx);
    MemAllocObject *o = (MemAllocObject *)value;
    return o->size;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "datatype2", 1,VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    ValkeyModuleTypeMethods tm = {
        .version = VALKEYMODULE_TYPE_METHOD_VERSION,
        .rdb_load = MemAllocRdbLoad,
        .rdb_save = MemAllocRdbSave,
        .aof_rewrite = MemAllocAofRewrite,
        .free = MemAllocFree,
        .digest = MemAllocDigest,
        .unlink = MemAllocUnlink,
        // .defrag = MemAllocDefrag, // Tested in defragtest.c
        .unlink2 = MemAllocUnlink2,
        .copy2 = MemAllocCopy2,
        .mem_usage2 = MemAllocMemUsage2,
        .free_effort2 = MemAllocMemFreeEffort2,
    };

    MemAllocType = ValkeyModule_CreateDataType(ctx, "mem_alloc", 0, &tm);
    if (MemAllocType == NULL) {
        return VALKEYMODULE_ERR;
    }

    ValkeyModule_AddModuleOptions(ctx, VALKEYMODULE_OPTIONS_SKIP_COMMAND_VALIDATION);

    if (ValkeyModule_CreateCommand(ctx, "mem.alloc", MemAlloc_RedisCommand, "write deny-oom", 1, 1, 1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "mem.free", MemFree_RedisCommand, "write deny-oom", 1, 1, 1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "mem.write", MemWrite_RedisCommand, "write deny-oom", 1, 1, 1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "mem.read", MemRead_RedisCommand, "readonly", 1, 1, 1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "mem.usage", MemUsage_RedisCommand, "readonly", 1, 1, 1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* used for internal aof rewrite */
    if (ValkeyModule_CreateCommand(ctx, "mem.allocandwrite", MemAllocAndWrite_RedisCommand, "write deny-oom", 1, 1, 1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    for(int i = 0; i < MAX_DB; i++){
        mem_pool[i] = ValkeyModule_CreateDict(NULL);
    }

    ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_FlushDB, flushdbCallback);
    ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_SwapDB, swapDbCallback);
  
    return VALKEYMODULE_OK;
}
