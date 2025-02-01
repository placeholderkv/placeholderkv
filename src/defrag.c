/*
 * Active memory defragmentation
 * Try to find key / value allocations that need to be re-allocated in order
 * to reduce external fragmentation.
 * We do that by scanning the keyspace and for each pointer we have, we can try to
 * ask the allocator if moving it to a new address will help reduce fragmentation.
 *
 * Copyright (c) 2020, Redis Ltd.
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

#include "server.h"
#include "hashtable.h"
#include "script.h"
#include "module.h"
#include <stddef.h>

#ifdef HAVE_DEFRAG

typedef enum { DEFRAG_NOT_DONE = 0,
               DEFRAG_DONE = 1 } doneStatus;


/*
 * Defragmentation is performed in stages.  Each stage is serviced by a stage function
 * (defragStageFn).  The stage function is passed a target (void*) to defrag.  The contents of that
 * target are unique to the particular stage - and may even be NULL for some stage functions.  The
 * same stage function can be used multiple times (for different stages) each having a different
 * target.
 *
 * The stage function is required to maintain an internal static state.  This allows the stage
 * function to continue when invoked in an iterative manner.  When invoked with a 0 endtime, the
 * stage function is required to clear it's internal state and prepare to begin a new stage.  It
 * should return false (more work to do) as it should NOT perform any real "work" during init.
 *
 * Parameters:
 *  endtime     - This is the monotonic time that the function should end and return.  This ensures
 *                a bounded latency due to defrag.  When endtime is 0, the internal state should be
 *                cleared, preparing to begin the stage with a new target.
 *  target      - This is the "thing" that should be defragged.  It's type is dependent on the
 *                type of the stage function.  This might be a dict, a kvstore, a DB, or other.
 *  privdata    - A pointer to arbitrary private data which is unique to the stage function.
 *
 * Returns:
 *  - DEFRAG_DONE if the stage is complete
 *  - DEFRAG_NOT_DONE if there is more work to do
 */
typedef doneStatus (*defragStageFn)(monotime endtime, void *target, void *privdata);

typedef struct {
    defragStageFn stage_fn; // The function to be invoked for the stage
    void *target;           // The target that the function will defrag
    void *privdata;         // Private data, unique to the stage function
} StageDescriptor;

/* Globals needed for the main defrag processing logic.
 * Doesn't include variables specific to a stage or type of data. */
struct DefragContext {
    monotime start_cycle;           // Time of beginning of defrag cycle
    long long start_defrag_hits;    // server.stat_active_defrag_hits captured at beginning of cycle
    list *remaining_stages;         // List of stages which remain to be processed
    StageDescriptor *current_stage; // The stage that's currently being processed

    long long timeproc_id;      // Eventloop ID of the timerproc (or AE_DELETED_EVENT_ID)
    monotime timeproc_end_time; // Ending time of previous timerproc execution
    long timeproc_overage_us;   // A correction value if over target CPU percent
};
static struct DefragContext defrag;


/* There are a number of stages which process a kvstore.  To simplify this, a stage helper function
 * `defragStageKvstoreHelper()` is defined.  This function aids in iterating over the kvstore.  It
 * uses these definitions.
 */
/* State of the kvstore helper.  The private data (privdata) passed to the kvstore helper MUST BEGIN
 *  with a kvstoreIterState (or be passed as NULL). */
#define KVS_SLOT_DEFRAG_LUT -2
#define KVS_SLOT_UNASSIGNED -1
typedef struct {
    kvstore *kvs;
    int slot;
    unsigned long cursor;
} kvstoreIterState;
/* The kvstore helper uses this function to perform tasks before continuing the iteration.  For the
 * main hash table, large items are set aside and processed by this function before continuing with
 * iteration over the kvstore.
 *  endtime     - This is the monotonic time that the function should end and return.
 *  privdata    - Private data for functions invoked by the helper.  If provided in the call to
 *                `defragStageKvstoreHelper()`, the `kvstoreIterState` portion (at the beginning)
 *                will be updated with the current kvstore iteration status.
 *
 * Returns:
 *  - DEFRAG_DONE if the pre-continue work is complete
 *  - DEFRAG_NOT_DONE if there is more work to do
 */
typedef doneStatus (*kvstoreHelperPreContinueFn)(monotime endtime, void *privdata);


// Private data for main dictionary keys
typedef struct {
    kvstoreIterState kvstate;
    int dbid;
} defragKeysCtx;
static_assert(offsetof(defragKeysCtx, kvstate) == 0, "defragStageKvstoreHelper requires this");

// Private data for pubsub kvstores
typedef dict *(*getClientChannelsFn)(client *);
typedef struct {
    getClientChannelsFn fn;
} getClientChannelsFnWrapper;

typedef struct {
    kvstoreIterState kvstate;
    getClientChannelsFn getPubSubChannels;
} defragPubSubCtx;
static_assert(offsetof(defragPubSubCtx, kvstate) == 0, "defragStageKvstoreHelper requires this");


/* When scanning a main kvstore, large elements are queued for later handling rather than
 * causing a large latency spike while processing a hash table bucket.  This list is only used
 * for stage: "defragStageDbKeys".  It will only contain values for the current kvstore being
 * defragged.
 * Note that this is a list of key names.  It's possible that the key may be deleted or modified
 * before "later" and we will search by key name to find the entry when we defrag the item later.
 */
static list *defrag_later;
static unsigned long defrag_later_cursor;

/* Defrag function which allocates and copies memory if needed, but DOESN'T free the old block.
 * It is the responsibility of the caller to free the old block if a non-NULL value (new block)
 * is returned.  (Returns NULL if no relocation was needed.)
 */
static void *activeDefragAllocWithoutFree(void *ptr, size_t *allocation_size) {
    size_t size;
    void *newptr;
    if (!allocatorShouldDefrag(ptr)) {
        server.stat_active_defrag_misses++;
        return NULL;
    }
    /* move this allocation to a new allocation.
     * make sure not to use the thread cache. so that we don't get back the same
     * pointers we try to free */
    size = zmalloc_size(ptr);
    newptr = allocatorDefragAlloc(size);
    memcpy(newptr, ptr, size);
    if (allocation_size) *allocation_size = size;

    server.stat_active_defrag_hits++;
    return newptr;
}

/* Defrag helper for generic allocations.
 *
 * Returns NULL in case the allocation wasn't moved.
 * When it returns a non-null value, the old pointer was already released
 * and should NOT be accessed. */
void *activeDefragAlloc(void *ptr) {
    size_t allocation_size;
    void *newptr = activeDefragAllocWithoutFree(ptr, &allocation_size);
    if (newptr) allocatorDefragFree(ptr, allocation_size);
    return newptr;
}

/* Defrag helper for sds strings
 *
 * Returns NULL in case the allocation wasn't moved.
 * When it returns a non-null value, the old pointer was already released
 * and should NOT be accessed. */
static sds activeDefragSds(sds sdsptr) {
    void *ptr = sdsAllocPtr(sdsptr);
    void *newptr = activeDefragAlloc(ptr);
    if (newptr) {
        size_t offset = sdsptr - (char *)ptr;
        sdsptr = (char *)newptr + offset;
        return sdsptr;
    }
    return NULL;
}

/* Performs defrag on a string-type (or generic) robj, but does not free the old robj.  This is the
 * caller's responsibility.  This is necessary for string objects with multiple references.  In this
 * case the caller can fix the references before freeing the original object.
 */
static robj *activeDefragStringObWithoutFree(robj *ob, size_t *allocation_size) {
    if (ob->type == OBJ_STRING && ob->encoding == OBJ_ENCODING_RAW) {
        // Try to defrag the linked sds, regardless of if robj will be moved
        sds newsds = activeDefragSds((sds)ob->ptr);
        if (newsds) ob->ptr = newsds;
    }

    robj *new_robj = activeDefragAllocWithoutFree(ob, allocation_size);

    if (new_robj && ob->type == OBJ_STRING && ob->encoding == OBJ_ENCODING_EMBSTR) {
        // If the robj is moved, correct the internal pointer
        long embstr_offset = (intptr_t)ob->ptr - (intptr_t)ob;
        new_robj->ptr = (void *)((intptr_t)new_robj + embstr_offset);
    }
    return new_robj;
}


/* Defrag helper for robj and/or string objects
 *
 * Returns NULL in case the allocation wasn't moved.
 * When it returns a non-null value, the old pointer was already released
 * and should NOT be accessed. */
robj *activeDefragStringOb(robj *ob) {
    size_t allocation_size;
    if (ob->refcount != 1) return NULL; // Unsafe to defrag if multiple refs
    robj *new_robj = activeDefragStringObWithoutFree(ob, &allocation_size);
    if (new_robj) allocatorDefragFree(ob, allocation_size);
    return new_robj;
}


/* Defrag helper for lua scripts
 *
 * Returns NULL in case the allocation wasn't moved.
 * When it returns a non-null value, the old pointer was already released
 * and should NOT be accessed. */
static luaScript *activeDefragLuaScript(luaScript *script) {
    luaScript *ret = NULL;

    /* try to defrag script struct */
    if ((ret = activeDefragAlloc(script))) {
        script = ret;
    }

    /* try to defrag actual script object */
    robj *ob = activeDefragStringOb(script->body);
    if (ob) script->body = ob;

    return ret;
}

/* Defrag helper for dict main allocations (dict struct, and hash tables).
 * Receives a pointer to the dict* and return a new dict* when the dict
 * struct itself was moved.
 *
 * Returns NULL in case the allocation wasn't moved.
 * When it returns a non-null value, the old pointer was already released
 * and should NOT be accessed. */
static dict *dictDefragTables(dict *d) {
    dict *ret = NULL;
    dictEntry **newtable;
    /* handle the dict struct */
    if ((ret = activeDefragAlloc(d))) d = ret;
    /* handle the first hash table */
    if (!d->ht_table[0]) return ret; /* created but unused */
    newtable = activeDefragAlloc(d->ht_table[0]);
    if (newtable) d->ht_table[0] = newtable;
    /* handle the second hash table */
    if (d->ht_table[1]) {
        newtable = activeDefragAlloc(d->ht_table[1]);
        if (newtable) d->ht_table[1] = newtable;
    }
    return ret;
}

/* Internal function used by zslDefrag */
static void zslUpdateNode(zskiplist *zsl, zskiplistNode *oldnode, zskiplistNode *newnode, zskiplistNode **update) {
    int i;
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == oldnode) update[i]->level[i].forward = newnode;
    }
    serverAssert(zsl->header != oldnode);
    if (newnode->level[0].forward) {
        serverAssert(newnode->level[0].forward->backward == oldnode);
        newnode->level[0].forward->backward = newnode;
    } else {
        serverAssert(zsl->tail == oldnode);
        zsl->tail = newnode;
    }
}

/* Hashtable scan callback for sorted set. It defragments a single skiplist
 * node, updates skiplist pointers, and updates the hashtable pointer to the
 * node. */
static void activeDefragZsetNode(void *privdata, void *entry_ref) {
    zskiplist *zsl = privdata;
    zskiplistNode **node_ref = (zskiplistNode **)entry_ref;
    zskiplistNode *node = *node_ref;

    /* defragment node internals */
    sds newsds = activeDefragSds(node->ele);
    if (newsds) node->ele = newsds;

    const double score = node->score;
    const sds ele = node->ele;

    /* find skiplist pointers that need to be updated if we end up moving the
     * skiplist node. */
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL];
    zskiplistNode *x = zsl->header;
    for (int i = zsl->level - 1; i >= 0; i--) {
        /* stop when we've reached the end of this level or the next node comes
         * after our target in sorted order */
        zskiplistNode *next = x->level[i].forward;
        while (next &&
               (next->score < score ||
                (next->score == score && sdscmp(next->ele, ele) < 0))) {
            x = next;
            next = x->level[i].forward;
        }
        update[i] = x;
    }
    /* should have arrived at intended node */
    serverAssert(x->level[0].forward == node);

    /* try to defrag the skiplist record itself */
    zskiplistNode *newnode = activeDefragAlloc(node);
    if (newnode) {
        zslUpdateNode(zsl, node, newnode, update);
        *node_ref = newnode; /* update hashtable pointer */
    }
}

#define DEFRAG_SDS_DICT_NO_VAL 0
#define DEFRAG_SDS_DICT_VAL_IS_SDS 1
#define DEFRAG_SDS_DICT_VAL_IS_STROB 2
#define DEFRAG_SDS_DICT_VAL_VOID_PTR 3
#define DEFRAG_SDS_DICT_VAL_LUA_SCRIPT 4

static void activeDefragSdsDictCallback(void *privdata, const dictEntry *de) {
    UNUSED(privdata);
    UNUSED(de);
}

/* Defrag a dict with sds key and optional value (either ptr, sds or robj string) */
static void activeDefragSdsDict(dict *d, int val_type) {
    unsigned long cursor = 0;
    dictDefragFunctions defragfns = {
        .defragAlloc = activeDefragAlloc,
        .defragKey = (dictDefragAllocFunction *)activeDefragSds,
        .defragVal = (val_type == DEFRAG_SDS_DICT_VAL_IS_SDS       ? (dictDefragAllocFunction *)activeDefragSds
                      : val_type == DEFRAG_SDS_DICT_VAL_IS_STROB   ? (dictDefragAllocFunction *)activeDefragStringOb
                      : val_type == DEFRAG_SDS_DICT_VAL_VOID_PTR   ? (dictDefragAllocFunction *)activeDefragAlloc
                      : val_type == DEFRAG_SDS_DICT_VAL_LUA_SCRIPT ? (dictDefragAllocFunction *)activeDefragLuaScript
                                                                   : NULL)};
    do {
        cursor = dictScanDefrag(d, cursor, activeDefragSdsDictCallback, &defragfns, NULL);
    } while (cursor != 0);
}

void activeDefragSdsHashtableCallback(void *privdata, void *entry_ref) {
    UNUSED(privdata);
    sds *sds_ref = (sds *)entry_ref;
    sds new_sds = activeDefragSds(*sds_ref);
    if (new_sds != NULL) *sds_ref = new_sds;
}

/* Defrag a list of ptr, sds or robj string values */
static void activeDefragQuickListNode(quicklist *ql, quicklistNode **node_ref) {
    quicklistNode *newnode, *node = *node_ref;
    unsigned char *newzl;
    if ((newnode = activeDefragAlloc(node))) {
        if (newnode->prev)
            newnode->prev->next = newnode;
        else
            ql->head = newnode;
        if (newnode->next)
            newnode->next->prev = newnode;
        else
            ql->tail = newnode;
        *node_ref = node = newnode;
    }
    if ((newzl = activeDefragAlloc(node->entry))) node->entry = newzl;
}

static void activeDefragQuickListNodes(quicklist *ql) {
    quicklistNode *node = ql->head;
    while (node) {
        activeDefragQuickListNode(ql, &node);
        node = node->next;
    }
}

/* when the value has lots of elements, we want to handle it later and not as
 * part of the main dictionary scan. this is needed in order to prevent latency
 * spikes when handling large items */
static void defragLater(robj *obj) {
    if (!defrag_later) {
        defrag_later = listCreate();
        listSetFreeMethod(defrag_later, sdsfreeVoid);
        defrag_later_cursor = 0;
    }
    sds key = sdsdup(objectGetKey(obj));
    listAddNodeTail(defrag_later, key);
}

/* returns 0 if no more work needs to be been done, and 1 if time is up and more work is needed. */
static long scanLaterList(robj *ob, unsigned long *cursor, monotime endtime) {
    quicklist *ql = ob->ptr;
    quicklistNode *node;
    long iterations = 0;
    int bookmark_failed = 0;
    if (ob->type != OBJ_LIST || ob->encoding != OBJ_ENCODING_QUICKLIST) return 0;

    if (*cursor == 0) {
        /* if cursor is 0, we start new iteration */
        node = ql->head;
    } else {
        node = quicklistBookmarkFind(ql, "_AD");
        if (!node) {
            /* if the bookmark was deleted, it means we reached the end. */
            *cursor = 0;
            return 0;
        }
        node = node->next;
    }

    (*cursor)++;
    while (node) {
        activeDefragQuickListNode(ql, &node);
        server.stat_active_defrag_scanned++;
        if (++iterations > 128 && !bookmark_failed) {
            if (getMonotonicUs() > endtime) {
                if (!quicklistBookmarkCreate(&ql, "_AD", node)) {
                    bookmark_failed = 1;
                } else {
                    ob->ptr = ql; /* bookmark creation may have re-allocated the quicklist */
                    return 1;
                }
            }
            iterations = 0;
        }
        node = node->next;
    }
    quicklistBookmarkDelete(ql, "_AD");
    *cursor = 0;
    return bookmark_failed ? 1 : 0;
}

static void scanLaterZsetCallback(void *privdata, void *element_ref) {
    activeDefragZsetNode(privdata, element_ref);
    server.stat_active_defrag_scanned++;
}

static void scanLaterZset(robj *ob, unsigned long *cursor) {
    if (ob->type != OBJ_ZSET || ob->encoding != OBJ_ENCODING_SKIPLIST) return;
    zset *zs = (zset *)ob->ptr;
    *cursor = hashtableScanDefrag(zs->ht, *cursor, scanLaterZsetCallback, zs->zsl, activeDefragAlloc, HASHTABLE_SCAN_EMIT_REF);
}

/* Used as hashtable scan callback when all we need is to defrag the hashtable
 * internals (the allocated buckets) and not the elements. */
static void scanHashtableCallbackCountScanned(void *privdata, void *elemref) {
    UNUSED(privdata);
    UNUSED(elemref);
    server.stat_active_defrag_scanned++;
}

static void scanLaterSet(robj *ob, unsigned long *cursor) {
    if (ob->type != OBJ_SET || ob->encoding != OBJ_ENCODING_HASHTABLE) return;
    hashtable *ht = ob->ptr;
    *cursor = hashtableScanDefrag(ht, *cursor, activeDefragSdsHashtableCallback, NULL, activeDefragAlloc, HASHTABLE_SCAN_EMIT_REF);
}

/* Hashtable scan callback for hash datatype */
static void activeDefragHashTypeEntry(void *privdata, void *element_ref) {
    UNUSED(privdata);
    hashTypeEntry **entry_ref = (hashTypeEntry **)element_ref;

    hashTypeEntry *new_entry = hashTypeEntryDefrag(*entry_ref, activeDefragAlloc, activeDefragSds);
    if (new_entry) *entry_ref = new_entry;
}

static void scanLaterHash(robj *ob, unsigned long *cursor) {
    if (ob->type != OBJ_HASH || ob->encoding != OBJ_ENCODING_HASHTABLE) return;
    hashtable *ht = ob->ptr;
    *cursor = hashtableScanDefrag(ht, *cursor, activeDefragHashTypeEntry, NULL, activeDefragAlloc, HASHTABLE_SCAN_EMIT_REF);
}

static void defragQuicklist(robj *ob) {
    quicklist *ql = ob->ptr, *newql;
    serverAssert(ob->type == OBJ_LIST && ob->encoding == OBJ_ENCODING_QUICKLIST);
    if ((newql = activeDefragAlloc(ql))) ob->ptr = ql = newql;
    if (ql->len > server.active_defrag_max_scan_fields)
        defragLater(ob);
    else
        activeDefragQuickListNodes(ql);
}

static void defragZsetSkiplist(robj *ob) {
    serverAssert(ob->type == OBJ_ZSET && ob->encoding == OBJ_ENCODING_SKIPLIST);
    zset *zs = (zset *)ob->ptr;

    zset *newzs;
    zskiplist *newzsl;
    struct zskiplistNode *newheader;
    if ((newzs = activeDefragAlloc(zs))) ob->ptr = zs = newzs;
    if ((newzsl = activeDefragAlloc(zs->zsl))) zs->zsl = newzsl;
    if ((newheader = activeDefragAlloc(zs->zsl->header))) zs->zsl->header = newheader;

    hashtable *newtable;
    if ((newtable = hashtableDefragTables(zs->ht, activeDefragAlloc))) zs->ht = newtable;

    if (hashtableSize(zs->ht) > server.active_defrag_max_scan_fields)
        defragLater(ob);
    else {
        unsigned long cursor = 0;
        do {
            cursor = hashtableScanDefrag(zs->ht, cursor, activeDefragZsetNode, zs->zsl, activeDefragAlloc, HASHTABLE_SCAN_EMIT_REF);
        } while (cursor != 0);
    }
}

static void defragHash(robj *ob) {
    serverAssert(ob->type == OBJ_HASH && ob->encoding == OBJ_ENCODING_HASHTABLE);
    hashtable *ht = ob->ptr;
    if (hashtableSize(ht) > server.active_defrag_max_scan_fields) {
        defragLater(ob);
    } else {
        unsigned long cursor = 0;
        do {
            cursor = hashtableScanDefrag(ht, cursor, activeDefragHashTypeEntry, NULL, activeDefragAlloc, HASHTABLE_SCAN_EMIT_REF);
        } while (cursor != 0);
    }
    /* defrag the hashtable struct and tables */
    hashtable *new_hashtable = hashtableDefragTables(ht, activeDefragAlloc);
    if (new_hashtable) ob->ptr = new_hashtable;
}

static void defragSet(robj *ob) {
    serverAssert(ob->type == OBJ_SET && ob->encoding == OBJ_ENCODING_HASHTABLE);
    hashtable *ht = ob->ptr;
    if (hashtableSize(ht) > server.active_defrag_max_scan_fields) {
        defragLater(ob);
    } else {
        unsigned long cursor = 0;
        do {
            cursor = hashtableScanDefrag(ht, cursor, activeDefragSdsHashtableCallback, NULL, activeDefragAlloc, HASHTABLE_SCAN_EMIT_REF);
        } while (cursor != 0);
    }
    /* defrag the hashtable struct and tables */
    hashtable *new_hashtable = hashtableDefragTables(ht, activeDefragAlloc);
    if (new_hashtable) ob->ptr = new_hashtable;
}

/* Defrag callback for radix tree iterator, called for each node,
 * used in order to defrag the nodes allocations. */
static int defragRaxNode(raxNode **noderef) {
    raxNode *newnode = activeDefragAlloc(*noderef);
    if (newnode) {
        *noderef = newnode;
        return 1;
    }
    return 0;
}

/* returns 0 if no more work needs to be been done, and 1 if time is up and more work is needed. */
static int scanLaterStreamListpacks(robj *ob, unsigned long *cursor, monotime endtime) {
    static unsigned char last[sizeof(streamID)];
    raxIterator ri;
    long iterations = 0;
    if (ob->type != OBJ_STREAM || ob->encoding != OBJ_ENCODING_STREAM) {
        *cursor = 0;
        return 0;
    }

    stream *s = ob->ptr;
    raxStart(&ri, s->rax);
    if (*cursor == 0) {
        /* if cursor is 0, we start new iteration */
        defragRaxNode(&s->rax->head);
        /* assign the iterator node callback before the seek, so that the
         * initial nodes that are processed till the first item are covered */
        ri.node_cb = defragRaxNode;
        raxSeek(&ri, "^", NULL, 0);
    } else {
        /* if cursor is non-zero, we seek to the static 'last' */
        if (!raxSeek(&ri, ">", last, sizeof(last))) {
            *cursor = 0;
            raxStop(&ri);
            return 0;
        }
        /* assign the iterator node callback after the seek, so that the
         * initial nodes that are processed till now aren't covered */
        ri.node_cb = defragRaxNode;
    }

    (*cursor)++;
    while (raxNext(&ri)) {
        void *newdata = activeDefragAlloc(ri.data);
        if (newdata) raxSetData(ri.node, ri.data = newdata);
        server.stat_active_defrag_scanned++;
        if (++iterations > 128) {
            if (getMonotonicUs() > endtime) {
                serverAssert(ri.key_len == sizeof(last));
                memcpy(last, ri.key, ri.key_len);
                raxStop(&ri);
                return 1;
            }
            iterations = 0;
        }
    }
    raxStop(&ri);
    *cursor = 0;
    return 0;
}

/* optional callback used defrag each rax element (not including the element pointer itself) */
typedef void *(raxDefragFunction)(raxIterator *ri, void *privdata);

/* defrag radix tree including:
 * 1) rax struct
 * 2) rax nodes
 * 3) rax entry data (only if defrag_data is specified)
 * 4) call a callback per element, and allow the callback to return a new pointer for the element */
static void defragRadixTree(rax **raxref, int defrag_data, raxDefragFunction *element_cb, void *element_cb_data) {
    raxIterator ri;
    rax *rax;
    if ((rax = activeDefragAlloc(*raxref))) *raxref = rax;
    rax = *raxref;
    raxStart(&ri, rax);
    ri.node_cb = defragRaxNode;
    defragRaxNode(&rax->head);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri)) {
        void *newdata = NULL;
        if (element_cb) newdata = element_cb(&ri, element_cb_data);
        if (defrag_data && !newdata) newdata = activeDefragAlloc(ri.data);
        if (newdata) raxSetData(ri.node, ri.data = newdata);
    }
    raxStop(&ri);
}

typedef struct {
    streamCG *cg;
    streamConsumer *c;
} PendingEntryContext;

static void *defragStreamConsumerPendingEntry(raxIterator *ri, void *privdata) {
    PendingEntryContext *ctx = privdata;
    streamNACK *nack = ri->data, *newnack;
    nack->consumer = ctx->c; /* update nack pointer to consumer */
    newnack = activeDefragAlloc(nack);
    if (newnack) {
        /* update consumer group pointer to the nack */
        void *prev;
        raxInsert(ctx->cg->pel, ri->key, ri->key_len, newnack, &prev);
        serverAssert(prev == nack);
    }
    return newnack;
}

static void *defragStreamConsumer(raxIterator *ri, void *privdata) {
    streamConsumer *c = ri->data;
    streamCG *cg = privdata;
    void *newc = activeDefragAlloc(c);
    if (newc) {
        c = newc;
    }
    sds newsds = activeDefragSds(c->name);
    if (newsds) c->name = newsds;
    if (c->pel) {
        PendingEntryContext pel_ctx = {cg, c};
        defragRadixTree(&c->pel, 0, defragStreamConsumerPendingEntry, &pel_ctx);
    }
    return newc; /* returns NULL if c was not defragged */
}

static void *defragStreamConsumerGroup(raxIterator *ri, void *privdata) {
    streamCG *cg = ri->data;
    UNUSED(privdata);
    if (cg->consumers) defragRadixTree(&cg->consumers, 0, defragStreamConsumer, cg);
    if (cg->pel) defragRadixTree(&cg->pel, 0, NULL, NULL);
    return NULL;
}

static void defragStream(robj *ob) {
    serverAssert(ob->type == OBJ_STREAM && ob->encoding == OBJ_ENCODING_STREAM);
    stream *s = ob->ptr, *news;

    /* handle the main struct */
    if ((news = activeDefragAlloc(s))) ob->ptr = s = news;

    if (raxSize(s->rax) > server.active_defrag_max_scan_fields) {
        rax *newrax = activeDefragAlloc(s->rax);
        if (newrax) s->rax = newrax;
        defragLater(ob);
    } else
        defragRadixTree(&s->rax, 1, NULL, NULL);

    if (s->cgroups) defragRadixTree(&s->cgroups, 1, defragStreamConsumerGroup, NULL);
}

/* Defrag a module key. This is either done immediately or scheduled
 * for later. Returns then number of pointers defragged.
 */
static void defragModule(serverDb *db, robj *obj) {
    serverAssert(obj->type == OBJ_MODULE);
    /* Fun fact (and a bug since forever): The key is passed to
     * moduleDefragValue as an sds string, but the parameter is declared to be
     * an robj and it's passed as such to the module type defrag callbacks.
     * Nobody can ever have used this, i.e. accessed the key name in the defrag
     * or free_effort module type callbacks. */
    void *sds_key_passed_as_robj = objectGetKey(obj);
    if (!moduleDefragValue(sds_key_passed_as_robj, obj, db->id)) defragLater(obj);
}

/* for each key we scan in the main dict, this function will attempt to defrag
 * all the various pointers it has. */
static void defragKey(defragKeysCtx *ctx, robj **elemref) {
    serverDb *db = &server.db[ctx->dbid];
    int slot = ctx->kvstate.slot;
    robj *newob, *ob;
    unsigned char *newzl;
    ob = *elemref;

    /* Try to defrag robj and/or string value. */
    if ((newob = activeDefragStringOb(ob))) {
        *elemref = newob;
        if (objectGetExpire(newob) >= 0) {
            /* Replace the pointer in the expire table without accessing the old
             * pointer. */
            hashtable *expires_ht = kvstoreGetHashtable(db->expires, slot);
            int replaced = hashtableReplaceReallocatedEntry(expires_ht, ob, newob);
            serverAssert(replaced);
        }
        ob = newob;
    }

    if (ob->type == OBJ_STRING) {
        /* Already handled in activeDefragStringOb. */
    } else if (ob->type == OBJ_LIST) {
        if (ob->encoding == OBJ_ENCODING_QUICKLIST) {
            defragQuicklist(ob);
        } else if (ob->encoding == OBJ_ENCODING_LISTPACK) {
            if ((newzl = activeDefragAlloc(ob->ptr))) ob->ptr = newzl;
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (ob->type == OBJ_SET) {
        if (ob->encoding == OBJ_ENCODING_HASHTABLE) {
            defragSet(ob);
        } else if (ob->encoding == OBJ_ENCODING_INTSET || ob->encoding == OBJ_ENCODING_LISTPACK) {
            void *newptr, *ptr = ob->ptr;
            if ((newptr = activeDefragAlloc(ptr))) ob->ptr = newptr;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (ob->type == OBJ_ZSET) {
        if (ob->encoding == OBJ_ENCODING_LISTPACK) {
            if ((newzl = activeDefragAlloc(ob->ptr))) ob->ptr = newzl;
        } else if (ob->encoding == OBJ_ENCODING_SKIPLIST) {
            defragZsetSkiplist(ob);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (ob->type == OBJ_HASH) {
        if (ob->encoding == OBJ_ENCODING_LISTPACK) {
            if ((newzl = activeDefragAlloc(ob->ptr))) ob->ptr = newzl;
        } else if (ob->encoding == OBJ_ENCODING_HASHTABLE) {
            defragHash(ob);
        } else {
            serverPanic("Unknown hash encoding");
        }
    } else if (ob->type == OBJ_STREAM) {
        defragStream(ob);
    } else if (ob->type == OBJ_MODULE) {
        defragModule(db, ob);
    } else {
        serverPanic("Unknown object type");
    }
}

/* Defrag scan callback for the main db dictionary. */
static void dbKeysScanCallback(void *privdata, void *elemref) {
    long long hits_before = server.stat_active_defrag_hits;
    defragKey((defragKeysCtx *)privdata, (robj **)elemref);
    if (server.stat_active_defrag_hits != hits_before)
        server.stat_active_defrag_key_hits++;
    else
        server.stat_active_defrag_key_misses++;
    server.stat_active_defrag_scanned++;
}

/* Defrag scan callback for a pubsub channels hashtable. */
static void defragPubsubScanCallback(void *privdata, void *elemref) {
    defragPubSubCtx *ctx = privdata;
    void **channel_dict_ref = (void **)elemref;
    dict *newclients, *clients = *channel_dict_ref;
    robj *newchannel, *channel = *(robj **)dictMetadata(clients);
    size_t allocation_size;

    /* Try to defrag the channel name. */
    serverAssert(channel->refcount == (int)dictSize(clients) + 1);
    newchannel = activeDefragStringObWithoutFree(channel, &allocation_size);
    if (newchannel) {
        *(robj **)dictMetadata(clients) = newchannel;

        /* The channel name is shared by the client's pubsub(shard) and server's
         * pubsub(shard), after defraging the channel name, we need to update
         * the reference in the clients' dictionary. */
        dictIterator *di = dictGetIterator(clients);
        dictEntry *clientde;
        while ((clientde = dictNext(di)) != NULL) {
            client *c = dictGetKey(clientde);
            dict *client_channels = ctx->getPubSubChannels(c);
            dictEntry *pubsub_channel = dictFind(client_channels, newchannel);
            serverAssert(pubsub_channel);
            dictSetKey(ctx->getPubSubChannels(c), pubsub_channel, newchannel);
        }
        dictReleaseIterator(di);
        // Now that we're done correcting the references, we can safely free the old channel robj
        allocatorDefragFree(channel, allocation_size);
    }

    /* Try to defrag the dictionary of clients that is stored as the value part. */
    if ((newclients = dictDefragTables(clients)))
        *channel_dict_ref = newclients;

    server.stat_active_defrag_scanned++;
}

/* returns 0 more work may or may not be needed (see non-zero cursor),
 * and 1 if time is up and more work is needed. */
static int defragLaterItem(robj *ob, unsigned long *cursor, monotime endtime, int dbid) {
    if (ob) {
        if (ob->type == OBJ_LIST) {
            return scanLaterList(ob, cursor, endtime);
        } else if (ob->type == OBJ_SET) {
            scanLaterSet(ob, cursor);
        } else if (ob->type == OBJ_ZSET) {
            scanLaterZset(ob, cursor);
        } else if (ob->type == OBJ_HASH) {
            scanLaterHash(ob, cursor);
        } else if (ob->type == OBJ_STREAM) {
            return scanLaterStreamListpacks(ob, cursor, endtime);
        } else if (ob->type == OBJ_MODULE) {
            /* Fun fact (and a bug since forever): The key is passed to
             * moduleLateDefrag as an sds string, but the parameter is declared
             * to be an robj and it's passed as such to the module type defrag
             * callbacks. Nobody can ever have used this, i.e. accessed the key
             * name in the defrag module type callback. */
            void *sds_key_passed_as_robj = objectGetKey(ob);
            long long endtimeWallClock = ustime() + (endtime - getMonotonicUs());
            return moduleLateDefrag(sds_key_passed_as_robj, ob, cursor, endtimeWallClock, dbid);
        } else {
            *cursor = 0; /* object type may have changed since we schedule it for later */
        }
    } else {
        *cursor = 0; /* object may have been deleted already */
    }
    return 0;
}


// A kvstoreHelperPreContinueFn
static doneStatus defragLaterStep(monotime endtime, void *privdata) {
    defragKeysCtx *ctx = privdata;

    unsigned int iterations = 0;
    unsigned long long prev_defragged = server.stat_active_defrag_hits;
    unsigned long long prev_scanned = server.stat_active_defrag_scanned;

    while (defrag_later && listLength(defrag_later) > 0) {
        listNode *head = listFirst(defrag_later);
        sds key = head->value;
        void *found = NULL;
        kvstoreHashtableFind(ctx->kvstate.kvs, ctx->kvstate.slot, key, &found);
        robj *ob = found;

        long long key_defragged = server.stat_active_defrag_hits;
        bool timeout = (defragLaterItem(ob, &defrag_later_cursor, endtime, ctx->dbid) == 1);
        if (key_defragged != server.stat_active_defrag_hits) {
            server.stat_active_defrag_key_hits++;
        } else {
            server.stat_active_defrag_key_misses++;
        }

        if (timeout) break;

        if (defrag_later_cursor == 0) {
            // the item is finished, move on
            listDelNode(defrag_later, head);
        }

        if (++iterations > 16 || server.stat_active_defrag_hits - prev_defragged > 512 ||
            server.stat_active_defrag_scanned - prev_scanned > 64) {
            if (getMonotonicUs() > endtime) break;
            iterations = 0;
            prev_defragged = server.stat_active_defrag_hits;
            prev_scanned = server.stat_active_defrag_scanned;
        }
    }

    return (!defrag_later || listLength(defrag_later) == 0) ? DEFRAG_DONE : DEFRAG_NOT_DONE;
}


/* This helper function handles most of the work for iterating over a kvstore.  'privdata', if
 * provided, MUST begin with 'kvstoreIterState' and this part is automatically updated by this
 * function during the iteration. */
static doneStatus defragStageKvstoreHelper(monotime endtime,
                                           kvstore *kvs,
                                           hashtableScanFunction scan_fn,
                                           kvstoreHelperPreContinueFn precontinue_fn,
                                           void *privdata) {
    static kvstoreIterState state; // STATIC - this persists
    if (endtime == 0) {
        // Starting the stage, set up the state information for this stage
        state.kvs = kvs;
        state.slot = KVS_SLOT_DEFRAG_LUT;
        state.cursor = 0;
        return DEFRAG_NOT_DONE;
    }
    if (kvs != state.kvs) {
        // There has been a change of the kvs (flushdb, swapdb, etc.).  Just complete the stage.
        return DEFRAG_DONE;
    }

    unsigned int iterations = 0;
    unsigned long long prev_defragged = server.stat_active_defrag_hits;
    unsigned long long prev_scanned = server.stat_active_defrag_scanned;

    if (state.slot == KVS_SLOT_DEFRAG_LUT) {
        // Before we start scanning the kvstore, handle the main structures
        do {
            state.cursor = kvstoreHashtableDefragTables(kvs, state.cursor, activeDefragAlloc);
            if (getMonotonicUs() >= endtime) return DEFRAG_NOT_DONE;
        } while (state.cursor != 0);
        state.slot = KVS_SLOT_UNASSIGNED;
    }

    while (true) {
        if (++iterations > 16 || server.stat_active_defrag_hits - prev_defragged > 512 || server.stat_active_defrag_scanned - prev_scanned > 64) {
            if (getMonotonicUs() >= endtime) break;
            iterations = 0;
            prev_defragged = server.stat_active_defrag_hits;
            prev_scanned = server.stat_active_defrag_scanned;
        }

        if (precontinue_fn) {
            if (privdata) *(kvstoreIterState *)privdata = state;
            if (precontinue_fn(endtime, privdata) == DEFRAG_NOT_DONE) return DEFRAG_NOT_DONE;
        }

        if (!state.cursor) {
            // If there's no cursor, we're ready to begin a new kvstore slot.
            if (state.slot == KVS_SLOT_UNASSIGNED) {
                state.slot = kvstoreGetFirstNonEmptyHashtableIndex(kvs);
            } else {
                state.slot = kvstoreGetNextNonEmptyHashtableIndex(kvs, state.slot);
            }

            if (state.slot == KVS_SLOT_UNASSIGNED) return DEFRAG_DONE;
        }

        // Whatever privdata's actual type, this function requires that it begins with kvstoreIterState.
        if (privdata) *(kvstoreIterState *)privdata = state;
        state.cursor = kvstoreHashtableScanDefrag(kvs, state.slot, state.cursor,
                                                  scan_fn, privdata, activeDefragAlloc,
                                                  HASHTABLE_SCAN_EMIT_REF);
    }

    return DEFRAG_NOT_DONE;
}


// Target is a DBID
static doneStatus defragStageDbKeys(monotime endtime, void *target, void *privdata) {
    UNUSED(privdata);
    int dbid = (uintptr_t)target;
    serverDb *db = &server.db[dbid];

    static defragKeysCtx ctx; // STATIC - this persists
    if (endtime == 0) {
        ctx.dbid = dbid;
        // Don't return yet.  Call the helper with endtime==0 below.
    }
    serverAssert(ctx.dbid == dbid);

    return defragStageKvstoreHelper(endtime, db->keys,
                                    dbKeysScanCallback, defragLaterStep, &ctx);
}


// Target is a DBID
static doneStatus defragStageExpiresKvstore(monotime endtime, void *target, void *privdata) {
    UNUSED(privdata);
    int dbid = (uintptr_t)target;
    serverDb *db = &server.db[dbid];
    return defragStageKvstoreHelper(endtime, db->expires,
                                    scanHashtableCallbackCountScanned, NULL, NULL);
}


static doneStatus defragStagePubsubKvstore(monotime endtime, void *target, void *privdata) {
    // target is server.pubsub_channels or server.pubsubshard_channels
    getClientChannelsFnWrapper *fnWrapper = privdata;
    defragPubSubCtx ctx;
    ctx.getPubSubChannels = fnWrapper->fn;
    return defragStageKvstoreHelper(endtime, (kvstore *)target,
                                    defragPubsubScanCallback, NULL, &ctx);
}


static doneStatus defragLuaScripts(monotime endtime, void *target, void *privdata) {
    UNUSED(target);
    UNUSED(privdata);
    if (endtime == 0) return DEFRAG_NOT_DONE; // required initialization
    /* In case we are in the process of eval some script we do not want to replace the script being run
     * so we just bail out without really defragging here. */
    if (scriptIsRunning()) return DEFRAG_DONE;
    activeDefragSdsDict(evalScriptsDict(), DEFRAG_SDS_DICT_VAL_LUA_SCRIPT);
    return DEFRAG_DONE;
}


static doneStatus defragModuleGlobals(monotime endtime, void *target, void *privdata) {
    UNUSED(target);
    UNUSED(privdata);
    if (endtime == 0) return DEFRAG_NOT_DONE; // required initialization
    moduleDefragGlobals();
    return DEFRAG_DONE;
}


static bool defragIsRunning(void) {
    return (defrag.timeproc_id > 0);
}


static void addDefragStage(defragStageFn stage_fn, void *target, void *privdata) {
    StageDescriptor *stage = zmalloc(sizeof(StageDescriptor));
    stage->stage_fn = stage_fn;
    stage->target = target;
    stage->privdata = privdata;
    listAddNodeTail(defrag.remaining_stages, stage);
}


// Called at the end of a complete defrag cycle, or when defrag is terminated
static void endDefragCycle(bool normal_termination) {
    if (normal_termination) {
        // For normal termination, we expect...
        serverAssert(!defrag.current_stage);
        serverAssert(listLength(defrag.remaining_stages) == 0);
        serverAssert(!defrag_later || listLength(defrag_later) == 0);
    } else {
        // Defrag is being terminated abnormally
        aeDeleteTimeEvent(server.el, defrag.timeproc_id);

        if (defrag.current_stage) {
            zfree(defrag.current_stage);
            defrag.current_stage = NULL;
        }
        listSetFreeMethod(defrag.remaining_stages, zfree);
    }
    defrag.timeproc_id = AE_DELETED_EVENT_ID;

    listRelease(defrag.remaining_stages);
    defrag.remaining_stages = NULL;

    if (defrag_later) {
        listRelease(defrag_later);
        defrag_later = NULL;
    }
    defrag_later_cursor = 0;

    size_t frag_bytes;
    float frag_pct = getAllocatorFragmentation(&frag_bytes);
    serverLog(LL_VERBOSE, "Active defrag done in %dms, reallocated=%d, frag=%.0f%%, frag_bytes=%zu",
              (int)elapsedMs(defrag.start_cycle), (int)(server.stat_active_defrag_hits - defrag.start_defrag_hits),
              frag_pct, frag_bytes);

    server.stat_total_active_defrag_time += elapsedUs(server.stat_last_active_defrag_time);
    server.stat_last_active_defrag_time = 0;
    server.active_defrag_cpu_percent = 0;

    /* Immediately check to see if we should start another defrag cycle. */
    monitorActiveDefrag();
}


/* Must be called at the start of the timeProc as it measures the delay from the end of the previous
 * timeProc invocation when performing the computation. */
static int computeDefragCycleUs(void) {
    long dutyCycleUs;

    int targetCpuPercent = server.active_defrag_cpu_percent;
    serverAssert(targetCpuPercent > 0 && targetCpuPercent < 100);

    static int prevCpuPercent = 0; // STATIC - this persists
    if (targetCpuPercent != prevCpuPercent) {
        /* If the targetCpuPercent changes, the value might be different from when the last wait
         *  time was computed.  In this case, don't consider wait time.  (This is really only an
         *  issue in crazy tests that dramatically increase CPU while defrag is running.) */
        defrag.timeproc_end_time = 0;
        prevCpuPercent = targetCpuPercent;
    }

    // Given when the last duty cycle ended, compute time needed to achieve the desired percentage.
    if (defrag.timeproc_end_time == 0) {
        // Either the first call to the timeProc, or we were paused for some reason.
        defrag.timeproc_overage_us = 0;
        dutyCycleUs = server.active_defrag_cycle_us;
    } else {
        long waitedUs = getMonotonicUs() - defrag.timeproc_end_time;
        /* Given the elapsed wait time between calls, compute the necessary duty time needed to
         *  achieve the desired CPU percentage.
         *  With:  D = duty time, W = wait time, P = percent
         *  Solve:    D          P
         *          -----   =  -----
         *          D + W       100
         *  Solving for D:
         *     D = P * W / (100 - P)
         *
         * Note that dutyCycleUs addresses starvation.  If the wait time was long, we will compensate
         *  with a proportionately long duty-cycle.  This won't significantly affect perceived
         *  latency, because clients are already being impacted by the long cycle time which caused
         *  the starvation of the timer. */
        dutyCycleUs = targetCpuPercent * waitedUs / (100 - targetCpuPercent);

        // Also adjust for any accumulated overage.
        dutyCycleUs -= defrag.timeproc_overage_us;
        defrag.timeproc_overage_us = 0;

        if (dutyCycleUs < server.active_defrag_cycle_us) {
            /* We never reduce our cycle time, that would increase overhead.  Instead, we track this
             *  as part of the overage, and increase wait time between cycles. */
            defrag.timeproc_overage_us = server.active_defrag_cycle_us - dutyCycleUs;
            dutyCycleUs = server.active_defrag_cycle_us;
        }
    }
    return dutyCycleUs;
}


/* Must be called at the end of the timeProc as it records the timeproc_end_time for use in the next
 * computeDefragCycleUs computation. */
static int computeDelayMs(monotime intendedEndtime) {
    defrag.timeproc_end_time = getMonotonicUs();
    long overage = defrag.timeproc_end_time - intendedEndtime;
    defrag.timeproc_overage_us += overage; // track over/under desired CPU
    /* Allow negative overage (underage) to count against existing overage, but don't allow
     * underage (from short stages) to be accumulated.  */
    if (defrag.timeproc_overage_us < 0) defrag.timeproc_overage_us = 0;

    int targetCpuPercent = server.active_defrag_cpu_percent;
    serverAssert(targetCpuPercent > 0 && targetCpuPercent < 100);

    // Given the desired duty cycle, what inter-cycle delay do we need to achieve that?
    // We want to achieve a specific CPU percent.  To do that, we can't use a skewed computation.
    // Example, if we run for 1ms and delay 10ms, that's NOT 10%, because the total cycle time is 11ms.
    // Instead, if we rum for 1ms, our total time should be 10ms.  So the delay is only 9ms.
    long totalCycleTimeUs = server.active_defrag_cycle_us * 100 / targetCpuPercent;
    long delayUs = totalCycleTimeUs - server.active_defrag_cycle_us;
    // Only increase delay by the fraction of the overage that would be non-duty-cycle
    delayUs += defrag.timeproc_overage_us * (100 - targetCpuPercent) / 100;
    if (delayUs < 0) delayUs = 0;
    long delayMs = delayUs / 1000; // round down
    return delayMs;
}


/* An independent time proc for defrag.  While defrag is running, this is called much more often
 *  than the server cron.  Frequent short calls provides low latency impact. */
static long long activeDefragTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    // This timer shouldn't be registered unless there's work to do.
    serverAssert(defrag.current_stage || listLength(defrag.remaining_stages) > 0);

    if (!server.active_defrag_enabled) {
        // Defrag has been disabled while running
        endDefragCycle(false);
        return AE_NOMORE;
    }

    if (hasActiveChildProcess()) {
        // If there's a child process, pause the defrag, polling until the child completes.
        defrag.timeproc_end_time = 0; // prevent starvation recovery
        return 100;
    }

    monotime starttime = getMonotonicUs();
    int dutyCycleUs = computeDefragCycleUs();
    monotime endtime = starttime + dutyCycleUs;
    bool haveMoreWork = true;

    mstime_t latency;
    latencyStartMonitor(latency);

    do {
        if (!defrag.current_stage) {
            defrag.current_stage = listNodeValue(listFirst(defrag.remaining_stages));
            listDelNode(defrag.remaining_stages, listFirst(defrag.remaining_stages));
            // Initialize the stage with endtime==0
            doneStatus status = defrag.current_stage->stage_fn(0, defrag.current_stage->target, defrag.current_stage->privdata);
            serverAssert(status == DEFRAG_NOT_DONE); // Initialization should always return DEFRAG_NOT_DONE
        }

        doneStatus status = defrag.current_stage->stage_fn(endtime, defrag.current_stage->target, defrag.current_stage->privdata);
        if (status == DEFRAG_DONE) {
            zfree(defrag.current_stage);
            defrag.current_stage = NULL;
        }

        haveMoreWork = (defrag.current_stage || listLength(defrag.remaining_stages) > 0);
        /* If we've completed a stage early, and still have a standard time allotment remaining,
         * we'll start another stage.  This can happen when defrag is running infrequently, and
         * starvation protection has increased the duty-cycle. */
    } while (haveMoreWork && getMonotonicUs() <= endtime - server.active_defrag_cycle_us);

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("active-defrag-cycle", latency);

    if (haveMoreWork) {
        return computeDelayMs(endtime);
    } else {
        endDefragCycle(true);
        return AE_NOMORE; // Ends the timer proc
    }
}


/* During long running scripts, or while loading, there is a periodic function for handling other
 * actions.  This interface allows defrag to continue running, avoiding a single long defrag step
 * after the long operation completes. */
void defragWhileBlocked(void) {
    // This is called infrequently, while timers are not active.  We might need to start defrag.
    if (!defragIsRunning()) monitorActiveDefrag();

    if (!defragIsRunning()) return;

    // Save off the timeproc_id.  If we have a normal termination, it will be cleared.
    long long timeproc_id = defrag.timeproc_id;

    // Simulate a single call of the timer proc
    long long reschedule_delay = activeDefragTimeProc(NULL, 0, NULL);
    if (reschedule_delay == AE_NOMORE) {
        // If it's done, deregister the timer
        aeDeleteTimeEvent(server.el, timeproc_id);
    }
    /* Otherwise, just ignore the reschedule_delay, the timer will pop the next time that the
     * event loop can process timers again. */
}


static void beginDefragCycle(void) {
    serverAssert(!defragIsRunning());

    serverAssert(defrag.remaining_stages == NULL);
    defrag.remaining_stages = listCreate();

    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        addDefragStage(defragStageDbKeys, (void *)(uintptr_t)dbid, NULL);
        addDefragStage(defragStageExpiresKvstore, (void *)(uintptr_t)dbid, NULL);
    }

    static getClientChannelsFnWrapper getClientPubSubChannelsFn = {getClientPubSubChannels};
    static getClientChannelsFnWrapper getClientPubSubShardChannelsFn = {getClientPubSubShardChannels};
    addDefragStage(defragStagePubsubKvstore, server.pubsub_channels, &getClientPubSubChannelsFn);
    addDefragStage(defragStagePubsubKvstore, server.pubsubshard_channels, &getClientPubSubShardChannelsFn);

    addDefragStage(defragLuaScripts, NULL, NULL);
    addDefragStage(defragModuleGlobals, NULL, NULL);

    defrag.current_stage = NULL;
    defrag.start_cycle = getMonotonicUs();
    defrag.start_defrag_hits = server.stat_active_defrag_hits;
    defrag.timeproc_end_time = 0;
    defrag.timeproc_overage_us = 0;
    defrag.timeproc_id = aeCreateTimeEvent(server.el, 0, activeDefragTimeProc, NULL, NULL);

    elapsedStart(&server.stat_last_active_defrag_time);
}


#define INTERPOLATE(x, x1, x2, y1, y2) ((y1) + ((x) - (x1)) * ((y2) - (y1)) / ((x2) - (x1)))
#define LIMIT(y, min, max) ((y) < (min) ? min : ((y) > (max) ? max : (y)))

/* decide if defrag is needed, and at what CPU effort to invest in it */
static void updateDefragCpuPercent(void) {
    size_t frag_bytes;
    float frag_pct = getAllocatorFragmentation(&frag_bytes);
    if (server.active_defrag_cpu_percent == 0) {
        if (frag_pct < server.active_defrag_threshold_lower ||
            frag_bytes < server.active_defrag_ignore_bytes) return;
    }

    /* Calculate the adaptive aggressiveness of the defrag based on the current
     * fragmentation and configurations. */
    int cpu_pct = INTERPOLATE(frag_pct, server.active_defrag_threshold_lower, server.active_defrag_threshold_upper,
                              server.active_defrag_cpu_min, server.active_defrag_cpu_max);
    cpu_pct = LIMIT(cpu_pct, server.active_defrag_cpu_min, server.active_defrag_cpu_max);

    /* Normally we allow increasing the aggressiveness during a scan, but don't
     * reduce it, since we should not lower the aggressiveness when fragmentation
     * drops. But when a configuration is made, we should reconsider it. */
    if (cpu_pct > server.active_defrag_cpu_percent || server.active_defrag_configuration_changed) {
        server.active_defrag_configuration_changed = 0;
        if (defragIsRunning()) {
            serverLog(LL_VERBOSE, "Changing active defrag CPU, frag=%.0f%%, frag_bytes=%zu, cpu=%d%%",
                      frag_pct, frag_bytes, cpu_pct);
        } else {
            serverLog(LL_VERBOSE, "Starting active defrag, frag=%.0f%%, frag_bytes=%zu, cpu=%d%%",
                      frag_pct, frag_bytes, cpu_pct);
        }
        server.active_defrag_cpu_percent = cpu_pct;
    }
}


void monitorActiveDefrag(void) {
    if (!server.active_defrag_enabled) return;

    /* Defrag gets paused while a child process is active.  So there's no point in starting a new
     *  cycle or adjusting the CPU percentage for an existing cycle. */
    if (hasActiveChildProcess()) return;

    updateDefragCpuPercent();

    if (server.active_defrag_cpu_percent > 0 && !defragIsRunning()) beginDefragCycle();
}

#else /* HAVE_DEFRAG */

void monitorActiveDefrag(void) {
    /* Not implemented yet. */
}

void *activeDefragAlloc(void *ptr) {
    UNUSED(ptr);
    return NULL;
}

robj *activeDefragStringOb(robj *ob) {
    UNUSED(ob);
    return NULL;
}

void defragWhileBlocked(void) {
}

#endif
