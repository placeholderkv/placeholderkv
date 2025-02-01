/*
 * Index-based KV store implementation
 * This file implements a KV store comprised of an array of hash tables (see hashtable.c)
 * The purpose of this KV store is to have easy access to all keys that belong
 * in the same hash table (i.e. are in the same hashtable-index)
 *
 * For example, when the server is running in cluster mode, we use kvstore to save
 * all keys that map to the same hash-slot in a separate hash table within the kvstore
 * struct.
 * This enables us to easily access all keys that map to a specific hash-slot.
 *
 * Copyright (c) Redis contributors.
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
#include "fmacros.h"

#include <string.h>
#include <stddef.h>
#include <stdlib.h>

#include "zmalloc.h"
#include "kvstore.h"
#include "serverassert.h"
#include "monotonic.h"

#define UNUSED(V) ((void)V)

static hashtable *kvstoreIteratorNextHashtable(kvstoreIterator *kvs_it);

struct _kvstore {
    int flags;
    hashtableType *dtype;
    hashtable **hashtables;
    int num_hashtables;
    int num_hashtables_bits;
    list *rehashing;                          /* List of hash tables in this kvstore that are currently rehashing. */
    int resize_cursor;                        /* Cron job uses this cursor to gradually resize hash tables (only used if num_hashtables > 1). */
    int allocated_hashtables;                 /* The number of allocated hashtables. */
    int non_empty_hashtables;                 /* The number of non-empty hashtables. */
    unsigned long long key_count;             /* Total number of keys in this kvstore. */
    unsigned long long bucket_count;          /* Total number of buckets in this kvstore across hash tables. */
    unsigned long long *hashtable_size_index; /* Binary indexed tree (BIT) that describes cumulative key frequencies up until
                                               * given hashtable-index. */
    size_t overhead_hashtable_lut;            /* Overhead of all hashtables in bytes. */
    size_t overhead_hashtable_rehashing;      /* Overhead of hash tables rehashing in bytes. */
};

/* Structure for kvstore iterator that allows iterating across multiple hashtables. */
struct _kvstoreIterator {
    kvstore *kvs;
    long long didx;
    long long next_didx;
    hashtableIterator di;
};

/* Structure for kvstore hashtable iterator that allows iterating the corresponding hashtable. */
struct _kvstoreHashtableIterator {
    kvstore *kvs;
    long long didx;
    hashtableIterator di;
};

/* Hashtable metadata for database, used for record the position in rehashing list. */
typedef struct {
    listNode *rehashing_node; /* list node in rehashing list */
    kvstore *kvs;
} kvstoreHashtableMetadata;

/**********************************/
/*** Helpers **********************/
/**********************************/

/* Get the hash table pointer based on hashtable-index. */
hashtable *kvstoreGetHashtable(kvstore *kvs, int didx) {
    return kvs->hashtables[didx];
}

static hashtable **kvstoreGetHashtableRef(kvstore *kvs, int didx) {
    return &kvs->hashtables[didx];
}

static int kvstoreHashtableIsRehashingPaused(kvstore *kvs, int didx) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    return ht ? hashtableIsRehashingPaused(ht) : 0;
}

/* Returns total (cumulative) number of keys up until given hashtable-index (inclusive).
 * Time complexity is O(log(kvs->num_hashtables)). */
static unsigned long long cumulativeKeyCountRead(kvstore *kvs, int didx) {
    if (kvs->num_hashtables == 1) {
        assert(didx == 0);
        return kvstoreSize(kvs);
    }
    int idx = didx + 1;
    unsigned long long sum = 0;
    while (idx > 0) {
        sum += kvs->hashtable_size_index[idx];
        idx -= (idx & -idx);
    }
    return sum;
}

static void addHashtableIndexToCursor(kvstore *kvs, int didx, unsigned long long *cursor) {
    if (kvs->num_hashtables == 1) return;
    /* didx can be -1 when iteration is over and there are no more hashtables to visit. */
    if (didx < 0) return;
    *cursor = (*cursor << kvs->num_hashtables_bits) | didx;
}

static int getAndClearHashtableIndexFromCursor(kvstore *kvs, unsigned long long *cursor) {
    if (kvs->num_hashtables == 1) return 0;
    int didx = (int)(*cursor & (kvs->num_hashtables - 1));
    *cursor = *cursor >> kvs->num_hashtables_bits;
    return didx;
}

/* Updates binary index tree (also known as Fenwick tree), increasing key count for a given hashtable.
 * You can read more about this data structure here https://en.wikipedia.org/wiki/Fenwick_tree
 * Time complexity is O(log(kvs->num_hashtables)). */
static void cumulativeKeyCountAdd(kvstore *kvs, int didx, long delta) {
    kvs->key_count += delta;

    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    size_t size = hashtableSize(ht);
    if (delta < 0 && size == 0) {
        kvs->non_empty_hashtables--; /* It became empty. */
    } else if (delta > 0 && size == (size_t)delta) {
        kvs->non_empty_hashtables++; /* It was empty before. */
    }

    /* BIT does not need to be calculated when there's only one hashtable. */
    if (kvs->num_hashtables == 1) return;

    /* Update the BIT */
    int idx = didx + 1; /* Unlike hashtable indices, BIT is 1-based, so we need to add 1. */
    while (idx <= kvs->num_hashtables) {
        if (delta < 0) {
            assert(kvs->hashtable_size_index[idx] >= (unsigned long long)labs(delta));
        }
        kvs->hashtable_size_index[idx] += delta;
        idx += (idx & -idx);
    }
}

/* Create the hashtable if it does not exist and return it. */
static hashtable *createHashtableIfNeeded(kvstore *kvs, int didx) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (ht) return ht;

    kvs->hashtables[didx] = hashtableCreate(kvs->dtype);
    kvstoreHashtableMetadata *metadata = (kvstoreHashtableMetadata *)hashtableMetadata(kvs->hashtables[didx]);
    metadata->kvs = kvs;
    /* Memory is counted by kvstoreHashtableTrackMemUsage, but when it's invoked
     * by hashtableCreate above, we don't know which hashtable it is for, because
     * the metadata has yet been initialized. Account for the newly created
     * hashtable here instead. */
    kvs->overhead_hashtable_lut += hashtableMemUsage(kvs->hashtables[didx]);
    kvs->allocated_hashtables++;
    return kvs->hashtables[didx];
}

/* Called when the hashtable will delete entries, the function will check
 * KVSTORE_FREE_EMPTY_HASHTABLES to determine whether the empty hashtable needs
 * to be freed.
 *
 * Note that for rehashing hashtables, that is, in the case of safe iterators
 * and Scan, we won't delete the hashtable. We will check whether it needs
 * to be deleted when we're releasing the iterator. */
static void freeHashtableIfNeeded(kvstore *kvs, int didx) {
    if (!(kvs->flags & KVSTORE_FREE_EMPTY_HASHTABLES) || !kvstoreGetHashtable(kvs, didx) || kvstoreHashtableSize(kvs, didx) != 0 ||
        kvstoreHashtableIsRehashingPaused(kvs, didx))
        return;
    hashtableRelease(kvs->hashtables[didx]);
    kvs->hashtables[didx] = NULL;
    kvs->allocated_hashtables--;
}

/*************************************/
/*** hashtable callbacks ***************/
/*************************************/

/* Adds hash table to the rehashing list, which allows us
 * to quickly find rehash targets during incremental rehashing.
 *
 * If there are multiple hashtables, updates the bucket count for the given hash table
 * in a DB, bucket count incremented with the new ht size during the rehashing phase.
 * If there's one hashtable, bucket count can be retrieved directly from single hashtable bucket. */
void kvstoreHashtableRehashingStarted(hashtable *ht) {
    kvstoreHashtableMetadata *metadata = (kvstoreHashtableMetadata *)hashtableMetadata(ht);
    kvstore *kvs = metadata->kvs;
    listAddNodeTail(kvs->rehashing, ht);
    metadata->rehashing_node = listLast(kvs->rehashing);

    size_t from, to;
    hashtableRehashingInfo(ht, &from, &to);
    kvs->bucket_count += to; /* Started rehashing (Add the new ht size) */
    kvs->overhead_hashtable_rehashing += from * HASHTABLE_BUCKET_SIZE;
}

/* Remove hash table from the rehashing list.
 *
 * Updates the bucket count for the given hash table in a DB. It removes
 * the old ht size of the hash table from the total sum of buckets for a DB.  */
void kvstoreHashtableRehashingCompleted(hashtable *ht) {
    kvstoreHashtableMetadata *metadata = (kvstoreHashtableMetadata *)hashtableMetadata(ht);
    kvstore *kvs = metadata->kvs;
    if (metadata->rehashing_node) {
        listDelNode(kvs->rehashing, metadata->rehashing_node);
        metadata->rehashing_node = NULL;
    }

    size_t from, to;
    hashtableRehashingInfo(ht, &from, &to);
    kvs->bucket_count -= from; /* Finished rehashing (Remove the old ht size) */
    kvs->overhead_hashtable_rehashing -= from * HASHTABLE_BUCKET_SIZE;
}

/* Hashtable callback to keep track of memory usage. */
void kvstoreHashtableTrackMemUsage(hashtable *ht, ssize_t delta) {
    kvstoreHashtableMetadata *metadata = (kvstoreHashtableMetadata *)hashtableMetadata(ht);
    if (metadata->kvs == NULL) {
        /* This is the initial allocation by hashtableCreate, when the metadata
         * hasn't been initialized yet. */
        return;
    }
    metadata->kvs->overhead_hashtable_lut += delta;
}

/* Returns the size of the DB hashtable metadata in bytes. */
size_t kvstoreHashtableMetadataSize(void) {
    return sizeof(kvstoreHashtableMetadata);
}

/**********************************/
/*** API **************************/
/**********************************/

/* Create an array of hash tables
 * num_hashtables_bits is the log2 of the amount of hash tables needed (e.g. 0 for 1 hashtable,
 * 3 for 8 hashtables, etc.)
 */
kvstore *kvstoreCreate(hashtableType *type, int num_hashtables_bits, int flags) {
    /* We can't support more than 2^16 hashtables because we want to save 48 bits
     * for the hashtable cursor, see kvstoreScan */
    assert(num_hashtables_bits <= 16);

    /* The hashtableType of kvstore needs to use the specific callbacks.
     * If there are any changes in the future, it will need to be modified. */
    assert(type->rehashingStarted == kvstoreHashtableRehashingStarted);
    assert(type->rehashingCompleted == kvstoreHashtableRehashingCompleted);
    assert(type->trackMemUsage == kvstoreHashtableTrackMemUsage);
    assert(type->getMetadataSize == kvstoreHashtableMetadataSize);

    kvstore *kvs = zcalloc(sizeof(*kvs));
    kvs->dtype = type;
    kvs->flags = flags;

    kvs->num_hashtables_bits = num_hashtables_bits;
    kvs->num_hashtables = 1 << kvs->num_hashtables_bits;
    kvs->hashtables = zcalloc(sizeof(hashtable *) * kvs->num_hashtables);
    if (!(kvs->flags & KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND)) {
        for (int i = 0; i < kvs->num_hashtables; i++) createHashtableIfNeeded(kvs, i);
    }

    kvs->rehashing = listCreate();
    kvs->key_count = 0;
    kvs->non_empty_hashtables = 0;
    kvs->resize_cursor = 0;
    kvs->hashtable_size_index = kvs->num_hashtables > 1 ? zcalloc(sizeof(unsigned long long) * (kvs->num_hashtables + 1)) : NULL;
    kvs->bucket_count = 0;
    kvs->overhead_hashtable_lut = 0;
    kvs->overhead_hashtable_rehashing = 0;

    return kvs;
}

void kvstoreEmpty(kvstore *kvs, void(callback)(hashtable *)) {
    for (int didx = 0; didx < kvs->num_hashtables; didx++) {
        hashtable *ht = kvstoreGetHashtable(kvs, didx);
        if (!ht) continue;
        kvstoreHashtableMetadata *metadata = (kvstoreHashtableMetadata *)hashtableMetadata(ht);
        if (metadata->rehashing_node) metadata->rehashing_node = NULL;
        hashtableEmpty(ht, callback);
        freeHashtableIfNeeded(kvs, didx);
    }

    listEmpty(kvs->rehashing);

    kvs->key_count = 0;
    kvs->non_empty_hashtables = 0;
    kvs->resize_cursor = 0;
    kvs->bucket_count = 0;
    if (kvs->hashtable_size_index) memset(kvs->hashtable_size_index, 0, sizeof(unsigned long long) * (kvs->num_hashtables + 1));
    kvs->overhead_hashtable_rehashing = 0;
}

void kvstoreRelease(kvstore *kvs) {
    for (int didx = 0; didx < kvs->num_hashtables; didx++) {
        hashtable *ht = kvstoreGetHashtable(kvs, didx);
        if (!ht) continue;
        kvstoreHashtableMetadata *metadata = (kvstoreHashtableMetadata *)hashtableMetadata(ht);
        if (metadata->rehashing_node) metadata->rehashing_node = NULL;
        hashtableRelease(ht);
    }
    assert(kvs->overhead_hashtable_lut == 0);
    zfree(kvs->hashtables);

    listRelease(kvs->rehashing);
    if (kvs->hashtable_size_index) zfree(kvs->hashtable_size_index);

    zfree(kvs);
}

unsigned long long int kvstoreSize(kvstore *kvs) {
    if (kvs->num_hashtables != 1) {
        return kvs->key_count;
    } else {
        return kvs->hashtables[0] ? hashtableSize(kvs->hashtables[0]) : 0;
    }
}

/* This method provides the cumulative sum of all the hash table buckets
 * across hash tables in a database. */
unsigned long kvstoreBuckets(kvstore *kvs) {
    if (kvs->num_hashtables != 1) {
        return kvs->bucket_count;
    } else {
        return kvs->hashtables[0] ? hashtableBuckets(kvs->hashtables[0]) : 0;
    }
}

size_t kvstoreMemUsage(kvstore *kvs) {
    size_t mem = sizeof(*kvs);
    mem += kvs->overhead_hashtable_lut;

    /* Values are hashtable* shared with kvs->hashtables */
    mem += listLength(kvs->rehashing) * sizeof(listNode);

    if (kvs->hashtable_size_index) mem += sizeof(unsigned long long) * (kvs->num_hashtables + 1);

    return mem;
}

/*
 * This method is used to iterate over the elements of the entire kvstore specifically across hashtables.
 * It's a three pronged approach.
 *
 * 1. It uses the provided cursor `cursor` to retrieve the hashtable index from it.
 * 2. If the hash table is in a valid state checked through the provided callback `hashtableScanValidFunction`,
 *    it performs a hashtableScan over the appropriate `keyType` hash table of `db`.
 * 3. If the hashtable is entirely scanned i.e. the cursor has reached 0, the next non empty hashtable is discovered.
 *    The hashtable information is embedded into the cursor and returned.
 *
 * To restrict the scan to a single hashtable, pass a valid hashtable index as
 * 'onlydidx', otherwise pass -1.
 */
unsigned long long kvstoreScan(kvstore *kvs,
                               unsigned long long cursor,
                               int onlydidx,
                               hashtableScanFunction scan_cb,
                               kvstoreScanShouldSkipHashtable *skip_cb,
                               void *privdata) {
    unsigned long long next_cursor = 0;
    /* During hash table traversal, 48 upper bits in the cursor are used for positioning in the HT.
     * Following lower bits are used for the hashtable index number, ranging from 0 to 2^num_hashtables_bits-1.
     * Hashtable index is always 0 at the start of iteration and can be incremented only if there are
     * multiple hashtables. */
    int didx = getAndClearHashtableIndexFromCursor(kvs, &cursor);
    if (onlydidx >= 0) {
        if (didx < onlydidx) {
            /* Fast-forward to onlydidx. */
            assert(onlydidx < kvs->num_hashtables);
            didx = onlydidx;
            cursor = 0;
        } else if (didx > onlydidx) {
            /* The cursor is already past onlydidx. */
            return 0;
        }
    }

    hashtable *ht = kvstoreGetHashtable(kvs, didx);

    int skip = !ht || (skip_cb && skip_cb(ht));
    if (!skip) {
        next_cursor = hashtableScan(ht, cursor, scan_cb, privdata);
        /* In hashtableScan, scan_cb may delete entries (e.g., in active expire case). */
        freeHashtableIfNeeded(kvs, didx);
    }
    /* scanning done for the current hash table or if the scanning wasn't possible, move to the next hashtable index. */
    if (next_cursor == 0 || skip) {
        if (onlydidx >= 0) return 0;
        didx = kvstoreGetNextNonEmptyHashtableIndex(kvs, didx);
    }
    if (didx == -1) {
        return 0;
    }
    addHashtableIndexToCursor(kvs, didx, &next_cursor);
    return next_cursor;
}

/*
 * This functions increases size of kvstore to match desired number.
 * It resizes all individual hash tables, unless skip_cb indicates otherwise.
 *
 * Based on the parameter `try_expand`, appropriate hashtable expand API is invoked.
 * if try_expand is set to 1, `hashtableTryExpand` is used else `hashtableExpand`.
 * The return code is either 1 or 0 for both the API(s).
 * 1 response is for successful expansion. However, 0 response signifies failure in allocation in
 * `hashtableTryExpand` call and in case of `hashtableExpand` call it signifies no expansion was performed.
 */
int kvstoreExpand(kvstore *kvs, uint64_t newsize, int try_expand, kvstoreExpandShouldSkipHashtableIndex *skip_cb) {
    if (newsize == 0) return 1;
    for (int i = 0; i < kvs->num_hashtables; i++) {
        if (skip_cb && skip_cb(i)) continue;
        /* If the hash table doesn't exist, create it. */
        hashtable *ht = createHashtableIfNeeded(kvs, i);
        if (try_expand) {
            if (!hashtableTryExpand(ht, newsize)) return 0;
        } else {
            hashtableExpand(ht, newsize);
        }
    }

    return 1;
}

/* Returns fair random hashtable index, probability of each hashtable being
 * returned is proportional to the number of elements that hash table holds.
 * This function guarantees that it returns a hashtable-index of a non-empty
 * hashtable, unless the entire kvstore is empty. Time complexity of this
 * function is O(log(kvs->num_hashtables)). */
int kvstoreGetFairRandomHashtableIndex(kvstore *kvs) {
    unsigned long target = kvstoreSize(kvs) ? (random() % kvstoreSize(kvs)) + 1 : 0;
    return kvstoreFindHashtableIndexByKeyIndex(kvs, target);
}

void kvstoreGetStats(kvstore *kvs, char *buf, size_t bufsize, int full) {
    buf[0] = '\0';

    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;
    hashtableStats *mainHtStats = NULL;
    hashtableStats *rehashHtStats = NULL;
    hashtable *ht;
    kvstoreIterator *kvs_it = kvstoreIteratorInit(kvs, HASHTABLE_ITER_SAFE);
    while ((ht = kvstoreIteratorNextHashtable(kvs_it))) {
        hashtableStats *stats = hashtableGetStatsHt(ht, 0, full);
        if (!mainHtStats) {
            mainHtStats = stats;
        } else {
            hashtableCombineStats(stats, mainHtStats);
            hashtableFreeStats(stats);
        }
        if (hashtableIsRehashing(ht)) {
            stats = hashtableGetStatsHt(ht, 1, full);
            if (!rehashHtStats) {
                rehashHtStats = stats;
            } else {
                hashtableCombineStats(stats, rehashHtStats);
                hashtableFreeStats(stats);
            }
        }
    }
    kvstoreIteratorRelease(kvs_it);

    if (mainHtStats && bufsize > 0) {
        l = hashtableGetStatsMsg(buf, bufsize, mainHtStats, full);
        hashtableFreeStats(mainHtStats);
        buf += l;
        bufsize -= l;
    }

    if (rehashHtStats && bufsize > 0) {
        l = hashtableGetStatsMsg(buf, bufsize, rehashHtStats, full);
        hashtableFreeStats(rehashHtStats);
        buf += l;
        bufsize -= l;
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize - 1] = '\0';
}

/* Finds a hashtable containing target element in a key space ordered by hashtable index.
 * Consider this example. Hash Tables are represented by brackets and keys by dots:
 *  #0   #1   #2     #3    #4
 * [..][....][...][.......][.]
 *                    ^
 *                 target
 *
 * In this case hashtable #3 contains key that we are trying to find.
 *
 * The return value is 0 based hashtable-index, and the range of the target is [1..kvstoreSize], kvstoreSize inclusive.
 *
 * To find the hashtable, we start with the root node of the binary index tree and search through its children
 * from the highest index (2^num_hashtables_bits in our case) to the lowest index. At each node, we check if the target
 * value is greater than the node's value. If it is, we remove the node's value from the target and recursively
 * search for the new target using the current node as the parent.
 * Time complexity of this function is O(log(kvs->num_hashtables))
 */
int kvstoreFindHashtableIndexByKeyIndex(kvstore *kvs, unsigned long target) {
    if (kvs->num_hashtables == 1 || kvstoreSize(kvs) == 0) return 0;
    assert(target <= kvstoreSize(kvs));

    int result = 0, bit_mask = 1 << kvs->num_hashtables_bits;
    for (int i = bit_mask; i != 0; i >>= 1) {
        int current = result + i;
        /* When the target index is greater than 'current' node value the we will update
         * the target and search in the 'current' node tree. */
        if (target > kvs->hashtable_size_index[current]) {
            target -= kvs->hashtable_size_index[current];
            result = current;
        }
    }
    /* Adjust the result to get the correct hashtable:
     * 1. result += 1;
     *    After the calculations, the index of target in hashtable_size_index should be the next one,
     *    so we should add 1.
     * 2. result -= 1;
     *    Unlike BIT(hashtable_size_index is 1-based), hashtable indices are 0-based, so we need to subtract 1.
     * As the addition and subtraction cancel each other out, we can simply return the result. */
    return result;
}

/* Wrapper for kvstoreFindHashtableIndexByKeyIndex to get the first non-empty hashtable index in the kvstore. */
int kvstoreGetFirstNonEmptyHashtableIndex(kvstore *kvs) {
    return kvstoreFindHashtableIndexByKeyIndex(kvs, 1);
}

/* Returns next non-empty hashtable index strictly after given one, or -1 if provided didx is the last one. */
int kvstoreGetNextNonEmptyHashtableIndex(kvstore *kvs, int didx) {
    if (kvs->num_hashtables == 1) {
        assert(didx == 0);
        return -1;
    }
    unsigned long long next_key = cumulativeKeyCountRead(kvs, didx) + 1;
    return next_key <= kvstoreSize(kvs) ? kvstoreFindHashtableIndexByKeyIndex(kvs, next_key) : -1;
}

int kvstoreNumNonEmptyHashtables(kvstore *kvs) {
    return kvs->non_empty_hashtables;
}

int kvstoreNumAllocatedHashtables(kvstore *kvs) {
    return kvs->allocated_hashtables;
}

int kvstoreNumHashtables(kvstore *kvs) {
    return kvs->num_hashtables;
}

/* Returns kvstore iterator that can be used to iterate through sub-hash tables.
 *
 * The caller should free the resulting kvs_it with kvstoreIteratorRelease. */
kvstoreIterator *kvstoreIteratorInit(kvstore *kvs, uint8_t flags) {
    kvstoreIterator *kvs_it = zmalloc(sizeof(*kvs_it));
    kvs_it->kvs = kvs;
    kvs_it->didx = -1;
    kvs_it->next_didx = kvstoreGetFirstNonEmptyHashtableIndex(kvs_it->kvs); /* Finds first non-empty hashtable index. */
    hashtableInitIterator(&kvs_it->di, NULL, flags);
    return kvs_it;
}

/* Free the kvs_it returned by kvstoreIteratorInit. */
void kvstoreIteratorRelease(kvstoreIterator *kvs_it) {
    hashtableIterator *iter = &kvs_it->di;
    hashtableResetIterator(iter);
    /* In the safe iterator context, we may delete entries. */
    freeHashtableIfNeeded(kvs_it->kvs, kvs_it->didx);
    zfree(kvs_it);
}

/* Returns next hash table from the iterator, or NULL if iteration is complete. */
static hashtable *kvstoreIteratorNextHashtable(kvstoreIterator *kvs_it) {
    if (kvs_it->next_didx == -1) return NULL;

    /* The hashtable may be deleted during the iteration process, so here need to check for NULL. */
    if (kvs_it->didx != -1 && kvstoreGetHashtable(kvs_it->kvs, kvs_it->didx)) {
        /* Before we move to the next hashtable, reset the iter of the previous hashtable. */
        hashtableIterator *iter = &kvs_it->di;
        hashtableResetIterator(iter);
        /* In the safe iterator context, we may delete entries. */
        freeHashtableIfNeeded(kvs_it->kvs, kvs_it->didx);
    }

    kvs_it->didx = kvs_it->next_didx;
    kvs_it->next_didx = kvstoreGetNextNonEmptyHashtableIndex(kvs_it->kvs, kvs_it->didx);
    return kvs_it->kvs->hashtables[kvs_it->didx];
}

int kvstoreIteratorGetCurrentHashtableIndex(kvstoreIterator *kvs_it) {
    assert(kvs_it->didx >= 0 && kvs_it->didx < kvs_it->kvs->num_hashtables);
    return kvs_it->didx;
}

/* Fetches the next element and returns 1. Returns 0 if there are no more elements. */
int kvstoreIteratorNext(kvstoreIterator *kvs_it, void **next) {
    if (kvs_it->didx != -1 && hashtableNext(&kvs_it->di, next)) {
        return 1;
    } else {
        /* No current hashtable or reached the end of the hash table. */
        hashtable *ht = kvstoreIteratorNextHashtable(kvs_it);
        if (!ht) return 0;
        hashtableReinitIterator(&kvs_it->di, ht);
        return hashtableNext(&kvs_it->di, next);
    }
}

/* This method traverses through kvstore hash tables and triggers a resize.
 * It first tries to shrink if needed, and if it isn't, it tries to expand. */
void kvstoreTryResizeHashtables(kvstore *kvs, int limit) {
    if (limit > kvs->num_hashtables) limit = kvs->num_hashtables;

    for (int i = 0; i < limit; i++) {
        int didx = kvs->resize_cursor;
        hashtable *ht = kvstoreGetHashtable(kvs, didx);
        if (ht && !hashtableShrinkIfNeeded(ht)) {
            hashtableExpandIfNeeded(ht);
        }
        kvs->resize_cursor = (didx + 1) % kvs->num_hashtables;
    }
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use threshold_us
 * of CPU time at every call of this function to perform some rehashing.
 *
 * The function returns the amount of microsecs spent if some rehashing was
 * performed, otherwise 0 is returned. */
uint64_t kvstoreIncrementallyRehash(kvstore *kvs, uint64_t threshold_us) {
    if (listLength(kvs->rehashing) == 0) return 0;

    /* Our goal is to rehash as many hash tables as we can before reaching threshold_us,
     * after each hash table completes rehashing, it removes itself from the list. */
    listNode *node;
    monotime timer;
    uint64_t elapsed_us = 0;
    elapsedStart(&timer);
    while ((node = listFirst(kvs->rehashing))) {
        hashtableRehashMicroseconds(listNodeValue(node), threshold_us - elapsed_us);

        elapsed_us = elapsedUs(timer);
        if (elapsed_us >= threshold_us) {
            break; /* Reached the time limit. */
        }
    }
    return elapsed_us;
}

/* Size in bytes of hash tables used by the hashtables. */
size_t kvstoreOverheadHashtableLut(kvstore *kvs) {
    return kvs->overhead_hashtable_lut;
}

size_t kvstoreOverheadHashtableRehashing(kvstore *kvs) {
    return kvs->overhead_hashtable_rehashing;
}

unsigned long kvstoreHashtableRehashingCount(kvstore *kvs) {
    return listLength(kvs->rehashing);
}

unsigned long kvstoreHashtableSize(kvstore *kvs, int didx) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    return hashtableSize(ht);
}

kvstoreHashtableIterator *kvstoreGetHashtableIterator(kvstore *kvs, int didx, uint8_t flags) {
    kvstoreHashtableIterator *kvs_di = zmalloc(sizeof(*kvs_di));
    kvs_di->kvs = kvs;
    kvs_di->didx = didx;
    hashtableInitIterator(&kvs_di->di, kvstoreGetHashtable(kvs, didx), flags);
    return kvs_di;
}

/* Free the kvs_di returned by kvstoreGetHashtableIterator. */
void kvstoreReleaseHashtableIterator(kvstoreHashtableIterator *kvs_di) {
    /* The hashtable may be deleted during the iteration process, so here need to check for NULL. */
    if (kvstoreGetHashtable(kvs_di->kvs, kvs_di->didx)) {
        hashtableResetIterator(&kvs_di->di);
        /* In the safe iterator context, we may delete entries. */
        freeHashtableIfNeeded(kvs_di->kvs, kvs_di->didx);
    }

    zfree(kvs_di);
}

/* Get the next element of the hashtable through kvstoreHashtableIterator and hashtableNext. */
int kvstoreHashtableIteratorNext(kvstoreHashtableIterator *kvs_di, void **next) {
    /* The hashtable may be deleted during the iteration process, so here need to check for NULL. */
    hashtable *ht = kvstoreGetHashtable(kvs_di->kvs, kvs_di->didx);
    if (!ht) return 0;
    return hashtableNext(&kvs_di->di, next);
}

int kvstoreHashtableRandomEntry(kvstore *kvs, int didx, void **entry) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    return hashtableRandomEntry(ht, entry);
}

int kvstoreHashtableFairRandomEntry(kvstore *kvs, int didx, void **entry) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    return hashtableFairRandomEntry(ht, entry);
}

unsigned int kvstoreHashtableSampleEntries(kvstore *kvs, int didx, void **dst, unsigned int count) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    return hashtableSampleEntries(ht, dst, count);
}

int kvstoreHashtableExpand(kvstore *kvs, int didx, unsigned long size) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    return hashtableExpand(ht, size);
}

unsigned long kvstoreHashtableScanDefrag(kvstore *kvs,
                                         int didx,
                                         unsigned long v,
                                         hashtableScanFunction fn,
                                         void *privdata,
                                         void *(*defragfn)(void *),
                                         int flags) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    return hashtableScanDefrag(ht, v, fn, privdata, defragfn, flags);
}

/* This function doesn't defrag the data (keys and values) within hashtable. It
 * only reallocates the memory used by the hashtable structure itself using the
 * provided allocation function. This feature was added for the active defrag
 * feature.
 *
 * A "cursor" is used to perform the operation iteratively.  When first called, a
 * cursor value of 0 should be provided.  The return value is an updated cursor which should be
 * provided on the next iteration.  The operation is complete when 0 is returned.
 *
 * The provided defragfn callback should return either NULL (if reallocation
 * isn't necessary) or return a pointer to reallocated memory like realloc(). */
unsigned long kvstoreHashtableDefragTables(kvstore *kvs, unsigned long cursor, void *(*defragfn)(void *)) {
    for (int didx = cursor; didx < kvs->num_hashtables; didx++) {
        hashtable **ref = kvstoreGetHashtableRef(kvs, didx), *new;
        if (!*ref) continue;
        new = hashtableDefragTables(*ref, defragfn);
        if (new) {
            *ref = new;
            kvstoreHashtableMetadata *metadata = hashtableMetadata(new);
            if (metadata->rehashing_node) metadata->rehashing_node->value = new;
        }
        return (didx + 1);
    }
    return 0;
}

uint64_t kvstoreGetHash(kvstore *kvs, const void *key) {
    return kvs->dtype->hashFunction(key);
}

int kvstoreHashtableFind(kvstore *kvs, int didx, void *key, void **found) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    return hashtableFind(ht, key, found);
}

void **kvstoreHashtableFindRef(kvstore *kvs, int didx, const void *key) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return NULL;
    return hashtableFindRef(ht, key);
}

int kvstoreHashtableAddOrFind(kvstore *kvs, int didx, void *key, void **existing) {
    hashtable *ht = createHashtableIfNeeded(kvs, didx);
    int ret = hashtableAddOrFind(ht, key, existing);
    if (ret) cumulativeKeyCountAdd(kvs, didx, 1);
    return ret;
}

int kvstoreHashtableAdd(kvstore *kvs, int didx, void *entry) {
    hashtable *ht = createHashtableIfNeeded(kvs, didx);
    int ret = hashtableAdd(ht, entry);
    if (ret) cumulativeKeyCountAdd(kvs, didx, 1);
    return ret;
}

int kvstoreHashtableFindPositionForInsert(kvstore *kvs, int didx, void *key, hashtablePosition *position, void **existing) {
    hashtable *ht = createHashtableIfNeeded(kvs, didx);
    return hashtableFindPositionForInsert(ht, key, position, existing);
}

/* Must be used together with kvstoreHashtableFindPositionForInsert, with returned
 * position and with the same didx. */
void kvstoreHashtableInsertAtPosition(kvstore *kvs, int didx, void *entry, void *position) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    hashtableInsertAtPosition(ht, entry, position);
    cumulativeKeyCountAdd(kvs, didx, 1);
}

void **kvstoreHashtableTwoPhasePopFindRef(kvstore *kvs, int didx, const void *key, void *position) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return NULL;
    return hashtableTwoPhasePopFindRef(ht, key, position);
}

void kvstoreHashtableTwoPhasePopDelete(kvstore *kvs, int didx, void *position) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    hashtableTwoPhasePopDelete(ht, position);
    cumulativeKeyCountAdd(kvs, didx, -1);
    freeHashtableIfNeeded(kvs, didx);
}

int kvstoreHashtablePop(kvstore *kvs, int didx, const void *key, void **popped) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    int ret = hashtablePop(ht, key, popped);
    if (ret) {
        cumulativeKeyCountAdd(kvs, didx, -1);
        freeHashtableIfNeeded(kvs, didx);
    }
    return ret;
}

int kvstoreHashtableDelete(kvstore *kvs, int didx, const void *key) {
    hashtable *ht = kvstoreGetHashtable(kvs, didx);
    if (!ht) return 0;
    int ret = hashtableDelete(ht, key);
    if (ret) {
        cumulativeKeyCountAdd(kvs, didx, -1);
        freeHashtableIfNeeded(kvs, didx);
    }
    return ret;
}
