#ifndef KVSTORE_H
#define KVSTORE_H

#include "hashtable.h"
#include "adlist.h"

typedef struct _kvstore kvstore;
typedef struct _kvstoreIterator kvstoreIterator;
typedef struct _kvstoreHashtableIterator kvstoreHashtableIterator;

typedef int(kvstoreScanShouldSkipHashtable)(hashtable *d);
typedef int(kvstoreExpandShouldSkipHashtableIndex)(int didx);

#define KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND (1 << 0)
#define KVSTORE_FREE_EMPTY_HASHTABLES (1 << 1)
kvstore *kvstoreCreate(hashtableType *type, int num_hashtables_bits, int flags);
void kvstoreEmpty(kvstore *kvs, void(callback)(hashtable *));
void kvstoreRelease(kvstore *kvs);
unsigned long long kvstoreSize(kvstore *kvs);
unsigned long kvstoreBuckets(kvstore *kvs);
size_t kvstoreMemUsage(kvstore *kvs);
unsigned long long kvstoreScan(kvstore *kvs,
                               unsigned long long cursor,
                               int onlydidx,
                               hashtableScanFunction scan_cb,
                               kvstoreScanShouldSkipHashtable *skip_cb,
                               void *privdata);
int kvstoreExpand(kvstore *kvs, uint64_t newsize, int try_expand, kvstoreExpandShouldSkipHashtableIndex *skip_cb);
int kvstoreGetFairRandomHashtableIndex(kvstore *kvs);
void kvstoreGetStats(kvstore *kvs, char *buf, size_t bufsize, int full);

int kvstoreFindHashtableIndexByKeyIndex(kvstore *kvs, unsigned long target);
int kvstoreGetFirstNonEmptyHashtableIndex(kvstore *kvs);
int kvstoreGetNextNonEmptyHashtableIndex(kvstore *kvs, int didx);
int kvstoreNumNonEmptyHashtables(kvstore *kvs);
int kvstoreNumAllocatedHashtables(kvstore *kvs);
int kvstoreNumHashtables(kvstore *kvs);
uint64_t kvstoreGetHash(kvstore *kvs, const void *key);

void kvstoreHashtableRehashingStarted(hashtable *d);
void kvstoreHashtableRehashingCompleted(hashtable *d);
void kvstoreHashtableTrackMemUsage(hashtable *s, ssize_t delta);
size_t kvstoreHashtableMetadataSize(void);

/* kvstore iterator specific functions */
kvstoreIterator *kvstoreIteratorInit(kvstore *kvs, uint8_t flags);
void kvstoreIteratorRelease(kvstoreIterator *kvs_it);
int kvstoreIteratorGetCurrentHashtableIndex(kvstoreIterator *kvs_it);
int kvstoreIteratorNext(kvstoreIterator *kvs_it, void **next);

/* Rehashing */
void kvstoreTryResizeHashtables(kvstore *kvs, int limit);
uint64_t kvstoreIncrementallyRehash(kvstore *kvs, uint64_t threshold_us);
size_t kvstoreOverheadHashtableLut(kvstore *kvs);
size_t kvstoreOverheadHashtableRehashing(kvstore *kvs);
unsigned long kvstoreHashtableRehashingCount(kvstore *kvs);

/* Specific hashtable access by hashtable-index */
unsigned long kvstoreHashtableSize(kvstore *kvs, int didx);
kvstoreHashtableIterator *kvstoreGetHashtableIterator(kvstore *kvs, int didx, uint8_t flags);
void kvstoreReleaseHashtableIterator(kvstoreHashtableIterator *kvs_id);
int kvstoreHashtableIteratorNext(kvstoreHashtableIterator *kvs_di, void **next);
int kvstoreHashtableRandomEntry(kvstore *kvs, int didx, void **found);
int kvstoreHashtableFairRandomEntry(kvstore *kvs, int didx, void **found);
unsigned int kvstoreHashtableSampleEntries(kvstore *kvs, int didx, void **dst, unsigned int count);
int kvstoreHashtableExpand(kvstore *kvs, int didx, unsigned long size);
unsigned long kvstoreHashtableScanDefrag(kvstore *kvs,
                                         int didx,
                                         unsigned long v,
                                         hashtableScanFunction fn,
                                         void *privdata,
                                         void *(*defragfn)(void *),
                                         int flags);
unsigned long kvstoreHashtableDefragTables(kvstore *kvs, unsigned long cursor, void *(*defragfn)(void *));
int kvstoreHashtableFind(kvstore *kvs, int didx, void *key, void **found);
void **kvstoreHashtableFindRef(kvstore *kvs, int didx, const void *key);
int kvstoreHashtableAddOrFind(kvstore *kvs, int didx, void *key, void **existing);
int kvstoreHashtableAdd(kvstore *kvs, int didx, void *entry);

int kvstoreHashtableFindPositionForInsert(kvstore *kvs, int didx, void *key, hashtablePosition *position, void **existing);
void kvstoreHashtableInsertAtPosition(kvstore *kvs, int didx, void *entry, void *position);

void **kvstoreHashtableTwoPhasePopFindRef(kvstore *kvs, int didx, const void *key, void *position);
void kvstoreHashtableTwoPhasePopDelete(kvstore *kvs, int didx, void *position);
int kvstoreHashtablePop(kvstore *kvs, int didx, const void *key, void **popped);
int kvstoreHashtableDelete(kvstore *kvs, int didx, const void *key);
hashtable *kvstoreGetHashtable(kvstore *kvs, int didx);

#endif /* KVSTORE_H */
