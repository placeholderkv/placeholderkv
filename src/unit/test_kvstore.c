#include "../kvstore.c"
#include "test_help.h"

uint64_t hashTestCallback(const void *key) {
    return hashtableGenHashFunction((char *)key, strlen((char *)key));
}

int cmpTestCallback(const void *k1, const void *k2) {
    return strcmp(k1, k2);
}

void freeTestCallback(void *val) {
    zfree(val);
}

hashtableType KvstoreHashtableTestType = {
    .hashFunction = hashTestCallback,
    .keyCompare = cmpTestCallback,
    .entryDestructor = freeTestCallback,
    .rehashingStarted = kvstoreHashtableRehashingStarted,
    .rehashingCompleted = kvstoreHashtableRehashingCompleted,
    .trackMemUsage = kvstoreHashtableTrackMemUsage,
    .getMetadataSize = kvstoreHashtableMetadataSize,
};

char *stringFromInt(int value) {
    char buf[32];
    int len;
    char *s;

    len = snprintf(buf, sizeof(buf), "%d", value);
    s = zmalloc(len + 1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

int test_kvstoreAdd16Keys(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;

    int didx = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES);

    for (i = 0; i < 16; i++) {
        TEST_ASSERT(kvstoreHashtableAdd(kvs1, didx, stringFromInt(i)));
        TEST_ASSERT(kvstoreHashtableAdd(kvs2, didx, stringFromInt(i)));
    }
    TEST_ASSERT(kvstoreHashtableSize(kvs1, didx) == 16);
    TEST_ASSERT(kvstoreSize(kvs1) == 16);
    TEST_ASSERT(kvstoreHashtableSize(kvs2, didx) == 16);
    TEST_ASSERT(kvstoreSize(kvs2) == 16);

    kvstoreRelease(kvs1);
    kvstoreRelease(kvs2);
    return 0;
}

int test_kvstoreIteratorRemoveAllKeysNoDeleteEmptyHashtable(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    kvstoreIterator *kvs_it;

    int didx = 0;
    int curr_slot = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);

    for (i = 0; i < 16; i++) {
        TEST_ASSERT(kvstoreHashtableAdd(kvs1, didx, stringFromInt(i)));
    }

    kvs_it = kvstoreIteratorInit(kvs1, HASHTABLE_ITER_SAFE);
    while (kvstoreIteratorNext(kvs_it, &key)) {
        curr_slot = kvstoreIteratorGetCurrentHashtableIndex(kvs_it);
        TEST_ASSERT(kvstoreHashtableDelete(kvs1, curr_slot, key));
    }
    kvstoreIteratorRelease(kvs_it);

    hashtable *ht = kvstoreGetHashtable(kvs1, didx);
    TEST_ASSERT(ht != NULL);
    TEST_ASSERT(kvstoreHashtableSize(kvs1, didx) == 0);
    TEST_ASSERT(kvstoreSize(kvs1) == 0);

    kvstoreRelease(kvs1);
    return 0;
}

int test_kvstoreIteratorRemoveAllKeysDeleteEmptyHashtable(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    kvstoreIterator *kvs_it;

    int didx = 0;
    int curr_slot = 0;
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES);

    for (i = 0; i < 16; i++) {
        TEST_ASSERT(kvstoreHashtableAdd(kvs2, didx, stringFromInt(i)));
    }

    kvs_it = kvstoreIteratorInit(kvs2, HASHTABLE_ITER_SAFE);
    while (kvstoreIteratorNext(kvs_it, &key)) {
        curr_slot = kvstoreIteratorGetCurrentHashtableIndex(kvs_it);
        TEST_ASSERT(kvstoreHashtableDelete(kvs2, curr_slot, key));
    }
    kvstoreIteratorRelease(kvs_it);

    /* Make sure the hashtable was removed from the rehashing list. */
    while (kvstoreIncrementallyRehash(kvs2, 1000)) {
    }

    hashtable *ht = kvstoreGetHashtable(kvs2, didx);
    TEST_ASSERT(ht == NULL);
    TEST_ASSERT(kvstoreHashtableSize(kvs2, didx) == 0);
    TEST_ASSERT(kvstoreSize(kvs2) == 0);

    kvstoreRelease(kvs2);
    return 0;
}

int test_kvstoreHashtableIteratorRemoveAllKeysNoDeleteEmptyHashtable(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    kvstoreHashtableIterator *kvs_di;

    int didx = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);

    for (i = 0; i < 16; i++) {
        TEST_ASSERT(kvstoreHashtableAdd(kvs1, didx, stringFromInt(i)));
    }

    kvs_di = kvstoreGetHashtableIterator(kvs1, didx, HASHTABLE_ITER_SAFE);
    while (kvstoreHashtableIteratorNext(kvs_di, &key)) {
        TEST_ASSERT(kvstoreHashtableDelete(kvs1, didx, key));
    }
    kvstoreReleaseHashtableIterator(kvs_di);

    hashtable *ht = kvstoreGetHashtable(kvs1, didx);
    TEST_ASSERT(ht != NULL);
    TEST_ASSERT(kvstoreHashtableSize(kvs1, didx) == 0);
    TEST_ASSERT(kvstoreSize(kvs1) == 0);

    kvstoreRelease(kvs1);
    return 0;
}

int test_kvstoreHashtableIteratorRemoveAllKeysDeleteEmptyHashtable(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    kvstoreHashtableIterator *kvs_di;

    int didx = 0;
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES);

    for (i = 0; i < 16; i++) {
        TEST_ASSERT(kvstoreHashtableAdd(kvs2, didx, stringFromInt(i)));
    }

    kvs_di = kvstoreGetHashtableIterator(kvs2, didx, HASHTABLE_ITER_SAFE);
    while (kvstoreHashtableIteratorNext(kvs_di, &key)) {
        TEST_ASSERT(kvstoreHashtableDelete(kvs2, didx, key));
    }
    kvstoreReleaseHashtableIterator(kvs_di);

    hashtable *ht = kvstoreGetHashtable(kvs2, didx);
    TEST_ASSERT(ht == NULL);
    TEST_ASSERT(kvstoreHashtableSize(kvs2, didx) == 0);
    TEST_ASSERT(kvstoreSize(kvs2) == 0);

    kvstoreRelease(kvs2);
    return 0;
}
