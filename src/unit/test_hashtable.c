#include "../hashtable.h"
#include "test_help.h"
#include "../mt19937-64.h"
#include "../zmalloc.h"
#include "../monotonic.h"

#include <stdio.h>
#include <limits.h>
#include <string.h>
#include <math.h>

/* Global variable to test the memory tracking callback. */
static size_t mem_usage;

/* From util.c: getRandomBytes to seed hash function. */
void getRandomBytes(unsigned char *p, size_t len);

/* Init hash function salt and seed random generator. */
static void randomSeed(void) {
    unsigned long long seed;
    getRandomBytes((void *)&seed, sizeof(seed));
    init_genrand64(seed);
    srandom((unsigned)seed);
    uint8_t hashseed[16];
    getRandomBytes(hashseed, sizeof(hashseed));
    hashtableSetHashFunctionSeed(hashseed);
}

/* An entry holding a string key and a string value in one allocation. */
typedef struct {
    unsigned int keysize; /* Sizes, including null-terminator */
    unsigned int valsize;
    char data[]; /* key and value */
} keyval;

static keyval *create_keyval(const char *key, const char *val) {
    size_t keysize = strlen(key) + 1;
    size_t valsize = strlen(val) + 1;
    keyval *e = malloc(sizeof(keyval) + keysize + valsize);
    e->keysize = keysize;
    e->valsize = valsize;
    memcpy(e->data, key, keysize);
    memcpy(e->data + keysize, val, valsize);
    return e;
}

static const void *getkey(const void *entry) {
    const keyval *e = entry;
    return e->data;
}

static const void *getval(const void *entry) {
    const keyval *e = entry;
    return e->data + e->keysize;
}

static uint64_t hashfunc(const void *key) {
    return hashtableGenHashFunction(key, strlen(key));
}

static int keycmp(const void *key1, const void *key2) {
    return strcmp(key1, key2);
}

static void freekeyval(void *keyval) {
    free(keyval);
}

static void trackmemusage(hashtable *ht, ssize_t delta) {
    UNUSED(ht);
    mem_usage += delta;
}

/* Hashtable type used for some of the tests. */
static hashtableType keyval_type = {
    .entryGetKey = getkey,
    .hashFunction = hashfunc,
    .keyCompare = keycmp,
    .entryDestructor = freekeyval,
    .trackMemUsage = trackmemusage,
};

/* Callback for testing hashtableEmpty(). */
static long empty_callback_call_counter;
void emptyCallback(hashtable *ht) {
    UNUSED(ht);
    empty_callback_call_counter++;
}

/* Prototypes for debugging */
void hashtableDump(hashtable *ht);
void hashtableHistogram(hashtable *ht);
int hashtableLongestBucketChain(hashtable *ht);
size_t nextCursor(size_t v, size_t mask);

int test_cursor(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);
    TEST_ASSERT(nextCursor(0x0000, 0xffff) == 0x8000);
    TEST_ASSERT(nextCursor(0x8000, 0xffff) == 0x4000);
    TEST_ASSERT(nextCursor(0x4001, 0xffff) == 0xc001);
    TEST_ASSERT(nextCursor(0xffff, 0xffff) == 0x0000);
    return 0;
}

int test_set_hash_function_seed(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);
    randomSeed();
    return 0;
}

static int add_find_delete_test_helper(int flags) {
    int count = (flags & UNIT_TEST_ACCURATE) ? 1000000 : 200;
    TEST_ASSERT(mem_usage == 0);
    hashtable *ht = hashtableCreate(&keyval_type);
    int j;

    /* Add */
    for (j = 0; j < count; j++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "%d", j);
        snprintf(val, sizeof(val), "%d", count - j + 42);
        keyval *e = create_keyval(key, val);
        TEST_ASSERT(hashtableAdd(ht, e));
    }
    TEST_ASSERT(hashtableMemUsage(ht) == mem_usage);

    if (count < 1000) {
        hashtableHistogram(ht);
        printf("Mem usage: %zu\n", hashtableMemUsage(ht));
    }

    /* Find */
    for (j = 0; j < count; j++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "%d", j);
        snprintf(val, sizeof(val), "%d", count - j + 42);
        void *found;
        TEST_ASSERT(hashtableFind(ht, key, &found));
        keyval *e = found;
        TEST_ASSERT(!strcmp(val, getval(e)));
    }

    /* Delete half of them */
    for (j = 0; j < count / 2; j++) {
        char key[32];
        snprintf(key, sizeof(key), "%d", j);
        if (j % 3 == 0) {
            /* Test hashtablePop */
            char val[32];
            snprintf(val, sizeof(val), "%d", count - j + 42);
            void *popped;
            TEST_ASSERT(hashtablePop(ht, key, &popped));
            keyval *e = popped;
            TEST_ASSERT(!strcmp(val, getval(e)));
            free(e);
        } else {
            TEST_ASSERT(hashtableDelete(ht, key));
        }
    }
    TEST_ASSERT(hashtableMemUsage(ht) == mem_usage);

    /* Empty, i.e. delete remaining entries, with progress callback. */
    empty_callback_call_counter = 0;
    hashtableEmpty(ht, emptyCallback);
    TEST_ASSERT(empty_callback_call_counter > 0);

    /* Release memory */
    hashtableRelease(ht);
    TEST_ASSERT(mem_usage == 0);
    return 0;
}

int test_add_find_delete(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    TEST_ASSERT(add_find_delete_test_helper(flags) == 0);
    TEST_ASSERT(zmalloc_used_memory() == 0);
    return 0;
}

int test_add_find_delete_avoid_resize(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    hashtableSetResizePolicy(HASHTABLE_RESIZE_AVOID);
    TEST_ASSERT(add_find_delete_test_helper(flags) == 0);
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
    TEST_ASSERT(zmalloc_used_memory() == 0);
    return 0;
}

int test_instant_rehashing(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    long count = 200;

    /* A set of longs, i.e. pointer-sized values. */
    hashtableType type = {.instant_rehashing = 1};
    hashtable *ht = hashtableCreate(&type);
    long j;

    /* Populate and check that rehashing is never ongoing. */
    for (j = 0; j < count; j++) {
        TEST_ASSERT(hashtableAdd(ht, (void *)j));
        TEST_ASSERT(!hashtableIsRehashing(ht));
    }

    /* Delete and check that rehashing is never ongoing. */
    for (j = 0; j < count; j++) {
        TEST_ASSERT(hashtableDelete(ht, (void *)j));
        TEST_ASSERT(!hashtableIsRehashing(ht));
    }

    hashtableRelease(ht);
    return 0;
}

int test_bucket_chain_length(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    unsigned long count = 1000000;

    /* A set of longs, i.e. pointer-sized integer values. */
    hashtableType type = {0};
    hashtable *ht = hashtableCreate(&type);
    unsigned long j;
    for (j = 0; j < count; j++) {
        TEST_ASSERT(hashtableAdd(ht, (void *)j));
    }
    /* If it's rehashing, add a few more until rehashing is complete. */
    while (hashtableIsRehashing(ht)) {
        j++;
        TEST_ASSERT(hashtableAdd(ht, (void *)j));
    }
    TEST_ASSERT(j < count * 2);
    int max_chainlen_not_rehashing = hashtableLongestBucketChain(ht);
    TEST_ASSERT(max_chainlen_not_rehashing < 10);

    /* Add more until rehashing starts again. */
    while (!hashtableIsRehashing(ht)) {
        j++;
        TEST_ASSERT(hashtableAdd(ht, (void *)j));
    }
    TEST_ASSERT(j < count * 2);
    int max_chainlen_rehashing = hashtableLongestBucketChain(ht);
    TEST_ASSERT(max_chainlen_rehashing < 10);

    hashtableRelease(ht);
    return 0;
}

int test_two_phase_insert_and_pop(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int count = (flags & UNIT_TEST_ACCURATE) ? 1000000 : 200;
    hashtable *ht = hashtableCreate(&keyval_type);
    int j;

    /* hashtableFindPositionForInsert + hashtableInsertAtPosition */
    for (j = 0; j < count; j++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "%d", j);
        snprintf(val, sizeof(val), "%d", count - j + 42);
        hashtablePosition position;
        int ret = hashtableFindPositionForInsert(ht, key, &position, NULL);
        TEST_ASSERT(ret == 1);
        keyval *e = create_keyval(key, val);
        hashtableInsertAtPosition(ht, e, &position);
    }

    if (count < 1000) {
        hashtableHistogram(ht);
    }

    /* Check that all entries were inserted. */
    for (j = 0; j < count; j++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "%d", j);
        snprintf(val, sizeof(val), "%d", count - j + 42);
        void *found;
        TEST_ASSERT(hashtableFind(ht, key, &found));
        keyval *e = found;
        TEST_ASSERT(!strcmp(val, getval(e)));
    }

    /* Test two-phase pop. */
    for (j = 0; j < count; j++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "%d", j);
        snprintf(val, sizeof(val), "%d", count - j + 42);
        hashtablePosition position;
        size_t size_before_find = hashtableSize(ht);
        void **ref = hashtableTwoPhasePopFindRef(ht, key, &position);
        TEST_ASSERT(ref != NULL);
        keyval *e = *ref;
        TEST_ASSERT(!strcmp(val, getval(e)));
        TEST_ASSERT(hashtableSize(ht) == size_before_find);
        hashtableTwoPhasePopDelete(ht, &position);
        TEST_ASSERT(hashtableSize(ht) == size_before_find - 1);
        free(e);
    }
    TEST_ASSERT(hashtableSize(ht) == 0);

    hashtableRelease(ht);
    return 0;
}

int test_replace_reallocated_entry(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int count = 100, j;
    hashtable *ht = hashtableCreate(&keyval_type);

    /* Add */
    for (j = 0; j < count; j++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "%d", j);
        snprintf(val, sizeof(val), "%d", count - j + 42);
        keyval *e = create_keyval(key, val);
        TEST_ASSERT(hashtableAdd(ht, e));
    }

    /* Find and replace */
    for (j = 0; j < count; j++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "%d", j);
        snprintf(val, sizeof(val), "%d", count - j + 42);
        void *found;
        TEST_ASSERT(hashtableFind(ht, key, &found));
        keyval *old = found;
        TEST_ASSERT(strcmp(getkey(old), key) == 0);
        TEST_ASSERT(strcmp(getval(old), val) == 0);
        snprintf(val, sizeof(val), "%d", j + 1234);
        keyval *new = create_keyval(key, val);
        /* If we free 'old' before the call to hashtableReplaceReallocatedEntry,
         * we get a use-after-free warning, so instead we just overwrite it with
         * junk. The purpose is to verify that the function doesn't use the
         * memory it points to. */
        memset(old->data, 'x', old->keysize + old->valsize);
        TEST_ASSERT(hashtableReplaceReallocatedEntry(ht, old, new));
        free(old);
    }

    /* Check */
    for (j = 0; j < count; j++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "%d", j);
        snprintf(val, sizeof(val), "%d", j + 1234);
        void *found;
        TEST_ASSERT(hashtableFind(ht, key, &found));
        keyval *e = found;
        TEST_ASSERT(!strcmp(val, getval(e)));
    }

    hashtableRelease(ht);
    TEST_ASSERT(zmalloc_used_memory() == 0);
    return 0;
}

int test_incremental_find(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    size_t count = 2000000;
    uint8_t element_array[count];
    memset(element_array, 0, sizeof element_array);

    /* A set of uint8_t pointers */
    hashtableType type = {0};
    hashtable *ht = hashtableCreate(&type);

    /* Populate */
    for (size_t j = 0; j < count; j++) {
        TEST_ASSERT(hashtableAdd(ht, element_array + j));
    }

    monotime timer;
    monotonicInit();

    /* Compare to looking up one by one. */
    elapsedStart(&timer);
    for (size_t i = 0; i < count; i++) {
        uint8_t *key = &element_array[i];
        void *found;
        TEST_ASSERT(hashtableFind(ht, key, &found) == 1);
        TEST_ASSERT(found == key);
    }
    uint64_t us2 = elapsedUs(timer);
    TEST_PRINT_INFO("Lookup %zu elements one by one took %lu microseconds.",
                    count, (unsigned long)us2);

    /* Lookup elements in batches. */
    for (size_t batch_size = 1; batch_size <= 64; batch_size *= 2) {
        elapsedStart(&timer);
        for (size_t batch = 0; batch < count / batch_size; batch++) {
            /* Init batches. */
            hashtableIncrementalFindState states[batch_size];
            for (size_t i = 0; i < batch_size; i++) {
                void *key = &element_array[batch * batch_size + i];
                hashtableIncrementalFindInit(&states[i], ht, key);
            }
            /* Work on batches in round-robin order until all are done. */
            size_t num_left;
            do {
                num_left = batch_size;
                for (size_t i = 0; i < batch_size; i++) {
                    if (hashtableIncrementalFindStep(&states[i]) == 0) {
                        num_left--;
                    }
                }
            } while (num_left > 0);

            /* Fetch results. */
            for (size_t i = 0; i < batch_size; i++) {
                void *found;
                TEST_ASSERT(hashtableIncrementalFindGetResult(&states[i], &found) == 1);
                TEST_ASSERT(found == &element_array[batch * batch_size + i]);
            }
        }
        uint64_t us1 = elapsedUs(timer);
        TEST_PRINT_INFO("Lookup %zu elements in batches of %zu took %lu microseconds.",
                        count, batch_size, (unsigned long)us1);
    }

    hashtableRelease(ht);
    return 0;
}

typedef struct {
    long count;
    uint8_t entry_seen[];
} scandata;

void scanfn(void *privdata, void *entry) {
    scandata *data = (scandata *)privdata;
    unsigned long j = (unsigned long)entry;
    data->entry_seen[j]++;
    data->count++;
}

int test_scan(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    long num_entries = (flags & UNIT_TEST_LARGE_MEMORY) ? 1000000 : 200000;
    int num_rounds = (flags & UNIT_TEST_ACCURATE) ? 20 : 5;

    /* A set of longs, i.e. pointer-sized values. */
    hashtableType type = {0};
    long j;

    for (int round = 0; round < num_rounds; round++) {
        /* First round count = num_entries, then some more. */
        long count = num_entries * (1 + 2 * (double)round / num_rounds);

        /* Seed, to make sure each round is different. */
        randomSeed();

        /* Populate */
        hashtable *ht = hashtableCreate(&type);
        for (j = 0; j < count; j++) {
            TEST_ASSERT(hashtableAdd(ht, (void *)j));
        }

        /* Scan */
        scandata *data = calloc(1, sizeof(scandata) + count);
        long max_entries_per_cycle = 0;
        unsigned num_cycles = 0;
        long scanned_count = 0;
        size_t cursor = 0;
        do {
            data->count = 0;
            cursor = hashtableScan(ht, cursor, scanfn, data);
            if (data->count > max_entries_per_cycle) {
                max_entries_per_cycle = data->count;
            }
            scanned_count += data->count;
            data->count = 0;
            num_cycles++;
        } while (cursor != 0);

        /* Verify that every entry was returned exactly once. */
        TEST_ASSERT(scanned_count == count);
        for (j = 0; j < count; j++) {
            TEST_ASSERT(data->entry_seen[j] >= 1);
            TEST_ASSERT(data->entry_seen[j] <= 2);
        }

        /* Print some information for curious readers. */
        TEST_PRINT_INFO("Scanned %ld; max emitted per call: %ld; avg emitted per call: %.2lf",
                        count, max_entries_per_cycle, (double)count / num_cycles);

        /* Cleanup */
        hashtableRelease(ht);
        free(data);
    }
    return 0;
}

typedef struct {
    uint64_t value;
    uint64_t hash;
} mock_hash_entry;

static mock_hash_entry *mock_hash_entry_create(uint64_t value, uint64_t hash) {
    mock_hash_entry *entry = malloc(sizeof(mock_hash_entry));
    entry->value = value;
    entry->hash = hash;
    return entry;
}

static uint64_t mock_hash_entry_get_hash(const void *entry) {
    if (entry == NULL) return 0UL;
    mock_hash_entry *mock = (mock_hash_entry *)entry;
    return (mock->hash != 0) ? mock->hash : mock->value;
}

int test_iterator(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    size_t count = 2000000;
    uint8_t entry_array[count];
    memset(entry_array, 0, sizeof entry_array);

    /* A set of uint8_t pointers */
    hashtableType type = {0};
    hashtable *ht = hashtableCreate(&type);

    /* Populate */
    for (size_t j = 0; j < count; j++) {
        TEST_ASSERT(hashtableAdd(ht, entry_array + j));
    }

    /* Iterate */
    size_t num_returned = 0;
    hashtableIterator iter;
    void *next;
    hashtableInitIterator(&iter, ht, 0);
    while (hashtableNext(&iter, &next)) {
        uint8_t *entry = next;
        num_returned++;
        TEST_ASSERT(entry >= entry_array && entry < entry_array + count);
        /* increment entry at this position as a counter */
        (*entry)++;
    }
    hashtableResetIterator(&iter);

    /* Check that all entries were returned exactly once. */
    TEST_ASSERT(num_returned == count);
    for (size_t j = 0; j < count; j++) {
        if (entry_array[j] != 1) {
            printf("Entry %zu returned %d times\n", j, entry_array[j]);
            return 0;
        }
    }

    hashtableRelease(ht);
    return 0;
}

int test_safe_iterator(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    size_t count = 1000;
    uint8_t entry_counts[count * 2];
    memset(entry_counts, 0, sizeof entry_counts);

    /* A set of pointers into the uint8_t array. */
    hashtableType type = {0};
    hashtable *ht = hashtableCreate(&type);

    /* Populate */
    for (size_t j = 0; j < count; j++) {
        TEST_ASSERT(hashtableAdd(ht, entry_counts + j));
    }

    /* Iterate */
    size_t num_returned = 0;
    hashtableIterator iter;
    void *next;
    hashtableInitIterator(&iter, ht, HASHTABLE_ITER_SAFE);
    while (hashtableNext(&iter, &next)) {
        uint8_t *entry = next;
        size_t index = entry - entry_counts;
        num_returned++;
        TEST_ASSERT(entry >= entry_counts && entry < entry_counts + count * 2);
        /* increment entry at this position as a counter */
        (*entry)++;
        if (index % 4 == 0) {
            TEST_ASSERT(hashtableDelete(ht, entry));
        }
        /* Add new item each time we see one of the original items */
        if (index < count) {
            TEST_ASSERT(hashtableAdd(ht, entry + count));
        }
    }
    hashtableResetIterator(&iter);

    /* Check that all entries present during the whole iteration were returned
     * exactly once. (Some are deleted after being returned.) */
    TEST_ASSERT(num_returned >= count);
    for (size_t j = 0; j < count; j++) {
        if (entry_counts[j] != 1) {
            printf("Entry %zu returned %d times\n", j, entry_counts[j]);
            return 0;
        }
    }
    /* Check that entries inserted during the iteration were returned at most
     * once. */
    unsigned long num_optional_returned = 0;
    for (size_t j = count; j < count * 2; j++) {
        TEST_ASSERT(entry_counts[j] <= 1);
        num_optional_returned += entry_counts[j];
    }
    printf("Safe iterator returned %lu of the %zu entries inserted while iterating.\n", num_optional_returned, count);

    hashtableRelease(ht);
    return 0;
}

int test_compact_bucket_chain(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Create a table with only one bucket chain. */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_AVOID);
    unsigned long count = 30;

    hashtableType type = {0};
    hashtable *ht = hashtableCreate(&type);

    /* Populate */
    unsigned long j;
    for (j = 0; j < count; j++) {
        TEST_ASSERT(hashtableAdd(ht, (void *)j));
    }
    TEST_ASSERT(hashtableBuckets(ht) == 1);
    printf("Populated a single bucket chain, avoiding resize.\n");
    hashtableHistogram(ht);

    /* Delete half of the entries while iterating. */
    size_t num_chained_buckets = hashtableChainedBuckets(ht, 0);
    size_t num_returned = 0;
    hashtableIterator iter;
    hashtableInitIterator(&iter, ht, HASHTABLE_ITER_SAFE);
    void *entry;
    while (hashtableNext(&iter, &entry)) {
        /* As long as the iterator is still returning entries from the same
         * bucket chain, the bucket chain is not compacted, so it still has the
         * same number of buckets. */
        TEST_ASSERT(hashtableChainedBuckets(ht, 0) == num_chained_buckets);
        num_returned++;
        if (num_returned % 2 == 0) {
            TEST_ASSERT(hashtableDelete(ht, entry));
        }
        if (num_returned == count) {
            printf("Last iteration. Half of them have been deleted.\n");
            hashtableHistogram(ht);
        }
    }
    hashtableResetIterator(&iter);

    /* Verify that the bucket chain has been compacted by filling the holes and
     * freeing empty child buckets. */
    printf("When the iterator leaves the bucket chain, compaction should happen.\n");
    hashtableHistogram(ht);
    TEST_ASSERT(hashtableChainedBuckets(ht, 0) < num_chained_buckets);

    hashtableRelease(ht);
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
    TEST_ASSERT(zmalloc_used_memory() == 0);
    return 0;
}

int test_random_entry(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);
    randomSeed();

    size_t count = (flags & UNIT_TEST_LARGE_MEMORY) ? 7000 : 400;
    long num_rounds = (flags & UNIT_TEST_ACCURATE) ? 1000000 : 10000;

    /* A set of ints */
    hashtableType type = {0};
    hashtable *ht = hashtableCreate(&type);

    /* Populate */
    unsigned times_picked[count];
    memset(times_picked, 0, sizeof(times_picked));
    for (size_t j = 0; j < count; j++) {
        TEST_ASSERT(hashtableAdd(ht, times_picked + j));
    }

    /* Pick entries, and count how many times each entry is picked. */
    for (long i = 0; i < num_rounds; i++) {
        /* Using void* variable to avoid a cast that violates strict aliasing */
        void *entry;
        TEST_ASSERT(hashtableFairRandomEntry(ht, &entry));
        unsigned *picked = entry;
        TEST_ASSERT(picked >= times_picked && picked < times_picked + count);
        /* increment entry at this position as a counter */
        (*picked)++;
    }
    hashtableRelease(ht);

    /* Fairness measurement
     * --------------------
     *
     * Selecting a single random entry: For any entry in the hash table, let
     * X=1 if the we selected the entry (success) and X=0 otherwise. With m
     * entries, our entry is sepected with probability p = 1/m, the expected
     * value is E(X) = 1/m, E(X^2) = 1/m and the variance:
     *
     *     Var(X) = E(X^2) - (E(X))^2 = 1/m - 1/(m^2) = (1/m) * (1 - 1/m).
     *
     * Repeating the selection of a random entry: Let's repeat the experiment
     * n times and let Y be the number of times our entry was selected. This
     * is a binomial distribution.
     *
     *     Y = X_1 + X_2 + ... + X_n
     *     E(Y) = n/m
     *
     * The variance of a sum of independent random variables is the sum of the
     * variances, so Y has variance np(1−p).
     *
     *     Var(Y) = npq = np(1 - p) = (n/m) * (1 - 1/m) = n * (m - 1) / (m * m)
     */
    double m = (double)count, n = (double)num_rounds;
    double expected = n / m;                 /* E(Y) */
    double variance = n * (m - 1) / (m * m); /* Var(Y) */
    double std_dev = sqrt(variance);

    /* With large n, the distribution approaches a normal distribution and we
     * can use p68 = within 1 std dev, p95 = within 2 std dev, p99.7 = within 3
     * std dev. */
    long p68 = 0, p95 = 0, p99 = 0, p4dev = 0, p5dev = 0, p10percent = 0;
    for (size_t j = 0; j < count; j++) {
        double dev = expected - times_picked[j];
        p68 += (dev >= -std_dev && dev <= std_dev);
        p95 += (dev >= -std_dev * 2 && dev <= std_dev * 2);
        p99 += (dev >= -std_dev * 3 && dev <= std_dev * 3);
        p4dev += (dev >= -std_dev * 4 && dev <= std_dev * 4);
        p5dev += (dev >= -std_dev * 5 && dev <= std_dev * 5);
        p10percent += (dev >= -0.1 * expected && dev <= 0.1 * expected);
    }

    printf("Random entry fairness test\n");
    printf("  Pick one of %zu entries, %ld times.\n", count, num_rounds);
    printf("  Expecting each entry to be picked %.2lf times, std dev %.3lf.\n", expected, std_dev);
    printf("  Within 1 std dev (p68) = %.2lf%%\n", 100 * p68 / m);
    printf("  Within 2 std dev (p95) = %.2lf%%\n", 100 * p95 / m);
    printf("  Within 3 std dev (p99) = %.2lf%%\n", 100 * p99 / m);
    printf("  Within 4 std dev       = %.2lf%%\n", 100 * p4dev / m);
    printf("  Within 5 std dev       = %.2lf%%\n", 100 * p5dev / m);
    printf("  Within 10%% dev         = %.2lf%%\n", 100 * p10percent / m);

    /* Conclusion? The number of trials (n) relative to the probabilities (p and
     * 1 − p) must be sufficiently large (n * p ≥ 5 and n * (1 − p) ≥ 5) to
     * approximate a binomial distribution with a normal distribution. */
    if (n / m >= 5 && n * (1 - 1 / m) >= 5) {
        /* Check that 80% of the elements are picked within 3 std deviations of
         * the expected number. This is a low bar, since typically the 99% of
         * the elements are within this range.
         *
         * There is an edge case. When n is very large and m is very small, the
         * std dev of a binomial distribution is very small, which becomes too
         * strict for our bucket layout and makes the test flaky. For example
         * with m = 400 and n = 1M, we get an expected value of 2500 and a std
         * dev of 50, which is just 2% of the expected value. We lower the bar
         * for this case and accept that 80% of elements are just within 10% of
         * the expected value. */
        TEST_ASSERT_MESSAGE("Too unfair randomness",
                            100 * p99 / m >= 80.0 || 100 * p10percent / m >= 80.0);
    } else {
        printf("To uncertain numbers to draw any conclusions about fairness.\n");
    }
    return 0;
}

int test_random_entry_with_long_chain(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* We use an estimator of true probability.
     * We determine how many samples to take based on how precise of a
     * measurement we want to take, and how certain we want to be that the
     * measurement is correct.
     * https://en.wikipedia.org/wiki/Checking_whether_a_coin_is_fair#Estimator_of_true_probability
     */

    /* In a thousand runs the worst deviation seen was 0.018 +/- 0.01.
     * This means the true deviation was at least 0.008 or 0.8%.
     * Accept a deviation of 5% to be on the safe side so we don't get
     * a flaky test case. */
    const double acceptable_probability_deviation = 0.05;

    const size_t num_chained_entries = 64;
    const size_t num_random_entries = 448;
    const double p_fair = (double)num_chained_entries / (num_chained_entries + num_random_entries);

    /* Precision of our measurement */
    const double precision = (flags & UNIT_TEST_ACCURATE) ? 0.001 : 0.01;

    /* This is confidence level for our measurement as the Z value of a normal
     * distribution. 5 sigma corresponds to 0.00002% probability that our
     * measurement is farther than 'precision' from the truth. This value is
     * used in particle physics. */
    const double z = 5;

    const double n = p_fair * (1 - p_fair) * z * z / (precision * precision);
    const size_t num_samples = (size_t)n + 1;

    hashtableType type = {
        .hashFunction = mock_hash_entry_get_hash,
        .entryDestructor = freekeyval,
    };

    hashtable *ht = hashtableCreate(&type);
    hashtableExpand(ht, num_random_entries + num_chained_entries);
    uint64_t chain_hash = (uint64_t)genrand64_int64();
    if (chain_hash == 0) chain_hash++;

    /* add random entries */
    for (size_t i = 0; i < num_random_entries; i++) {
        uint64_t random_hash = (uint64_t)genrand64_int64();
        if (random_hash == chain_hash) random_hash++;
        hashtableAdd(ht, mock_hash_entry_create(random_hash, 0));
    }

    /* create long chain */
    for (size_t i = 0; i < num_chained_entries; i++) {
        hashtableAdd(ht, mock_hash_entry_create(i, chain_hash));
    }

    TEST_ASSERT(!hashtableIsRehashing(ht));

    printf("Created a table with a long bucket chain.\n");
    hashtableHistogram(ht);

    printf("Taking %zu random samples\n", num_samples);
    size_t count_chain_entry_picked = 0;
    for (size_t i = 0; i < num_samples; i++) {
        void *entry;
        TEST_ASSERT(hashtableFairRandomEntry(ht, &entry));
        mock_hash_entry *mock_entry = entry;
        if (mock_entry->hash == chain_hash) {
            count_chain_entry_picked++;
        }
    }
    const double measured_probability = (double)count_chain_entry_picked / num_samples;
    const double deviation = fabs(measured_probability - p_fair);
    printf("Measured probability: %.1f%%\n", measured_probability * 100);
    printf("Expected probability: %.1f%%\n", p_fair * 100);
    printf("Measured probability deviated %1.1f%% +/- %1.1f%% from expected probability\n",
           deviation * 100, precision * 100);
    TEST_ASSERT(deviation <= precision + acceptable_probability_deviation);

    hashtableRelease(ht);
    return 0;
}

int test_all_memory_freed(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);
    TEST_ASSERT(zmalloc_used_memory() == 0);
    return 0;
}
