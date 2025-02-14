/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

/* Hashtable
 * =========
 *
 * This is an implementation of a hash table with cache-line sized buckets. It's
 * designed for speed and low memory overhead. It provides the following
 * features:
 *
 * - Incremental rehashing using two tables.
 *
 * - Stateless iteration using 'scan'.
 *
 * - A hash table contains pointers to user-defined entries. An entry needs to
 *   contain a key. Other than that, the hash table implementation doesn't care
 *   what it contains. To use it as a set, an entry is just a key. Using as a
 *   key-value map requires combining key and value into an entry object and
 *   inserting this object into the hash table. A callback for fetching the key
 *   from within the entry object is provided by the caller when creating the
 *   hash table.
 *
 * - The entry type, key type, hash function and other properties are
 *   configurable as callbacks in a 'type' structure provided when creating a
 *   hash table.
 *
 * Conventions
 * -----------
 *
 * Functions and types are prefixed by "hashtable", macros by "HASHTABLE". Internal
 * names don't use the prefix. Internal functions are 'static'.
 *
 * Credits
 * -------
 *
 * - The hashtable was designed by Viktor Söderqvist.
 * - The bucket chaining is based on an idea by Madelyn Olson.
 * - The cache-line sized bucket is inspired by ideas used in 'Swiss tables'
 *   (Benzaquen, Evlogimenos, Kulukundis, and Perepelitsa et. al.).
 * - The incremental rehashing using two tables and much of the API is based on
 *   the design used in dict, designed by Salvatore Sanfilippo.
 * - The original scan algorithm was designed by Pieter Noordhuis.
 */
#include "hashtable.h"
#include "serverassert.h"
#include "zmalloc.h"
#include "mt19937-64.h"
#include "monotonic.h"
#include "config.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* The default hashing function uses the SipHash implementation in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

/* --- Global variables --- */

static uint8_t hash_function_seed[16];
static hashtableResizePolicy resize_policy = HASHTABLE_RESIZE_ALLOW;

/* --- Fill factor --- */

/* We use a soft and a hard limit for the minimum and maximum fill factor. The
 * hard limits are used when resizing should be avoided, according to the resize
 * policy. Resizing is typically to be avoided when we have forked child process
 * running. Then, we don't want to move too much memory around, since the fork
 * is using copy-on-write.
 *
 * Even if we resize and start inserting new entries in the new table, we can
 * avoid actively moving entries from the old table to the new table. When the
 * resize policy is AVOID, we perform a step of incremental rehashing only on
 * insertions and not on lookups. */

#define MAX_FILL_PERCENT_SOFT 100
#define MAX_FILL_PERCENT_HARD 500

#define MIN_FILL_PERCENT_SOFT 13
#define MIN_FILL_PERCENT_HARD 3

/* --- Hash function API --- */

/* The seed needs to be 16 bytes. */
void hashtableSetHashFunctionSeed(const uint8_t *seed) {
    memcpy(hash_function_seed, seed, sizeof(hash_function_seed));
}

uint8_t *hashtableGetHashFunctionSeed(void) {
    return hash_function_seed;
}

uint64_t hashtableGenHashFunction(const char *buf, size_t len) {
    return siphash((const uint8_t *)buf, len, hash_function_seed);
}

uint64_t hashtableGenCaseHashFunction(const char *buf, size_t len) {
    return siphash_nocase((const uint8_t *)buf, len, hash_function_seed);
}

/* --- Global resize policy API --- */

/* The global resize policy is one of
 *
 *   - HASHTABLE_RESIZE_ALLOW: Rehash as required for optimal performance.
 *
 *   - HASHTABLE_RESIZE_AVOID: Don't rehash and move memory if it can be avoided;
 *     used when there is a fork running and we want to avoid affecting
 *     copy-on-write memory.
 *
 *   - HASHTABLE_RESIZE_FORBID: Don't rehash at all. Used in a child process which
 *     doesn't add any keys.
 *
 * Incremental rehashing works in the following way: A new table is allocated
 * and entries are incrementally moved from the old to the new table.
 *
 * To avoid affecting copy-on-write, we avoid rehashing when there is a forked
 * child process.
 *
 * We don't completely forbid resizing the table but the fill factor is
 * significantly larger when the resize policy is set to HASHTABLE_RESIZE_AVOID
 * and we resize with incremental rehashing paused, so new entries are added to
 * the new table and the old entries are rehashed only when the child process is
 * done.
 */
void hashtableSetResizePolicy(hashtableResizePolicy policy) {
    resize_policy = policy;
}

/* --- Hash table layout --- */

#if SIZE_MAX == UINT64_MAX /* 64-bit version */

#define ENTRIES_PER_BUCKET 7
#define BUCKET_BITS_TYPE uint8_t
#define BITS_NEEDED_TO_STORE_POS_WITHIN_BUCKET 3

/* Selecting the number of buckets.
 *
 * When resizing the table, we want to select an appropriate number of buckets
 * without an expensive division. Division by a power of two is cheap, but any
 * other division is expensive. We pick a fill factor to make division cheap for
 * our choice of ENTRIES_PER_BUCKET.
 *
 * The number of buckets we want is NUM_ENTRIES / (ENTRIES_PER_BUCKET * FILL_FACTOR),
 * rounded up. The fill is the number of entries we have, or want to put, in
 * the table.
 *
 * Instead of the above fraction, we multiply by an integer BUCKET_FACTOR and
 * divide by a power-of-two BUCKET_DIVISOR. This gives us a fill factor of at
 * most MAX_FILL_PERCENT_SOFT, the soft limit for expanding.
 *
 *     NUM_BUCKETS = ceil(NUM_ENTRIES * BUCKET_FACTOR / BUCKET_DIVISOR)
 *
 * This gives us
 *
 *     FILL_FACTOR = NUM_ENTRIES / (NUM_BUCKETS * ENTRIES_PER_BUCKET)
 *                 = 1 / (BUCKET_FACTOR / BUCKET_DIVISOR) / ENTRIES_PER_BUCKET
 *                 = BUCKET_DIVISOR / BUCKET_FACTOR / ENTRIES_PER_BUCKET
 */

#define BUCKET_FACTOR 5
#define BUCKET_DIVISOR 32
/* When resizing, we get a fill of at most 91.43% (32 / 5 / 7). */

#define randomSizeT() ((size_t)genrand64_int64())

#elif SIZE_MAX == UINT32_MAX /* 32-bit version */

#define ENTRIES_PER_BUCKET 12
#define BUCKET_BITS_TYPE uint16_t
#define BITS_NEEDED_TO_STORE_POS_WITHIN_BUCKET 4
#define BUCKET_FACTOR 3
#define BUCKET_DIVISOR 32
/* When resizing, we get a fill of at most 88.89% (32 / 3 / 12). */

#define randomSizeT() ((size_t)random())

#else
#error "Only 64-bit or 32-bit architectures are supported"
#endif /* 64-bit vs 32-bit version */

#ifndef static_assert
#define static_assert _Static_assert
#endif

static_assert(100 * BUCKET_DIVISOR / BUCKET_FACTOR / ENTRIES_PER_BUCKET <= MAX_FILL_PERCENT_SOFT,
              "Expand must result in a fill below the soft max fill factor");
static_assert(MAX_FILL_PERCENT_SOFT <= MAX_FILL_PERCENT_HARD, "Soft vs hard fill factor");

/* --- Random entry --- */

#define FAIR_RANDOM_SAMPLE_SIZE (ENTRIES_PER_BUCKET * 40)
#define WEAK_RANDOM_SAMPLE_SIZE ENTRIES_PER_BUCKET

/* --- Types --- */

/* Design
 * ------
 *
 * We use a design with buckets of 64 bytes (one cache line). Each bucket
 * contains metadata and entry slots for a fixed number of entries. In a 64-bit
 * system, there are up to 7 entries per bucket. These are unordered and an
 * entry can be inserted in any of the free slots. Additionally, the bucket
 * contains metadata for the entries. This includes a few bits of the hash of
 * the key of each entry, which are used to rule out false positives when
 * looking up entries.
 *
 * Bucket chaining
 * ---------------
 *
 * Each key hashes to a bucket in the hash table. If a bucket is full, the last
 * entry is replaced by a pointer to a separately allocated child bucket.
 * Child buckets form a bucket chain.
 *
 *           Bucket          Bucket          Bucket
 *     -----+---------------+---------------+---------------+-----
 *      ... | x x x x x x p | x x x x x x x | x x x x x x x | ...
 *     -----+-------------|-+---------------+---------------+-----
 *                        |
 *                        v  Child bucket
 *                      +---------------+
 *                      | x x x x x x p |
 *                      +-------------|-+
 *                                    |
 *                                    v  Child bucket
 *                                  +---------------+
 *                                  | x x x x x x x |
 *                                  +---------------+
 *
 * Bucket layout
 * -------------
 *
 * Within each bucket chain, the entries are unordered. To avoid false positives
 * when looking up an entry, a few bits of the hash value is stored in a bucket
 * metadata section in each bucket. The bucket metadata also contains a bit that
 * indicates that the bucket has a child bucket.
 *
 *         +------------------------------------------------------------------+
 *         | Metadata | Entry | Entry | Entry | Entry | Entry | Entry | Entry |
 *         +------------------------------------------------------------------+
 *        /            ` - - . _ _
 *       /                         `- - . _ _
 *      /                                     ` - . _
 *     +----------------------------------------------+
 *     | c ppppppp hash hash hash hash hash hash hash |
 *     +----------------------------------------------+
 *      |    |       |
 *      |    |      One byte of hash for each entry position in the bucket.
 *      |    |
 *      |   Presence bits. One bit for each entry position, indicating if an
 *      |   entry present or not.
 *      |
 *     Chained? One bit. If set, the last entry is a child bucket pointer.
 *
 * 64-bit version, 7 entries per bucket:
 *
 *     1 bit     7 bits    [1 byte] x 7  [8 bytes] x 7 = 64 bytes
 *     chained   presence  hashes        entries
 *
 * 32-bit version, 12 entries per bucket:
 *
 *     1 bit     12 bits   3 bits  [1 byte] x 12  2 bytes  [4 bytes] x 12 = 64 bytes
 *     chained   presence  unused  hashes         unused   entries
 */

typedef struct hashtableBucket {
    BUCKET_BITS_TYPE chained : 1;
    BUCKET_BITS_TYPE presence : ENTRIES_PER_BUCKET;
    uint8_t hashes[ENTRIES_PER_BUCKET];
    void *entries[ENTRIES_PER_BUCKET];
} bucket;

/* A key property is that the bucket size is one cache line. */
static_assert(sizeof(bucket) == HASHTABLE_BUCKET_SIZE, "Bucket size mismatch");

struct hashtable {
    hashtableType *type;
    ssize_t rehash_idx;        /* -1 = rehashing not in progress. */
    bucket *tables[2];         /* 0 = main table, 1 = rehashing target.  */
    size_t used[2];            /* Number of entries in each table. */
    int8_t bucket_exp[2];      /* Exponent for num buckets (num = 1 << exp). */
    int16_t pause_rehash;      /* Non-zero = rehashing is paused */
    int16_t pause_auto_shrink; /* Non-zero = automatic resizing disallowed. */
    size_t child_buckets[2];   /* Number of allocated child buckets. */
    void *metadata[];
};

typedef struct {
    hashtable *hashtable;
    bucket *bucket;
    long index;
    uint16_t pos_in_bucket;
    uint8_t table;
    uint8_t flags;
    union {
        /* Unsafe iterator fingerprint for misuse detection. */
        uint64_t fingerprint;
        /* Safe iterator temporary storage for bucket chain compaction. */
        uint64_t last_seen_size;
    };
} iter;

/* The opaque hashtableIterator is defined as a blob of bytes. */
static_assert(sizeof(hashtableIterator) >= sizeof(iter),
              "Opaque iterator size");

/* Position, used by some hashtable functions such as two-phase insert and delete. */
typedef struct {
    bucket *bucket;
    uint16_t pos_in_bucket;
    uint16_t table_index;
} position;

static_assert(sizeof(hashtablePosition) >= sizeof(position),
              "Opaque iterator size");

/* State for incremental find. */
typedef struct {
    enum {
        HASHTABLE_CHECK_ENTRY,
        HASHTABLE_NEXT_ENTRY,
        HASHTABLE_NEXT_BUCKET,
        HASHTABLE_FOUND,
        HASHTABLE_NOT_FOUND
    } state;
    short table;
    short pos;
    hashtable *hashtable;
    bucket *bucket;
    const void *key;
    uint64_t hash;
} incrementalFind;

static_assert(sizeof(hashtableIncrementalFindState) >= sizeof(incrementalFind),
              "Opaque incremental find state size");

/* Struct used for stats functions. */
struct hashtableStats {
    int table_index;                /* 0 or 1 (old or new while rehashing). */
    unsigned long toplevel_buckets; /* Number of buckets in table. */
    unsigned long child_buckets;    /* Number of child buckets. */
    unsigned long size;             /* Capacity of toplevel buckets. */
    unsigned long used;             /* Number of entries in the table. */
    unsigned long max_chain_len;    /* Length of longest bucket chain. */
    unsigned long *clvector;        /* Chain length vector; entry i counts
                                     * bucket chains of length i. */
};

/* Struct for sampling entries using scan, used by random key functions. */

typedef struct {
    unsigned size;  /* Size of the entries array. */
    unsigned seen;  /* Number of entries seen. */
    void **entries; /* Array of sampled entries. */
} scan_samples;

/* --- Internal functions --- */

static bucket *findBucketForInsert(hashtable *ht, uint64_t hash, int *pos_in_bucket, int *table_index);

static inline void freeEntry(hashtable *ht, void *entry) {
    if (ht->type->entryDestructor) ht->type->entryDestructor(entry);
}

static inline int compareKeys(hashtable *ht, const void *key1, const void *key2) {
    if (ht->type->keyCompare != NULL) {
        return ht->type->keyCompare(key1, key2);
    } else {
        return key1 != key2;
    }
}

static inline const void *entryGetKey(hashtable *ht, const void *entry) {
    if (ht->type->entryGetKey != NULL) {
        return ht->type->entryGetKey(entry);
    } else {
        return entry;
    }
}

static inline uint64_t hashKey(hashtable *ht, const void *key) {
    if (ht->type->hashFunction != NULL) {
        return ht->type->hashFunction(key);
    } else {
        return hashtableGenHashFunction((const char *)&key, sizeof(key));
    }
}

static inline uint64_t hashEntry(hashtable *ht, const void *entry) {
    return hashKey(ht, entryGetKey(ht, entry));
}


/* For the hash bits stored in the bucket, we use the highest bits of the hash
 * value, since these are not used for selecting the bucket. */
static inline uint8_t highBits(uint64_t hash) {
    return hash >> (CHAR_BIT * 7);
}

static inline int numBucketPositions(bucket *b) {
    return ENTRIES_PER_BUCKET - (b->chained ? 1 : 0);
}

static inline int bucketIsFull(bucket *b) {
    return b->presence == (1 << numBucketPositions(b)) - 1;
}

/* Returns non-zero if the position within the bucket is occupied. */
static inline int isPositionFilled(bucket *b, int position) {
    return b->presence & (1 << position);
}
static void resetTable(hashtable *ht, int table_idx) {
    ht->tables[table_idx] = NULL;
    ht->used[table_idx] = 0;
    ht->bucket_exp[table_idx] = -1;
    ht->child_buckets[table_idx] = 0;
}

/* Number of top-level buckets. */
static inline size_t numBuckets(int exp) {
    return exp == -1 ? 0 : (size_t)1 << exp;
}

/* Bitmask for masking the hash value to get bucket index. */
static inline size_t expToMask(int exp) {
    return exp == -1 ? 0 : numBuckets(exp) - 1;
}

/* Returns the 'exp', where num_buckets = 1 << exp. The number of
 * buckets is a power of two. */
static signed char nextBucketExp(size_t min_capacity) {
    if (min_capacity == 0) return -1;
    /* ceil(x / y) = floor((x - 1) / y) + 1 */
    size_t min_buckets = (min_capacity * BUCKET_FACTOR - 1) / BUCKET_DIVISOR + 1;
    if (min_buckets >= SIZE_MAX / 2) return CHAR_BIT * sizeof(size_t) - 1;
    if (min_buckets == 1) return 0;
    return CHAR_BIT * sizeof(size_t) - __builtin_clzl(min_buckets - 1);
}

/* Swaps the tables and frees the old table. */
static void rehashingCompleted(hashtable *ht) {
    if (ht->type->rehashingCompleted) ht->type->rehashingCompleted(ht);
    if (ht->tables[0]) {
        zfree(ht->tables[0]);
        if (ht->type->trackMemUsage) {
            ht->type->trackMemUsage(ht, -sizeof(bucket) * numBuckets(ht->bucket_exp[0]));
        }
    }
    ht->bucket_exp[0] = ht->bucket_exp[1];
    ht->tables[0] = ht->tables[1];
    ht->used[0] = ht->used[1];
    ht->child_buckets[0] = ht->child_buckets[1];
    resetTable(ht, 1);
    ht->rehash_idx = -1;
}

/* Reverse bits, adapted to use bswap, from
 * https://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static size_t rev(size_t v) {
#if SIZE_MAX == UINT64_MAX
    /* Swap odd and even bits. */
    v = ((v >> 1) & 0x5555555555555555) | ((v & 0x5555555555555555) << 1);
    /* Swap consecutive pairs. */
    v = ((v >> 2) & 0x3333333333333333) | ((v & 0x3333333333333333) << 2);
    /* Swap nibbles. */
    v = ((v >> 4) & 0x0F0F0F0F0F0F0F0F) | ((v & 0x0F0F0F0F0F0F0F0F) << 4);
    /* Reverse bytes. */
    v = __builtin_bswap64(v);
#else
    /* 32-bit version. */
    v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
    v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
    v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
    v = __builtin_bswap32(v);
#endif
    return v;
}

/* Advances a scan cursor to the next value. It increments the reverse bit
 * representation of the masked bits of v. This algorithm was invented by Pieter
 * Noordhuis. */
size_t nextCursor(size_t v, size_t mask) {
    v |= ~mask; /* Set the unmasked (high) bits. */
    v = rev(v); /* Reverse. The unmasked bits are now the low bits. */
    v++;        /* Increment the reversed cursor, flipping the unmasked bits to
                 * 0 and increments the masked bits. */
    v = rev(v); /* Reverse the bits back to normal. */
    return v;
}

/* Returns the next bucket in a bucket chain, or NULL if there's no next. */
static bucket *getChildBucket(bucket *b) {
    return b->chained ? b->entries[ENTRIES_PER_BUCKET - 1] : NULL;
}

/* Attempts to defrag bucket 'b' using the defrag callback function. If the
 * defrag callback function returns a pointer to a new allocation, this pointer
 * is returned and the 'prev' bucket is updated to point to the new allocation.
 * Otherwise, the 'b' pointer is returned. */
static bucket *bucketDefrag(bucket *prev, bucket *b, void *(*defragfn)(void *)) {
    bucket *reallocated = defragfn(b);
    if (reallocated == NULL) return b;
    prev->entries[ENTRIES_PER_BUCKET - 1] = reallocated;
    return reallocated;
}

/* Rehashes one bucket. */
static void rehashBucket(hashtable *ht, bucket *b) {
    int pos;
    for (pos = 0; pos < numBucketPositions(b); pos++) {
        if (!isPositionFilled(b, pos)) continue; /* empty */
        void *entry = b->entries[pos];
        uint8_t h2 = b->hashes[pos];
        /* Insert into table 1. */
        uint64_t hash;
        /* When shrinking, it's possible to avoid computing the hash. We can
         * just use idx has the hash. */
        if (ht->bucket_exp[1] < ht->bucket_exp[0]) {
            hash = ht->rehash_idx;
        } else {
            hash = hashEntry(ht, entry);
        }
        int pos_in_dst_bucket;
        bucket *dst = findBucketForInsert(ht, hash, &pos_in_dst_bucket, NULL);
        dst->entries[pos_in_dst_bucket] = entry;
        dst->hashes[pos_in_dst_bucket] = h2;
        dst->presence |= (1 << pos_in_dst_bucket);
        ht->used[0]--;
        ht->used[1]++;
    }
    /* Mark the source bucket as empty. */
    b->presence = 0;
}

static void rehashStep(hashtable *ht) {
    assert(hashtableIsRehashing(ht));
    size_t idx = ht->rehash_idx;
    bucket *b = &ht->tables[0][idx];
    rehashBucket(ht, b);
    if (b->chained) {
        /* Rehash and free child buckets. */
        bucket *next = getChildBucket(b);
        b->chained = 0;
        b = next;
        while (b != NULL) {
            rehashBucket(ht, b);
            next = getChildBucket(b);
            zfree(b);
            if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, -sizeof(bucket));
            ht->child_buckets[0]--;
            b = next;
        }
    }

    /* Advance to the next bucket. */
    ht->rehash_idx++;
    if ((size_t)ht->rehash_idx >= numBuckets(ht->bucket_exp[0])) {
        rehashingCompleted(ht);
    }
}

/* Called internally on lookup and other reads to the table. */
static inline void rehashStepOnReadIfNeeded(hashtable *ht) {
    if (!hashtableIsRehashing(ht) || ht->pause_rehash) return;
    if (resize_policy != HASHTABLE_RESIZE_ALLOW) return;
    rehashStep(ht);
}

/* When inserting or deleting, we first do a find (read) and rehash one step if
 * resize policy is set to ALLOW, so here we only do it if resize policy is
 * AVOID. The reason for doing it on insert and delete is to ensure that we
 * finish rehashing before we need to resize the table again. */
static inline void rehashStepOnWriteIfNeeded(hashtable *ht) {
    if (!hashtableIsRehashing(ht) || ht->pause_rehash) return;
    if (resize_policy != HASHTABLE_RESIZE_AVOID) return;
    rehashStep(ht);
}

/* Allocates a new table and initiates incremental rehashing if necessary.
 * Returns 1 on resize (success), 0 on no resize (failure). If 0 is returned and
 * 'malloc_failed' is provided, it is set to 1 if allocation failed. If
 * 'malloc_failed' is not provided, an allocation failure triggers a panic. */
static int resize(hashtable *ht, size_t min_capacity, int *malloc_failed) {
    if (malloc_failed) *malloc_failed = 0;

    /* Adjust minimum size. We don't resize to zero currently. */
    if (min_capacity == 0) min_capacity = 1;

    /* Size of new table. */
    signed char exp = nextBucketExp(min_capacity);
    size_t num_buckets = numBuckets(exp);
    size_t new_capacity = num_buckets * ENTRIES_PER_BUCKET;
    if (new_capacity < min_capacity || num_buckets * sizeof(bucket) < num_buckets) {
        /* Overflow */
        return 0;
    }

    signed char old_exp = ht->bucket_exp[hashtableIsRehashing(ht) ? 1 : 0];
    size_t alloc_size = num_buckets * sizeof(bucket);
    if (exp == old_exp) {
        /* Can't resize to same size. */
        return 0;
    }

    if (ht->type->resizeAllowed) {
        double fill_factor = (double)min_capacity / ((double)numBuckets(old_exp) * ENTRIES_PER_BUCKET);
        if (fill_factor * 100 < MAX_FILL_PERCENT_HARD && !ht->type->resizeAllowed(alloc_size, fill_factor)) {
            /* Resize callback says no. */
            return 0;
        }
    }

    /* We can't resize if rehashing is already ongoing. Fast-forward ongoing
     * rehashing before we continue. This can happen only in exceptional
     * scenarios, such as when many insertions are made while rehashing is
     * paused. */
    if (hashtableIsRehashing(ht)) {
        if (hashtableIsRehashingPaused(ht)) return 0;
        while (hashtableIsRehashing(ht)) {
            rehashStep(ht);
        }
    }

    /* Allocate the new hash table. */
    bucket *new_table;
    if (malloc_failed) {
        new_table = ztrycalloc(alloc_size);
        if (new_table == NULL) {
            *malloc_failed = 1;
            return 0;
        }
    } else {
        new_table = zcalloc(alloc_size);
    }
    if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, alloc_size);
    ht->bucket_exp[1] = exp;
    ht->tables[1] = new_table;
    ht->used[1] = 0;
    ht->rehash_idx = 0;
    if (ht->type->rehashingStarted) ht->type->rehashingStarted(ht);

    /* If the old table was empty, the rehashing is completed immediately. */
    if (ht->tables[0] == NULL || ht->used[0] == 0) {
        rehashingCompleted(ht);
    } else if (ht->type->instant_rehashing) {
        while (hashtableIsRehashing(ht)) {
            rehashStep(ht);
        }
    }
    return 1;
}

/* Returns 1 if the table is expanded, 0 if not expanded. If 0 is returned and
 * 'malloc_failed' is provided, it is set to 1 if malloc failed and 0
 * otherwise. */
static int expand(hashtable *ht, size_t size, int *malloc_failed) {
    if (size < hashtableSize(ht)) {
        return 0;
    }
    return resize(ht, size, malloc_failed);
}

/* Finds an entry matching the key. If a match is found, returns a pointer to
 * the bucket containing the matching entry and points 'pos_in_bucket' to the
 * index within the bucket. Returns NULL if no matching entry was found.
 *
 * If 'table_index' is provided, it is set to the index of the table (0 or 1)
 * the returned bucket belongs to. */
static bucket *findBucket(hashtable *ht, uint64_t hash, const void *key, int *pos_in_bucket, int *table_index) {
    if (hashtableSize(ht) == 0) return 0;
    uint8_t h2 = highBits(hash);
    int table;

    /* Do some incremental rehashing. */
    rehashStepOnReadIfNeeded(ht);

    for (table = 0; table <= 1; table++) {
        if (ht->used[table] == 0) continue;
        size_t mask = expToMask(ht->bucket_exp[table]);
        size_t bucket_idx = hash & mask;
        /* Skip already rehashed buckets. */
        if (table == 0 && ht->rehash_idx >= 0 && bucket_idx < (size_t)ht->rehash_idx) {
            continue;
        }
        bucket *b = &ht->tables[table][bucket_idx];
        do {
            /* Find candidate entries with presence flag set and matching h2 hash. */
            for (int pos = 0; pos < numBucketPositions(b); pos++) {
                if (isPositionFilled(b, pos) && b->hashes[pos] == h2) {
                    /* It's a candidate. */
                    void *entry = b->entries[pos];
                    const void *elem_key = entryGetKey(ht, entry);
                    if (compareKeys(ht, key, elem_key) == 0) {
                        /* It's a match. */
                        assert(pos_in_bucket != NULL);
                        *pos_in_bucket = pos;
                        if (table_index) *table_index = table;
                        return b;
                    }
                }
            }
            b = getChildBucket(b);
        } while (b != NULL);
    }
    return NULL;
}

/* Move an entry from one bucket to another. */
static void moveEntry(bucket *bucket_to, int pos_to, bucket *bucket_from, int pos_from) {
    assert(!isPositionFilled(bucket_to, pos_to));
    assert(isPositionFilled(bucket_from, pos_from));
    bucket_to->entries[pos_to] = bucket_from->entries[pos_from];
    bucket_to->hashes[pos_to] = bucket_from->hashes[pos_from];
    bucket_to->presence |= (1 << pos_to);
    bucket_from->presence &= ~(1 << pos_from);
}

/* Converts a full bucket b to a chained bucket and adds a new child bucket. */
static void bucketConvertToChained(hashtable *ht, bucket *b) {
    assert(!b->chained);
    /* We'll move the last entry from the bucket to the new child bucket. */
    int pos = ENTRIES_PER_BUCKET - 1;
    assert(isPositionFilled(b, pos));
    bucket *child = zcalloc(sizeof(bucket));
    if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, sizeof(bucket));
    moveEntry(child, 0, b, pos);
    b->chained = 1;
    b->entries[pos] = child;
}

/* Converts a bucket with a next-bucket pointer to one without one. */
static void bucketConvertToUnchained(bucket *b) {
    assert(b->chained);
    b->chained = 0;
    assert(!isPositionFilled(b, ENTRIES_PER_BUCKET - 1));
}

/* If the last bucket is empty, free it. The before-last bucket is converted
 * back to an "unchained" bucket, becoming the new last bucket in the chain. If
 * there's only one entry left in the last bucket, it's moved to the
 * before-last bucket's last position, to take the place of the next-bucket
 * link.
 *
 * This function needs the penultimate 'before_last' bucket in the chain, to be
 * able to update it when the last bucket is freed. */
static void pruneLastBucket(hashtable *ht, bucket *before_last, bucket *last, int table_index) {
    assert(before_last->chained && getChildBucket(before_last) == last);
    assert(!last->chained);
    assert(last->presence == 0 || __builtin_popcount(last->presence) == 1);
    bucketConvertToUnchained(before_last);
    if (last->presence != 0) {
        /* Move the last remaining entry to the new last position in the
         * before-last bucket. */
        int pos_in_last = __builtin_ctz(last->presence);
        moveEntry(before_last, ENTRIES_PER_BUCKET - 1, last, pos_in_last);
    }
    zfree(last);
    if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, -sizeof(bucket));
    ht->child_buckets[table_index]--;
}

/* After removing an entry in a bucket with children, we can fill the hole
 * with an entry from the end of the bucket chain and potentially free the
 * last bucket in the chain. */
static void fillBucketHole(hashtable *ht, bucket *b, int pos_in_bucket, int table_index) {
    assert(b->chained && !isPositionFilled(b, pos_in_bucket));
    /* Find the last bucket */
    bucket *before_last = b;
    bucket *last = getChildBucket(b);
    while (last->chained) {
        before_last = last;
        last = getChildBucket(last);
    }
    /* Unless the last bucket is empty, find an entry in the last bucket and
     * move it to the hole in b. */
    if (last->presence != 0) {
        int pos_in_last = __builtin_ctz(last->presence);
        assert(pos_in_last < ENTRIES_PER_BUCKET && isPositionFilled(last, pos_in_last));
        moveEntry(b, pos_in_bucket, last, pos_in_last);
    }
    /* Free the last bucket if it becomes empty. */
    if (last->presence == 0 || __builtin_popcount(last->presence) == 1) {
        pruneLastBucket(ht, before_last, last, table_index);
    }
}

/* When entries are deleted while rehashing is paused, they leave empty holes in
 * the buckets. This functions attempts to fill the holes by moving entries from
 * the end of the bucket chain to fill the holes and free any empty buckets in
 * the end of the chain. */
static void compactBucketChain(hashtable *ht, size_t bucket_index, int table_index) {
    bucket *b = &ht->tables[table_index][bucket_index];
    while (b->chained) {
        bucket *next = getChildBucket(b);
        if (next->chained && next->presence == 0) {
            /* Empty bucket in the middle of the chain. Remove it from the chain. */
            bucket *next_next = getChildBucket(next);
            b->entries[ENTRIES_PER_BUCKET - 1] = next_next;
            zfree(next);
            if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, -sizeof(bucket));
            ht->child_buckets[table_index]--;
            continue;
        }

        if (!next->chained && (next->presence == 0 || __builtin_popcount(next->presence) == 1)) {
            /* Next is the last bucket and it's empty or has only one entry.
             * Delete it and turn b into an "unchained" bucket. */
            pruneLastBucket(ht, b, next, table_index);
            return;
        }

        if (__builtin_popcount(b->presence) < ENTRIES_PER_BUCKET - 1) {
            /* Fill the holes in the bucket. */
            for (int pos = 0; pos < ENTRIES_PER_BUCKET - 1; pos++) {
                if (!isPositionFilled(b, pos)) {
                    fillBucketHole(ht, b, pos, table_index);
                    if (!b->chained) return;
                }
            }
        }

        /* Bucket is full. Move forward to next bucket. */
        b = next;
    }
}

/* Find an empty position in the table for inserting an entry with the given hash. */
static bucket *findBucketForInsert(hashtable *ht, uint64_t hash, int *pos_in_bucket, int *table_index) {
    int table = hashtableIsRehashing(ht) ? 1 : 0;
    assert(ht->tables[table]);
    size_t mask = expToMask(ht->bucket_exp[table]);
    size_t bucket_idx = hash & mask;
    bucket *b = &ht->tables[table][bucket_idx];
    /* Find bucket that's not full, or create one. */
    while (bucketIsFull(b)) {
        if (!b->chained) {
            bucketConvertToChained(ht, b);
            ht->child_buckets[table]++;
        }
        b = getChildBucket(b);
    }
    /* Find a free slot in the bucket. There must be at least one. */
    int pos;
    for (pos = 0; pos < ENTRIES_PER_BUCKET; pos++) {
        if (!isPositionFilled(b, pos)) break;
    }
    assert(pos < ENTRIES_PER_BUCKET);
    assert(pos_in_bucket != NULL);
    *pos_in_bucket = pos;
    if (table_index) *table_index = table;
    return b;
}

/* Helper to insert an entry. Doesn't check if an entry with a matching key
 * already exists. This must be ensured by the caller. */
static void insert(hashtable *ht, uint64_t hash, void *entry) {
    hashtableExpandIfNeeded(ht);
    rehashStepOnWriteIfNeeded(ht);
    int pos_in_bucket;
    int table_index;
    bucket *b = findBucketForInsert(ht, hash, &pos_in_bucket, &table_index);
    b->entries[pos_in_bucket] = entry;
    b->presence |= (1 << pos_in_bucket);
    b->hashes[pos_in_bucket] = highBits(hash);
    ht->used[table_index]++;
}

/* A 64-bit fingerprint of some of the state of the hash table. */
static uint64_t hashtableFingerprint(hashtable *ht) {
    uint64_t integers[6], hash = 0;
    integers[0] = (uintptr_t)ht->tables[0];
    integers[1] = ht->bucket_exp[0];
    integers[2] = ht->used[0];
    integers[3] = (uintptr_t)ht->tables[1];
    integers[4] = ht->bucket_exp[1];
    integers[5] = ht->used[1];

    /* Result = hash(hash(hash(int1)+int2)+int3) */
    for (int j = 0; j < 6; j++) {
        hash += integers[j];
        /* Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); /* hash = (hash << 21) - hash - 1; */
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); /* hash * 265 */
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); /* hash * 21 */
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

/* Scan callback function used by hashtableGetSomeEntries() for sampling entries
 * using scan. */
static void sampleEntriesScanFn(void *privdata, void *entry) {
    scan_samples *samples = privdata;
    if (samples->seen < samples->size) {
        samples->entries[samples->seen++] = entry;
    } else {
        /* More entries than we wanted. This can happen if there are long
         * bucket chains. Replace random entries using reservoir sampling. */
        samples->seen++;
        unsigned idx = random() % samples->seen;
        if (idx < samples->size) samples->entries[idx] = entry;
    }
}

/* Conversion from internal iterator struct to user-facing opaque type. */
static inline hashtableIterator *iteratorToOpaque(iter *iterator) {
    return (hashtableIterator *)(void *)iterator;
}

/* Conversion from user-facing opaque iterator type to internal struct. */
static inline iter *iteratorFromOpaque(hashtableIterator *iterator) {
    return (iter *)(void *)iterator;
}

/* Conversion from user-facing opaque type to internal struct. */
static inline position *positionFromOpaque(hashtablePosition *p) {
    return (position *)(void *)p;
}

/* Conversion from user-facing opaque type to internal struct. */
static inline incrementalFind *incrementalFindFromOpaque(hashtableIncrementalFindState *state) {
    return (incrementalFind *)(void *)state;
}

/* Prefetches all filled entries in the given bucket to optimize future memory access. */
static void prefetchBucketEntries(bucket *b) {
    if (!b->presence) return;
    for (int pos = 0; pos < numBucketPositions(b); pos++) {
        if (isPositionFilled(b, pos)) {
            valkey_prefetch(b->entries[pos]);
        }
    }
}

/* Returns the child bucket if chained, otherwise the next bucket in the table. returns NULL if neither exists. */
static bucket *getNextBucket(bucket *current_bucket, size_t bucket_index, hashtable *ht, int table_index) {
    bucket *next_bucket = NULL;
    if (current_bucket->chained) {
        next_bucket = getChildBucket(current_bucket);
    } else {
        size_t table_size = numBuckets(ht->bucket_exp[table_index]);
        size_t next_index = bucket_index + 1;
        if (next_index < table_size) {
            next_bucket = &ht->tables[table_index][next_index];
        }
    }
    return next_bucket;
}

/* This function prefetches data that will be needed in subsequent iterations:
 * - The entries of the next bucket
 * - The next of the next bucket
 * It attempts to bring this data closer to the L1 cache to reduce future memory access latency.
 *
 * Cache state before this function is called(due to last call for this function):
 * 1. The current bucket and its entries are likely already in cache.
 * 2. The next bucket is in cache.
 */
static void prefetchNextBucketEntries(iter *iter, bucket *current_bucket) {
    size_t next_index = iter->index + 1;
    bucket *next_bucket = getNextBucket(current_bucket, next_index, iter->hashtable, iter->table);
    if (next_bucket) {
        prefetchBucketEntries(next_bucket);
        bucket *next_next_bucket = getNextBucket(next_bucket, next_index + 1, iter->hashtable, iter->table);
        if (next_next_bucket) {
            valkey_prefetch(next_next_bucket);
        }
    }
}

/* Prefetches the values associated with the entries in the given bucket by
 * calling the entryPrefetchValue callback in the hashtableType */
static void prefetchBucketValues(bucket *b, hashtable *ht) {
    if (!b->presence) return;
    assert(ht->type->entryPrefetchValue != NULL);
    for (int pos = 0; pos < numBucketPositions(b); pos++) {
        if (isPositionFilled(b, pos)) {
            ht->type->entryPrefetchValue(b->entries[pos]);
        }
    }
}

static inline int isSafe(iter *iter) {
    return (iter->flags & HASHTABLE_ITER_SAFE);
}

static inline int shouldPrefetchValues(iter *iter) {
    return (iter->flags & HASHTABLE_ITER_PREFETCH_VALUES);
}

/* --- API functions --- */

/* Allocates and initializes a new hashtable specified by the given type. */
hashtable *hashtableCreate(hashtableType *type) {
    size_t metasize = type->getMetadataSize ? type->getMetadataSize() : 0;
    size_t alloc_size = sizeof(hashtable) + metasize;
    hashtable *ht = zmalloc(alloc_size);
    if (metasize > 0) {
        memset(&ht->metadata, 0, metasize);
    }
    ht->type = type;
    ht->rehash_idx = -1;
    ht->pause_rehash = 0;
    ht->pause_auto_shrink = 0;
    resetTable(ht, 0);
    resetTable(ht, 1);
    if (type->trackMemUsage) type->trackMemUsage(ht, alloc_size);
    return ht;
}

/* Deletes all the entries. If a callback is provided, it is called from time
 * to time to indicate progress. */
void hashtableEmpty(hashtable *ht, void(callback)(hashtable *)) {
    if (hashtableIsRehashing(ht)) {
        /* Pretend rehashing completed. */
        if (ht->type->rehashingCompleted) ht->type->rehashingCompleted(ht);
        ht->rehash_idx = -1;
    }
    for (int table_index = 0; table_index <= 1; table_index++) {
        if (ht->bucket_exp[table_index] < 0) {
            continue;
        }
        if (ht->used[table_index] > 0) {
            for (size_t idx = 0; idx < numBuckets(ht->bucket_exp[table_index]); idx++) {
                if (callback && (idx & 65535) == 0) callback(ht);
                bucket *b = &ht->tables[table_index][idx];
                do {
                    /* Call the destructor with each entry. */
                    if (ht->type->entryDestructor != NULL && b->presence != 0) {
                        for (int pos = 0; pos < ENTRIES_PER_BUCKET; pos++) {
                            if (isPositionFilled(b, pos)) {
                                ht->type->entryDestructor(b->entries[pos]);
                            }
                        }
                    }
                    bucket *next = getChildBucket(b);

                    /* Free allocated bucket. */
                    if (b != &ht->tables[table_index][idx]) {
                        zfree(b);
                        if (ht->type->trackMemUsage) {
                            ht->type->trackMemUsage(ht, -sizeof(bucket));
                        }
                    }
                    b = next;
                } while (b != NULL);
            }
        }
        zfree(ht->tables[table_index]);
        if (ht->type->trackMemUsage) {
            ht->type->trackMemUsage(ht, -sizeof(bucket) * numBuckets(ht->bucket_exp[table_index]));
        }
        resetTable(ht, table_index);
    }
}

/* Deletes all the entries and frees the table. */
void hashtableRelease(hashtable *ht) {
    hashtableEmpty(ht, NULL);
    /* Call trackMemUsage before zfree, so trackMemUsage can access ht. */
    if (ht->type->trackMemUsage) {
        size_t alloc_size = sizeof(hashtable);
        if (ht->type->getMetadataSize) alloc_size += ht->type->getMetadataSize();
        ht->type->trackMemUsage(ht, -alloc_size);
    }
    zfree(ht);
}

/* Returns the type of the hashtable. */
hashtableType *hashtableGetType(hashtable *ht) {
    return ht->type;
}

/* Returns a pointer to the table's metadata (userdata) section. */
void *hashtableMetadata(hashtable *ht) {
    return &ht->metadata;
}

/* Returns the number of entries stored. */
size_t hashtableSize(const hashtable *ht) {
    return ht->used[0] + ht->used[1];
}

/* Returns the number of buckets in the hash table itself. */
size_t hashtableBuckets(hashtable *ht) {
    return numBuckets(ht->bucket_exp[0]) + numBuckets(ht->bucket_exp[1]);
}

/* Returns the number of buckets that have a child bucket. Equivalently, the
 * number of allocated buckets, outside of the hash table itself. */
size_t hashtableChainedBuckets(hashtable *ht, int table) {
    return ht->child_buckets[table];
}

/* Returns the number of entry positions per bucket. */
unsigned hashtableEntriesPerBucket(void) {
    return ENTRIES_PER_BUCKET;
}

/* Returns the size of the hashtable structures, in bytes (not including the sizes
 * of the entries, if the entries are pointers to allocated objects). */
size_t hashtableMemUsage(hashtable *ht) {
    size_t num_buckets = numBuckets(ht->bucket_exp[0]) + numBuckets(ht->bucket_exp[1]);
    num_buckets += ht->child_buckets[0] + ht->child_buckets[1];
    size_t metasize = ht->type->getMetadataSize ? ht->type->getMetadataSize() : 0;
    return sizeof(hashtable) + metasize + sizeof(bucket) * num_buckets;
}

/* Pauses automatic shrinking. This can be called before deleting a lot of
 * entries, to prevent automatic shrinking from being triggered multiple times.
 * Call hashtableResumeAutoShrink afterwards to restore automatic shrinking. */
void hashtablePauseAutoShrink(hashtable *ht) {
    ht->pause_auto_shrink++;
}

/* Re-enables automatic shrinking, after it has been paused. If you have deleted
 * many entries while automatic shrinking was paused, you may want to call
 * hashtableShrinkIfNeeded. */
void hashtableResumeAutoShrink(hashtable *ht) {
    ht->pause_auto_shrink--;
    if (ht->pause_auto_shrink == 0) {
        hashtableShrinkIfNeeded(ht);
    }
}

/* Pauses incremental rehashing. When rehashing is paused, bucket chains are not
 * automatically compacted when entries are deleted. Doing so may leave empty
 * spaces, "holes", in the bucket chains, which wastes memory. */
static void hashtablePauseRehashing(hashtable *ht) {
    ht->pause_rehash++;
}

/* Resumes incremental rehashing, after pausing it. */
static void hashtableResumeRehashing(hashtable *ht) {
    ht->pause_rehash--;
}

/* Returns 1 if incremental rehashing is paused, 0 if it isn't. */
int hashtableIsRehashingPaused(hashtable *ht) {
    return ht->pause_rehash > 0;
}

/* Returns 1 if incremental rehashing is in progress, 0 otherwise. */
int hashtableIsRehashing(hashtable *ht) {
    return ht->rehash_idx != -1;
}

/* Provides the number of buckets in the old and new tables during rehashing. To
 * get the sizes in bytes, multiply by HASHTABLE_BUCKET_SIZE. This function can
 * only be used when rehashing is in progress, and from the rehashingStarted and
 * rehashingCompleted callbacks. */
void hashtableRehashingInfo(hashtable *ht, size_t *from_size, size_t *to_size) {
    assert(hashtableIsRehashing(ht));
    *from_size = numBuckets(ht->bucket_exp[0]);
    *to_size = numBuckets(ht->bucket_exp[1]);
}

/* Performs incremental rehashing for the specified number of microseconds.
 * Returns the number of rehashed buckets chains. */
int hashtableRehashMicroseconds(hashtable *ht, uint64_t us) {
    if (ht->pause_rehash > 0) return 0;
    if (resize_policy != HASHTABLE_RESIZE_ALLOW) return 0;

    monotime timer;
    elapsedStart(&timer);
    int rehashes = 0;

    while (hashtableIsRehashing(ht)) {
        rehashStep(ht);
        rehashes++;
        if (rehashes % 128 == 0 && elapsedUs(timer) >= us) break;
    }
    return rehashes;
}

/* Return 1 if expand was performed; 0 otherwise. */
int hashtableExpand(hashtable *ht, size_t size) {
    return expand(ht, size, NULL);
}

/* Returns 1 if expand was performed or if expand is not needed. Returns 0 if
 * expand failed due to memory allocation failure. */
int hashtableTryExpand(hashtable *ht, size_t size) {
    int malloc_failed = 0;
    return expand(ht, size, &malloc_failed) || !malloc_failed;
}

/* Expanding is done automatically on insertion, but less eagerly if resize
 * policy is set to AVOID or FORBID. After restoring resize policy to ALLOW, you
 * may want to call hashtableExpandIfNeeded. Returns 1 if expanding, 0 if not
 * expanding. */
int hashtableExpandIfNeeded(hashtable *ht) {
    size_t min_capacity = ht->used[0] + ht->used[1] + 1;
    size_t num_buckets = numBuckets(ht->bucket_exp[hashtableIsRehashing(ht) ? 1 : 0]);
    size_t current_capacity = num_buckets * ENTRIES_PER_BUCKET;
    unsigned max_fill_percent = resize_policy == HASHTABLE_RESIZE_AVOID ? MAX_FILL_PERCENT_HARD : MAX_FILL_PERCENT_SOFT;
    if (min_capacity * 100 <= current_capacity * max_fill_percent) {
        return 0;
    }
    return resize(ht, min_capacity, NULL);
}

/* Shrinking is done automatically on deletion, but less eagerly if resize
 * policy is set to AVOID and not at all if set to FORBID. After restoring
 * resize policy to ALLOW, you may want to call hashtableShrinkIfNeeded. */
int hashtableShrinkIfNeeded(hashtable *ht) {
    /* Don't shrink if rehashing is already in progress. */
    if (hashtableIsRehashing(ht) || resize_policy == HASHTABLE_RESIZE_FORBID) {
        return 0;
    }
    size_t current_capacity = numBuckets(ht->bucket_exp[0]) * ENTRIES_PER_BUCKET;
    unsigned min_fill_percent = resize_policy == HASHTABLE_RESIZE_AVOID ? MIN_FILL_PERCENT_HARD : MIN_FILL_PERCENT_SOFT;
    if (ht->used[0] * 100 > current_capacity * min_fill_percent) {
        return 0;
    }
    return resize(ht, ht->used[0], NULL);
}

/* Defragment the main allocations of the hashtable by reallocating them. The
 * provided defragfn callback should either return NULL (if reallocation is not
 * necessary) or reallocate the memory like realloc() would do.
 *
 * Note that this doesn't cover allocated chained buckets. To defragment them,
 * you need to do a scan using hashtableScanDefrag with the same 'defragfn'.
 *
 * Returns NULL if the hashtable's top-level struct hasn't been reallocated.
 * Returns non-NULL if the top-level allocation has been allocated and thus
 * making the 'ht' pointer invalid. */
hashtable *hashtableDefragTables(hashtable *ht, void *(*defragfn)(void *)) {
    /* The hashtable struct */
    hashtable *ht1 = defragfn(ht);
    if (ht1 != NULL) ht = ht1;
    /* The tables */
    for (int i = 0; i <= 1; i++) {
        if (ht->tables[i] == NULL) continue;
        void *table = defragfn(ht->tables[i]);
        if (table != NULL) ht->tables[i] = table;
    }
    return ht1;
}

/* Used for releasing memory to OS to avoid unnecessary CoW. Called when we've
 * forked and memory won't be used again. See zmadvise_dontneed() */
void dismissHashtable(hashtable *ht) {
    for (int i = 0; i < 2; i++) {
        zmadvise_dontneed(ht->tables[i], numBuckets(ht->bucket_exp[i]) * sizeof(bucket *));
    }
}

/* Returns 1 if an entry was found matching the key. Also points *found to it,
 * if found is provided. Returns 0 if no matching entry was found. */
int hashtableFind(hashtable *ht, const void *key, void **found) {
    if (hashtableSize(ht) == 0) return 0;
    uint64_t hash = hashKey(ht, key);
    int pos_in_bucket = 0;
    bucket *b = findBucket(ht, hash, key, &pos_in_bucket, NULL);
    if (b) {
        if (found) *found = b->entries[pos_in_bucket];
        return 1;
    } else {
        return 0;
    }
}

/* Returns a pointer to where an entry is stored within the hash table, or
 * NULL if not found. To get the entry, dereference the returned pointer. The
 * pointer can be used to replace the entry with an equivalent entry (same
 * key, same hash value), but note that the pointer may be invalidated by future
 * accesses to the hash table due to incermental rehashing, so use with care. */
void **hashtableFindRef(hashtable *ht, const void *key) {
    if (hashtableSize(ht) == 0) return NULL;
    uint64_t hash = hashKey(ht, key);
    int pos_in_bucket = 0;
    bucket *b = findBucket(ht, hash, key, &pos_in_bucket, NULL);
    return b ? &b->entries[pos_in_bucket] : NULL;
}

/* Adds an entry. Returns 1 on success. Returns 0 if there was already an entry
 * with the same key. */
int hashtableAdd(hashtable *ht, void *entry) {
    return hashtableAddOrFind(ht, entry, NULL);
}

/* Adds an entry and returns 1 on success. Returns 0 if there was already an
 * entry with the same key and, if an 'existing' pointer is provided, it is
 * pointed to the existing entry. */
int hashtableAddOrFind(hashtable *ht, void *entry, void **existing) {
    const void *key = entryGetKey(ht, entry);
    uint64_t hash = hashKey(ht, key);
    int pos_in_bucket = 0;
    bucket *b = findBucket(ht, hash, key, &pos_in_bucket, NULL);
    if (b != NULL) {
        if (existing) *existing = b->entries[pos_in_bucket];
        return 0;
    } else {
        insert(ht, hash, entry);
        return 1;
    }
}

/* Finds a position within the hashtable where an entry with the
 * given key should be inserted using hashtableInsertAtPosition. This is the first
 * phase in a two-phase insert operation and it can be used if you want to avoid
 * creating an entry before you know if it already exists in the table or not,
 * and without a separate lookup to the table.
 *
 * The function returns 1 if a position was found where an entry with the
 * given key can be inserted. The position is stored in provided 'position'
 * argument, which can be stack-allocated. This position should then be used in
 * a call to hashtableInsertAtPosition.
 *
 * If the function returns 0, it means that an an entry with the given key
 * already exists in the table. If an 'existing' pointer is provided, it is
 * pointed to the existing entry with the matching key.
 *
 * Example:
 *
 *     hashtablePosition position;
 *     void *existing;
 *     if (hashtableFindPositionForInsert(ht, key, &position, &existing)) {
 *         // Position found where we can insert an entry with this key.
 *         void *entry = createNewEntryWithKeyAndValue(key, some_value);
 *         hashtableInsertAtPosition(ht, entry, &position);
 *     } else {
 *         // Existing entry found with the matching key.
 *         doSomethingWithExistingEntry(existing);
 *     }
 */
int hashtableFindPositionForInsert(hashtable *ht, void *key, hashtablePosition *pos, void **existing) {
    position *p = positionFromOpaque(pos);
    uint64_t hash = hashKey(ht, key);
    int pos_in_bucket, table_index;
    bucket *b = findBucket(ht, hash, key, &pos_in_bucket, NULL);
    if (b != NULL) {
        if (existing) *existing = b->entries[pos_in_bucket];
        return 0;
    } else {
        hashtableExpandIfNeeded(ht);
        rehashStepOnWriteIfNeeded(ht);
        b = findBucketForInsert(ht, hash, &pos_in_bucket, &table_index);
        assert(!isPositionFilled(b, pos_in_bucket));

        /* Store the hash bits now, so we don't need to compute the hash again
         * when hashtableInsertAtPosition() is called. */
        b->hashes[pos_in_bucket] = highBits(hash);

        /* Populate position struct. */
        assert(p != NULL);
        p->bucket = b;
        p->pos_in_bucket = pos_in_bucket;
        p->table_index = table_index;
        return 1;
    }
}

/* Inserts an entry at the position previously acquired using
 * hashtableFindPositionForInsert(). The entry must match the key provided when
 * finding the position. You must not access the hashtable in any way between
 * hashtableFindPositionForInsert() and hashtableInsertAtPosition(), since even a
 * hashtableFind() may cause incremental rehashing to move entries in memory. */
void hashtableInsertAtPosition(hashtable *ht, void *entry, hashtablePosition *pos) {
    position *p = positionFromOpaque(pos);
    bucket *b = p->bucket;
    int pos_in_bucket = p->pos_in_bucket;
    int table_index = p->table_index;
    assert(!isPositionFilled(b, pos_in_bucket));
    b->presence |= (1 << pos_in_bucket);
    b->entries[pos_in_bucket] = entry;
    ht->used[table_index]++;
    /* Hash bits are already set by hashtableFindPositionForInsert. */
}

/* Removes the entry with the matching key and returns it. The entry
 * destructor is not called. Returns 1 and points 'popped' to the entry if a
 * matching entry was found. Returns 0 if no matching entry was found. */
int hashtablePop(hashtable *ht, const void *key, void **popped) {
    if (hashtableSize(ht) == 0) return 0;
    uint64_t hash = hashKey(ht, key);
    int pos_in_bucket = 0;
    int table_index = 0;
    bucket *b = findBucket(ht, hash, key, &pos_in_bucket, &table_index);
    if (b) {
        if (popped) *popped = b->entries[pos_in_bucket];
        b->presence &= ~(1 << pos_in_bucket);
        ht->used[table_index]--;
        if (b->chained && !hashtableIsRehashingPaused(ht)) {
            /* Rehashing is paused while iterating and when a scan callback is
             * running. In those cases, we do the compaction in the scan and
             * iterator code instead. */
            fillBucketHole(ht, b, pos_in_bucket, table_index);
        }
        hashtableShrinkIfNeeded(ht);
        return 1;
    } else {
        return 0;
    }
}

/* Deletes the entry with the matching key. Returns 1 if an entry was
 * deleted, 0 if no matching entry was found. */
int hashtableDelete(hashtable *ht, const void *key) {
    void *entry;
    if (hashtablePop(ht, key, &entry)) {
        freeEntry(ht, entry);
        return 1;
    } else {
        return 0;
    }
}

/* When an entry has been reallocated, it can be replaced in a hash table
 * without dereferencing the old pointer which may no longer be valid. The new
 * entry with the same key and hash is used for finding the old entry and
 * replacing it with the new entry. Returns 1 if the entry was replaced and 0 if
 * the entry wasn't found. */
int hashtableReplaceReallocatedEntry(hashtable *ht, const void *old_entry, void *new_entry) {
    const void *key = entryGetKey(ht, new_entry);
    uint64_t hash = hashKey(ht, key);
    uint8_t h2 = highBits(hash);
    for (int table = 0; table <= 1; table++) {
        if (ht->used[table] == 0) continue;
        size_t mask = expToMask(ht->bucket_exp[table]);
        size_t bucket_idx = hash & mask;
        /* Skip already rehashed buckets. */
        if (table == 0 && ht->rehash_idx >= 0 && bucket_idx < (size_t)ht->rehash_idx) {
            continue;
        }
        bucket *b = &ht->tables[table][bucket_idx];
        do {
            for (int pos = 0; pos < numBucketPositions(b); pos++) {
                if (isPositionFilled(b, pos) && b->hashes[pos] == h2 && b->entries[pos] == old_entry) {
                    /* It's a match. */
                    b->entries[pos] = new_entry;
                    return 1;
                }
            }
            b = getChildBucket(b);
        } while (b != NULL);
    }
    return 0;
}

/* Two-phase pop: Look up an entry, do something with it, then delete it
 * without searching the hash table again.
 *
 * hashtableTwoPhasePopFindRef finds an entry in the table and also the position
 * of the entry within the table, so that it can be deleted without looking it
 * up in the table again. The function returns a pointer to the entry pointer
 * within the hash table, if an entry with a matching key is found, and NULL
 * otherwise.
 *
 * If non-NULL is returned, call 'hashtableTwoPhasePopDelete' with the returned
 * 'position' afterwards to actually delete the entry from the table. These two
 * functions are designed be used in pair. `hashtableTwoPhasePopFindRef` pauses
 * rehashing and `hashtableTwoPhasePopDelete` resumes rehashing.
 *
 * While hashtablePop finds and returns an entry, the purpose of two-phase pop
 * is to provide an optimized equivalent of hashtableFindRef followed by
 * hashtableDelete, where the first call finds the entry but doesn't delete it
 * from the hash table and the latter doesn't need to look up the entry in the
 * hash table again.
 *
 * Example:
 *
 *     hashtablePosition position;
 *     void **ref = hashtableTwoPhasePopFindRef(ht, key, &position)
 *     if (ref != NULL) {
 *         void *entry = *ref;
 *         // do something with the entry, then...
 *         hashtableTwoPhasePopDelete(ht, &position);
 *     }
 */

/* Like hashtableTwoPhasePopFind, but returns a pointer to where the entry is
 * stored in the table, or NULL if no matching entry is found. The 'position'
 * argument is populated with a representation of where the entry is stored.
 * This must be provided to hashtableTwoPhasePopDelete to complete the
 * operation. */
void **hashtableTwoPhasePopFindRef(hashtable *ht, const void *key, hashtablePosition *pos) {
    position *p = positionFromOpaque(pos);
    if (hashtableSize(ht) == 0) return NULL;
    uint64_t hash = hashKey(ht, key);
    int pos_in_bucket = 0;
    int table_index = 0;
    bucket *b = findBucket(ht, hash, key, &pos_in_bucket, &table_index);
    if (b) {
        hashtablePauseRehashing(ht);

        /* Store position. */
        assert(p != NULL);
        p->bucket = b;
        p->pos_in_bucket = pos_in_bucket;
        p->table_index = table_index;
        return &b->entries[pos_in_bucket];
    } else {
        return NULL;
    }
}

/* Clears the position of the entry in the hashtable and resumes rehashing. The
 * entry destructor is NOT called. The position is acquired using a preceding
 * call to hashtableTwoPhasePopFindRef(). */
void hashtableTwoPhasePopDelete(hashtable *ht, hashtablePosition *pos) {
    /* Read position. */
    position *p = positionFromOpaque(pos);
    bucket *b = p->bucket;
    int pos_in_bucket = p->pos_in_bucket;
    int table_index = p->table_index;

    /* Delete the entry and resume rehashing. */
    assert(isPositionFilled(b, pos_in_bucket));
    b->presence &= ~(1 << pos_in_bucket);
    ht->used[table_index]--;
    hashtableResumeRehashing(ht);
    if (b->chained && !hashtableIsRehashingPaused(ht)) {
        /* Rehashing paused also means bucket chain compaction paused. It is
         * paused while iterating and when a scan callback is running, to be
         * able to live up to the scan and iterator guarantees. In those cases,
         * we do the compaction in the scan and iterator code instead. */
        fillBucketHole(ht, b, pos_in_bucket, table_index);
    }
    hashtableShrinkIfNeeded(ht);
}

/* Initializes the state for an incremental find operation.
 *
 * Incremental find can be used to speed up the loading of multiple objects by
 * utilizing CPU branch predictions to parallelize memory accesses. Initialize
 * the data for a number of incremental find operations. Then call
 * hashtableIncrementalFindStep on them in a round-robin order until all of them
 * are complete. Finally, if necessary, call hashtableIncrementalFindGetResult.
 */
void hashtableIncrementalFindInit(hashtableIncrementalFindState *state, hashtable *ht, const void *key) {
    incrementalFind *data = incrementalFindFromOpaque(state);
    if (hashtableSize(ht) == 0) {
        data->state = HASHTABLE_NOT_FOUND;
    } else {
        data->state = HASHTABLE_NEXT_BUCKET;
        data->bucket = NULL;
        data->hashtable = ht;
        data->key = key;
        data->hash = hashKey(ht, key);
    }
}

/* Returns 1 if more work is needed, 0 when done. Call this function repeatedly
 * until it returns 0. Then use hashtableIncrementalFindGetResult to fetch the
 * result. */
int hashtableIncrementalFindStep(hashtableIncrementalFindState *state) {
    incrementalFind *data = incrementalFindFromOpaque(state);
    switch (data->state) {
    case HASHTABLE_CHECK_ENTRY:
        /* Current entry is prefetched. Now check if it's a match. */
        {
            hashtable *ht = data->hashtable;
            void *entry = data->bucket->entries[data->pos];
            const void *elem_key = entryGetKey(ht, entry);
            if (compareKeys(ht, data->key, elem_key) == 0) {
                /* It's a match. */
                data->state = HASHTABLE_FOUND;
                return 0;
            }
            /* No match. Look for next candidate entry in the bucket. */
            data->pos++;
        }
        /* fall through */
    case HASHTABLE_NEXT_ENTRY:
        /* Current bucket is prefetched. Prefetch next potential
         * matching entry in the current bucket. */
        if (data->bucket->presence != 0 && data->pos < numBucketPositions(data->bucket)) {
            bucket *b = data->bucket;
            uint8_t h2 = highBits(data->hash);
            for (int pos = data->pos; pos < numBucketPositions(b); pos++) {
                if (isPositionFilled(b, pos) && b->hashes[pos] == h2) {
                    /* It's a candidate. */
                    valkey_prefetch(b->entries[pos]);
                    data->pos = pos;
                    data->state = HASHTABLE_CHECK_ENTRY;
                    return 1;
                }
            }
        }
        /* fall through */
    case HASHTABLE_NEXT_BUCKET:
        /* Current bucket is prefetched, if any. Find the next bucket in the
         * chain, or in next table, and prefetch it. */
        {
            hashtable *ht = data->hashtable;
            if (data->bucket == NULL) {
                data->table = 0;
                size_t mask = expToMask(ht->bucket_exp[0]);
                size_t bucket_idx = data->hash & mask;
                if (ht->rehash_idx >= 0 && bucket_idx < (size_t)ht->rehash_idx) {
                    /* Skip already rehashed bucket in table 0. */
                    data->table = 1;
                    mask = expToMask(ht->bucket_exp[1]);
                    bucket_idx = data->hash & mask;
                }
                data->bucket = &ht->tables[data->table][bucket_idx];
            } else if (getChildBucket(data->bucket) != NULL) {
                data->bucket = getChildBucket(data->bucket);
            } else if (data->table == 0 && ht->rehash_idx >= 0) {
                data->table = 1;
                size_t mask = expToMask(ht->bucket_exp[1]);
                size_t bucket_idx = data->hash & mask;
                data->bucket = &ht->tables[data->table][bucket_idx];
            } else {
                /* No more tables. */
                data->state = HASHTABLE_NOT_FOUND;
                return 0;
            }
            valkey_prefetch(data->bucket);
            data->state = HASHTABLE_NEXT_ENTRY;
            data->pos = 0;
        }
        return 1;
    case HASHTABLE_FOUND:
        return 0;
    case HASHTABLE_NOT_FOUND:
        return 0;
    }
    assert(0);
}

/* Call only when hashtableIncrementalFindStep has returned 0.
 *
 * Returns 1 and points 'found' to the entry if an entry was found, 0 if it
 * was not found. */
int hashtableIncrementalFindGetResult(hashtableIncrementalFindState *state, void **found) {
    incrementalFind *data = incrementalFindFromOpaque(state);
    if (data->state == HASHTABLE_FOUND) {
        if (found) *found = data->bucket->entries[data->pos];
        return 1;
    } else {
        assert(data->state == HASHTABLE_NOT_FOUND);
        return 0;
    }
}

/* --- Scan --- */

/* Scan is a stateless iterator. It works with a cursor that is returned to the
 * caller and which should be provided to the next call to continue scanning.
 * The hash table can be modified in any way between two scan calls. The scan
 * still continues iterating where it was.
 *
 * A full scan is performed like this: Start with a cursor of 0. The scan
 * callback is invoked for each entry scanned and a new cursor is returned. Next
 * time, call this function with the new cursor. Continue until the function
 * returns 0.
 *
 * We say that an entry is *emitted* when it's passed to the scan callback.
 *
 * Scan guarantees:
 *
 * - An entry that is present in the hash table during an entire full scan will
 *   be returned (emitted) at least once. (Most of the time exactly once, but
 *   sometimes twice.)
 *
 * - An entry that is inserted or deleted during a full scan may or may not be
 *   returned during the scan.
 *
 * Scan callback rules:
 *
 * - The scan callback may delete the entry that was passed to it.
 *
 * - It may not delete other entries, because that may lead to internal
 *   fragmentation in the form of "holes" in the bucket chains.
 *
 * - The scan callback may insert or replace any entry.
 */
size_t hashtableScan(hashtable *ht, size_t cursor, hashtableScanFunction fn, void *privdata) {
    return hashtableScanDefrag(ht, cursor, fn, privdata, NULL, 0);
}

/* Like hashtableScan, but additionally reallocates the memory used by the dict
 * entries using the provided allocation function. This feature was added for
 * the active defrag feature.
 *
 * The 'defragfn' callback is called with a pointer to memory that callback can
 * reallocate. The callbacks should return a new memory address or NULL, where
 * NULL means that no reallocation happened and the old memory is still valid.
 * The 'defragfn' can be NULL if you don't need defrag reallocation.
 *
 * The 'flags' argument can be used to tweak the behaviour. It's a bitwise-or
 * (zero means no flags) of the following:
 *
 * - HASHTABLE_SCAN_EMIT_REF: Emit a pointer to the entry's location in the
 *   table to the scan function instead of the actual entry. This can be used
 *   for advanced things like reallocating the memory of an entry (for the
 *   purpose of defragmentation) and updating the pointer to the entry inside
 *   the hash table.
 */
size_t hashtableScanDefrag(hashtable *ht, size_t cursor, hashtableScanFunction fn, void *privdata, void *(*defragfn)(void *), int flags) {
    if (hashtableSize(ht) == 0) return 0;

    /* Prevent entries from being moved around during the scan call, as a
     * side-effect of the scan callback. */
    hashtablePauseRehashing(ht);

    /* Flags. */
    int emit_ref = (flags & HASHTABLE_SCAN_EMIT_REF);

    if (!hashtableIsRehashing(ht)) {
        /* Emit entries at the cursor index. */
        size_t mask = expToMask(ht->bucket_exp[0]);
        bucket *b = &ht->tables[0][cursor & mask];
        do {
            if (b->presence != 0) {
                int pos;
                for (pos = 0; pos < ENTRIES_PER_BUCKET; pos++) {
                    if (isPositionFilled(b, pos)) {
                        void *emit = emit_ref ? &b->entries[pos] : b->entries[pos];
                        fn(privdata, emit);
                    }
                }
            }
            bucket *next = getChildBucket(b);
            if (next != NULL && defragfn != NULL) {
                next = bucketDefrag(b, next, defragfn);
            }
            b = next;
        } while (b != NULL);

        /* Advance cursor. */
        cursor = nextCursor(cursor, mask);
    } else {
        int table_small, table_large;
        if (ht->bucket_exp[0] <= ht->bucket_exp[1]) {
            table_small = 0;
            table_large = 1;
        } else {
            table_small = 1;
            table_large = 0;
        }

        size_t mask_small = expToMask(ht->bucket_exp[table_small]);
        size_t mask_large = expToMask(ht->bucket_exp[table_large]);

        /* Emit entries in the smaller table, if this index hasn't already been
         * rehashed. */
        size_t idx = cursor & mask_small;
        if (table_small == 1 || ht->rehash_idx == -1 || idx >= (size_t)ht->rehash_idx) {
            size_t used_before = ht->used[table_small];
            bucket *b = &ht->tables[table_small][idx];
            do {
                if (b->presence) {
                    for (int pos = 0; pos < ENTRIES_PER_BUCKET; pos++) {
                        if (isPositionFilled(b, pos)) {
                            void *emit = emit_ref ? &b->entries[pos] : b->entries[pos];
                            fn(privdata, emit);
                        }
                    }
                }
                bucket *next = getChildBucket(b);
                if (next != NULL && defragfn != NULL) {
                    next = bucketDefrag(b, next, defragfn);
                }
                b = next;
            } while (b != NULL);
            /* If any entries were deleted, fill the holes. */
            if (ht->used[table_small] < used_before) {
                compactBucketChain(ht, idx, table_small);
            }
        }

        /* Iterate over indices in larger table that are the expansion of the
         * index pointed to by the cursor in the smaller table. */
        do {
            /* Emit entries in the larger table at this cursor, if this index
             * hash't already been rehashed. */
            idx = cursor & mask_large;
            if (table_large == 1 || ht->rehash_idx == -1 || idx >= (size_t)ht->rehash_idx) {
                size_t used_before = ht->used[table_large];
                bucket *b = &ht->tables[table_large][idx];
                do {
                    if (b->presence) {
                        for (int pos = 0; pos < ENTRIES_PER_BUCKET; pos++) {
                            if (isPositionFilled(b, pos)) {
                                void *emit = emit_ref ? &b->entries[pos] : b->entries[pos];
                                fn(privdata, emit);
                            }
                        }
                    }
                    bucket *next = getChildBucket(b);
                    if (next != NULL && defragfn != NULL) {
                        next = bucketDefrag(b, next, defragfn);
                    }
                    b = next;
                } while (b != NULL);
                /* If any entries were deleted, fill the holes. */
                if (ht->used[table_large] < used_before) {
                    compactBucketChain(ht, idx, table_large);
                }
            }

            /* Increment the reverse cursor not covered by the smaller mask. */
            cursor = nextCursor(cursor, mask_large);

            /* Continue while bits covered by mask difference is non-zero. */
        } while (cursor & (mask_small ^ mask_large));
    }
    hashtableResumeRehashing(ht);
    return cursor;
}

/* --- Iterator --- */

/* Initialize an iterator for a hashtable.
 *
 * The 'flags' argument can be used to tweak the behaviour. It's a bitwise-or
 * (zero means no flags) of the following:
 *
 * - HASHTABLE_ITER_SAFE: Use a safe iterator that can handle
 *   modifications to the hash table while iterating.
 * - HASHTABLE_ITER_PREFETCH_VALUES: Enables prefetching of entries values,
 *   which can improve performance in some scenarios. Because the hashtable is generic and
 *   doesn't care which object we store, the callback entryPrefetchValue must be set to help
 *   us prefetch necessary fields of specific object types stored in the hashtable.
 *
 * For a non-safe iterator (default, when HASHTABLE_ITER_SAFE is not set):
 * It is not allowed to insert, delete or even lookup entries in the hashtable,
 * because such operations can trigger incremental rehashing which moves entries
 * around and confuses the iterator. Only hashtableNext is allowed. Each entry
 * is returned exactly once.
 *
 * For a safe iterator (when HASHTABLE_ITER_SAFE is set):
 * It is allowed to modify the hash table while iterating. It pauses incremental
 * rehashing to prevent entries from moving around. It's allowed to insert and
 * replace entries. Deleting entries is only allowed for the entry that was just
 * returned by hashtableNext. Deleting other entries is possible, but doing so
 * can cause internal fragmentation, so don't.
 *
 * Guarantees for safe iterators:
 *
 * - Entries that are in the hash table for the entire iteration are returned
 *   exactly once.
 *
 * - Entries that are deleted or replaced after they have been returned are not
 *   returned again.
 *
 * - Entries that are replaced before they've been returned by the iterator will
 *   be returned.
 *
 * - Entries that are inserted during the iteration may or may not be returned
 *   by the iterator.
 *
 * Call hashtableNext to fetch each entry. You must call hashtableResetIterator
 * when you are done with the iterator.
 */
void hashtableInitIterator(hashtableIterator *iterator, hashtable *ht, uint8_t flags) {
    iter *iter;
    iter = iteratorFromOpaque(iterator);
    iter->hashtable = ht;
    iter->table = 0;
    iter->index = -1;
    iter->flags = flags;
}

/* Reinitializes the iterator for the provided hashtable while
 * preserving the flags from its previous initialization. */
void hashtableReinitIterator(hashtableIterator *iterator, hashtable *ht) {
    iter *iter = iteratorFromOpaque(iterator);
    hashtableInitIterator(iterator, ht, iter->flags);
}

/* Resets a stack-allocated iterator. */
void hashtableResetIterator(hashtableIterator *iterator) {
    iter *iter = iteratorFromOpaque(iterator);
    if (!(iter->index == -1 && iter->table == 0)) {
        if (isSafe(iter)) {
            hashtableResumeRehashing(iter->hashtable);
            assert(iter->hashtable->pause_rehash >= 0);
        } else {
            assert(iter->fingerprint == hashtableFingerprint(iter->hashtable));
        }
    }
}

/* Allocates and initializes an iterator. */
hashtableIterator *hashtableCreateIterator(hashtable *ht, uint8_t flags) {
    iter *iter = zmalloc(sizeof(*iter));
    hashtableIterator *opaque = iteratorToOpaque(iter);
    hashtableInitIterator(opaque, ht, flags);
    return opaque;
}

/* Resets and frees the memory of an allocated iterator, i.e. one created using
 * hashtableCreate(Safe)Iterator. */
void hashtableReleaseIterator(hashtableIterator *iterator) {
    hashtableResetIterator(iterator);
    iter *iter = iteratorFromOpaque(iterator);
    zfree(iter);
}

/* Points elemptr to the next entry and returns 1 if there is a next entry.
 * Returns 0 if there are no more entries. */
int hashtableNext(hashtableIterator *iterator, void **elemptr) {
    iter *iter = iteratorFromOpaque(iterator);
    while (1) {
        if (iter->index == -1 && iter->table == 0) {
            /* It's the first call to next. */
            if (isSafe(iter)) {
                hashtablePauseRehashing(iter->hashtable);
                iter->last_seen_size = iter->hashtable->used[iter->table];
            } else {
                iter->fingerprint = hashtableFingerprint(iter->hashtable);
            }
            if (iter->hashtable->tables[0] == NULL) {
                /* Empty hashtable. We're done. */
                break;
            }
            iter->index = 0;
            /* Skip already rehashed buckets. */
            if (hashtableIsRehashing(iter->hashtable)) {
                iter->index = iter->hashtable->rehash_idx;
            }
            iter->bucket = &iter->hashtable->tables[iter->table][iter->index];
            iter->pos_in_bucket = 0;
        } else {
            /* Advance to the next position within the bucket, or to the next
             * child bucket in a chain, or to the next bucket index, or to the
             * next table. */
            iter->pos_in_bucket++;
            if (iter->bucket->chained && iter->pos_in_bucket >= ENTRIES_PER_BUCKET - 1) {
                iter->pos_in_bucket = 0;
                iter->bucket = getChildBucket(iter->bucket);
            } else if (iter->pos_in_bucket >= ENTRIES_PER_BUCKET) {
                /* Bucket index done. */
                if (isSafe(iter)) {
                    /* If entries in this bucket chain have been deleted,
                     * they've left empty spaces in the buckets. The chain is
                     * not automatically compacted when rehashing is paused. If
                     * this iterator is the only reason for pausing rehashing,
                     * we can do the compaction now when we're done with a
                     * bucket chain, before we move on to the next index. */
                    if (iter->hashtable->pause_rehash == 1 &&
                        iter->hashtable->used[iter->table] < iter->last_seen_size) {
                        compactBucketChain(iter->hashtable, iter->index, iter->table);
                    }
                    iter->last_seen_size = iter->hashtable->used[iter->table];
                }
                iter->pos_in_bucket = 0;
                iter->index++;
                if ((size_t)iter->index >= numBuckets(iter->hashtable->bucket_exp[iter->table])) {
                    if (hashtableIsRehashing(iter->hashtable) && iter->table == 0) {
                        iter->index = 0;
                        iter->table++;
                    } else {
                        /* Done. */
                        break;
                    }
                }
                iter->bucket = &iter->hashtable->tables[iter->table][iter->index];
            }
        }
        bucket *b = iter->bucket;
        if (iter->pos_in_bucket == 0) {
            if (shouldPrefetchValues(iter)) {
                prefetchBucketValues(b, iter->hashtable);
            }
            prefetchNextBucketEntries(iter, b);
        }
        if (!isPositionFilled(b, iter->pos_in_bucket)) {
            /* No entry here. */
            continue;
        }
        /* Return the entry at this position. */
        if (elemptr) {
            *elemptr = b->entries[iter->pos_in_bucket];
        }
        return 1;
    }
    return 0;
}

/* --- Random entries --- */

/* Points 'found' to a random entry in the hash table and returns 1. Returns 0
 * if the table is empty. */
int hashtableRandomEntry(hashtable *ht, void **found) {
    void *samples[WEAK_RANDOM_SAMPLE_SIZE];
    unsigned count = hashtableSampleEntries(ht, &samples[0], WEAK_RANDOM_SAMPLE_SIZE);
    if (count == 0) return 0;
    unsigned idx = random() % count;
    *found = samples[idx];
    return 1;
}

/* Points 'found' to a random entry in the hash table and returns 1. Returns 0
 * if the table is empty. This one is more fair than hashtableRandomEntry(). */
int hashtableFairRandomEntry(hashtable *ht, void **found) {
    void *samples[FAIR_RANDOM_SAMPLE_SIZE];
    unsigned count = hashtableSampleEntries(ht, &samples[0], FAIR_RANDOM_SAMPLE_SIZE);
    if (count == 0) return 0;
    unsigned idx = random() % count;
    *found = samples[idx];
    return 1;
}

/* This function samples a sequence of entries starting at a random location in
 * the hash table.
 *
 * The sampled entries are stored in the array 'dst' which must have space for
 * at least 'count' entries.
 *
 * The function returns the number of sampled entries, which is 'count' except
 * if 'count' is greater than the total number of entries in the hash table. */
unsigned hashtableSampleEntries(hashtable *ht, void **dst, unsigned count) {
    /* Adjust count. */
    if (count > hashtableSize(ht)) count = hashtableSize(ht);
    scan_samples samples;
    samples.size = count;
    samples.seen = 0;
    samples.entries = dst;
    size_t cursor = randomSizeT();
    while (samples.seen < count) {
        cursor = hashtableScan(ht, cursor, sampleEntriesScanFn, &samples);
    }
    rehashStepOnReadIfNeeded(ht);
    /* samples.seen is the number of entries scanned. It may be greater than
     * the requested count and the size of the dst array. */
    return samples.seen <= count ? samples.seen : count;
}

/* --- Stats --- */

#define HASHTABLE_STATS_VECTLEN 50
void hashtableFreeStats(hashtableStats *stats) {
    zfree(stats->clvector);
    zfree(stats);
}

void hashtableCombineStats(hashtableStats *from, hashtableStats *into) {
    into->toplevel_buckets += from->toplevel_buckets;
    into->child_buckets += from->child_buckets;
    into->max_chain_len = (from->max_chain_len > into->max_chain_len) ? from->max_chain_len : into->max_chain_len;
    into->size += from->size;
    into->used += from->used;
    for (int i = 0; i < HASHTABLE_STATS_VECTLEN; i++) {
        into->clvector[i] += from->clvector[i];
    }
}

hashtableStats *hashtableGetStatsHt(hashtable *ht, int table_index, int full) {
    unsigned long *clvector = zcalloc(sizeof(unsigned long) * HASHTABLE_STATS_VECTLEN);
    hashtableStats *stats = zcalloc(sizeof(hashtableStats));
    stats->table_index = table_index;
    stats->clvector = clvector;
    stats->toplevel_buckets = numBuckets(ht->bucket_exp[table_index]);
    stats->child_buckets = ht->child_buckets[table_index];
    stats->size = numBuckets(ht->bucket_exp[table_index]) * ENTRIES_PER_BUCKET;
    stats->used = ht->used[table_index];
    if (!full) return stats;
    /* Compute stats about bucket chain lengths. */
    stats->max_chain_len = 0;
    for (size_t idx = 0; idx < numBuckets(ht->bucket_exp[table_index]); idx++) {
        bucket *b = &ht->tables[table_index][idx];
        unsigned long chainlen = 0;
        while (b->chained) {
            chainlen++;
            b = getChildBucket(b);
        }
        if (chainlen > stats->max_chain_len) {
            stats->max_chain_len = chainlen;
        }
        if (chainlen >= HASHTABLE_STATS_VECTLEN) {
            chainlen = HASHTABLE_STATS_VECTLEN - 1;
        }
        clvector[chainlen]++;
    }
    return stats;
}

/* Generates human readable stats. */
size_t hashtableGetStatsMsg(char *buf, size_t bufsize, hashtableStats *stats, int full) {
    if (stats->used == 0) {
        return snprintf(buf, bufsize,
                        "Hash table %d stats (%s):\n"
                        "No stats available for empty hash tables\n",
                        stats->table_index,
                        (stats->table_index == 0) ? "main hash table" : "rehashing target");
    }
    size_t l = 0;
    l += snprintf(buf + l, bufsize - l,
                  "Hash table %d stats (%s):\n"
                  " table size: %lu\n"
                  " number of entries: %lu\n",
                  stats->table_index,
                  (stats->table_index == 0) ? "main hash table" : "rehashing target", stats->size,
                  stats->used);
    if (full) {
        l += snprintf(buf + l, bufsize - l,
                      " top-level buckets: %lu\n"
                      " child buckets: %lu\n"
                      " max chain length: %lu\n"
                      " avg chain length: %.02f\n"
                      " chain length distribution:\n",
                      stats->toplevel_buckets,
                      stats->child_buckets,
                      stats->max_chain_len,
                      (float)stats->child_buckets / stats->toplevel_buckets);
        for (unsigned long i = 0; i < HASHTABLE_STATS_VECTLEN - 1; i++) {
            if (stats->clvector[i] == 0) continue;
            if (l >= bufsize) break;
            l += snprintf(buf + l, bufsize - l, "   %ld: %ld (%.02f%%)\n", i, stats->clvector[i],
                          ((float)stats->clvector[i] / stats->toplevel_buckets) * 100);
        }
    }

    /* Make sure there is a NULL term at the end. */
    buf[bufsize - 1] = '\0';
    /* Unlike snprintf(), return the number of characters actually written. */
    return strlen(buf);
}

void hashtableGetStats(char *buf, size_t bufsize, hashtable *ht, int full) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    hashtableStats *mainHtStats = hashtableGetStatsHt(ht, 0, full);
    l = hashtableGetStatsMsg(buf, bufsize, mainHtStats, full);
    hashtableFreeStats(mainHtStats);
    buf += l;
    bufsize -= l;
    if (hashtableIsRehashing(ht) && bufsize > 0) {
        hashtableStats *rehashHtStats = hashtableGetStatsHt(ht, 1, full);
        hashtableGetStatsMsg(buf, bufsize, rehashHtStats, full);
        hashtableFreeStats(rehashHtStats);
    }
    /* Make sure there is a NULL term at the end. */
    orig_buf[orig_bufsize - 1] = '\0';
}

/* --- DEBUG --- */

void hashtableDump(hashtable *ht) {
    for (int table = 0; table <= 1; table++) {
        printf("Table %d, used %zu, exp %d, top-level buckets %zu, child buckets %zu\n",
               table, ht->used[table], ht->bucket_exp[table],
               numBuckets(ht->bucket_exp[table]), ht->child_buckets[table]);
        for (size_t idx = 0; idx < numBuckets(ht->bucket_exp[table]); idx++) {
            bucket *b = &ht->tables[table][idx];
            int level = 0;
            do {
                printf("Bucket %d:%zu level:%d\n", table, idx, level);
                for (int pos = 0; pos < ENTRIES_PER_BUCKET; pos++) {
                    printf("  %d ", pos);
                    if (isPositionFilled(b, pos)) {
                        printf("h2 %02x, key \"%s\"\n", b->hashes[pos],
                               (const char *)entryGetKey(ht, b->entries[pos]));
                    } else {
                        printf("(empty)\n");
                    }
                }
                b = getChildBucket(b);
                level++;
            } while (b != NULL);
        }
    }
}

/* Prints a histogram-like view of the number of entries in each bucket and
 * sub-bucket. Example:
 *
 *     Bucket fill table=0 size=32 children=9 used=200:
 *         67453462673764475436556656776756
 *         2     3 2   3      3  45 5     3
 */
void hashtableHistogram(hashtable *ht) {
    for (int table = 0; table <= 1; table++) {
        if (ht->bucket_exp[table] < 0) continue;
        size_t size = numBuckets(ht->bucket_exp[table]);
        bucket *buckets[size];
        for (size_t idx = 0; idx < size; idx++) {
            buckets[idx] = &ht->tables[table][idx];
        }
        size_t chains_left = size;
        printf("Bucket fill table=%d size=%zu children=%zu used=%zu:\n",
               table, size, ht->child_buckets[table], ht->used[table]);
        do {
            printf("    ");
            for (size_t idx = 0; idx < size; idx++) {
                bucket *b = buckets[idx];
                if (b == NULL) {
                    printf(" ");
                    continue;
                }
                printf("%X", __builtin_popcount(b->presence));
                buckets[idx] = getChildBucket(b);
                if (buckets[idx] == NULL) chains_left--;
            }
            printf("\n");
        } while (chains_left > 0);
    }
}

int hashtableLongestBucketChain(hashtable *ht) {
    int maxlen = 0;
    for (int table = 0; table <= 1; table++) {
        if (ht->bucket_exp[table] < 0) {
            continue; /* table not used */
        }
        for (size_t i = 0; i < numBuckets(ht->bucket_exp[table]); i++) {
            int chainlen = 0;
            bucket *b = &ht->tables[table][i];
            while (b->chained) {
                ++chainlen;
                b = getChildBucket(b);
            }
            if (chainlen > maxlen) {
                maxlen = chainlen;
            }
        }
    }
    return maxlen;
}
