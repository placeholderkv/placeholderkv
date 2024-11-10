/* Copyright 2024- Valkey contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <stdio.h>
#include "zmalloc.h"
#include "serverassert.h"
#include "allocator_defrag.h"

#define UNUSED(x) (void)(x)

#if defined(HAVE_DEFRAG) && defined(USE_JEMALLOC)

#define STRINGIFY_(x) #x
#define STRINGIFY(x) STRINGIFY_(x)

#define BATCH_QUERY_ARGS_OUT 3
#define SLAB_NFREE(out, i) out[(i) * BATCH_QUERY_ARGS_OUT]
#define SLAB_LEN(out, i) out[(i) * BATCH_QUERY_ARGS_OUT + 2]
#define SLAB_NUM_REGS(out, i) out[(i) * BATCH_QUERY_ARGS_OUT + 1]

#define UTILIZATION_THRESHOLD_FACTOR_MILI (125) // 12.5% additional utilization

typedef struct jeMallctlKey {
    size_t key[6];
    size_t keylen;
} jeMallctlKey;

/*  Helper struct to store MIB (Management Information Base) information for jemalloc bin queries. */
typedef struct jeBinInfoKeys {
    jeMallctlKey curr_slabs;
    jeMallctlKey nonfull_slabs;
    jeMallctlKey curr_regs;
    jeMallctlKey nmalloc;
    jeMallctlKey ndealloc;
} jeBinInfoKeys;

/*  Struct representing bin information. */
typedef struct jeBinInfo {
    unsigned long reg_size;  /* < Size of each region in the bin. */
    unsigned long nregs;     /* < Total number of regions in the bin. */
    unsigned long len;       /* < Length or size of the bin (unused in this implementation). */
    jeBinInfoKeys info_keys; /* < Helper struct containing MIB information for bin queries. */
} jeBinInfo;

/*  Struct representing the configuration for jemalloc bins. */
typedef struct jeBinsConf {
    unsigned long nbins; /* < Number of bins in the configuration. */
    jeBinInfo *bin_info; /* < Array of bin information structs. */
    jeMallctlKey util_batch_query;
    jeMallctlKey epoch;
} jeBinsConf;

/*  Struct representing defragmentation statistics for a bin. */
typedef struct jeDefragBinStats {
    unsigned long hits;     /* < Number of hits (regions that should be defragmented). */
    unsigned long misses;   /* < Number of misses (regions that should not be defragmented). */
    unsigned long nmalloc;  /* < Number of malloc operations (unused in this implementation). */
    unsigned long ndealloc; /* < Number of dealloc operations (unused in this implementation). */
} jeDefragBinStats;

/*  Struct representing overall defragmentation statistics. */
typedef struct jeDefragStats {
    unsigned long hits;       /* < Total number of hits (regions that should be defragmented). */
    unsigned long misses;     /* < Total number of misses (regions that should not be defragmented). */
    unsigned long hit_bytes;  /* < Total number of bytes that should be defragmented. */
    unsigned long miss_bytes; /* < Total number of bytes that should not be defragmented. */
    unsigned long ncalls;     /* < Number of calls to the defragmentation function. */
    unsigned long nptrs;      /* < Total number of pointers analyzed for defragmentation. */
} jeDefragStats;

/*  Struct representing the latest usage information for a bin. */
typedef struct jeBusage {
    unsigned long curr_slabs;         /* < Current number of slabs in the bin. */
    unsigned long curr_nonfull_slabs; /* < Current number of non-full slabs in the bin. */
    unsigned long curr_full_slabs;    /* < Current number of full slabs in the bin (calculated from other fields). */
    unsigned long curr_regs;          /* < Current number of regions in the bin. */
    jeDefragBinStats stat;            /* < Defragmentation statistics for the bin. */
} jeBusage;

/*  Struct representing the latest usage information across all bins. */
typedef struct jeUsageLatest {
    jeBusage *bins_usage; /* < Array of bin usage information structs. */
    jeDefragStats stats;  /* < Overall defragmentation statistics. */
} jeUsageLatest;

static int defrag_supported = 0;
static size_t jemalloc_quantum = 0;
static jeBinsConf arena_bin_conf = {0, NULL, {{0}, 0}, {{0}, 0}};
static jeUsageLatest usage_latest = {NULL, {0}};


/* -----------------------------------------------------------------------------
 * Alloc/Free API that are cooperative with defrag
 * -------------------------------------------------------------------------- */
/* Allocation and free functions that bypass the thread cache
 * and go straight to the allocator arena bins.
 * Currently implemented only for jemalloc. Used for online defragmentation. */
void *allocatorDefragAlloc(size_t size) {
    void *ptr = je_mallocx(size, MALLOCX_TCACHE_NONE);
    return ptr;
}
void allocatorDefragFree(void *ptr, size_t size) {
    if (ptr == NULL) return;
    je_sdallocx(ptr, size, MALLOCX_TCACHE_NONE);
}

/* -----------------------------------------------------------------------------
 * Helper functions for jemalloc translation between size and index
 * -------------------------------------------------------------------------- */
#define LG_QUANTOM_8_FIRST_POW2 3
#define SIZE_CLASS_GROUP_SZ 4

#define LG_QUANTOM_OFFSET_3 ((64 >> LG_QUANTOM_8_FIRST_POW2) - 1)
#define LG_QUANTOM_OFFSET_4 (64 >> 4)

#define getBinindNormal(_sz, _offset, _last_sz_pow2)                                                             \
    ((SIZE_CLASS_GROUP_SZ - (((1 << (_last_sz_pow2)) - (_sz)) >> ((_last_sz_pow2) - LG_QUANTOM_8_FIRST_POW2))) + \
     (((_last_sz_pow2) - (LG_QUANTOM_8_FIRST_POW2 + 3)) - 1) * SIZE_CLASS_GROUP_SZ + (_offset))
/* Get the bin index in bin array from the reg_size.
 *
 * these are reverse engineered mapping of reg_size -> binind. We need this information because the utilization query
 * returns the size of the buffer and not the bin index, and we need the bin index to access it's usage information
 *
 * Note: In case future PR will return the binind (that is better API anyway) we can get rid of
 * these conversion functions*/
inline unsigned jeSize2BinIndexLgQ3(size_t sz) {
    if (sz <= (1 << (LG_QUANTOM_8_FIRST_POW2 + 3))) {
        // for sizes: 8, 16, 24, 32, 40, 48, 56, 64
        return (sz >> 3) - 1;
    }
    // following groups have SIZE_CLASS_GROUP_SZ size-class that are
    uint64_t last_sz_in_group_pow2 = 64 - __builtin_clzll(sz - 1);
    return getBinindNormal(sz, LG_QUANTOM_OFFSET_3, last_sz_in_group_pow2);
}

/* -----------------------------------------------------------------------------
 * Get INFO string about the defrag.
 * -------------------------------------------------------------------------- */
/*
 * add defrag info string into info
 */
sds allocatorDefragCatFragmentationInfo(sds info) {
    if (!defrag_supported) return info;
    jeBinInfo *bin_info;
    jeBusage *bin_usage;
    unsigned nbins = arena_bin_conf.nbins;
    if (nbins > 0) {
        info = sdscatprintf(info,
                            "jemalloc_quantum:%d\r\n"
                            "defrag_hit_ratio:%.2f\r\n"
                            "defrag_hits:%lu\r\n"
                            "defrag_misses:%lu\r\n"
                            "defrag_hit_bytes:%lu\r\n"
                            "defrag_miss_bytes:%lu\r\n"
                            "defrag_check_num_calls:%lu\r\n"
                            "defrag_check_num_ptrs:%lu\r\n",
                            (int)jemalloc_quantum,
                            (usage_latest.stats.hits + usage_latest.stats.misses)
                                ? (float)usage_latest.stats.hits / (float)(usage_latest.stats.hits + usage_latest.stats.misses)
                                : 0,
                            usage_latest.stats.hits, usage_latest.stats.misses, usage_latest.stats.hit_bytes,
                            usage_latest.stats.miss_bytes, usage_latest.stats.ncalls, usage_latest.stats.nptrs);
        for (unsigned j = 0; j < nbins; j++) {
            bin_info = &arena_bin_conf.bin_info[j];
            bin_usage = &usage_latest.bins_usage[j];
            info = sdscatprintf(info,
                                "binstats[bin_size=%lu]:"
                                "num_regs=%lu,num_slabs:%lu,num_nonfull_slabs=%lu,"
                                "hit_rate=%.2f,hits=%lu,miss=%lu,num_malloc_calls=%lu,num_dealloc_calls=%lu\r\n",
                                bin_info->reg_size, bin_usage->curr_regs, bin_usage->curr_slabs, bin_usage->curr_nonfull_slabs,
                                (bin_usage->stat.hits + bin_usage->stat.misses)
                                    ? (float)bin_usage->stat.hits / (float)(bin_usage->stat.hits + bin_usage->stat.misses)
                                    : 0,
                                bin_usage->stat.hits, bin_usage->stat.misses, bin_usage->stat.nmalloc, bin_usage->stat.ndealloc);
        }
    }
    return info;
}

/* -----------------------------------------------------------------------------
 * Interface functions to get fragmentation info from jemalloc
 * -------------------------------------------------------------------------- */
#define ARENA_TO_QUERY MALLCTL_ARENAS_ALL

static inline void jeRefreshStats(const jeBinsConf *arena_bin_conf) {
    uint64_t epoch = 1; // Value doesn't matter
    size_t sz = sizeof(epoch);
    je_mallctlbymib(arena_bin_conf->epoch.key, arena_bin_conf->epoch.keylen, &epoch, &sz, &epoch, sz); // Refresh stats
}

/* Extract key that corresponds to the given name for fast query. This should be called once for each key_name */
static int jeQueryKeyInit(const char *key_name, jeMallctlKey *key_info) {
    key_info->keylen = sizeof(key_info->key) / sizeof(key_info->key[0]);
    int res = je_mallctlnametomib(key_name, key_info->key, &key_info->keylen);
    assert(key_info->keylen <= sizeof(key_info->key) / sizeof(key_info->key[0]));
    return res;
}

/* Query jemalloc control interface using previously extracted key (with jeQueryKeyInit) instead of name string.
 * This interface (named MIB in jemalloc) is faster as it avoids string dict lookup at run-time. */
static inline int jeQueryCtlInterface(const jeMallctlKey *key_info, void *value, size_t *size) {
    return je_mallctlbymib(key_info->key, key_info->keylen, value, size, NULL, 0);
}

static int binQueryHelperInitialization(jeBinInfoKeys *helper, unsigned bin_index) {
    char buf[128];

    /* Mib of fetch number of used regions in the bin */
    snprintf(buf, sizeof(buf), "stats.arenas." STRINGIFY(ARENA_TO_QUERY) ".bins.%d.curregs", bin_index);
    if (jeQueryKeyInit(buf, &helper->curr_regs) != 0) return -1;
    /* Mib of fetch number of current slabs in the bin */
    snprintf(buf, sizeof(buf), "stats.arenas." STRINGIFY(ARENA_TO_QUERY) ".bins.%d.curslabs", bin_index);
    if (jeQueryKeyInit(buf, &helper->curr_slabs) != 0) return -1;
    /* Mib of fetch nonfull slabs */
    snprintf(buf, sizeof(buf), "stats.arenas." STRINGIFY(ARENA_TO_QUERY) ".bins.%d.nonfull_slabs", bin_index);
    if (jeQueryKeyInit(buf, &helper->nonfull_slabs) != 0) return -1;
    /* Mib of fetch num of alloc op */
    snprintf(buf, sizeof(buf), "stats.arenas." STRINGIFY(ARENA_TO_QUERY) ".bins.%d.nmalloc", bin_index);
    if (jeQueryKeyInit(buf, &helper->nmalloc) != 0) return -1;
    /* Mib of fetch num of dealloc op */
    snprintf(buf, sizeof(buf), "stats.arenas." STRINGIFY(ARENA_TO_QUERY) ".bins.%d.ndalloc", bin_index);
    if (jeQueryKeyInit(buf, &helper->ndealloc) != 0) return -1;

    return 0;
}

/**
 * Initializes the defragmentation system for the jemalloc memory allocator.
 *
 * This function performs the necessary setup and initialization steps for the defragmentation system.
 * It retrieves the configuration information for the jemalloc arenas and bins, and initializes the usage
 * statistics data structure.
 *
 * return 0 on success, or a non-zero error code on failure.
 *
 * The initialization process involves the following steps:
 * 1. Check if defragmentation is supported by the current jemalloc version.
 * 2. Retrieve the arena bin configuration information using the `je_mallctlbymib` function.
 * 3. Initialize the `usage_latest` structure with the bin usage statistics and configuration data.
 * 4. Set the `defrag_supported` flag to indicate that defragmentation is enabled.
 *
 * Note: This function must be called before using any other defragmentation-related functionality.
 * It should be called during the initialization phase of the code that uses the
 * defragmentation feature.
 *  */
int allocatorDefragInit(void) {
    char buf[100];
    jeBinInfo *bin_info;
    unsigned nbins;
    size_t sz = sizeof(nbins);
    int je_res = 0;

    // the init should be called only once, fail if unexpected call
    assert(!defrag_supported);

    // Get the mib of the per memory pointers query command that is used during defrag scan over memory
    if (jeQueryKeyInit("experimental.utilization.batch_query", &arena_bin_conf.util_batch_query) != 0) return -1;

    je_res = jeQueryKeyInit("epoch", &arena_bin_conf.epoch);
    assert(je_res == 0);
    jeRefreshStats(&arena_bin_conf);

    size_t len = sizeof(jemalloc_quantum);
    je_mallctl("arenas.quantum", &jemalloc_quantum, &len, NULL, 0);
    // lg-quantum should be 3
    assert(jemalloc_quantum == 8);

    je_res = je_mallctl("arenas.nbins", &nbins, &sz, NULL, 0);
    assert(je_res == 0 && nbins != 0);
    arena_bin_conf.nbins = nbins;

    arena_bin_conf.bin_info = zcalloc(sizeof(jeBinInfo) * nbins);
    assert(arena_bin_conf.bin_info != NULL);
    usage_latest.bins_usage = zcalloc(sizeof(jeBusage) * nbins);
    assert(usage_latest.bins_usage != NULL);

    for (unsigned j = 0; j < nbins; j++) {
        bin_info = &arena_bin_conf.bin_info[j];
        /* The size of the current bin */
        snprintf(buf, sizeof(buf), "arenas.bin.%d.size", j);
        sz = sizeof(size_t);
        je_res = je_mallctl(buf, &bin_info->reg_size, &sz, NULL, 0);
        assert(je_res == 0);
        /* Number of regions per slab */
        snprintf(buf, sizeof(buf), "arenas.bin.%d.nregs", j);
        sz = sizeof(uint32_t);
        je_res = je_mallctl(buf, &bin_info->nregs, &sz, NULL, 0);
        assert(je_res == 0);

        bin_info->len = bin_info->reg_size * bin_info->nregs;

        // init bin specific fast query keys
        je_res = binQueryHelperInitialization(&bin_info->info_keys, j);
        assert(je_res == 0);

        /* verify the reverse map of reg_size to bin index */
        assert(jeSize2BinIndexLgQ3(bin_info->reg_size) == j);
    }

    // defrag is supported mark it to enable defrag queries
    defrag_supported = 1;
    return 0;
}

/**
 * Total size of consumed meomry in unused regs in small bins (AKA external fragmentation).
 * The function will refresh the epoch.
 *
 * return total fragmentation bytes
 *  */
unsigned long allocatorDefragGetFragSmallbins(void) {
    assert(defrag_supported);
    unsigned long frag = 0;
    jeRefreshStats(&arena_bin_conf);
    for (unsigned j = 0; j < arena_bin_conf.nbins; j++) {
        size_t sz;
        jeBinInfo *bin_info = &arena_bin_conf.bin_info[j];
        jeBusage *bin_usage = &usage_latest.bins_usage[j];

        /* Number of used regions in the bin */
        sz = sizeof(size_t);
        /* Number of current slabs in the bin */
        jeQueryCtlInterface(&bin_info->info_keys.curr_regs, &bin_usage->curr_regs, &sz);
        /* Number of current slabs in the bin */
        jeQueryCtlInterface(&bin_info->info_keys.curr_slabs, &bin_usage->curr_slabs, &sz);
        /* Number of non full slabs in the bin */
        jeQueryCtlInterface(&bin_info->info_keys.nonfull_slabs, &bin_usage->curr_nonfull_slabs, &sz);
        /* Num alloc op */
        jeQueryCtlInterface(&bin_info->info_keys.nmalloc, &bin_usage->stat.nmalloc, &sz);
        /* Num dealloc op */
        jeQueryCtlInterface(&bin_info->info_keys.ndealloc, &bin_usage->stat.ndealloc, &sz);

        bin_usage->curr_full_slabs = bin_usage->curr_slabs - bin_usage->curr_nonfull_slabs;
        /* Calculate the fragmentation bytes for the current bin and add it to the total. */
        frag += ((bin_info->nregs * bin_usage->curr_slabs) - bin_usage->curr_regs) * bin_info->reg_size;
    }
    return frag;
}

/*
 * Determines whether defragmentation should be performed on a pointer based on jemalloc information.
 *
 * bin_info Pointer to the bin information structure.
 * bin_usage Pointer to the bin usage structure.
 * nalloced Number of allocated regions in the bin.
 *
 * return 1 if defragmentation should be performed, 0 otherwise.
 *
 * This function checks the following conditions to determine if defragmentation should be performed:
 * 1. If the number of allocated regions (nalloced) is equal to the total number of regions (bin_info->nregs),
 *    defragmentation is not necessary as moving regions is guaranteed not to change the fragmentation ratio.
 * 2. If the number of non-full slabs (bin_usage->curr_nonfull_slabs) is less than 2, defragmentation is not performed
 *    because there is no other slab to move regions to.
 * 3. If slab utilization < 'avg utilization'*1.125 [code 1.125 == (1000+UTILIZATION_THRESHOLD_FACTOR_MILI)/1000]
 *    than we should defrag. This is aligned with previous je_defrag_hint implementation.
 */
static inline int makeDefragDecision(jeBinInfo *bin_info, jeBusage *bin_usage, unsigned long nalloced) {
    size_t allocated_nonfull = bin_usage->curr_regs - bin_usage->curr_full_slabs * bin_info->nregs;
    if (bin_info->nregs == nalloced || bin_usage->curr_nonfull_slabs < 2 ||
        1000 * nalloced * bin_usage->curr_nonfull_slabs > (1000 + UTILIZATION_THRESHOLD_FACTOR_MILI) * allocated_nonfull) {
        return 0;
    }
    return 1;
}

/*
 * Handles the results of the defragmentation analysis for multiple memory regions.
 *
 * conf - Pointer to the configuration structure for the jemalloc arenas and bins.
 * usage - Pointer to the usage statistics structure for the jemalloc arenas and bins.
 * results - Array of results for each memory region to be analyzed.
 * ptrs - Array of pointers to the memory regions to be analyzed.
 * num - Number of memory regions in the ptrs array.
 *
 * For each result it checks if defragmentation should be performed based on makeDefragDecision function.
 * If defragmentation should NOT be performed, it sets the corresponding pointer in the ptrs array to NULL.
 * */
static void handleResponses(jeBinsConf *conf,
                            jeUsageLatest *usage,
                            size_t *results,
                            void **ptrs,
                            size_t num) {
    for (unsigned i = 0; i < num; i++) {
        unsigned long num_regs = SLAB_NUM_REGS(results, i);
        unsigned long slablen = SLAB_LEN(results, i);
        unsigned long nfree = SLAB_NFREE(results, i);
        assert(num_regs > 0);
        assert(slablen > 0);
        assert(nfree != (unsigned long)-1);
        unsigned bsz = slablen / num_regs;
        // check that the allocation is not too large
        if (bsz > conf->bin_info[conf->nbins - 1].reg_size) {
            ptrs[i] = NULL;
            continue;
        }
        // get the index based on quantum used
        unsigned binind = jeSize2BinIndexLgQ3(bsz);
        // make sure binind is in range and reverse map is correct
        assert(binind < conf->nbins && bsz == conf->bin_info[binind].reg_size);

        jeBinInfo *bin_info = &conf->bin_info[binind];
        jeBusage *bin_usage = &usage->bins_usage[binind];

        if (!makeDefragDecision(bin_info, bin_usage, bin_info->nregs - nfree)) {
            // MISS: utilization level is higher than threshold then set the ptr to NULL and caller will not defrag it
            ptrs[i] = NULL;
            // update miss statistics
            bin_usage->stat.misses++;
            usage->stats.misses++;
            usage->stats.miss_bytes += bsz;
        } else { // HIT
            // update hit statistics
            bin_usage->stat.hits++;
            usage->stats.hits++;
            usage->stats.hit_bytes += bsz;
        }
    }
}

/**
 * Performs defragmentation analysis for multiple memory regions.
 *
 * ptrs - Array of pointers to memory regions to be analyzed.
 * num - Number of memory regions in the ptrs array.
 *
 * This function analyzes the provided memory regions and determines whether defragmentation should be performed
 * for each region based on the utilization and fragmentation levels. It updates the statistics for hits and misses
 * based on the defragmentation decision.
 *
 * Sets to NULL any ptr that should not be defragged.
 * Ptrs that should not be defragged are not modified.
 *
 *  */
void allocatorDefragCheckMulti(void **ptrs, unsigned long num) {
    assert(defrag_supported);
    assert(num == 1);
    static __thread size_t out[BATCH_QUERY_ARGS_OUT] = {0};
    size_t out_sz = sizeof(size_t) * num * BATCH_QUERY_ARGS_OUT;
    size_t in_sz = sizeof(const void *) * num;
    jeBinsConf *conf = &arena_bin_conf;
    jeUsageLatest *usage = &usage_latest;
    for (unsigned j = 0; j < num * BATCH_QUERY_ARGS_OUT; j++) {
        out[j] = -1;
    }
    je_mallctlbymib(arena_bin_conf.util_batch_query.key, arena_bin_conf.util_batch_query.keylen, out, &out_sz, ptrs,
                    in_sz);
    // handle results with appropriate quantum value
    handleResponses(conf, usage, out, ptrs, num);
    // update overall stats, regardless of hits or misses
    usage->stats.ncalls++;
    usage->stats.nptrs += num;
}
#else
int allocatorDefragInit(void) {
    return -1;
}
void allocatorDefragFree(void *ptr, size_t size) {
    UNUSED(ptr);
    UNUSED(size);
}
__attribute__((malloc)) void *allocatorDefragAlloc(size_t size) {
    UNUSED(size);
    return NULL;
}
unsigned long allocatorDefragGetFragSmallbins(void) {
    return 0;
}
sds allocatorDefragCatFragmentationInfo(sds info) {
    return info;
}
void allocatorDefragCheckMulti(void **ptrs, unsigned long num) {
    UNUSED(ptrs);
    UNUSED(num);
}
#endif
