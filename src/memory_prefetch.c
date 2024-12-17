/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * This file utilizes prefetching keys and data for multiple commands in a batch,
 * to improve performance by amortizing memory access costs across multiple operations.
 */

#include "memory_prefetch.h"
#include "server.h"

typedef enum {
    PREFETCH_ENTRY, /* Initial state, prefetch entries associated with the given key's hash */
    PREFETCH_VALUE, /* prefetch the value object of the entry found in the previous step */
    PREFETCH_DONE   /* Indicates that prefetching for this key is complete */
} PrefetchState;

typedef struct KeyPrefetchInfo {
    PrefetchState state; /* Current state of the prefetch operation */
    hashtableIncrementalFindState hashtab_state;
} KeyPrefetchInfo;

/* PrefetchCommandsBatch structure holds the state of the current batch of client commands being processed. */
typedef struct PrefetchCommandsBatch {
    size_t cur_idx;                 /* Index of the current key being processed */
    size_t keys_done;               /* Number of keys that have been prefetched */
    size_t key_count;               /* Number of keys in the current batch */
    size_t client_count;            /* Number of clients in the current batch */
    size_t max_prefetch_size;       /* Maximum number of keys to prefetch in a batch */
    size_t executed_commands;       /* Number of commands executed in the current batch */
    int *slots;                     /* Array of slots for each key */
    void **keys;                    /* Array of keys to prefetch in the current batch */
    client **clients;               /* Array of clients in the current batch */
    hashtable **keys_tables;        /* Main table for each key */
    KeyPrefetchInfo *prefetch_info; /* Prefetch info for each key */
} PrefetchCommandsBatch;

static PrefetchCommandsBatch *batch = NULL;

void freePrefetchCommandsBatch(void) {
    if (batch == NULL) {
        return;
    }

    zfree(batch->clients);
    zfree(batch->keys);
    zfree(batch->keys_tables);
    zfree(batch->slots);
    zfree(batch->prefetch_info);
    zfree(batch);
    batch = NULL;
}

void prefetchCommandsBatchInit(void) {
    serverAssert(!batch);
    size_t max_prefetch_size = server.prefetch_batch_max_size;

    if (max_prefetch_size == 0) {
        return;
    }

    batch = zcalloc(sizeof(PrefetchCommandsBatch));
    batch->max_prefetch_size = max_prefetch_size;
    batch->clients = zcalloc(max_prefetch_size * sizeof(client *));
    batch->keys = zcalloc(max_prefetch_size * sizeof(void *));
    batch->keys_tables = zcalloc(max_prefetch_size * sizeof(hashtable *));
    batch->slots = zcalloc(max_prefetch_size * sizeof(int));
    batch->prefetch_info = zcalloc(max_prefetch_size * sizeof(KeyPrefetchInfo));
}

void onMaxBatchSizeChange(void) {
    if (batch && batch->client_count > 0) {
        /* We need to process the current batch before updating the size */
        return;
    }

    freePrefetchCommandsBatch();
    prefetchCommandsBatchInit();
}

/* Move to the next key in the batch. */
static void moveToNextKey(void) {
    batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
}

static void markKeyAsdone(KeyPrefetchInfo *info) {
    info->state = PREFETCH_DONE;
    server.stat_total_prefetch_entries++;
    batch->keys_done++;
}

/* Returns the next KeyPrefetchInfo structure that needs to be processed. */
static KeyPrefetchInfo *getNextPrefetchInfo(void) {
    size_t start_idx = batch->cur_idx;
    do {
        KeyPrefetchInfo *info = &batch->prefetch_info[batch->cur_idx];
        if (info->state != PREFETCH_DONE) return info;
        batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
    } while (batch->cur_idx != start_idx);
    return NULL;
}

static void initBatchInfo(hashtable **tables) {
    /* Initialize the prefetch info */
    for (size_t i = 0; i < batch->key_count; i++) {
        KeyPrefetchInfo *info = &batch->prefetch_info[i];
        if (!tables[i] || hashtableSize(tables[i]) == 0) {
            info->state = PREFETCH_DONE;
            batch->keys_done++;
            continue;
        }
        info->state = PREFETCH_ENTRY;
        hashtableIncrementalFindInit(&info->hashtab_state, tables[i], batch->keys[i]);
    }
}

static void prefetchEntry(KeyPrefetchInfo *info) {
    if (hashtableIncrementalFindStep(&info->hashtab_state) == 1) {
        /* Not done yet */
        moveToNextKey();
        /* If reply offload enabled no need to prefetch value because main thread will not access it */
    } else if (server.reply_offload_enabled) {
         markKeyAsdone(info);
    } else {
        info->state = PREFETCH_VALUE;
    }
}

/* Prefetch the entry's value. If the value is found.*/
static void prefetchValue(KeyPrefetchInfo *info) {
    void *entry;
    if (hashtableIncrementalFindGetResult(&info->hashtab_state, &entry)) {
        robj *val = entry;
        if (val->encoding == OBJ_ENCODING_RAW && val->type == OBJ_STRING) {
            valkey_prefetch(val->ptr);
        }
    }

    markKeyAsdone(info);
}

/* Prefetch hashtable data for an array of keys.
 *
 * This function takes an array of tables and keys, attempting to bring
 * data closer to the L1 cache that might be needed for hashtable operations
 * on those keys.
 *
 * tables - An array of hashtables to prefetch data from.
 * prefetch_value - If true, we prefetch the value data for each key.
 * to bring the key's value data closer to the L1 cache as well.
 */
static void hashtablePrefetch(hashtable **tables) {
    initBatchInfo(tables);
    KeyPrefetchInfo *info;
    while ((info = getNextPrefetchInfo())) {
        switch (info->state) {
        case PREFETCH_ENTRY: prefetchEntry(info); break;
        case PREFETCH_VALUE: prefetchValue(info); break;
        default: serverPanic("Unknown prefetch state %d", info->state);
        }
    }
}

static void resetCommandsBatch(void) {
    batch->cur_idx = 0;
    batch->keys_done = 0;
    batch->key_count = 0;
    batch->client_count = 0;
    batch->executed_commands = 0;
}

/* Prefetch command-related data:
 * 1. Prefetch the command arguments allocated by the I/O thread to bring them closer to the L1 cache.
 * 2. Prefetch the keys and values for all commands in the current batch from the main hashtable. */
static void prefetchCommands(void) {
    /* Prefetch argv's for all clients */
    for (size_t i = 0; i < batch->client_count; i++) {
        client *c = batch->clients[i];
        if (!c || c->argc <= 1) continue;
        /* Skip prefetching first argv (cmd name) it was already looked up by the I/O thread. */
        for (int j = 1; j < c->argc; j++) {
            valkey_prefetch(c->argv[j]);
        }
    }

    /* Prefetch the argv->ptr if required */
    for (size_t i = 0; i < batch->client_count; i++) {
        client *c = batch->clients[i];
        if (!c || c->argc <= 1) continue;
        for (int j = 1; j < c->argc; j++) {
            if (c->argv[j]->encoding == OBJ_ENCODING_RAW) {
                valkey_prefetch(c->argv[j]->ptr);
            }
        }
    }

    /* Get the keys ptrs - we do it here after the key obj was prefetched. */
    for (size_t i = 0; i < batch->key_count; i++) {
        batch->keys[i] = ((robj *)batch->keys[i])->ptr;
    }

    /* Prefetch hashtable keys for all commands. Prefetching is beneficial only if there are more than one key. */
    if (batch->key_count > 1) {
        server.stat_total_prefetch_batches++;
        /* Prefetch keys from the main hashtable */
        hashtablePrefetch(batch->keys_tables);
    }
}

/* Processes all the prefetched commands in the current batch. */
void processClientsCommandsBatch(void) {
    if (!batch || batch->client_count == 0) return;

    /* If executed_commands is not 0,
     * it means that we are in the middle of processing a batch and this is a recursive call */
    if (batch->executed_commands == 0) {
        prefetchCommands();
    }

    /* Process the commands */
    for (size_t i = 0; i < batch->client_count; i++) {
        client *c = batch->clients[i];
        if (c == NULL) continue;

        /* Set the client to null immediately to avoid accessing it again recursively when ProcessingEventsWhileBlocked */
        batch->clients[i] = NULL;
        batch->executed_commands++;
        if (processPendingCommandAndInputBuffer(c) != C_ERR) beforeNextClient(c);
    }

    resetCommandsBatch();

    /* Handle the case where the max prefetch size has been changed. */
    if (batch->max_prefetch_size != (size_t)server.prefetch_batch_max_size) {
        onMaxBatchSizeChange();
    }
}

/* Adds the client's command to the current batch and processes the batch
 * if it becomes full.
 *
 * Returns C_OK if the command was added successfully, C_ERR otherwise. */
int addCommandToBatchAndProcessIfFull(client *c) {
    if (!batch) return C_ERR;

    batch->clients[batch->client_count++] = c;

    /* Get command's keys positions */
    if (c->io_parsed_cmd) {
        getKeysResult result;
        initGetKeysResult(&result);
        int num_keys = getKeysFromCommand(c->io_parsed_cmd, c->argv, c->argc, &result);
        for (int i = 0; i < num_keys && batch->key_count < batch->max_prefetch_size; i++) {
            batch->keys[batch->key_count] = c->argv[result.keys[i].pos];
            batch->slots[batch->key_count] = c->slot > 0 ? c->slot : 0;
            batch->keys_tables[batch->key_count] = kvstoreGetHashtable(c->db->keys, batch->slots[batch->key_count]);
            batch->key_count++;
        }
        getKeysFreeResult(&result);
    }

    /* If the batch is full, process it.
     * We also check the client count to handle cases where
     * no keys exist for the clients' commands. */
    if (batch->client_count == batch->max_prefetch_size || batch->key_count == batch->max_prefetch_size) {
        processClientsCommandsBatch();
    }

    return C_OK;
}

/* Removes the given client from the pending prefetch batch, if present. */
void removeClientFromPendingCommandsBatch(client *c) {
    if (!batch) return;

    for (size_t i = 0; i < batch->client_count; i++) {
        if (batch->clients[i] == c) {
            batch->clients[i] = NULL;
            return;
        }
    }
}
