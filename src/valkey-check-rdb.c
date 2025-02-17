/*
 * Copyright (c) 2016, Redis Ltd.
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

#include "mt19937-64.h"
#include "server.h"
#include "rdb.h"
#include "module.h"
#include "hdr_histogram.h"
#include "fpconv_dtoa.h"

#include <stdarg.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>

void createSharedObjects(void);
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len);
void computeDatasetProfile(int dbid, robj *keyobj, robj *o);

int rdbCheckMode = 0;
int rdbCheckProfiler = 0;

#define LOW_TRACKE_VALUE 1
#define MAX_ELEMENTS_TRACKE 200 * 1024
#define MAX_ELEMENTS_SIZE_TRACKE 1024 * 1024

typedef struct rdbProfiler {
    size_t type;
    unsigned long keys;
    unsigned long expires;
    unsigned long already_expired;

    unsigned long all_key_size;
    unsigned long all_value_size;

    unsigned long elements;
    unsigned long all_elements_size;

    unsigned long elements_max;
    unsigned long elements_size_max;

    struct hdr_histogram *element_count_histogram;
    struct hdr_histogram *element_size_histogram;
} rdbProfiler;

struct {
    rio *rio;
    robj *key;                     /* Current key we are reading. */
    int key_type;                  /* Current key type if != -1. */
    unsigned long keys;            /* Number of keys processed. */
    unsigned long expires;         /* Number of keys with an expire. */
    unsigned long already_expired; /* Number of keys already expired. */
    int doing;                     /* The state while reading the RDB. */
    int error_set;                 /* True if error is populated. */
    char error[1024];
    int databases;
    int format;

    /* profiler */
    rdbProfiler **profiler; /* profiler group by datatype,encoding,isexpired */
    int profiler_num;
} rdbstate;

/* At every loading step try to remember what we were about to do, so that
 * we can log this information when an error is encountered. */
#define RDB_CHECK_DOING_START 0
#define RDB_CHECK_DOING_READ_TYPE 1
#define RDB_CHECK_DOING_READ_EXPIRE 2
#define RDB_CHECK_DOING_READ_KEY 3
#define RDB_CHECK_DOING_READ_OBJECT_VALUE 4
#define RDB_CHECK_DOING_CHECK_SUM 5
#define RDB_CHECK_DOING_READ_LEN 6
#define RDB_CHECK_DOING_READ_AUX 7
#define RDB_CHECK_DOING_READ_MODULE_AUX 8
#define RDB_CHECK_DOING_READ_FUNCTIONS 9

#define OUTPUT_FORMAT_INFO 0
#define OUTPUT_FORMAT_FORM 1

char *rdb_check_doing_string[] = {
    "start",
    "read-type",
    "read-expire",
    "read-key",
    "read-object-value",
    "check-sum",
    "read-len",
    "read-aux",
    "read-module-aux",
    "read-functions",
};

char *rdb_type_string[] = {
    "string",
    "list-linked",
    "set-hashtable",
    "zset-v1",
    "hash-hashtable",
    "zset-v2",
    "module-pre-release",
    "module-value",
    "",
    "hash-zipmap",
    "list-ziplist",
    "set-intset",
    "zset-ziplist",
    "hash-ziplist",
    "quicklist",
    "stream",
    "hash-listpack",
    "zset-listpack",
    "quicklist-v2",
    "stream-v2",
    "set-listpack",
    "stream-v3",
};

char *type_name[OBJ_TYPE_MAX] = {"string", "list", "set", "zset", "hash", "module", /* module type is special */
                                 "stream"};

/********************** Rdb profiler **********************/
void profiler_record_count(size_t eleCount, rdbProfiler *profiler) {
    if (!profiler) return;

    profiler->elements += eleCount;
    if (profiler->elements_max < eleCount) {
        profiler->elements_max = eleCount;
    }
    hdr_record_value(profiler->element_count_histogram, (int64_t)eleCount);
}

void profiler_record_element_size(size_t eleSize, size_t count, rdbProfiler *profiler) {
    if (!profiler) return;

    profiler->all_value_size += eleSize * count;

    profiler->all_elements_size += eleSize * count;
    if (profiler->elements_size_max < eleSize) {
        profiler->elements_size_max = eleSize;
    }

    hdr_record_value(profiler->element_size_histogram, (int64_t)eleSize);
}

void profiler_record_simple(size_t eleSize, size_t eleCount, rdbProfiler *profiler) {
    profiler_record_count(eleCount, profiler);
    profiler_record_element_size(eleSize, eleCount, profiler);
}

void profiler_record_element_size_add(rdbProfiler *to, rdbProfiler *from) {
    if (!to || !from) return;

    to->all_value_size += from->all_value_size;

    to->all_elements_size += from->all_elements_size;
    if (to->elements_size_max < from->elements_size_max) {
        to->elements_size_max = from->elements_size_max;
    }

    hdr_add(to->element_size_histogram, from->element_size_histogram);
}

rdbProfiler *newRdbProfiler(size_t type) {
    rdbProfiler *profiler = zcalloc(sizeof(rdbProfiler));
    if (!profiler) return NULL;

    profiler->type = type;
    hdr_init(LOW_TRACKE_VALUE, MAX_ELEMENTS_TRACKE, 3, &profiler->element_count_histogram);
    hdr_init(LOW_TRACKE_VALUE, MAX_ELEMENTS_SIZE_TRACKE, 3, &profiler->element_size_histogram);
    return profiler;
}

void deleteRdbProfiler(rdbProfiler *profiler) {
    hdr_close(profiler->element_count_histogram);
    hdr_close(profiler->element_size_histogram);
    zfree(profiler);
}

rdbProfiler **initRdbProfiler(size_t num) {
    rdbProfiler **tmp = zmalloc(sizeof(struct rdbProfiler *) * num);

    for (size_t i = 0; i < num; i++) {
        tmp[i] = newRdbProfiler(i % OBJ_TYPE_MAX);
    }

    return tmp;
}

rdbProfiler **tryExpandRdbProfiler(rdbProfiler **profilers, size_t old_num, size_t num) {
    if (old_num >= num) {
        return profilers;
    }

    rdbProfiler **tmp = zrealloc(profilers, sizeof(struct rdbProfiler *) * num);
    serverAssert(tmp != NULL);
    for (size_t i = old_num; i < num; i++) {
        tmp[i] = newRdbProfiler(i % OBJ_TYPE_MAX);
    }

    return tmp;
}

void freeRdbProfile(rdbProfiler **profilers, size_t num) {
    for (size_t i = 0; i < num; i++) {
        deleteRdbProfiler(profilers[i]);
    }

    zfree(profilers);
}

void computeDatasetProfile(int dbid, robj *keyobj, robj *o) {
    UNUSED(dbid);
    UNUSED(keyobj);
    char buf[128];

    rdbProfiler *profiler = rdbstate.profiler[o->type + dbid * OBJ_TYPE_MAX];

    profiler->all_key_size += sdslen(keyobj->ptr);
    profiler->keys++;
    /* Save the key and associated value */
    if (o->type == OBJ_STRING) {
        profiler_record_simple(stringObjectLen(o), 1, profiler);
    } else if (o->type == OBJ_LIST) {
        listTypeIterator *li = listTypeInitIterator(o, 0, LIST_TAIL);
        listTypeEntry entry;
        while (listTypeNext(li, &entry)) {
            robj *eleobj = listTypeGet(&entry);
            profiler_record_element_size(stringObjectLen(eleobj), 1, profiler);
            decrRefCount(eleobj);
        }
        listTypeReleaseIterator(li);
        profiler_record_count(listTypeLength(o), profiler);
    } else if (o->type == OBJ_SET) {
        setTypeIterator *si = setTypeInitIterator(o);
        sds sdsele;
        while ((sdsele = setTypeNextObject(si)) != NULL) {
            profiler_record_element_size(sdslen(sdsele), 1, profiler);
            sdsfree(sdsele);
        }
        setTypeReleaseIterator(si);
        profiler_record_count(setTypeSize(o), profiler);
    } else if (o->type == OBJ_ZSET) {
        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            unsigned char *zl = o->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vll;
            double score;

            eptr = lpSeek(zl, 0);
            serverAssert(eptr != NULL);
            sptr = lpNext(zl, eptr);
            serverAssert(sptr != NULL);

            while (eptr != NULL) {
                size_t eleLen = 0;

                vstr = lpGetValue(eptr, &vlen, &vll);
                score = zzlGetScore(sptr);

                if (vstr != NULL) {
                    eleLen += vlen;
                } else {
                    ll2string(buf, sizeof(buf), vll);
                    eleLen += strlen(buf);
                }
                const int len = fpconv_dtoa(score, buf);
                buf[len] = '\0';
                eleLen += strlen(buf);
                profiler_record_element_size(eleLen, 1, profiler);
                zzlNext(zl, &eptr, &sptr);
            }
            profiler_record_count(lpLength(o->ptr), profiler);
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            hashtableIterator iter;
            hashtableInitIterator(&iter, zs->ht, 0);

            void *next;
            while (hashtableNext(&iter, &next)) {
                zskiplistNode *node = next;
                size_t eleLen = 0;

                const int len = fpconv_dtoa(node->score, buf);
                buf[len] = '\0';
                eleLen += sdslen(node->ele) + strlen(buf);
                profiler_record_element_size(eleLen, 1, profiler);
            }
            hashtableResetIterator(&iter);
            profiler_record_count(hashtableSize(zs->ht), profiler);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        hashTypeIterator hi;
        hashTypeInitIterator(o, &hi);
        while (hashTypeNext(&hi) != C_ERR) {
            sds sdsele;
            size_t eleLen = 0;

            sdsele = hashTypeCurrentObjectNewSds(&hi, OBJ_HASH_FIELD);
            eleLen += sdslen(sdsele);
            sdsfree(sdsele);
            sdsele = hashTypeCurrentObjectNewSds(&hi, OBJ_HASH_VALUE);
            eleLen += sdslen(sdsele);
            sdsfree(sdsele);

            profiler_record_element_size(eleLen, 1, profiler);
        }
        hashTypeResetIterator(&hi);
        profiler_record_count(hashTypeLength(o), profiler);
    } else if (o->type == OBJ_STREAM) {
        streamIterator si;
        streamIteratorStart(&si, o->ptr, NULL, NULL, 0);
        streamID id;
        int64_t numfields;

        while (streamIteratorGetID(&si, &id, &numfields)) {
            while (numfields--) {
                unsigned char *field, *value;
                int64_t field_len, value_len;
                streamIteratorGetField(&si, &field, &value, &field_len, &value_len);
                profiler_record_element_size(field_len + value_len, 1, profiler);
            }
        }
        streamIteratorStop(&si);
        profiler_record_count(streamLength(o), profiler);
    } else if (o->type == OBJ_MODULE) {
        profiler_record_count(1, profiler);
    } else {
        serverPanic("Unknown object type");
    }
}

char *profiler_field_string[] = {
    "type.name",
    "keys.total",
    "expire_keys.total",
    "already_expired.total",
    "keys.size",
    "keys.value_size",
    "elements.total",
    "elements.size",
    "elements.num.max",
    "elements.num.avg",
    "elements.num.p99",
    "elements.num.p90",
    "elements.num.p50",
    "elements.size.max",
    "elements.size.avg",
    "elements.size.p99",
    "elements.size.p90",
    "elements.size.p50",
    NULL};

void rdbProfilerPrintInfo(rdbProfiler *profiler, char *field_string) {
    if (!strcasecmp(field_string, "type.name")) printf("%-5s", type_name[profiler->type]);
    if (!strcasecmp(field_string, "keys.total")) printf("%-5lu", profiler->keys);
    if (!strcasecmp(field_string, "expire_keys.total")) printf("%-5lu", profiler->expires);
    if (!strcasecmp(field_string, "already_expired.total")) printf("%-5lu", profiler->already_expired);
    if (!strcasecmp(field_string, "keys.size")) printf("%-5lu", profiler->all_key_size);
    if (!strcasecmp(field_string, "keys.value_size")) printf("%-5lu", profiler->all_value_size);
    if (!strcasecmp(field_string, "elements.total")) printf("%-5lu", profiler->elements);
    if (!strcasecmp(field_string, "elements.size")) printf("%-5lu", profiler->all_elements_size);
    if (!strcasecmp(field_string, "elements.num.max")) printf("%-5lu", profiler->elements_max);
    if (!strcasecmp(field_string, "elements.num.avg")) printf("%-5.2lf", (profiler->keys > 0 ? (float)profiler->elements / (float)profiler->keys : 0));
    if (!strcasecmp(field_string, "elements.num.p99")) printf("%-5.2lf", (float)hdr_value_at_percentile(profiler->element_count_histogram, 99.0));
    if (!strcasecmp(field_string, "elements.num.p90")) printf("%-5.2lf", (float)hdr_value_at_percentile(profiler->element_count_histogram, 90.0));
    if (!strcasecmp(field_string, "elements.num.p50")) printf("%-5.2lf", (float)hdr_value_at_percentile(profiler->element_count_histogram, 50.0));
    if (!strcasecmp(field_string, "elements.size.max")) printf("%-5lu", profiler->elements_size_max);
    if (!strcasecmp(field_string, "elements.size.avg")) printf("%-5.2lf", (profiler->elements > 0 ? (float)profiler->all_elements_size / (float)profiler->elements : 0));
    if (!strcasecmp(field_string, "elements.size.p99")) printf("%-5.2lf", (float)hdr_value_at_percentile(profiler->element_size_histogram, 99.0));
    if (!strcasecmp(field_string, "elements.size.p90")) printf("%-5.2lf", (float)hdr_value_at_percentile(profiler->element_size_histogram, 90.0));
    if (!strcasecmp(field_string, "elements.size.p50")) printf("%-5.2lf", (float)hdr_value_at_percentile(profiler->element_size_histogram, 50.0));
}

/* Show a few stats collected into 'rdbstate' */
void rdbShowGenericInfo(void) {
    printf("[info] %lu keys read\n", rdbstate.keys);
    printf("[info] %lu expires\n", rdbstate.expires);
    printf("[info] %lu already expired\n", rdbstate.already_expired);

    if (rdbCheckProfiler) {
        char field_string[80];
        for (int dbid = 0; dbid <= rdbstate.databases; dbid++) {
            for (size_t i = 0; profiler_field_string[i] != NULL; i++) {
                if (rdbstate.format == OUTPUT_FORMAT_FORM) {
                    snprintf(field_string, sizeof(field_string), "db.%d.%s", dbid, profiler_field_string[i]);
                    printf("%-30s", field_string);
                }

                for (size_t obj_type = 0; obj_type < OBJ_TYPE_MAX; obj_type++) {
                    const size_t profiler_idx = obj_type + dbid * OBJ_TYPE_MAX;
                    rdbProfiler *profiler = rdbstate.profiler[profiler_idx];

                    if (rdbstate.format == OUTPUT_FORMAT_INFO) {
                        if (i == 0) continue;
                        snprintf(field_string, sizeof(field_string), "[info] db.%d.type.%s.%s", dbid, type_name[profiler->type], profiler_field_string[i]);
                        printf("%s:", field_string);
                    }

                    if (rdbstate.format == OUTPUT_FORMAT_FORM) {
                        printf("\t");
                    }

                    rdbProfilerPrintInfo(profiler, profiler_field_string[i]);
                    if (rdbstate.format == OUTPUT_FORMAT_INFO) {
                        printf("\n");
                    }
                }
                if (rdbstate.format == OUTPUT_FORMAT_FORM)
                    printf("\n");
            }
        }
    }
}

/* Called on RDB errors. Provides details about the RDB and the offset
 * we were when the error was detected. */
void rdbCheckError(const char *fmt, ...) {
    char msg[1024];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    printf("--- RDB ERROR DETECTED ---\n");
    printf("[offset %llu] %s\n", (unsigned long long)(rdbstate.rio ? rdbstate.rio->processed_bytes : 0), msg);
    printf("[additional info] While doing: %s\n", rdb_check_doing_string[rdbstate.doing]);
    if (rdbstate.key) printf("[additional info] Reading key '%s'\n", (char *)rdbstate.key->ptr);
    if (rdbstate.key_type != -1)
        printf("[additional info] Reading type %d (%s)\n", rdbstate.key_type,
               ((unsigned)rdbstate.key_type < sizeof(rdb_type_string) / sizeof(char *))
                   ? rdb_type_string[rdbstate.key_type]
                   : "unknown");
    rdbShowGenericInfo();
}

/* Print information during RDB checking. */
void rdbCheckInfo(const char *fmt, ...) {
    char msg[1024];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    printf("[offset %llu] %s\n", (unsigned long long)(rdbstate.rio ? rdbstate.rio->processed_bytes : 0), msg);
}

/* Used inside rdb.c in order to log specific errors happening inside
 * the RDB loading internals. */
void rdbCheckSetError(const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(rdbstate.error, sizeof(rdbstate.error), fmt, ap);
    va_end(ap);
    rdbstate.error_set = 1;
}

/* During RDB check we setup a special signal handler for memory violations
 * and similar conditions, so that we can log the offending part of the RDB
 * if the crash is due to broken content. */
void rdbCheckHandleCrash(int sig, siginfo_t *info, void *secret) {
    UNUSED(sig);
    UNUSED(info);
    UNUSED(secret);

    rdbCheckError("Server crash checking the specified RDB file!");
    exit(1);
}

void rdbCheckSetupSignals(void) {
    struct sigaction act;

    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = rdbCheckHandleCrash;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
}

/* Check the specified RDB file. Return 0 if the RDB looks sane, otherwise
 * 1 is returned.
 * The file is specified as a filename in 'rdbfilename' if 'fp' is NULL,
 * otherwise the already open file 'fp' is checked. */
int redis_check_rdb(char *rdbfilename, FILE *fp) {
    uint64_t dbid;
    int selected_dbid = -1;
    int type, rdbver;
    char buf[1024];
    long long expiretime, now = mstime();
    static rio rdb; /* Pointed by global struct riostate. */
    struct stat sb;

    int closefile = (fp == NULL);
    if (fp == NULL && (fp = fopen(rdbfilename, "r")) == NULL) return 1;

    if (fstat(fileno(fp), &sb) == -1) sb.st_size = 0;

    startLoadingFile(sb.st_size, rdbfilename, RDBFLAGS_NONE);
    rioInitWithFile(&rdb, fp);
    rdbstate.rio = &rdb;
    rdb.update_cksum = rdbLoadProgressCallback;
    if (rioRead(&rdb, buf, 9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf, "REDIS", 5) != 0) {
        rdbCheckError("Wrong signature trying to load DB from file");
        goto err;
    }
    rdbver = atoi(buf + 5);
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        rdbCheckError("Can't handle RDB format version %d", rdbver);
        goto err;
    }

    expiretime = -1;
    while (1) {
        robj *key, *val;

        /* Read type. */
        rdbstate.doing = RDB_CHECK_DOING_READ_TYPE;
        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;

        /* Handle special types. */
        if (type == RDB_OPCODE_EXPIRETIME) {
            rdbstate.doing = RDB_CHECK_DOING_READ_EXPIRE;
            /* EXPIRETIME: load an expire associated with the next key
             * to load. Note that after loading an expire we need to
             * load the actual type, and continue. */
            expiretime = rdbLoadTime(&rdb);
            expiretime *= 1000;
            if (rioGetReadError(&rdb)) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            /* EXPIRETIME_MS: milliseconds precision expire times introduced
             * with RDB v3. Like EXPIRETIME but no with more precision. */
            rdbstate.doing = RDB_CHECK_DOING_READ_EXPIRE;
            expiretime = rdbLoadMillisecondTime(&rdb, rdbver);
            if (rioGetReadError(&rdb)) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_FREQ) {
            /* FREQ: LFU frequency. */
            uint8_t byte;
            if (rioRead(&rdb, &byte, 1) == 0) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_IDLE) {
            /* IDLE: LRU idle time. */
            if (rdbLoadLen(&rdb, NULL) == RDB_LENERR) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_EOF) {
            /* EOF: End of file, exit the main loop. */
            break;
        } else if (type == RDB_OPCODE_SELECTDB) {
            /* SELECTDB: Select the specified database. */
            rdbstate.doing = RDB_CHECK_DOING_READ_LEN;
            if ((dbid = rdbLoadLen(&rdb, NULL)) == RDB_LENERR) goto eoferr;
            rdbCheckInfo("Selecting DB ID %llu", (unsigned long long)dbid);
            selected_dbid = dbid;
            if (selected_dbid > rdbstate.databases) {
                rdbstate.databases = dbid;
            }

            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_RESIZEDB) {
            /* RESIZEDB: Hint about the size of the keys in the currently
             * selected data base, in order to avoid useless rehashing. */
            uint64_t db_size, expires_size;
            rdbstate.doing = RDB_CHECK_DOING_READ_LEN;
            if ((db_size = rdbLoadLen(&rdb, NULL)) == RDB_LENERR) goto eoferr;
            if ((expires_size = rdbLoadLen(&rdb, NULL)) == RDB_LENERR) goto eoferr;
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_AUX) {
            /* AUX: generic string-string fields. Use to add state to RDB
             * which is backward compatible. Implementations of RDB loading
             * are required to skip AUX fields they don't understand.
             *
             * An AUX field is composed of two strings: key and value. */
            robj *auxkey, *auxval;
            rdbstate.doing = RDB_CHECK_DOING_READ_AUX;
            if ((auxkey = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
            if ((auxval = rdbLoadStringObject(&rdb)) == NULL) {
                decrRefCount(auxkey);
                goto eoferr;
            }

            rdbCheckInfo("AUX FIELD %s = '%s'", (char *)auxkey->ptr, (char *)auxval->ptr);
            decrRefCount(auxkey);
            decrRefCount(auxval);
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_MODULE_AUX) {
            /* AUX: Auxiliary data for modules. */
            uint64_t moduleid, when_opcode, when;
            rdbstate.doing = RDB_CHECK_DOING_READ_MODULE_AUX;
            if ((moduleid = rdbLoadLen(&rdb, NULL)) == RDB_LENERR) goto eoferr;
            if ((when_opcode = rdbLoadLen(&rdb, NULL)) == RDB_LENERR) goto eoferr;
            if ((when = rdbLoadLen(&rdb, NULL)) == RDB_LENERR) goto eoferr;
            if (when_opcode != RDB_MODULE_OPCODE_UINT) {
                rdbCheckError("bad when_opcode");
                goto err;
            }

            char name[10];
            moduleTypeNameByID(name, moduleid);
            rdbCheckInfo("MODULE AUX for: %s", name);

            robj *o = rdbLoadCheckModuleValue(&rdb, name);
            decrRefCount(o);
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_FUNCTION_PRE_GA) {
            rdbCheckError("Pre-release function format not supported %d", rdbver);
            goto err;
        } else if (type == RDB_OPCODE_FUNCTION2) {
            sds err = NULL;
            rdbstate.doing = RDB_CHECK_DOING_READ_FUNCTIONS;
            if (rdbFunctionLoad(&rdb, rdbver, NULL, 0, &err) != C_OK) {
                rdbCheckError("Failed loading library, %s", err);
                sdsfree(err);
                goto err;
            }
            continue;
        } else {
            if (!rdbIsObjectType(type)) {
                rdbCheckError("Invalid object type: %d", type);
                goto err;
            }
            rdbstate.key_type = type;
        }

        /* Read key */
        rdbstate.doing = RDB_CHECK_DOING_READ_KEY;
        if ((key = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
        rdbstate.key = key;
        rdbstate.keys++;
        /* Read value */
        rdbstate.doing = RDB_CHECK_DOING_READ_OBJECT_VALUE;
        if ((val = rdbLoadObject(type, &rdb, key->ptr, selected_dbid, NULL)) == NULL) goto eoferr;
        if (rdbCheckProfiler) {
            int max_profiler_num = (rdbstate.databases + 1) * OBJ_TYPE_MAX;
            if (max_profiler_num > rdbstate.profiler_num) {
                rdbstate.profiler = tryExpandRdbProfiler(rdbstate.profiler, rdbstate.profiler_num, max_profiler_num);
                rdbstate.profiler_num = max_profiler_num;
            }

            computeDatasetProfile(selected_dbid, key, val);
        }
        /* Check if the key already expired. */
        if (expiretime != -1 && expiretime < now) rdbstate.already_expired++;
        if (expiretime != -1) rdbstate.expires++;
        rdbstate.key = NULL;
        decrRefCount(key);
        decrRefCount(val);
        rdbstate.key_type = -1;
        expiretime = -1;
    }
    /* Verify the checksum if RDB version is >= 5 */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;

        rdbstate.doing = RDB_CHECK_DOING_CHECK_SUM;
        if (rioRead(&rdb, &cksum, 8) == 0) goto eoferr;
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            rdbCheckInfo("RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            rdbCheckError("RDB CRC error");
            goto err;
        } else {
            rdbCheckInfo("Checksum OK");
        }
    }

    if (closefile) fclose(fp);
    stopLoading(1);
    return 0;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    if (rdbstate.error_set) {
        rdbCheckError(rdbstate.error);
    } else {
        rdbCheckError("Unexpected EOF reading RDB file");
    }
err:
    if (closefile) fclose(fp);
    stopLoading(0);
    return 1;
}

void parseCheckRdbOptions(int argc, char **argv, FILE *fp) {
    int i = 1;
    int lastarg;

    if (argc < 2 && fp == NULL) {
        goto checkRdbUsage;
    }

    rdbstate.format = OUTPUT_FORMAT_INFO;

    for (i = 2; i < argc; i++) {
        lastarg = (i == (argc - 1));
        if (!strcmp(argv[1], "-v") || !strcmp(argv[1], "--version")) {
            sds version = getVersion();
            printf("valkey-check-rdb %s\n", version);
            sdsfree(version);
            exit(0);
        } else if (!strcmp(argv[i], "--profiler")) {
            rdbCheckProfiler = 1;
        } else if (!strcmp(argv[i], "--format")) {
            if (lastarg) goto checkRdbUsage;
            char *format = argv[i + 1];
            if (!strcmp(format, "form")) {
                rdbstate.format = OUTPUT_FORMAT_FORM;
            } else if (!strcmp(format, "info")) {
                rdbstate.format = OUTPUT_FORMAT_INFO;
            } else {
                goto checkRdbUsage;
            }
            i++;
        } else {
            goto checkRdbUsage;
        }
    }

    return;

checkRdbUsage:
    fprintf(stderr, "Usage: %s <rdb-file-name> [--format form|info] [--profiler]\n", argv[0]);
    exit(1);
}

/* RDB check main: called form server.c when the server is executed with the
 * valkey-check-rdb alias, on during RDB loading errors.
 *
 * The function works in two ways: can be called with argc/argv as a
 * standalone executable, or called with a non NULL 'fp' argument if we
 * already have an open file to check. This happens when the function
 * is used to check an RDB preamble inside an AOF file.
 *
 * When called with fp = NULL, the function never returns, but exits with the
 * status code according to success (RDB is sane) or error (RDB is corrupted).
 * Otherwise if called with a non NULL fp, the function returns C_OK or
 * C_ERR depending on the success or failure. */
int redis_check_rdb_main(int argc, char **argv, FILE *fp) {
    parseCheckRdbOptions(argc, argv, fp);

    struct timeval tv;

    gettimeofday(&tv, NULL);
    init_genrand64(((long long)tv.tv_sec * 1000000 + tv.tv_usec) ^ getpid());

    rdbstate.profiler = initRdbProfiler(OBJ_TYPE_MAX);
    rdbstate.profiler_num = OBJ_TYPE_MAX;
    rdbstate.databases = 1;

    /* In order to call the loading functions we need to create the shared
     * integer objects, however since this function may be called from
     * an already initialized server instance, check if we really need to. */
    if (shared.integers[0] == NULL) createSharedObjects();
    server.loading_process_events_interval_bytes = 0;
    server.sanitize_dump_payload = SANITIZE_DUMP_YES;
    rdbCheckMode = 1;
    rdbCheckInfo("Checking RDB file %s", argv[1]);
    rdbCheckSetupSignals();
    int retval = redis_check_rdb(argv[1], fp);
    if (retval == 0) {
        rdbCheckInfo("\\o/ RDB looks OK! \\o/");
        rdbShowGenericInfo();
    }
    if (fp) return (retval == 0) ? C_OK : C_ERR;
    freeRdbProfile(rdbstate.profiler, rdbstate.profiler_num);
    exit(retval);
}
