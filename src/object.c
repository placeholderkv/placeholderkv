/* Object implementation.
 *
 * Copyright (c) 2009-2012, Redis Ltd.
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
#include "serverassert.h"
#include "functions.h"
#include "intset.h" /* Compact integer set structure */
#include "zmalloc.h"
#include "sds.h"
#include "module.h"
#include <math.h>
#include <ctype.h>

#ifdef __CYGWIN__
#define strtold(a, b) ((long double)strtod((a), (b)))
#endif

/* For objects with large embedded keys, we reserve space for an expire field,
 * so if expire is set later, we don't need to reallocate the object. */
#define KEY_SIZE_TO_INCLUDE_EXPIRE_THRESHOLD 128

/* ===================== Creation and parsing of objects ==================== */

/* Creates an object, optionally with embedded key and expire fields. The key
 * and expire fields can be omitted by passing NULL and -1, respectively. */
robj *createObjectWithKeyAndExpire(int type, void *ptr, const sds key, long long expire) {
    /* Calculate sizes */
    int has_expire = (expire != -1 ||
                      (key != NULL && sdslen(key) >= KEY_SIZE_TO_INCLUDE_EXPIRE_THRESHOLD));
    size_t key_sds_size = 0;
    size_t min_size = sizeof(robj);
    if (has_expire) {
        min_size += sizeof(long long);
    }
    if (key != NULL) {
        /* Size of embedded key, incl. 1 byte for prefixed sds hdr size. */
        key_sds_size = sdscopytobuffer(NULL, 0, key, NULL);
        min_size += 1 + key_sds_size;
    }
    /* Allocate and set the declared fields. */
    size_t bufsize = 0;
    robj *o = zmalloc_usable(min_size, &bufsize);
    o->type = type;
    o->encoding = OBJ_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;
    o->lru = 0;
    o->hasembkey = (key != NULL);

    /* If the allocation has enough space for an expire field, add it even if we
     * don't need it now. Then we don't need to realloc if it's needed later. */
    if (key != NULL && !has_expire && bufsize >= min_size + sizeof(long long)) {
        has_expire = 1;
        min_size += sizeof(long long);
    }
    o->hasexpire = has_expire;

    /* The memory after the struct where we embedded data. */
    unsigned char *data = (void *)(o + 1);

    /* Set the expire field. */
    if (o->hasexpire) {
        *(long long *)data = expire;
        data += sizeof(long long);
    }

    /* Copy embedded key. */
    if (o->hasembkey) {
        sdscopytobuffer(data + 1, key_sds_size, key, data);
        data += 1 + key_sds_size;
    }

    return o;
}

robj *createObject(int type, void *ptr) {
    return createObjectWithKeyAndExpire(type, ptr, NULL, -1);
}

void initObjectLRUOrLFU(robj *o) {
    if (o->refcount == OBJ_SHARED_REFCOUNT) return;
    /* Set the LRU to the current lruclock (minutes resolution), or
     * alternatively the LFU counter. */
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        o->lru = (LFUGetTimeInMinutes() << 8) | LFU_INIT_VAL;
    } else {
        o->lru = LRU_CLOCK();
    }
    return;
}

/* Set a special refcount in the object to make it "shared":
 * incrRefCount and decrRefCount() will test for this special refcount
 * and will not touch the object. This way it is free to access shared
 * objects such as small integers from different threads without any
 * mutex.
 *
 * A common pattern to create shared objects:
 *
 * robj *myobject = makeObjectShared(createObject(...));
 *
 */
robj *makeObjectShared(robj *o) {
    serverAssert(o->refcount == 1);
    o->refcount = OBJ_SHARED_REFCOUNT;
    return o;
}

/* Create a string object with encoding OBJ_ENCODING_RAW, that is a plain
 * string object where o->ptr points to a proper sds string. */
robj *createRawStringObject(const char *ptr, size_t len) {
    return createObject(OBJ_STRING, sdsnewlen(ptr, len));
}

/* Creates a new embedded string object and copies the content of key, val and
 * expire to the new object. LRU is set to 0. */
static robj *createEmbeddedStringObjectWithKeyAndExpire(const char *val_ptr,
                                                        size_t val_len,
                                                        const sds key,
                                                        long long expire) {
    /* Calculate sizes */
    size_t key_sds_size = 0;
    size_t min_size = sizeof(robj);
    if (expire != -1) {
        min_size += sizeof(long long);
    }
    if (key != NULL) {
        /* Size of embedded key, incl. 1 byte for prefixed sds hdr size. */
        key_sds_size = sdscopytobuffer(NULL, 0, key, NULL);
        min_size += 1 + key_sds_size;
    }
    /* Size of embedded value (EMBSTR) including \0 term. */
    min_size += sizeof(struct sdshdr8) + val_len + 1;

    /* Allocate and set the declared fields. */
    size_t bufsize = 0;
    robj *o = zmalloc_usable(min_size, &bufsize);
    o->type = OBJ_STRING;
    o->encoding = OBJ_ENCODING_EMBSTR;
    o->refcount = 1;
    o->lru = 0;
    o->hasexpire = (expire != -1);
    o->hasembkey = (key != NULL);

    /* If the allocation has enough space for an expire field, add it even if we
     * don't need it now. Then we don't need to realloc if it's needed later. */
    if (!o->hasexpire && bufsize >= min_size + sizeof(long long)) {
        o->hasexpire = 1;
        min_size += sizeof(long long);
    }

    /* The memory after the struct where we embedded data. */
    unsigned char *data = (void *)(o + 1);

    /* Set the expire field. */
    if (o->hasexpire) {
        *(long long *)data = expire;
        data += sizeof(long long);
    }

    /* Copy embedded key. */
    if (o->hasembkey) {
        sdscopytobuffer(data + 1, key_sds_size, key, data);
        data += 1 + key_sds_size;
    }

    /* Copy embedded value (EMBSTR). */
    struct sdshdr8 *sh = (void *)data;
    sh->flags = SDS_TYPE_8;
    sh->len = val_len;
    size_t capacity = bufsize - (min_size - val_len);
    sh->alloc = capacity;
    serverAssert(capacity == sh->alloc); /* Overflow check. */
    if (val_ptr == SDS_NOINIT) {
        sh->buf[val_len] = '\0';
    } else if (val_ptr != NULL) {
        memcpy(sh->buf, val_ptr, val_len);
        sh->buf[val_len] = '\0';
    } else {
        memset(sh->buf, 0, val_len + 1);
    }
    o->ptr = sh->buf;

    return o;
}

/* Create a string object with encoding OBJ_ENCODING_EMBSTR, that is
 * an object where the sds string is actually an unmodifiable string
 * allocated in the same chunk as the object itself. */
robj *createEmbeddedStringObject(const char *ptr, size_t len) {
    return createEmbeddedStringObjectWithKeyAndExpire(ptr, len, NULL, -1);
}

/* Create a string object with EMBSTR encoding if it is smaller than
 * OBJ_ENCODING_EMBSTR_SIZE_LIMIT, otherwise the RAW encoding is
 * used.
 *
 * The current limit of 44 is chosen so that the biggest string object
 * we allocate as EMBSTR will still fit into the 64 byte arena of jemalloc. */
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44
robj *createStringObject(const char *ptr, size_t len) {
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr, len);
    else
        return createRawStringObject(ptr, len);
}

robj *createStringObjectWithKeyAndExpire(const char *ptr, size_t len, const sds key, long long expire) {
    /* When to embed? Embed when the sum is up to 64 bytes. There may be better
     * heuristics, e.g. we can look at the jemalloc sizes (16-byte intervals up
     * to 128 bytes). */
    size_t size = sizeof(robj);
    size += (key != NULL) * (sdslen(key) + 3); /* hdr size (1) + hdr (1) + nullterm (1) */
    size += (expire != -1) * sizeof(long long);
    size += 4 + len; /* embstr header (3) + nullterm (1) */
    if (size <= 64) {
        return createEmbeddedStringObjectWithKeyAndExpire(ptr, len, key, expire);
    } else {
        return createObjectWithKeyAndExpire(OBJ_STRING, sdsnewlen(ptr, len), key, expire);
    }
}

sds objectGetKey(const robj *val) {
    unsigned char *data = (void *)(val + 1);
    if (val->hasexpire) {
        /* Skip expire field */
        data += sizeof(long long);
    }
    if (val->hasembkey) {
        uint8_t hdr_size = *(uint8_t *)data;
        data += 1 + hdr_size;
        return (sds)data;
    }
    return NULL;
}

long long objectGetExpire(const robj *val) {
    unsigned char *data = (void *)(val + 1);
    if (val->hasexpire) {
        return *(long long *)data;
    } else {
        return -1;
    }
}

/* This functions may reallocate the value. The new allocation is returned and
 * the old object's reference counter is decremented and possibly freed. Use the
 * returned object instead of 'val' after calling this function. */
robj *objectSetExpire(robj *val, long long expire) {
    if (val->hasexpire) {
        /* Update existing expire field. */
        unsigned char *data = (void *)(val + 1);
        *(long long *)data = expire;
        return val;
    } else if (expire == -1) {
        return val;
    } else {
        return objectSetKeyAndExpire(val, objectGetKey(val), expire);
    }
}

/* This functions may reallocate the value. The new allocation is returned and
 * the old object's reference counter is decremented and possibly freed. Use the
 * returned object instead of 'val' after calling this function. */
robj *objectSetKeyAndExpire(robj *val, sds key, long long expire) {
    if (val->type == OBJ_STRING && val->encoding == OBJ_ENCODING_EMBSTR) {
        robj *new = createStringObjectWithKeyAndExpire(val->ptr, sdslen(val->ptr), key, expire);
        new->lru = val->lru;
        decrRefCount(val);
        return new;
    }

    /* Create a new object with embedded key. Reuse ptr if possible. */
    void *ptr;
    if (val->refcount == 1) {
        /* Reuse the ptr. There are no other references to val. */
        ptr = val->ptr;
        val->ptr = NULL;
    } else if (val->type == OBJ_STRING && val->encoding == OBJ_ENCODING_INT) {
        /* The pointer is not allocated memory. We can just copy the pointer. */
        ptr = val->ptr;
    } else if (val->type == OBJ_STRING && val->encoding == OBJ_ENCODING_RAW) {
        /* Dup the string. */
        ptr = sdsdup(val->ptr);
    } else {
        serverAssert(val->type != OBJ_STRING);
        /* There are multiple references to this non-string object. Most types
         * can be duplicated, but for a module type is not always possible. */
        serverPanic("Not implemented");
    }
    robj *new = createObjectWithKeyAndExpire(val->type, ptr, key, expire);
    new->encoding = val->encoding;
    new->lru = val->lru;
    decrRefCount(val);
    return new;
}

/* Same as CreateRawStringObject, can return NULL if allocation fails */
robj *tryCreateRawStringObject(const char *ptr, size_t len) {
    sds str = sdstrynewlen(ptr, len);
    if (!str) return NULL;
    return createObject(OBJ_STRING, str);
}

/* Same as createStringObject, can return NULL if allocation fails */
robj *tryCreateStringObject(const char *ptr, size_t len) {
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr, len);
    else
        return tryCreateRawStringObject(ptr, len);
}

/* Create a string object from a long long value according to the specified flag. */
#define LL2STROBJ_AUTO 0       /* automatically create the optimal string object */
#define LL2STROBJ_NO_SHARED 1  /* disallow shared objects */
#define LL2STROBJ_NO_INT_ENC 2 /* disallow integer encoded objects. */
robj *createStringObjectFromLongLongWithOptions(long long value, int flag) {
    robj *o;

    if (value >= 0 && value < OBJ_SHARED_INTEGERS && flag == LL2STROBJ_AUTO) {
        o = shared.integers[value];
    } else {
        if ((value >= LONG_MIN && value <= LONG_MAX) && flag != LL2STROBJ_NO_INT_ENC) {
            o = createObject(OBJ_STRING, NULL);
            o->encoding = OBJ_ENCODING_INT;
            o->ptr = (void *)((long)value);
        } else {
            char buf[LONG_STR_SIZE];
            int len = ll2string(buf, sizeof(buf), value);
            o = createStringObject(buf, len);
        }
    }
    return o;
}

/* Wrapper for createStringObjectFromLongLongWithOptions() always demanding
 * to create a shared object if possible. */
robj *createStringObjectFromLongLong(long long value) {
    return createStringObjectFromLongLongWithOptions(value, LL2STROBJ_AUTO);
}

/* The function doesn't return a shared integer when the object is used as a
 * value in the key space (for instance when the INCR command is used). */
robj *createStringObjectFromLongLongForValue(long long value) {
    return createStringObjectFromLongLongWithOptions(value, LL2STROBJ_NO_SHARED);
}

/* Create a string object that contains an sds inside it. That means it can't be
 * integer encoded (OBJ_ENCODING_INT), and it'll always be an EMBSTR type. */
robj *createStringObjectFromLongLongWithSds(long long value) {
    return createStringObjectFromLongLongWithOptions(value, LL2STROBJ_NO_INT_ENC);
}

/* Create a string object from a long double. If humanfriendly is non-zero
 * it does not use exponential format and trims trailing zeroes at the end,
 * however this results in loss of precision. Otherwise exp format is used
 * and the output of snprintf() is not modified.
 *
 * The 'humanfriendly' option is used for INCRBYFLOAT and HINCRBYFLOAT. */
robj *createStringObjectFromLongDouble(long double value, int humanfriendly) {
    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf, sizeof(buf), value, humanfriendly ? LD_STR_HUMAN : LD_STR_AUTO);
    return createStringObject(buf, len);
}

/* Duplicate a string object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * This function also guarantees that duplicating a small integer object
 * (or a string object that contains a representation of a small integer)
 * will always result in a fresh object that is unshared (refcount == 1).
 *
 * The resulting object always has refcount set to 1. */
robj *dupStringObject(const robj *o) {
    robj *d;

    serverAssert(o->type == OBJ_STRING);

    switch (o->encoding) {
    case OBJ_ENCODING_RAW: return createRawStringObject(o->ptr, sdslen(o->ptr));
    case OBJ_ENCODING_EMBSTR: return createEmbeddedStringObject(o->ptr, sdslen(o->ptr));
    case OBJ_ENCODING_INT:
        d = createObject(OBJ_STRING, NULL);
        d->encoding = OBJ_ENCODING_INT;
        d->ptr = o->ptr;
        return d;
    default: serverPanic("Wrong encoding."); break;
    }
}

robj *createQuicklistObject(int fill, int compress) {
    quicklist *l = quicklistNew(fill, compress);
    robj *o = createObject(OBJ_LIST, l);
    o->encoding = OBJ_ENCODING_QUICKLIST;
    return o;
}

robj *createListListpackObject(void) {
    unsigned char *lp = lpNew(0);
    robj *o = createObject(OBJ_LIST, lp);
    o->encoding = OBJ_ENCODING_LISTPACK;
    return o;
}

robj *createSetObject(void) {
    hashtable *ht = hashtableCreate(&setHashtableType);
    robj *o = createObject(OBJ_SET, ht);
    o->encoding = OBJ_ENCODING_HASHTABLE;
    return o;
}

robj *createIntsetObject(void) {
    intset *is = intsetNew();
    robj *o = createObject(OBJ_SET, is);
    o->encoding = OBJ_ENCODING_INTSET;
    return o;
}

robj *createSetListpackObject(void) {
    unsigned char *lp = lpNew(0);
    robj *o = createObject(OBJ_SET, lp);
    o->encoding = OBJ_ENCODING_LISTPACK;
    return o;
}

robj *createHashObject(void) {
    unsigned char *zl = lpNew(0);
    robj *o = createObject(OBJ_HASH, zl);
    o->encoding = OBJ_ENCODING_LISTPACK;
    return o;
}

robj *createZsetObject(void) {
    zset *zs = zmalloc(sizeof(*zs));
    robj *o;

    zs->ht = hashtableCreate(&zsetHashtableType);
    zs->zsl = zslCreate();
    o = createObject(OBJ_ZSET, zs);
    o->encoding = OBJ_ENCODING_SKIPLIST;
    return o;
}

robj *createZsetListpackObject(void) {
    unsigned char *lp = lpNew(0);
    robj *o = createObject(OBJ_ZSET, lp);
    o->encoding = OBJ_ENCODING_LISTPACK;
    return o;
}

robj *createStreamObject(void) {
    stream *s = streamNew();
    robj *o = createObject(OBJ_STREAM, s);
    o->encoding = OBJ_ENCODING_STREAM;
    return o;
}

robj *createModuleObject(moduleType *mt, void *value) {
    moduleValue *mv = zmalloc(sizeof(*mv));
    mv->type = mt;
    mv->value = value;
    return createObject(OBJ_MODULE, mv);
}

void freeStringObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

void freeListObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistRelease(o->ptr);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        lpFree(o->ptr);
    } else {
        serverPanic("Unknown list encoding type");
    }
}

void freeSetObject(robj *o) {
    switch (o->encoding) {
    case OBJ_ENCODING_HASHTABLE: hashtableRelease((hashtable *)o->ptr); break;
    case OBJ_ENCODING_INTSET:
    case OBJ_ENCODING_LISTPACK: zfree(o->ptr); break;
    default: serverPanic("Unknown set encoding type");
    }
}

void freeZsetObject(robj *o) {
    zset *zs;
    switch (o->encoding) {
    case OBJ_ENCODING_SKIPLIST:
        zs = o->ptr;
        hashtableRelease(zs->ht);
        zslFree(zs->zsl);
        zfree(zs);
        break;
    case OBJ_ENCODING_LISTPACK: zfree(o->ptr); break;
    default: serverPanic("Unknown sorted set encoding");
    }
}

void freeHashObject(robj *o) {
    switch (o->encoding) {
    case OBJ_ENCODING_HASHTABLE: hashtableRelease((hashtable *)o->ptr); break;
    case OBJ_ENCODING_LISTPACK: lpFree(o->ptr); break;
    default: serverPanic("Unknown hash encoding type"); break;
    }
}

void freeModuleObject(robj *o) {
    moduleValue *mv = o->ptr;
    mv->type->free(mv->value);
    zfree(mv);
}

void freeStreamObject(robj *o) {
    freeStream(o->ptr);
}

void incrRefCount(robj *o) {
    if (o->refcount < OBJ_FIRST_SPECIAL_REFCOUNT) {
        o->refcount++;
    } else {
        if (o->refcount == OBJ_SHARED_REFCOUNT) {
            /* Nothing to do: this refcount is immutable. */
        } else if (o->refcount == OBJ_STATIC_REFCOUNT) {
            serverPanic("You tried to retain an object allocated in the stack");
        }
    }
}

void decrRefCount(robj *o) {
    if (o->refcount == 1) {
        if (o->ptr != NULL) {
            switch (o->type) {
            case OBJ_STRING: freeStringObject(o); break;
            case OBJ_LIST: freeListObject(o); break;
            case OBJ_SET: freeSetObject(o); break;
            case OBJ_ZSET: freeZsetObject(o); break;
            case OBJ_HASH: freeHashObject(o); break;
            case OBJ_MODULE: freeModuleObject(o); break;
            case OBJ_STREAM: freeStreamObject(o); break;
            default: serverPanic("Unknown object type"); break;
            }
        }
        zfree(o);
    } else {
        if (o->refcount <= 0) serverPanic("decrRefCount against refcount <= 0");
        if (o->refcount != OBJ_SHARED_REFCOUNT) o->refcount--;
    }
}

/* See dismissObject(). sds is an exception, because the allocation
 * size is known. Instead of dismissing it with madvise(MADV_DONTNEED)
 * we free it via the allocator, which has minimal overhead when the
 * size is known. This has advantage that it allows the allocator to
 * accumulate free buffers to free whole pages, while madvise is nop
 * if the buffer is less than a page.  */
void dismissSds(sds s) {
    sdsfree(s);
}

/* See dismissObject() */
void dismissStringObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_RAW) {
        dismissSds(o->ptr);
    }
}

/* See dismissObject() */
void dismissListObject(robj *o, size_t size_hint) {
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = o->ptr;
        serverAssert(ql->len != 0);
        /* We iterate all nodes only when average node size is bigger than a
         * page size, and there's a high chance we'll actually dismiss something. */
        if (size_hint / ql->len >= server.page_size) {
            quicklistNode *node = ql->head;
            while (node) {
                if (quicklistNodeIsCompressed(node)) {
                    dismissMemory(node->entry, ((quicklistLZF *)node->entry)->sz);
                } else {
                    dismissMemory(node->entry, node->sz);
                }
                node = node->next;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        dismissMemory(o->ptr, lpBytes((unsigned char *)o->ptr));
    } else {
        serverPanic("Unknown list encoding type");
    }
}

/* See dismissObject() */
void dismissSetObject(robj *o, size_t size_hint) {
    if (o->encoding == OBJ_ENCODING_HASHTABLE) {
        hashtable *ht = o->ptr;
        serverAssert(hashtableSize(ht) != 0);
        /* We iterate all nodes only when average member size is bigger than a
         * page size, and there's a high chance we'll actually dismiss something. */
        if (size_hint / hashtableSize(ht) >= server.page_size) {
            hashtableIterator iter;
            hashtableInitIterator(&iter, ht, 0);
            void *next;
            while (hashtableNext(&iter, &next)) {
                sds item = next;
                dismissSds(item);
            }
            hashtableResetIterator(&iter);
        }

        dismissHashtable(ht);
    } else if (o->encoding == OBJ_ENCODING_INTSET) {
        dismissMemory(o->ptr, intsetBlobLen((intset *)o->ptr));
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        dismissMemory(o->ptr, lpBytes((unsigned char *)o->ptr));
    } else {
        serverPanic("Unknown set encoding type");
    }
}

/* See dismissObject() */
void dismissZsetObject(robj *o, size_t size_hint) {
    if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        zskiplist *zsl = zs->zsl;
        serverAssert(zsl->length != 0);
        /* We iterate all nodes only when average member size is bigger than a
         * page size, and there's a high chance we'll actually dismiss something. */
        if (size_hint / zsl->length >= server.page_size) {
            zskiplistNode *zn = zsl->tail;
            while (zn != NULL) {
                dismissSds(zn->ele);
                zn = zn->backward;
            }
        }

        dismissHashtable(zs->ht);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        dismissMemory(o->ptr, lpBytes((unsigned char *)o->ptr));
    } else {
        serverPanic("Unknown zset encoding type");
    }
}

/* See dismissObject() */
void dismissHashObject(robj *o, size_t size_hint) {
    if (o->encoding == OBJ_ENCODING_HASHTABLE) {
        hashtable *ht = o->ptr;
        serverAssert(hashtableSize(ht) != 0);
        /* We iterate all fields only when average field/value size is bigger than
         * a page size, and there's a high chance we'll actually dismiss something. */
        if (size_hint / hashtableSize(ht) >= server.page_size) {
            hashtableIterator iter;
            hashtableInitIterator(&iter, ht, 0);
            void *next;
            while (hashtableNext(&iter, &next)) {
                dismissHashTypeEntry(next);
            }
            hashtableResetIterator(&iter);
        }

        dismissHashtable(ht);
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        dismissMemory(o->ptr, lpBytes((unsigned char *)o->ptr));
    } else {
        serverPanic("Unknown hash encoding type");
    }
}

/* See dismissObject() */
void dismissStreamObject(robj *o, size_t size_hint) {
    stream *s = o->ptr;
    rax *rax = s->rax;
    if (raxSize(rax) == 0) return;

    /* Iterate only on stream entries, although size_hint may include serialized
     * consumer groups info, but usually, stream entries take up most of
     * the space. */
    if (size_hint / raxSize(rax) >= server.page_size) {
        raxIterator ri;
        raxStart(&ri, rax);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri)) {
            dismissMemory(ri.data, lpBytes(ri.data));
        }
        raxStop(&ri);
    }
}

/* When creating a snapshot in a fork child process, the main process and child
 * process share the same physical memory pages, and if / when the parent
 * modifies any keys due to write traffic, it'll cause CoW which consume
 * physical memory. In the child process, after serializing the key and value,
 * the data is definitely not accessed again, so to avoid unnecessary CoW, we
 * try to release their memory back to OS. see zmadvise_dontneed().
 *
 * Because of the cost of iterating all node/field/member/entry of complex data
 * types, we iterate and dismiss them only when approximate average we estimate
 * the size of an individual allocation is more than a page size of OS.
 * 'size_hint' is the size of serialized value. This method is not accurate, but
 * it can reduce unnecessary iteration for complex data types that are probably
 * not going to release any memory. */
void dismissObject(robj *o, size_t size_hint) {
    /* madvise(MADV_DONTNEED) may not work if Transparent Huge Pages is enabled. */
    if (server.thp_enabled) return;

        /* Currently we use zmadvise_dontneed only when we use jemalloc with Linux.
         * so we avoid these pointless loops when they're not going to do anything. */
#if defined(USE_JEMALLOC) && defined(__linux__)
    if (o->refcount != 1) return;
    switch (o->type) {
    case OBJ_STRING: dismissStringObject(o); break;
    case OBJ_LIST: dismissListObject(o, size_hint); break;
    case OBJ_SET: dismissSetObject(o, size_hint); break;
    case OBJ_ZSET: dismissZsetObject(o, size_hint); break;
    case OBJ_HASH: dismissHashObject(o, size_hint); break;
    case OBJ_STREAM: dismissStreamObject(o, size_hint); break;
    default: break;
    }
#else
    UNUSED(o);
    UNUSED(size_hint);
#endif
}

int checkType(client *c, robj *o, int type) {
    /* A NULL is considered an empty key */
    if (o && o->type != type) {
        addReplyErrorObject(c, shared.wrongtypeerr);
        return 1;
    }
    return 0;
}

int isSdsRepresentableAsLongLong(sds s, long long *llval) {
    return string2ll(s, sdslen(s), llval) ? C_OK : C_ERR;
}

int isObjectRepresentableAsLongLong(robj *o, long long *llval) {
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
    if (o->encoding == OBJ_ENCODING_INT) {
        if (llval) *llval = (long)o->ptr;
        return C_OK;
    } else {
        return isSdsRepresentableAsLongLong(o->ptr, llval);
    }
}

/* Optimize the SDS string inside the string object to require little space,
 * in case there is more than 10% of free space at the end of the SDS. */
void trimStringObjectIfNeeded(robj *o, int trim_small_values) {
    if (o->encoding != OBJ_ENCODING_RAW) return;
    /* A string may have free space in the following cases:
     * 1. When an arg len is greater than PROTO_MBULK_BIG_ARG the query buffer may be used directly as the SDS string.
     * 2. When utilizing the argument caching mechanism in Lua.
     * 3. When calling from RM_TrimStringAllocation (trim_small_values is true). */
    size_t len = sdslen(o->ptr);
    if (len >= PROTO_MBULK_BIG_ARG || trim_small_values ||
        (server.executing_client && server.executing_client->flag.script && len < LUA_CMD_OBJCACHE_MAX_LEN)) {
        if (sdsavail(o->ptr) > len / 10) {
            o->ptr = sdsRemoveFreeSpace(o->ptr, 0);
        }
    }
}

/* Try to encode a string object in order to save space */
robj *tryObjectEncodingEx(robj *o, int try_trim) {
    long value;
    sds s = o->ptr;
    size_t len;

    /* Make sure this is a string object, the only type we encode
     * in this function. Other types use encoded memory efficient
     * representations but are handled by the commands implementing
     * the type. */
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);

    /* We try some specialized encoding only for objects that are
     * RAW or EMBSTR encoded, in other words objects that are still
     * in represented by an actually array of chars. */
    if (!sdsEncodedObject(o)) return o;

    /* It's not safe to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of the server and may end in places where
     * they are not handled. We handle them only as values in the keyspace. */
    if (o->refcount > 1) return o;

    /* Check if we can represent this string as a long integer.
     * Note that we are sure that a string larger than 20 chars is not
     * representable as a 32 nor 64 bit integer. */
    len = sdslen(s);
    if (len <= 20 && string2l(s, len, &value)) {
        /* This object is encodable as a long. */
        if (o->encoding == OBJ_ENCODING_RAW) {
            sdsfree(o->ptr);
            o->encoding = OBJ_ENCODING_INT;
            o->ptr = (void *)value;
            return o;
        } else if (o->encoding == OBJ_ENCODING_EMBSTR) {
            decrRefCount(o);
            return createStringObjectFromLongLongForValue(value);
        }
    }

    /* If the string is small and is still RAW encoded,
     * try the EMBSTR encoding which is more efficient.
     * In this representation the object and the SDS string are allocated
     * in the same chunk of memory to save space and cache misses. */
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT) {
        robj *emb;

        if (o->encoding == OBJ_ENCODING_EMBSTR) return o;
        emb = createEmbeddedStringObject(s, sdslen(s));
        decrRefCount(o);
        return emb;
    }

    /* We can't encode the object...
     * Do the last try, and at least optimize the SDS string inside */
    if (try_trim) trimStringObjectIfNeeded(o, 0);

    /* Return the original object. */
    return o;
}

robj *tryObjectEncoding(robj *o) {
    return tryObjectEncodingEx(o, 1);
}

/* Get a decoded version of an encoded object (returned as a new object).
 * If the object is already raw-encoded just increment the ref count. */
robj *getDecodedObject(robj *o) {
    robj *dec;

    if (sdsEncodedObject(o)) {
        incrRefCount(o);
        return o;
    }
    if (o->type == OBJ_STRING && o->encoding == OBJ_ENCODING_INT) {
        char buf[32];

        ll2string(buf, 32, (long)o->ptr);
        dec = createStringObject(buf, strlen(buf));
        return dec;
    } else {
        serverPanic("Unknown encoding type");
    }
}

/* Compare two string objects via strcmp() or strcoll() depending on flags.
 * Note that the objects may be integer-encoded. In such a case we
 * use ll2string() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * Important note: when STRING_COMPARE_BINARY is used a binary-safe comparison
 * is used. */

#define STRING_COMPARE_BINARY (1 << 0)
#define STRING_COMPARE_COLL (1 << 1)

int compareStringObjectsWithFlags(const robj *a, const robj *b, int flags) {
    serverAssertWithInfo(NULL, a, a->type == OBJ_STRING && b->type == OBJ_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    size_t alen, blen, minlen;

    if (a == b) return 0;
    if (sdsEncodedObject(a)) {
        astr = a->ptr;
        alen = sdslen(astr);
    } else {
        alen = ll2string(bufa, sizeof(bufa), (long)a->ptr);
        astr = bufa;
    }
    if (sdsEncodedObject(b)) {
        bstr = b->ptr;
        blen = sdslen(bstr);
    } else {
        blen = ll2string(bufb, sizeof(bufb), (long)b->ptr);
        bstr = bufb;
    }
    if (flags & STRING_COMPARE_COLL) {
        return strcoll(astr, bstr);
    } else {
        int cmp;

        minlen = (alen < blen) ? alen : blen;
        cmp = memcmp(astr, bstr, minlen);
        if (cmp == 0) return alen - blen;
        return cmp;
    }
}

/* Wrapper for compareStringObjectsWithFlags() using binary comparison. */
int compareStringObjects(const robj *a, const robj *b) {
    return compareStringObjectsWithFlags(a, b, STRING_COMPARE_BINARY);
}

/* Wrapper for compareStringObjectsWithFlags() using collation. */
int collateStringObjects(const robj *a, const robj *b) {
    return compareStringObjectsWithFlags(a, b, STRING_COMPARE_COLL);
}

/* Equal string objects return 1 if the two objects are the same from the
 * point of view of a string comparison, otherwise 0 is returned. Note that
 * this function is faster then checking for (compareStringObject(a,b) == 0)
 * because it can perform some more optimization. */
int equalStringObjects(robj *a, robj *b) {
    if (a->encoding == OBJ_ENCODING_INT && b->encoding == OBJ_ENCODING_INT) {
        /* If both strings are integer encoded just check if the stored
         * long is the same. */
        return a->ptr == b->ptr;
    } else {
        return compareStringObjects(a, b) == 0;
    }
}

size_t stringObjectLen(robj *o) {
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
    if (sdsEncodedObject(o)) {
        return sdslen(o->ptr);
    } else {
        return sdigits10((long)o->ptr);
    }
}

int getDoubleFromObject(const robj *o, double *target) {
    double value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (!string2d(o->ptr, sdslen(o->ptr), &value)) return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    *target = value;
    return C_OK;
}

int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg) {
    double value;
    if (getDoubleFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c, (char *)msg);
        } else {
            addReplyError(c, "value is not a valid float");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

int getLongDoubleFromObject(robj *o, long double *target) {
    long double value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (!string2ld(o->ptr, sdslen(o->ptr), &value)) return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    *target = value;
    return C_OK;
}

int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg) {
    long double value;
    if (getLongDoubleFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c, (char *)msg);
        } else {
            addReplyError(c, "value is not a valid float");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

int getLongLongFromObject(robj *o, long long *target) {
    long long value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (string2ll(o->ptr, sdslen(o->ptr), &value) == 0) return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    if (target) *target = value;
    return C_OK;
}

int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg) {
    long long value;
    if (getLongLongFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c, (char *)msg);
        } else {
            addReplyError(c, "value is not an integer or out of range");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg) {
    long long value;

    if (getLongLongFromObjectOrReply(c, o, &value, msg) != C_OK) return C_ERR;
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c, (char *)msg);
        } else {
            addReplyError(c, "value is out of range");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

int getRangeLongFromObjectOrReply(client *c, robj *o, long min, long max, long *target, const char *msg) {
    if (getLongFromObjectOrReply(c, o, target, msg) != C_OK) return C_ERR;
    if (*target < min || *target > max) {
        if (msg != NULL) {
            addReplyError(c, (char *)msg);
        } else {
            addReplyErrorFormat(c, "value is out of range, value must between %ld and %ld", min, max);
        }
        return C_ERR;
    }
    return C_OK;
}

int getPositiveLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg) {
    if (msg) {
        return getRangeLongFromObjectOrReply(c, o, 0, LONG_MAX, target, msg);
    } else {
        return getRangeLongFromObjectOrReply(c, o, 0, LONG_MAX, target, "value is out of range, must be positive");
    }
}

int getIntFromObjectOrReply(client *c, robj *o, int *target, const char *msg) {
    long value;

    if (getRangeLongFromObjectOrReply(c, o, INT_MIN, INT_MAX, &value, msg) != C_OK) return C_ERR;

    *target = value;
    return C_OK;
}

char *strEncoding(int encoding) {
    switch (encoding) {
    case OBJ_ENCODING_RAW: return "raw";
    case OBJ_ENCODING_INT: return "int";
    case OBJ_ENCODING_HASHTABLE: return "hashtable";
    case OBJ_ENCODING_QUICKLIST: return "quicklist";
    case OBJ_ENCODING_LISTPACK: return "listpack";
    case OBJ_ENCODING_INTSET: return "intset";
    case OBJ_ENCODING_SKIPLIST: return "skiplist";
    case OBJ_ENCODING_EMBSTR: return "embstr";
    case OBJ_ENCODING_STREAM: return "stream";
    default: return "unknown";
    }
}

/* =========================== Memory introspection ========================= */


/* Returns the size in bytes consumed by the key's value in RAM.
 * Note that the returned value is just an approximation, especially in the
 * case of aggregated data types where only "sample_size" elements
 * are checked and averaged to estimate the total size. */
#define OBJ_COMPUTE_SIZE_DEF_SAMPLES 5 /* Default sample size. */
size_t objectComputeSize(robj *key, robj *o, size_t sample_size, int dbid) {
    size_t asize = 0, elesize = 0, samples = 0;

    if (o->type == OBJ_STRING) {
        if (o->encoding == OBJ_ENCODING_INT) {
            asize = sizeof(*o);
        } else if (o->encoding == OBJ_ENCODING_RAW) {
            asize = sdsAllocSize(o->ptr) + sizeof(*o);
        } else if (o->encoding == OBJ_ENCODING_EMBSTR) {
            asize = zmalloc_size((void *)o);
        } else {
            serverPanic("Unknown string encoding");
        }
    } else if (o->type == OBJ_LIST) {
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;
            asize = sizeof(*o) + sizeof(quicklist);
            do {
                elesize += sizeof(quicklistNode) + zmalloc_size(node->entry);
                samples++;
            } while ((node = node->next) && samples < sample_size);
            asize += (double)elesize / samples * ql->len;
        } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
            asize = sizeof(*o) + zmalloc_size(o->ptr);
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        if (o->encoding == OBJ_ENCODING_HASHTABLE) {
            hashtable *ht = o->ptr;
            asize = sizeof(*o) + hashtableMemUsage(ht);

            hashtableIterator iter;
            hashtableInitIterator(&iter, ht, 0);
            void *next;
            while (hashtableNext(&iter, &next) && samples < sample_size) {
                sds element = next;
                elesize += sdsAllocSize(element);
                samples++;
            }
            hashtableResetIterator(&iter);
            if (samples) asize += (double)elesize / samples * hashtableSize(ht);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            asize = sizeof(*o) + zmalloc_size(o->ptr);
        } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
            asize = sizeof(*o) + zmalloc_size(o->ptr);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            asize = sizeof(*o) + zmalloc_size(o->ptr);
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            hashtable *ht = ((zset *)o->ptr)->ht;
            zskiplist *zsl = ((zset *)o->ptr)->zsl;
            zskiplistNode *znode = zsl->header->level[0].forward;
            asize = sizeof(*o) + sizeof(zset) + sizeof(zskiplist) +
                    hashtableMemUsage(ht) + zmalloc_size(zsl->header);
            while (znode != NULL && samples < sample_size) {
                elesize += sdsAllocSize(znode->ele);
                elesize += zmalloc_size(znode);
                samples++;
                znode = znode->level[0].forward;
            }
            if (samples) asize += (double)elesize / samples * hashtableSize(ht);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        if (o->encoding == OBJ_ENCODING_LISTPACK) {
            asize = sizeof(*o) + zmalloc_size(o->ptr);
        } else if (o->encoding == OBJ_ENCODING_HASHTABLE) {
            hashtable *ht = o->ptr;
            hashtableIterator iter;
            hashtableInitIterator(&iter, ht, 0);
            void *next;

            asize = sizeof(*o) + hashtableMemUsage(ht);
            while (hashtableNext(&iter, &next) && samples < sample_size) {
                elesize += hashTypeEntryAllocSize(next);
                samples++;
            }
            hashtableResetIterator(&iter);
            if (samples) asize += (double)elesize / samples * hashtableSize(ht);
        } else {
            serverPanic("Unknown hash encoding");
        }
    } else if (o->type == OBJ_STREAM) {
        stream *s = o->ptr;
        asize = sizeof(*o) + sizeof(*s);
        asize += raxAllocSize(s->rax);

        /* Now we have to add the listpacks. The last listpack is often non
         * complete, so we estimate the size of the first N listpacks, and
         * use the average to compute the size of the first N-1 listpacks, and
         * finally add the real size of the last node. */
        raxIterator ri;
        raxStart(&ri, s->rax);
        raxSeek(&ri, "^", NULL, 0);
        size_t lpsize = 0, samples = 0;
        while (samples < sample_size && raxNext(&ri)) {
            unsigned char *lp = ri.data;
            /* Use the allocated size, since we overprovision the node initially. */
            lpsize += zmalloc_size(lp);
            samples++;
        }
        if (s->rax->numele <= samples) {
            asize += lpsize;
        } else {
            if (samples) lpsize /= samples; /* Compute the average. */
            asize += lpsize * (s->rax->numele - 1);
            /* No need to check if seek succeeded, we enter this branch only
             * if there are a few elements in the radix tree. */
            raxSeek(&ri, "$", NULL, 0);
            raxNext(&ri);
            /* Use the allocated size, since we overprovision the node initially. */
            asize += zmalloc_size(ri.data);
        }
        raxStop(&ri);

        /* Consumer groups also have a non trivial memory overhead if there
         * are many consumers and many groups, let's count at least the
         * overhead of the pending entries in the groups and consumers
         * PELs. */
        if (s->cgroups) {
            raxStart(&ri, s->cgroups);
            raxSeek(&ri, "^", NULL, 0);
            while (raxNext(&ri)) {
                streamCG *cg = ri.data;
                asize += sizeof(*cg);
                asize += raxAllocSize(cg->pel);
                asize += sizeof(streamNACK) * raxSize(cg->pel);

                /* For each consumer we also need to add the basic data
                 * structures and the PEL memory usage. */
                raxIterator cri;
                raxStart(&cri, cg->consumers);
                raxSeek(&cri, "^", NULL, 0);
                while (raxNext(&cri)) {
                    streamConsumer *consumer = cri.data;
                    asize += sizeof(*consumer);
                    asize += sdslen(consumer->name);
                    asize += raxAllocSize(consumer->pel);
                    /* Don't count NACKs again, they are shared with the
                     * consumer group PEL. */
                }
                raxStop(&cri);
            }
            raxStop(&ri);
        }
    } else if (o->type == OBJ_MODULE) {
        asize = moduleGetMemUsage(key, o, sample_size, dbid);
    } else {
        serverPanic("Unknown object type");
    }
    return asize;
}

/* Release data obtained with getMemoryOverheadData(). */
void freeMemoryOverheadData(struct serverMemOverhead *mh) {
    zfree(mh->db);
    zfree(mh);
}

/* Return a struct serverMemOverhead filled with memory overhead
 * information used for the MEMORY OVERHEAD and INFO command. The returned
 * structure pointer should be freed calling freeMemoryOverheadData(). */
struct serverMemOverhead *getMemoryOverheadData(void) {
    int j;
    size_t mem_total = 0;
    size_t mem = 0;
    size_t zmalloc_used = zmalloc_used_memory();
    struct serverMemOverhead *mh = zcalloc(sizeof(*mh));

    mh->total_allocated = zmalloc_used;
    mh->startup_allocated = server.initial_memory_usage;
    mh->peak_allocated = server.stat_peak_memory;
    mh->total_frag = (float)server.cron_malloc_stats.process_rss / server.cron_malloc_stats.zmalloc_used;
    mh->total_frag_bytes = server.cron_malloc_stats.process_rss - server.cron_malloc_stats.zmalloc_used;
    mh->allocator_frag =
        (float)server.cron_malloc_stats.allocator_frag_smallbins_bytes / server.cron_malloc_stats.allocator_allocated +
        1;
    mh->allocator_frag_bytes = server.cron_malloc_stats.allocator_frag_smallbins_bytes;
    mh->allocator_rss = (float)server.cron_malloc_stats.allocator_resident / server.cron_malloc_stats.allocator_active;
    mh->allocator_rss_bytes = server.cron_malloc_stats.allocator_resident - server.cron_malloc_stats.allocator_active;
    mh->rss_extra = (float)server.cron_malloc_stats.process_rss / server.cron_malloc_stats.allocator_resident;
    mh->rss_extra_bytes = server.cron_malloc_stats.process_rss - server.cron_malloc_stats.allocator_resident;

    mem_total += server.initial_memory_usage;

    /* Replication backlog and replicas share one global replication buffer,
     * only if replication buffer memory is more than the repl backlog setting,
     * we consider the excess as replicas' memory. Otherwise, replication buffer
     * memory is the consumption of repl backlog. */
    if (listLength(server.replicas) && (long long)server.repl_buffer_mem > server.repl_backlog_size) {
        mh->clients_replicas = server.repl_buffer_mem - server.repl_backlog_size;
        mh->repl_backlog = server.repl_backlog_size;
    } else {
        mh->clients_replicas = 0;
        mh->repl_backlog = server.repl_buffer_mem;
    }
    if (server.repl_backlog) {
        /* The approximate memory of rax tree for indexed blocks. */
        mh->repl_backlog += server.repl_backlog->blocks_index->numnodes * sizeof(raxNode) +
                            raxSize(server.repl_backlog->blocks_index) * sizeof(void *);
    }
    mem_total += mh->repl_backlog;
    mem_total += mh->clients_replicas;

    /* Computing the memory used by the clients would be O(N) if done
     * here online. We use our values computed incrementally by
     * updateClientMemoryUsage(). */
    mh->clients_normal = server.stat_clients_type_memory[CLIENT_TYPE_PRIMARY] +
                         server.stat_clients_type_memory[CLIENT_TYPE_PUBSUB] +
                         server.stat_clients_type_memory[CLIENT_TYPE_NORMAL];
    mem_total += mh->clients_normal;

    mh->cluster_links = server.stat_cluster_links_memory;
    mem_total += mh->cluster_links;

    mem = 0;
    if (server.aof_state != AOF_OFF) {
        mem += sdsAllocSize(server.aof_buf);
    }
    mh->aof_buffer = mem;
    mem_total += mem;

    mem = evalScriptsMemory();
    mh->lua_caches = mem;
    mem_total += mem;
    mh->functions_caches = functionsMemoryOverhead();
    mem_total += mh->functions_caches;

    for (j = 0; j < server.dbnum; j++) {
        serverDb *db = server.db + j;
        if (!kvstoreNumAllocatedHashtables(db->keys)) continue;

        unsigned long long keyscount = kvstoreSize(db->keys);

        mh->total_keys += keyscount;
        mh->db = zrealloc(mh->db, sizeof(mh->db[0]) * (mh->num_dbs + 1));
        mh->db[mh->num_dbs].dbid = j;

        mem = kvstoreMemUsage(db->keys) + keyscount * sizeof(robj);
        mh->db[mh->num_dbs].overhead_ht_main = mem;
        mem_total += mem;

        mem = kvstoreMemUsage(db->expires);
        mh->db[mh->num_dbs].overhead_ht_expires = mem;
        mem_total += mem;

        mh->num_dbs++;

        mh->overhead_db_hashtable_lut += kvstoreOverheadHashtableLut(db->keys);
        mh->overhead_db_hashtable_lut += kvstoreOverheadHashtableLut(db->expires);
        mh->overhead_db_hashtable_rehashing += kvstoreOverheadHashtableRehashing(db->keys);
        mh->overhead_db_hashtable_rehashing += kvstoreOverheadHashtableRehashing(db->expires);
        mh->db_dict_rehashing_count += kvstoreHashtableRehashingCount(db->keys);
        mh->db_dict_rehashing_count += kvstoreHashtableRehashingCount(db->expires);
    }

    mh->overhead_total = mem_total;
    mh->dataset = zmalloc_used - mem_total;
    mh->peak_perc = (float)zmalloc_used * 100 / mh->peak_allocated;

    /* Metrics computed after subtracting the startup memory from
     * the total memory. */
    size_t net_usage = 1;
    if (zmalloc_used > mh->startup_allocated) net_usage = zmalloc_used - mh->startup_allocated;
    mh->dataset_perc = (float)mh->dataset * 100 / net_usage;
    mh->bytes_per_key = mh->total_keys ? (mh->dataset / mh->total_keys) : 0;

    return mh;
}

/* Helper for "MEMORY allocator-stats", used as a callback for the jemalloc
 * stats output. */
void inputCatSds(void *result, const char *str) {
    /* result is actually a (sds *), so re-cast it here */
    sds *info = (sds *)result;
    *info = sdscat(*info, str);
}

/* This implements MEMORY DOCTOR. An human readable analysis of the server
 * memory condition. */
sds getMemoryDoctorReport(void) {
    int empty = 0;           /* Instance is empty or almost empty. */
    int big_peak = 0;        /* Memory peak is much larger than used mem. */
    int high_frag = 0;       /* High fragmentation. */
    int high_alloc_frag = 0; /* High allocator fragmentation. */
    int high_proc_rss = 0;   /* High process rss overhead. */
    int high_alloc_rss = 0;  /* High rss overhead. */
    int big_replica_buf = 0; /* Replica buffers are too big. */
    int big_client_buf = 0;  /* Client buffers are too big. */
    int many_scripts = 0;    /* Script cache has too many scripts. */
    int num_reports = 0;
    struct serverMemOverhead *mh = getMemoryOverheadData();

    if (mh->total_allocated < (1024 * 1024 * 5)) {
        empty = 1;
        num_reports++;
    } else {
        /* Peak is > 150% of current used memory? */
        if (((float)mh->peak_allocated / mh->total_allocated) > 1.5) {
            big_peak = 1;
            num_reports++;
        }

        /* Fragmentation is higher than 1.4 and 10MB ?*/
        if (mh->total_frag > 1.4 && mh->total_frag_bytes > 10 << 20) {
            high_frag = 1;
            num_reports++;
        }

        /* External fragmentation is higher than 1.1 and 10MB? */
        if (mh->allocator_frag > 1.1 && mh->allocator_frag_bytes > 10 << 20) {
            high_alloc_frag = 1;
            num_reports++;
        }

        /* Allocator rss is higher than 1.1 and 10MB ? */
        if (mh->allocator_rss > 1.1 && mh->allocator_rss_bytes > 10 << 20) {
            high_alloc_rss = 1;
            num_reports++;
        }

        /* Non-Allocator rss is higher than 1.1 and 10MB ? */
        if (mh->rss_extra > 1.1 && mh->rss_extra_bytes > 10 << 20) {
            high_proc_rss = 1;
            num_reports++;
        }

        /* Clients using more than 200k each average? */
        long num_replicas = listLength(server.replicas);
        long numclients = listLength(server.clients) - num_replicas;
        if (mh->clients_normal / numclients > (1024 * 200)) {
            big_client_buf = 1;
            num_reports++;
        }

        /* Replicas using more than 10 MB each? */
        if (num_replicas > 0 && mh->clients_replicas > (1024 * 1024 * 10)) {
            big_replica_buf = 1;
            num_reports++;
        }

        /* Too many scripts are cached? */
        if (dictSize(evalScriptsDict()) > 1000) {
            many_scripts = 1;
            num_reports++;
        }
    }

    sds s;
    if (num_reports == 0) {
        s = sdsnew("Hi Sam, I can't find any memory issue in your instance. "
                   "I can only account for what occurs on this base.\n");
    } else if (empty == 1) {
        s = sdsnew("Hi Sam, this instance is empty or is using very little memory, "
                   "my issues detector can't be used in these conditions. "
                   "Please, leave for your mission on Earth and fill it with some data. "
                   "The new Sam and I will be back to our programming as soon as I "
                   "finished rebooting.\n");
    } else {
        s = sdsnew("Sam, I detected a few issues in this Valkey instance memory implants:\n\n");
        if (big_peak) {
            s = sdscat(s,
                       " * Peak memory: In the past this instance used more than 150% the memory that is currently "
                       "using. The allocator is normally not able to release memory after a peak, so you can expect to "
                       "see a big fragmentation ratio, however this is actually harmless and is only due to the memory "
                       "peak, and if the Valkey instance Resident Set Size (RSS) is currently bigger than expected, "
                       "the memory will be used as soon as you fill the Valkey instance with more data. If the memory "
                       "peak was only occasional and you want to try to reclaim memory, please try the MEMORY PURGE "
                       "command, otherwise the only other option is to shutdown and restart the instance.\n\n");
        }
        if (high_frag) {
            s = sdscatprintf(
                s,
                " * High total RSS: This instance has a memory fragmentation and RSS overhead greater than 1.4 (this "
                "means that the Resident Set Size of the Valkey process is much larger than the sum of the logical "
                "allocations Valkey performed). This problem is usually due either to a large peak memory (check if "
                "there is a peak memory entry above in the report) or may result from a workload that causes the "
                "allocator to fragment memory a lot. If the problem is a large peak memory, then there is no issue. "
                "Otherwise, make sure you are using the Jemalloc allocator and not the default libc malloc. Note: The "
                "currently used allocator is \"%s\".\n\n",
                ZMALLOC_LIB);
        }
        if (high_alloc_frag) {
            s = sdscatprintf(
                s, " * High allocator fragmentation: This instance has an allocator external fragmentation greater "
                   "than 1.1. This problem is usually due either to a large peak memory (check if there is a peak "
                   "memory entry above in the report) or may result from a workload that causes the allocator to "
                   "fragment memory a lot. You can try enabling 'activedefrag' config option.\n\n");
        }
        if (high_alloc_rss) {
            s = sdscatprintf(
                s, " * High allocator RSS overhead: This instance has an RSS memory overhead is greater than 1.1 (this "
                   "means that the Resident Set Size of the allocator is much larger than the sum what the allocator "
                   "actually holds). This problem is usually due to a large peak memory (check if there is a peak "
                   "memory entry above in the report), you can try the MEMORY PURGE command to reclaim it.\n\n");
        }
        if (high_proc_rss) {
            s = sdscatprintf(
                s, " * High process RSS overhead: This instance has non-allocator RSS memory overhead is greater than "
                   "1.1 (this means that the Resident Set Size of the Valkey process is much larger than the RSS the "
                   "allocator holds). This problem may be due to Lua scripts or Modules.\n\n");
        }
        if (big_replica_buf) {
            s = sdscat(s,
                       " * Big replica buffers: The replica output buffers in this instance are greater than 10MB for "
                       "each replica (on average). This likely means that there is some replica instance that is "
                       "struggling receiving data, either because it is too slow or because of networking issues. As a "
                       "result, data piles on the primary output buffers. Please try to identify what replica is not "
                       "receiving data correctly and why. You can use the INFO output in order to check the replicas "
                       "delays and the CLIENT LIST command to check the output buffers of each replica.\n\n");
        }
        if (big_client_buf) {
            s = sdscat(s, " * Big client buffers: The clients output buffers in this instance are greater than 200K "
                          "per client (on average). This may result from different causes, like Pub/Sub clients "
                          "subscribed to channels bot not receiving data fast enough, so that data piles on the Valkey "
                          "instance output buffer, or clients sending commands with large replies or very large "
                          "sequences of commands in the same pipeline. Please use the CLIENT LIST command in order to "
                          "investigate the issue if it causes problems in your instance, or to understand better why "
                          "certain clients are using a big amount of memory.\n\n");
        }
        if (many_scripts) {
            s = sdscat(s, " * Many scripts: There seem to be many cached scripts in this instance (more than 1000). "
                          "This may be because scripts are generated and `EVAL`ed, instead of being parameterized "
                          "(with KEYS and ARGV), `SCRIPT LOAD`ed and `EVALSHA`ed. Unless `SCRIPT FLUSH` is called "
                          "periodically, the scripts' caches may end up consuming most of your memory.\n\n");
        }
        s = sdscat(s, "I'm here to keep you safe, Sam. I want to help you.\n");
    }
    freeMemoryOverheadData(mh);
    return s;
}

/* Set the object LRU/LFU depending on server.maxmemory_policy.
 * The lfu_freq arg is only relevant if policy is MAXMEMORY_FLAG_LFU.
 * The lru_idle and lru_clock args are only relevant if policy
 * is MAXMEMORY_FLAG_LRU.
 * Either or both of them may be <0, in that case, nothing is set. */
int objectSetLRUOrLFU(robj *val, long long lfu_freq, long long lru_idle, long long lru_clock, int lru_multiplier) {
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        if (lfu_freq >= 0) {
            serverAssert(lfu_freq <= 255);
            val->lru = (LFUGetTimeInMinutes() << 8) | lfu_freq;
            return 1;
        }
    } else if (lru_idle >= 0) {
        /* Provided LRU idle time is in seconds. Scale
         * according to the LRU clock resolution this
         * instance was compiled with (normally 1000 ms, so the
         * below statement will expand to lru_idle*1000/1000. */
        lru_idle = lru_idle * lru_multiplier / LRU_CLOCK_RESOLUTION;
        long lru_abs = lru_clock - lru_idle; /* Absolute access time. */
        /* If the LRU field underflows (since lru_clock is a wrapping clock),
         * we need to make it positive again. This will be handled by the unwrapping
         * code in estimateObjectIdleTime. I.e. imagine a day when lru_clock
         * wrap arounds (happens once in some 6 months), and becomes a low
         * value, like 10, an lru_idle of 1000 should be near LRU_CLOCK_MAX. */
        if (lru_abs < 0) lru_abs += LRU_CLOCK_MAX;
        val->lru = lru_abs;
        return 1;
    }
    return 0;
}

/* ======================= The OBJECT and MEMORY commands =================== */

/* This is a helper function for the OBJECT command. We need to lookup keys
 * without any modification of LRU or other parameters. */
robj *objectCommandLookup(client *c, robj *key) {
    return lookupKeyReadWithFlags(c->db, key, LOOKUP_NOTOUCH | LOOKUP_NONOTIFY);
}

robj *objectCommandLookupOrReply(client *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c, key);
    if (!o) addReplyOrErrorObject(c, reply);
    return o;
}

/* Object command allows to inspect the internals of an Object.
 * Usage: OBJECT <refcount|encoding|idletime|freq> <key> */
void objectCommand(client *c) {
    robj *o;

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "help")) {
        const char *help[] = {"ENCODING <key>",
                              "    Return the kind of internal representation used in order to store the value",
                              "    associated with a <key>.",
                              "FREQ <key>",
                              "    Return the access frequency index of the <key>. The returned integer is",
                              "    proportional to the logarithm of the recent access frequency of the key.",
                              "IDLETIME <key>",
                              "    Return the idle time of the <key>, that is the approximated number of",
                              "    seconds elapsed since the last access to the key.",
                              "REFCOUNT <key>",
                              "    Return the number of references of the value associated with the specified",
                              "    <key>.",
                              NULL};
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr, "refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c, c->argv[2], shared.null[c->resp])) == NULL) return;
        addReplyLongLong(c, o->refcount);
    } else if (!strcasecmp(c->argv[1]->ptr, "encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c, c->argv[2], shared.null[c->resp])) == NULL) return;
        addReplyBulkCString(c, strEncoding(o->encoding));
    } else if (!strcasecmp(c->argv[1]->ptr, "idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c, c->argv[2], shared.null[c->resp])) == NULL) return;
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            addReplyError(c, "An LFU maxmemory policy is selected, idle time not tracked. Please note that when "
                             "switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return;
        }
        addReplyLongLong(c, estimateObjectIdleTime(o) / 1000);
    } else if (!strcasecmp(c->argv[1]->ptr, "freq") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c, c->argv[2], shared.null[c->resp])) == NULL) return;
        if (!(server.maxmemory_policy & MAXMEMORY_FLAG_LFU)) {
            addReplyError(c,
                          "An LFU maxmemory policy is not selected, access frequency not tracked. Please note that "
                          "when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return;
        }
        /* LFUDecrAndReturn should be called
         * in case of the key has not been accessed for a long time,
         * because we update the access time only
         * when the key is read or overwritten. */
        addReplyLongLong(c, LFUDecrAndReturn(o));
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* The memory command will eventually be a complete interface for the
 * memory introspection capabilities of the server.
 *
 * Usage: MEMORY usage <key> */
void memoryCommand(client *c) {
    if (!strcasecmp(c->argv[1]->ptr, "help") && c->argc == 2) {
        const char *help[] = {
            "DOCTOR",
            "    Return memory problems reports.",
            "MALLOC-STATS",
            "    Return internal statistics report from the memory allocator.",
            "PURGE",
            "    Attempt to purge dirty pages for reclamation by the allocator.",
            "STATS",
            "    Return information about the memory usage of the server.",
            "USAGE <key> [SAMPLES <count>]",
            "    Return memory in bytes used by <key> and its value. Nested values are",
            "    sampled up to <count> times (default: 5, 0 means sample all).",
            NULL,
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr, "usage") && c->argc >= 3) {
        long long samples = OBJ_COMPUTE_SIZE_DEF_SAMPLES;
        for (int j = 3; j < c->argc; j++) {
            if (!strcasecmp(c->argv[j]->ptr, "samples") && j + 1 < c->argc) {
                if (getLongLongFromObjectOrReply(c, c->argv[j + 1], &samples, NULL) == C_ERR) return;
                if (samples < 0) {
                    addReplyErrorObject(c, shared.syntaxerr);
                    return;
                }
                if (samples == 0) samples = LLONG_MAX;
                j++; /* skip option argument. */
            } else {
                addReplyErrorObject(c, shared.syntaxerr);
                return;
            }
        }
        robj *obj = dbFind(c->db, c->argv[2]->ptr);
        if (obj == NULL) {
            addReplyNull(c);
            return;
        }
        size_t usage = objectComputeSize(c->argv[2], obj, samples, c->db->id);
        addReplyLongLong(c, usage);
    } else if (!strcasecmp(c->argv[1]->ptr, "stats") && c->argc == 2) {
        struct serverMemOverhead *mh = getMemoryOverheadData();

        addReplyMapLen(c, 31 + mh->num_dbs);

        addReplyBulkCString(c, "peak.allocated");
        addReplyLongLong(c, mh->peak_allocated);

        addReplyBulkCString(c, "total.allocated");
        addReplyLongLong(c, mh->total_allocated);

        addReplyBulkCString(c, "startup.allocated");
        addReplyLongLong(c, mh->startup_allocated);

        addReplyBulkCString(c, "replication.backlog");
        addReplyLongLong(c, mh->repl_backlog);

        addReplyBulkCString(c, "clients.slaves");
        addReplyLongLong(c, mh->clients_replicas);

        addReplyBulkCString(c, "clients.normal");
        addReplyLongLong(c, mh->clients_normal);

        addReplyBulkCString(c, "cluster.links");
        addReplyLongLong(c, mh->cluster_links);

        addReplyBulkCString(c, "aof.buffer");
        addReplyLongLong(c, mh->aof_buffer);

        addReplyBulkCString(c, "lua.caches");
        addReplyLongLong(c, mh->lua_caches);

        addReplyBulkCString(c, "functions.caches");
        addReplyLongLong(c, mh->functions_caches);

        for (size_t j = 0; j < mh->num_dbs; j++) {
            char dbname[32];
            snprintf(dbname, sizeof(dbname), "db.%zd", mh->db[j].dbid);
            addReplyBulkCString(c, dbname);
            addReplyMapLen(c, 2);

            addReplyBulkCString(c, "overhead.hashtable.main");
            addReplyLongLong(c, mh->db[j].overhead_ht_main);

            addReplyBulkCString(c, "overhead.hashtable.expires");
            addReplyLongLong(c, mh->db[j].overhead_ht_expires);
        }

        addReplyBulkCString(c, "overhead.db.hashtable.lut");
        addReplyLongLong(c, mh->overhead_db_hashtable_lut);

        addReplyBulkCString(c, "overhead.db.hashtable.rehashing");
        addReplyLongLong(c, mh->overhead_db_hashtable_rehashing);

        addReplyBulkCString(c, "overhead.total");
        addReplyLongLong(c, mh->overhead_total);

        addReplyBulkCString(c, "db.dict.rehashing.count");
        addReplyLongLong(c, mh->db_dict_rehashing_count);

        addReplyBulkCString(c, "keys.count");
        addReplyLongLong(c, mh->total_keys);

        addReplyBulkCString(c, "keys.bytes-per-key");
        addReplyLongLong(c, mh->bytes_per_key);

        addReplyBulkCString(c, "dataset.bytes");
        addReplyLongLong(c, mh->dataset);

        addReplyBulkCString(c, "dataset.percentage");
        addReplyDouble(c, mh->dataset_perc);

        addReplyBulkCString(c, "peak.percentage");
        addReplyDouble(c, mh->peak_perc);

        addReplyBulkCString(c, "allocator.allocated");
        addReplyLongLong(c, server.cron_malloc_stats.allocator_allocated);

        addReplyBulkCString(c, "allocator.active");
        addReplyLongLong(c, server.cron_malloc_stats.allocator_active);

        addReplyBulkCString(c, "allocator.resident");
        addReplyLongLong(c, server.cron_malloc_stats.allocator_resident);

        addReplyBulkCString(c, "allocator.muzzy");
        addReplyLongLong(c, server.cron_malloc_stats.allocator_muzzy);

        addReplyBulkCString(c, "allocator-fragmentation.ratio");
        addReplyDouble(c, mh->allocator_frag);

        addReplyBulkCString(c, "allocator-fragmentation.bytes");
        addReplyLongLong(c, mh->allocator_frag_bytes);

        addReplyBulkCString(c, "allocator-rss.ratio");
        addReplyDouble(c, mh->allocator_rss);

        addReplyBulkCString(c, "allocator-rss.bytes");
        addReplyLongLong(c, mh->allocator_rss_bytes);

        addReplyBulkCString(c, "rss-overhead.ratio");
        addReplyDouble(c, mh->rss_extra);

        addReplyBulkCString(c, "rss-overhead.bytes");
        addReplyLongLong(c, mh->rss_extra_bytes);

        addReplyBulkCString(c, "fragmentation"); /* this is the total RSS overhead, including fragmentation */
        addReplyDouble(c, mh->total_frag);       /* it is kept here for backwards compatibility */

        addReplyBulkCString(c, "fragmentation.bytes");
        addReplyLongLong(c, mh->total_frag_bytes);

        freeMemoryOverheadData(mh);
    } else if (!strcasecmp(c->argv[1]->ptr, "malloc-stats") && c->argc == 2) {
#if defined(USE_JEMALLOC)
        sds info = sdsempty();
        je_malloc_stats_print(inputCatSds, &info, NULL);
        addReplyVerbatim(c, info, sdslen(info), "txt");
        sdsfree(info);
#else
        addReplyBulkCString(c, "Stats not supported for the current allocator");
#endif
    } else if (!strcasecmp(c->argv[1]->ptr, "doctor") && c->argc == 2) {
        sds report = getMemoryDoctorReport();
        addReplyVerbatim(c, report, sdslen(report), "txt");
        sdsfree(report);
    } else if (!strcasecmp(c->argv[1]->ptr, "purge") && c->argc == 2) {
        if (jemalloc_purge() == 0)
            addReply(c, shared.ok);
        else
            addReplyError(c, "Error purging dirty pages");
    } else {
        addReplySubcommandSyntaxError(c);
    }
}
