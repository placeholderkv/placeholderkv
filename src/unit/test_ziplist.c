#include <sys/time.h>
#include "../ziplist.c"
#include "../adlist.h"
#include "test_help.h"


static unsigned char *createList(void) {
    unsigned char *zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char *)"foo", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)"quux", 4, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)"hello", 5, ZIPLIST_HEAD);
    zl = ziplistPush(zl, (unsigned char *)"1024", 4, ZIPLIST_TAIL);
    return zl;
}

static unsigned char *createIntList(void) {
    unsigned char *zl = ziplistNew();
    char buf[32];

    snprintf(buf, sizeof(buf), "100");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
    snprintf(buf, sizeof(buf), "128000");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
    snprintf(buf, sizeof(buf), "-100");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_HEAD);
    snprintf(buf, sizeof(buf), "4294967296");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_HEAD);
    snprintf(buf, sizeof(buf), "non integer");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
    snprintf(buf, sizeof(buf), "much much longer non integer");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
    return zl;
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec) * 1000000) + tv.tv_usec;
}

static void stress(int pos, int num, int maxsize, int dnum) {
    int i, j, k;
    unsigned char *zl;
    for (i = 0; i < maxsize; i += dnum) {
        zl = ziplistNew();
        for (j = 0; j < i; j++) {
            zl = ziplistPush(zl, (unsigned char *)"quux", 4, ZIPLIST_TAIL);
        }

        /* Do num times a push+pop from pos */
        for (k = 0; k < num; k++) {
            zl = ziplistPush(zl, (unsigned char *)"quux", 4, pos);
            zl = ziplistDeleteRange(zl, 0, 1);
        }
        zfree(zl);
    }
}

static unsigned char *pop(unsigned char *zl, int where) {
    unsigned char *p, *vstr;
    unsigned int vlen;
    long long vlong = 0;

    p = ziplistIndex(zl, where == ZIPLIST_HEAD ? 0 : -1);
    if (ziplistGet(p, &vstr, &vlen, &vlong)) {
        return ziplistDelete(zl, &p);
    } else {
        exit(1);
    }
}

static int randstring(char *target, unsigned int min, unsigned int max) {
    int p = 0;
    int len = min + rand() % (max - min + 1);
    int minval, maxval;
    switch (rand() % 3) {
    case 0:
        minval = 0;
        maxval = 255;
        break;
    case 1:
        minval = 48;
        maxval = 122;
        break;
    case 2:
        minval = 48;
        maxval = 52;
        break;
    default: assert(NULL);
    }

    while (p < len) target[p++] = minval + rand() % (maxval - minval + 1);
    return len;
}

static void verify(unsigned char *zl, zlentry *e) {
    int len = ziplistLen(zl);
    zlentry _e;

    ZIPLIST_ENTRY_ZERO(&_e);

    for (int i = 0; i < len; i++) {
        memset(&e[i], 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, i), &e[i]);

        memset(&_e, 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, -len + i), &_e);

        assert(memcmp(&e[i], &_e, sizeof(zlentry)) == 0);
    }
}

static unsigned char *insertHelper(unsigned char *zl, char ch, size_t len, unsigned char *pos) {
    assert(len <= ZIP_BIG_PREVLEN);
    unsigned char data[ZIP_BIG_PREVLEN] = {0};
    memset(data, ch, len);
    return ziplistInsert(zl, pos, data, len);
}

static int compareHelper(unsigned char *zl, char ch, size_t len, int index) {
    assert(len <= ZIP_BIG_PREVLEN);
    unsigned char data[ZIP_BIG_PREVLEN] = {0};
    memset(data, ch, len);
    unsigned char *p = ziplistIndex(zl, index);
    assert(p != NULL);
    return ziplistCompare(p, data, len);
}

static size_t strEntryBytesSmall(size_t slen) {
    return slen + zipStorePrevEntryLength(NULL, 0) + zipStoreEntryEncoding(NULL, 0, slen);
}

static size_t strEntryBytesLarge(size_t slen) {
    return slen + zipStorePrevEntryLength(NULL, ZIP_BIG_PREVLEN) + zipStoreEntryEncoding(NULL, 0, slen);
}

unsigned char *zl, *p;
unsigned char *entry;
unsigned int elen;
long long value;
int iteration;

int test_ziplistCreateIntList(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));

    zl = createIntList();
    /* "4294967296", "-100", "100", "128000", "non integer", "much much longer non integer" */

    p = ziplistIndex(zl, 0);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"4294967296", 10));

    p = ziplistIndex(zl, 1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"-100", 4));

    p = ziplistIndex(zl, 2);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"100", 3));

    p = ziplistIndex(zl, 3);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"128000", 6));

    p = ziplistIndex(zl, 4);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"non integer", 11));

    p = ziplistIndex(zl, 5);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"much much longer non integer", 28));

    zfree(zl);
    return 0;
}

int test_ziplistPop(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));

    zl = createList(); /* "hello", "foo", "quux", "1024" */

    p = ziplistIndex(zl, -1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"1024", 4));

    zl = pop(zl, ZIPLIST_TAIL); /* "hello", "foo", "quux" */

    p = ziplistIndex(zl, -1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"quux", 4));

    p = ziplistIndex(zl, 0);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"hello", 5));

    zl = pop(zl, ZIPLIST_HEAD); /* "foo", "quux" */

    p = ziplistIndex(zl, 0);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"foo", 3));

    zl = pop(zl, ZIPLIST_TAIL); /* "foo" */

    p = ziplistIndex(zl, -1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"foo", 3));

    zl = pop(zl, ZIPLIST_TAIL);

    p = ziplistIndex(zl, 0);
    TEST_ASSERT(p == NULL);

    zfree(zl);
    return 0;
}

int test_ziplistGetElementAtIndex3(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */
    p = ziplistIndex(zl, 3);
    TEST_ASSERT(p != NULL);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"1024", 4));
    zfree(zl);
    return 0;
}

int test_ziplistGetElementOutOfRange(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    p = ziplistIndex(zl, 4);
    TEST_ASSERT(p == NULL);
    zfree(zl);
    return 0;
}

int test_ziplistGetLastElement(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */
    p = ziplistIndex(zl, -1);
    TEST_ASSERT(p != NULL);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"1024", 4));
    zfree(zl);
    return 0;
}

int test_ziplistGetFirstElement(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */
    p = ziplistIndex(zl, -4);
    TEST_ASSERT(p != NULL);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"hello", 5));
    zfree(zl);
    return 0;
}

int test_ziplistGetElementOutOfRangeReverse(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */
    p = ziplistIndex(zl, -5);
    TEST_ASSERT(p == NULL);
    zfree(zl);
    return 0;
}

int test_ziplistIterateThroughFullList(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    p = ziplistIndex(zl, 0);
    while (ziplistGet(p, &entry, &elen, &value)) {
        TEST_ASSERT(p != NULL);
        p = ziplistNext(zl, p);
    }
    zfree(zl);
    return 0;
}

int test_ziplistIterateThroughListFrom1ToEnd(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    p = ziplistIndex(zl, 1);
    while (ziplistGet(p, &entry, &elen, &value)) {
        TEST_ASSERT(p != NULL);
        p = ziplistNext(zl, p);
    }
    zfree(zl);
    return 0;
}

int test_ziplistIterateThroughListFrom2ToEnd(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    p = ziplistIndex(zl, 2);
    while (ziplistGet(p, &entry, &elen, &value)) {
        TEST_ASSERT(p != NULL);
        p = ziplistNext(zl, p);
    }
    zfree(zl);
    return 0;
}

int test_ziplistIterateThroughStartOutOfRange(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    p = ziplistIndex(zl, 4);
    TEST_ASSERT(p == NULL);
    zfree(zl);
    return 0;
}

int test_ziplistIterateBackToFront(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    p = ziplistIndex(zl, -1);
    while (ziplistGet(p, &entry, &elen, &value)) {
        TEST_ASSERT(p != NULL);
        p = ziplistPrev(zl, p);
    }
    zfree(zl);
    return 0;
}

int test_ziplistIterateBackToFrontDeletingAllItems(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    p = ziplistIndex(zl, -1);
    while (ziplistGet(p, &entry, &elen, &value)) {
        TEST_ASSERT(p != NULL);
        zl = ziplistDelete(zl, &p);
        p = ziplistPrev(zl, p);
    }
    zfree(zl);
    return 0;
}

int test_ziplistDeleteInclusiveRange0To0(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */

    p = ziplistIndex(zl, 0);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"hello", 5));
    int orig_len = ziplistLen(zl);

    zl = ziplistDeleteRange(zl, 0, 1);
    p = ziplistIndex(zl, 0);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"foo", 3));
    int new_len = ziplistLen(zl);
    TEST_ASSERT(orig_len - 1 == new_len);
    zfree(zl);
    return 0;
}

int test_ziplistDeleteInclusiveRange0To1(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */

    p = ziplistIndex(zl, 0);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"hello", 5));
    p = ziplistIndex(zl, 1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"foo", 3));
    int orig_len = ziplistLen(zl);

    zl = ziplistDeleteRange(zl, 0, 2); /* "quux", "1024" */

    p = ziplistIndex(zl, 0);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"quux", 4));
    p = ziplistIndex(zl, 1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"1024", 4));
    int new_len = ziplistLen(zl);
    TEST_ASSERT(orig_len - 2 == new_len);
    zfree(zl);
    return 0;
}

int test_ziplistDeleteInclusiveRange1To2(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */

    p = ziplistIndex(zl, 1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"foo", 3));
    p = ziplistIndex(zl, 2);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"quux", 4));
    int orig_len = ziplistLen(zl);

    zl = ziplistDeleteRange(zl, 1, 2); /* "hello", "1024" */

    p = ziplistIndex(zl, 1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"1024", 4));
    int new_len = ziplistLen(zl);
    TEST_ASSERT(orig_len - 2 == new_len);
    zfree(zl);
    return 0;
}

int test_ziplistDeleteWithStartIndexOutOfRange(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    int orig_len = ziplistLen(zl);
    zl = ziplistDeleteRange(zl, 5, 1);
    int new_len = ziplistLen(zl);
    TEST_ASSERT(orig_len == new_len);
    zfree(zl);
    return 0;
}

int test_ziplistDeleteWithNumOverflow(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */

    int orig_len = ziplistLen(zl);
    zl = ziplistDeleteRange(zl, 1, 5);
    int new_len = ziplistLen(zl);
    TEST_ASSERT(orig_len - 3 == new_len);
    zfree(zl);
    return 0;
}

int test_ziplistDeleteFooWhileIterating(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */
    p = ziplistIndex(zl, 0);
    while (ziplistGet(p, &entry, &elen, &value)) {
        TEST_ASSERT(p != NULL);
        if (entry && strncmp("foo", (char *)entry, elen) == 0) {
            zl = ziplistDelete(zl, &p);
        } else {
            p = ziplistNext(zl, p);
        }
    }
    p = ziplistIndex(zl, 1);
    ziplistGet(p, &entry, &elen, &value);
    TEST_ASSERT(!ziplistCompare(p, (unsigned char *)"foo", 3));
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"quux", 4));
    zfree(zl);
    return 0;
}

int test_ziplistReplaceWithSameSize(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */
    unsigned char *orig_zl = zl;
    p = ziplistIndex(zl, 0);
    zl = ziplistReplace(zl, p, (unsigned char *)"zoink", 5);
    p = ziplistIndex(zl, 3);
    zl = ziplistReplace(zl, p, (unsigned char *)"yy", 2);
    p = ziplistIndex(zl, 1);
    zl = ziplistReplace(zl, p, (unsigned char *)"65536", 5);
    p = ziplistIndex(zl, 0);
    TEST_ASSERT(!memcmp((char *)p,
                        "\x00\x05zoink"
                        "\x07\xf0\x00\x00\x01" /* 65536 as int24 */
                        "\x05\x04quux"
                        "\x06\x02yy"
                        "\xff",
                        23));
    TEST_ASSERT(zl == orig_zl); /* no reallocations have happened */
    zfree(zl);
    return 0;
}

int test_ziplistReplaceWithDifferentSize(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList(); /* "hello", "foo", "quux", "1024" */
    p = ziplistIndex(zl, 1);
    zl = ziplistReplace(zl, p, (unsigned char *)"squirrel", 8);
    p = ziplistIndex(zl, 0);
    TEST_ASSERT(!strncmp((char *)p,
                         "\x00\x05hello"
                         "\x07\x08squirrel"
                         "\x0a\x04quux"
                         "\x06\xc0\x00\x04"
                         "\xff",
                         28));
    zfree(zl);
    return 0;
}

int test_ziplistRegressionTestForOver255ByteStrings(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    char v1[257] = {0}, v2[257] = {0};
    memset(v1, 'x', 256);
    memset(v2, 'y', 256);
    zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char *)v1, strlen(v1), ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)v2, strlen(v2), ZIPLIST_TAIL);

    /* Pop values again and compare their value. */
    p = ziplistIndex(zl, 0);
    TEST_ASSERT(ziplistGet(p, &entry, &elen, &value));
    TEST_ASSERT(strncmp(v1, (char *)entry, elen) == 0);
    p = ziplistIndex(zl, 1);
    TEST_ASSERT(ziplistGet(p, &entry, &elen, &value));
    TEST_ASSERT(strncmp(v2, (char *)entry, elen) == 0);
    zfree(zl);
    return 0;
}

int test_ziplistRegressionTestDeleteNextToLastEntries(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    char v[3][257] = {{0}};
    zlentry e[3] = {
        {.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0, .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};
    size_t i;

    for (i = 0; i < (sizeof(v) / sizeof(v[0])); i++) {
        memset(v[i], 'a' + i, sizeof(v[0]));
    }

    v[0][256] = '\0';
    v[1][1] = '\0';
    v[2][256] = '\0';

    zl = ziplistNew();
    for (i = 0; i < (sizeof(v) / sizeof(v[0])); i++) {
        zl = ziplistPush(zl, (unsigned char *)v[i], strlen(v[i]), ZIPLIST_TAIL);
    }

    verify(zl, e);

    TEST_ASSERT(e[0].prevrawlensize == 1);
    TEST_ASSERT(e[1].prevrawlensize == 5);
    TEST_ASSERT(e[2].prevrawlensize == 1);

    /* Deleting entry 1 will increase `prevrawlensize` for entry 2 */
    unsigned char *p = e[1].p;
    zl = ziplistDelete(zl, &p);

    verify(zl, e);

    TEST_ASSERT(e[0].prevrawlensize == 1);
    TEST_ASSERT(e[1].prevrawlensize == 5);

    zfree(zl);
    return 0;
}

int test_ziplistCreateLongListAndCheckIndices(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = ziplistNew();
    char buf[32];
    int i, len;
    for (i = 0; i < 1000; i++) {
        len = snprintf(buf, sizeof(buf), "%d", i);
        zl = ziplistPush(zl, (unsigned char *)buf, len, ZIPLIST_TAIL);
    }
    for (i = 0; i < 1000; i++) {
        p = ziplistIndex(zl, i);
        TEST_ASSERT(ziplistGet(p, NULL, NULL, &value));
        TEST_ASSERT(i == value);

        p = ziplistIndex(zl, -i - 1);
        TEST_ASSERT(ziplistGet(p, NULL, NULL, &value));
        TEST_ASSERT(999 - i == value);
    }
    zfree(zl);
    return 0;
}

int test_ziplistCompareStringWithZiplistEntries(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    zl = createList();
    p = ziplistIndex(zl, 0);

    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"hello", 5));
    TEST_ASSERT(!ziplistCompare(p, (unsigned char *)"hella", 5));

    p = ziplistIndex(zl, 3);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"1024", 4));
    TEST_ASSERT(!ziplistCompare(p, (unsigned char *)"1025", 4));
    zfree(zl);
    return 0;
}

int test_ziplistMergeTest(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    /* create list gives us: [hello, foo, quux, 1024] */
    zl = createList();
    unsigned char *zl2 = createList();

    unsigned char *zl3 = ziplistNew();
    unsigned char *zl4 = ziplistNew();

    TEST_ASSERT(!ziplistMerge(&zl4, &zl4));

    /* Merge two empty ziplists, get empty result back. */
    zl4 = ziplistMerge(&zl3, &zl4);
    TEST_ASSERT(!ziplistLen(zl4));
    zfree(zl4);

    zl2 = ziplistMerge(&zl, &zl2);
    /* merge gives us: [hello, foo, quux, 1024, hello, foo, quux, 1024] */

    TEST_ASSERT(ziplistLen(zl2) == 8);

    p = ziplistIndex(zl2, 0);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"hello", 5));
    TEST_ASSERT(!ziplistCompare(p, (unsigned char *)"hella", 5));

    p = ziplistIndex(zl2, 3);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"1024", 4));
    TEST_ASSERT(!ziplistCompare(p, (unsigned char *)"1025", 4));

    p = ziplistIndex(zl2, 4);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"hello", 5));
    TEST_ASSERT(!ziplistCompare(p, (unsigned char *)"hella", 5));

    p = ziplistIndex(zl2, 7);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"1024", 4));
    TEST_ASSERT(!ziplistCompare(p, (unsigned char *)"1025", 4));

    zfree(zl);
    return 0;
}

int test_ziplistStressWithRandomPayloadsOfDifferentEncoding(int argc, char **argv, int flags) {
    if (argc >= 4) srand(atoi(argv[3]));
    int accurate = (flags & UNIT_TEST_ACCURATE);
    int i, j, len, where;
    unsigned char *p;
    char buf[1024];
    int buflen;
    list *ref;
    listNode *refnode;

    /* Hold temp vars from ziplist */
    unsigned char *sstr;
    unsigned int slen;
    long long sval = 0;

    iteration = accurate ? 20000 : 20;
    for (i = 0; i < iteration; i++) {
        zl = ziplistNew();
        ref = listCreate();
        listSetFreeMethod(ref, sdsfreeVoid);
        len = rand() % 256;

        /* Create lists */
        for (j = 0; j < len; j++) {
            where = (rand() & 1) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
            if (rand() % 2) {
                buflen = randstring(buf, 1, sizeof(buf) - 1);
            } else {
                switch (rand() % 3) {
                case 0: buflen = snprintf(buf, sizeof(buf), "%lld", (0LL + rand()) >> 20); break;
                case 1: buflen = snprintf(buf, sizeof(buf), "%lld", (0LL + rand())); break;
                case 2: buflen = snprintf(buf, sizeof(buf), "%lld", (0LL + rand()) << 20); break;
                default: TEST_ASSERT(NULL);
                }
            }

            /* Add to ziplist */
            zl = ziplistPush(zl, (unsigned char *)buf, buflen, where);

            /* Add to reference list */
            if (where == ZIPLIST_HEAD) {
                listAddNodeHead(ref, sdsnewlen(buf, buflen));
            } else if (where == ZIPLIST_TAIL) {
                listAddNodeTail(ref, sdsnewlen(buf, buflen));
            } else {
                TEST_ASSERT(NULL);
            }
        }

        TEST_ASSERT(listLength(ref) == ziplistLen(zl));
        for (j = 0; j < len; j++) {
            /* Naive way to get elements, but similar to the stresser
             * executed from the Tcl test suite. */
            p = ziplistIndex(zl, j);
            refnode = listIndex(ref, j);

            TEST_ASSERT(ziplistGet(p, &sstr, &slen, &sval));
            if (sstr == NULL) {
                buflen = snprintf(buf, sizeof(buf), "%lld", sval);
            } else {
                buflen = slen;
                memcpy(buf, sstr, buflen);
                buf[buflen] = '\0';
            }
            TEST_ASSERT(memcmp(buf, listNodeValue(refnode), buflen) == 0);
        }
        zfree(zl);
        listRelease(ref);
    }
    return 0;
}

int test_ziplistCascadeUpdateEdgeCases(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    /* Inserting a entry with data length greater than ZIP_BIG_PREVLEN-4
     * will leads to cascade update. */
    size_t s1 = ZIP_BIG_PREVLEN - 4, s2 = ZIP_BIG_PREVLEN - 3;
    zl = ziplistNew();

    zlentry e[4] = {
        {.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0, .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};

    zl = insertHelper(zl, 'a', s1, ZIPLIST_ENTRY_HEAD(zl));
    verify(zl, e);

    TEST_ASSERT(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
    TEST_ASSERT(compareHelper(zl, 'a', s1, 0));

    /* No expand. */
    zl = insertHelper(zl, 'b', s1, ZIPLIST_ENTRY_HEAD(zl));
    verify(zl, e);

    TEST_ASSERT(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
    TEST_ASSERT(compareHelper(zl, 'b', s1, 0));

    TEST_ASSERT(e[1].prevrawlensize == 1 && e[1].prevrawlen == strEntryBytesSmall(s1));
    TEST_ASSERT(compareHelper(zl, 'a', s1, 1));


    /* Expand(tail included). */
    zl = insertHelper(zl, 'c', s2, ZIPLIST_ENTRY_HEAD(zl));
    verify(zl, e);

    TEST_ASSERT(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
    TEST_ASSERT(compareHelper(zl, 'c', s2, 0));

    TEST_ASSERT(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
    TEST_ASSERT(compareHelper(zl, 'b', s1, 1));

    TEST_ASSERT(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s1));
    TEST_ASSERT(compareHelper(zl, 'a', s1, 2));


    /* Expand(only previous head entry). */
    zl = insertHelper(zl, 'd', s2, ZIPLIST_ENTRY_HEAD(zl));
    verify(zl, e);

    TEST_ASSERT(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
    TEST_ASSERT(compareHelper(zl, 'd', s2, 0));

    TEST_ASSERT(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
    TEST_ASSERT(compareHelper(zl, 'c', s2, 1));

    TEST_ASSERT(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s2));
    TEST_ASSERT(compareHelper(zl, 'b', s1, 2));

    TEST_ASSERT(e[3].prevrawlensize == 5 && e[3].prevrawlen == strEntryBytesLarge(s1));
    TEST_ASSERT(compareHelper(zl, 'a', s1, 3));


    /* Delete from mid. */
    unsigned char *p = ziplistIndex(zl, 2);
    zl = ziplistDelete(zl, &p);
    verify(zl, e);

    TEST_ASSERT(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
    TEST_ASSERT(compareHelper(zl, 'd', s2, 0));

    TEST_ASSERT(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
    TEST_ASSERT(compareHelper(zl, 'c', s2, 1));

    TEST_ASSERT(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s2));
    TEST_ASSERT(compareHelper(zl, 'a', s1, 2));


    zfree(zl);
    return 0;
}

int test_ziplistInsertEdgeCase(int argc, char **argv, int flags) {
    UNUSED(flags);
    if (argc >= 4) srand(atoi(argv[3]));
    // From issue #7170
    zl = ziplistNew();

    /* We set some values to almost reach the critical point - 254 */
    char A_252[253] = {0}, A_250[251] = {0};
    memset(A_252, 'A', 252);
    memset(A_250, 'A', 250);

    /* After the rpush, the list look like: [one two A_252 A_250 three 10] */
    zl = ziplistPush(zl, (unsigned char *)"one", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)"two", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)A_252, strlen(A_252), ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)A_250, strlen(A_250), ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)"three", 5, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)"10", 2, ZIPLIST_TAIL);

    p = ziplistIndex(zl, 2);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)A_252, strlen(A_252)));

    /* When we remove A_252, the list became: [one two A_250 three 10]
     * A_250's prev node became node two, because node two quite small
     * So A_250's prevlenSize shrink to 1, A_250's total size became 253(1+2+250)
     * The prev node of node three is still node A_250.
     * We will not shrink the node three's prevlenSize, keep it at 5 bytes */
    zl = ziplistDelete(zl, &p);

    p = ziplistIndex(zl, 3);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"three", 5));

    /* We want to insert a node after A_250, the list became: [one two A_250 10 three 10]
     * Because the new node is quite small, node three prevlenSize will shrink to 1 */
    zl = ziplistInsert(zl, p, (unsigned char *)"10", 2);

    /* Last element should equal 10 */
    p = ziplistIndex(zl, -1);
    TEST_ASSERT(ziplistCompare(p, (unsigned char *)"10", 2));

    zfree(zl);
    return 0;
}

int test_ziplistStressWithVariableSize(int argc, char **argv, int flags) {
    if (argc >= 4) srand(atoi(argv[3]));
    int accurate = (flags & UNIT_TEST_ACCURATE);

    unsigned long long start = usec();
    int maxsize = accurate ? 16384 : 16;
    stress(ZIPLIST_HEAD, 100000, maxsize, 256);
    TEST_PRINT_INFO("Stress with variable size HEAD: usec=%lld", usec() - start);

    start = usec();
    stress(ZIPLIST_TAIL, 100000, maxsize, 256);
    TEST_PRINT_INFO("Stress with variable size TAIL: usec=%lld", usec() - start);

    return 0;
}

int test_BenchmarkziplistFind(int argc, char **argv, int flags) {
    if (argc >= 4) srand(atoi(argv[3]));
    int accurate = (flags & UNIT_TEST_ACCURATE);

    zl = ziplistNew();
    iteration = accurate ? 100000 : 100;
    for (int i = 0; i < iteration; i++) {
        char buf[4096] = "asdf";
        zl = ziplistPush(zl, (unsigned char *)buf, 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 40, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 400, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 4000, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1", 1, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10", 2, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100", 3, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1000", 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10000", 5, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100000", 6, ZIPLIST_TAIL);
    }

    unsigned long long start = usec();
    for (int i = 0; i < 2000; i++) {
        unsigned char *fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        fptr = ziplistFind(zl, fptr, (unsigned char *)"nothing", 7, 1);
    }
    TEST_PRINT_INFO("Benchmark ziplistFind: usec=%lld", usec() - start);

    zfree(zl);
    return 0;
}

int test_BenchmarkziplistIndex(int argc, char **argv, int flags) {
    if (argc >= 4) srand(atoi(argv[3]));
    int accurate = (flags & UNIT_TEST_ACCURATE);

    zl = ziplistNew();
    iteration = accurate ? 100000 : 100;
    for (int i = 0; i < iteration; i++) {
        char buf[4096] = "asdf";
        zl = ziplistPush(zl, (unsigned char *)buf, 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 40, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 400, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 4000, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1", 1, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10", 2, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100", 3, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1000", 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10000", 5, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100000", 6, ZIPLIST_TAIL);
    }

    unsigned long long start = usec();
    for (int i = 0; i < 2000; i++) {
        ziplistIndex(zl, 99999);
    }
    TEST_PRINT_INFO("Benchmark ziplistIndex: usec=%lld", usec() - start);

    zfree(zl);
    return 0;
}

int test_BenchmarkziplistValidateIntegrity(int argc, char **argv, int flags) {
    if (argc >= 4) srand(atoi(argv[3]));
    int accurate = (flags & UNIT_TEST_ACCURATE);
    zl = ziplistNew();
    iteration = accurate ? 100000 : 100;
    for (int i = 0; i < iteration; i++) {
        char buf[4096] = "asdf";
        zl = ziplistPush(zl, (unsigned char *)buf, 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 40, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 400, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 4000, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1", 1, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10", 2, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100", 3, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1000", 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10000", 5, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100000", 6, ZIPLIST_TAIL);
    }
    unsigned long long start = usec();
    for (int i = 0; i < 2000; i++) {
        ziplistValidateIntegrity(zl, ziplistBlobLen(zl), 1, NULL, NULL);
    }
    TEST_PRINT_INFO("Benchmark ziplistValidateIntegrity: usec=%lld", usec() - start);

    zfree(zl);
    return 0;
}

int test_BenchmarkziplistCompareWithString(int argc, char **argv, int flags) {
    if (argc >= 4) srand(atoi(argv[3]));
    int accurate = (flags & UNIT_TEST_ACCURATE);
    zl = ziplistNew();
    iteration = accurate ? 100000 : 100;
    for (int i = 0; i < iteration; i++) {
        char buf[4096] = "asdf";
        zl = ziplistPush(zl, (unsigned char *)buf, 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 40, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 400, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 4000, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1", 1, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10", 2, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100", 3, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1000", 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10000", 5, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100000", 6, ZIPLIST_TAIL);
    }
    unsigned long long start = usec();
    for (int i = 0; i < 2000; i++) {
        unsigned char *eptr = ziplistIndex(zl, 0);
        while (eptr != NULL) {
            ziplistCompare(eptr, (unsigned char *)"nothing", 7);
            eptr = ziplistNext(zl, eptr);
        }
    }
    TEST_PRINT_INFO("Benchmark ziplistCompare with string: usec=%lld", usec() - start);

    zfree(zl);
    return 0;
}

int test_BenchmarkziplistCompareWithNumber(int argc, char **argv, int flags) {
    if (argc >= 4) srand(atoi(argv[3]));
    int accurate = (flags & UNIT_TEST_ACCURATE);
    zl = ziplistNew();
    iteration = accurate ? 100000 : 100;
    for (int i = 0; i < iteration; i++) {
        char buf[4096] = "asdf";
        zl = ziplistPush(zl, (unsigned char *)buf, 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 40, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 400, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)buf, 4000, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1", 1, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10", 2, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100", 3, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"1000", 4, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"10000", 5, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)"100000", 6, ZIPLIST_TAIL);
    }
    unsigned long long start = usec();
    for (int i = 0; i < 2000; i++) {
        unsigned char *eptr = ziplistIndex(zl, 0);
        while (eptr != NULL) {
            ziplistCompare(eptr, (unsigned char *)"99999", 5);
            eptr = ziplistNext(zl, eptr);
        }
    }
    TEST_PRINT_INFO("Benchmark ziplistCompare with number: usec=%lld", usec() - start);

    zfree(zl);
    return 0;
}

int test_ziplistStress__ziplistCascadeUpdate(int argc, char **argv, int flags) {
    if (argc >= 4) srand(atoi(argv[3]));
    int accurate = (flags & UNIT_TEST_ACCURATE);
    char data[ZIP_BIG_PREVLEN];
    zl = ziplistNew();
    iteration = accurate ? 100000 : 100;
    for (int i = 0; i < iteration; i++) {
        zl = ziplistPush(zl, (unsigned char *)data, ZIP_BIG_PREVLEN - 4, ZIPLIST_TAIL);
    }
    unsigned long long start = usec();
    zl = ziplistPush(zl, (unsigned char *)data, ZIP_BIG_PREVLEN - 3, ZIPLIST_HEAD);
    TEST_PRINT_INFO("Stress __ziplistCascadeUpdate: usec=%lld", usec() - start);


    zfree(zl);
    return 0;
}
