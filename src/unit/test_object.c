#include "../object.c"
#include "test_help.h"

#include <stdio.h>
#include <limits.h>
#include <string.h>
#include <assert.h>
#include <math.h>


int test_object_with_key(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);
    sds key = sdsnew("foo");
    robj *val = createStringObject("bar", strlen("bar"));
    TEST_ASSERT(val->encoding == OBJ_ENCODING_EMBSTR);

    /* Prevent objectSetKeyAndExpire from freeing the old val when reallocating it. */
    incrRefCount(val);

    /* Create robj with key. */
    robj *valkey = objectSetKeyAndExpire(val, key, -1);
    TEST_ASSERT(valkey->encoding == OBJ_ENCODING_EMBSTR);
    TEST_ASSERT(objectGetKey(valkey) != NULL);

    /* Check embedded key "foo" */
    TEST_ASSERT(sdslen(objectGetKey(valkey)) == 3);
    TEST_ASSERT(sdslen(key) == 3);
    TEST_ASSERT(sdscmp(objectGetKey(valkey), key) == 0);
    TEST_ASSERT(strcmp(objectGetKey(valkey), "foo") == 0);

    /* Check embedded value "bar" (EMBSTR content) */
    TEST_ASSERT(sdscmp(objectGetVal(valkey), objectGetVal(val)) == 0);
    TEST_ASSERT(strcmp(objectGetVal(valkey), "bar") == 0);

    /* Either they're two separate objects, or one object with refcount == 2. */
    if (valkey == val) {
        TEST_ASSERT(valkey->refcount == 2);
    } else {
        TEST_ASSERT(valkey->refcount == 1);
        TEST_ASSERT(val->refcount == 1);
    }

    /* Free them. */
    sdsfree(key);
    decrRefCount(val);
    decrRefCount(valkey);
    return 0;
}
