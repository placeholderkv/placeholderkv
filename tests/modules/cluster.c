#include "valkeymodule.h"

#define UNUSED(x) (void)(x)

int test_cluster_slots(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    UNUSED(argv);

    if (argc != 1) return ValkeyModule_WrongArity(ctx);

    ValkeyModuleCallReply *rep = ValkeyModule_Call(ctx, "CLUSTER", "c", "SLOTS");
    if (!rep) {
        ValkeyModule_ReplyWithError(ctx, "ERR NULL reply returned");
    } else {
        ValkeyModule_ReplyWithCallReply(ctx, rep);
        ValkeyModule_FreeCallReply(rep);
    }

    return VALKEYMODULE_OK;
}

int test_cluster_shards(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    UNUSED(argv);

    if (argc != 1) return ValkeyModule_WrongArity(ctx);

    ValkeyModuleCallReply *rep = ValkeyModule_Call(ctx, "CLUSTER", "c", "SHARDS");
    if (!rep) {
        ValkeyModule_ReplyWithError(ctx, "ERR NULL reply returned");
    } else {
        ValkeyModule_ReplyWithCallReply(ctx, rep);
        ValkeyModule_FreeCallReply(rep);
    }

    return VALKEYMODULE_OK;
}

#define MSGTYPE_DING 1
#define MSGTYPE_DONG 2

/* test.pingall */
int PingallCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_SendClusterMessage(ctx, NULL, MSGTYPE_DING, "Hey", 3);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

void DingReceiver(ValkeyModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len) {
    ValkeyModule_Log(ctx, "notice", "DING (type %d) RECEIVED from %.*s: '%.*s'", type, VALKEYMODULE_NODE_ID_LEN, sender_id, (int)len, payload);
    ValkeyModule_SendClusterMessage(ctx, sender_id, MSGTYPE_DONG, "Message Received!", 17);
}

void DongReceiver(ValkeyModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len) {
    ValkeyModule_Log(ctx, "notice", "DONG (type %d) RECEIVED from %.*s: '%.*s'", type, VALKEYMODULE_NODE_ID_LEN, sender_id, (int)len, payload);
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "cluster", 1, VALKEYMODULE_APIVER_1)== VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "test.pingall", PingallCommand, "readonly", 0, 0, 0) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "test.cluster_slots", test_cluster_slots, "", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "test.cluster_shards", test_cluster_shards, "", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    /* Register our handlers for different message types. */
    ValkeyModule_RegisterClusterMessageReceiver(ctx, MSGTYPE_DING, DingReceiver);
    ValkeyModule_RegisterClusterMessageReceiver(ctx, MSGTYPE_DONG, DongReceiver);
    return VALKEYMODULE_OK;
}
