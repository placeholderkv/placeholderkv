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

#define MSGTYPE_PING 1
#define MSGTYPE_PONG 2

/* test.pingall */
int PingallCommand_ValkeyCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_SendClusterMessage(ctx, NULL, MSGTYPE_PING, "Hey", 3);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

/* Callback for message MSGTYPE_PING */
void PingReceiver(ValkeyModuleCtx *ctx,
                  const char *sender_id,
                  uint8_t type,
                  const unsigned char *payload,
                  uint32_t len) {
    ValkeyModule_Log(ctx, "notice", "PING (type %d) RECEIVED from %.*s: '%.*s'", type, VALKEYMODULE_NODE_ID_LEN,
                     sender_id, (int)len, payload);
    ValkeyModule_SendClusterMessage(ctx, NULL, MSGTYPE_PONG, "Ohi!", 4);
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "INCR", "c", "pings_received");
    ValkeyModule_FreeCallReply(reply);
}

/* Callback for message MSGTYPE_PONG */
void PongReceiver(ValkeyModuleCtx *ctx,
                  const char *sender_id,
                  uint8_t type,
                  const unsigned char *payload,
                  uint32_t len) {
    ValkeyModule_Log(ctx, "notice", "PONG (type %d) RECEIVED from %.*s: '%.*s'", type, VALKEYMODULE_NODE_ID_LEN,
                     sender_id, (int)len, payload);
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "cluster", 1, VALKEYMODULE_APIVER_1)== VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "test.pingall", PingallCommand_ValkeyCommand, "readonly", 0, 0, 0) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "test.cluster_slots", test_cluster_slots, "", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "test.cluster_shards", test_cluster_shards, "", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}
