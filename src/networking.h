#ifndef __NETWORKING_H
#define __NETWORKING_H

#include "server.h"

int willClientOutputBufferExceedLimits(client *c, unsigned long long command_size);

#endif
