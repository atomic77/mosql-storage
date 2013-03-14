#ifndef _REMOTE_MOCK_H_
#define _REMOTE_MOCK_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "dsmDB_priv.h"

struct remote_mock;

struct remote_mock* remote_mock_new();
void remote_mock_free(struct remote_mock* rm);
val* mock_recover_key(struct remote_mock* rm, key* k);

#ifdef __cplusplus
}
#endif
#endif