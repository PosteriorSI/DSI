#ifndef MASTERDATA_H_
#define MASTERDATA_H_

#include "type.h"

extern void ProcessStartTransaction(uint64_t *recv_buffer, int conn, int mindex);
extern void ProcessEndTimestamp(uint64_t *recv_buffer, int conn, int mindex);
extern void ProcessUpdateProcarray(uint64_t *recv_buffer, int conn, int mindex);

extern void ProcessGlobalCID(uint64_t *recv_buffer, int conn, int mindex);

#endif
