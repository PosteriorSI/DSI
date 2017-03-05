#ifndef PROCARRAY_H_
#define PROCARRAY_H_

#include <stdbool.h>
#include <pthread.h>
#include "timestamp.h"
#include "master.h"
#include "type.h"

#define MAXPROCS THREADNUM*NODENUM

struct PROC
{
	TransactionId tid;
    pthread_t pid;

	int index;//the index for per thread.
};

typedef struct PROC PROC;

struct TransIdMgr
{
	TransactionId curid;
	TransactionId maxid;
	TransactionId latestcompletedId;
	pthread_mutex_t IdLock;
};


typedef struct TransIdMgr IDMGR;

extern IDMGR* CentIdMgr;

extern PROC* procbase;

extern void InitProc(void);

extern void ResetProc(void);

extern Size ProcArraySize(void);

extern void AtEnd_ProcArray(int index);

extern void InitTransactionIdAssign(void);

extern TransactionId AssignTransactionId(void);

extern TimeStampTz SetCurrentTransactionStartTimestamp(void);

extern TimeStampTz SetCurrentTransactionStopTimestamp(void);

extern void GetServerTransactionSnapshot(int index, int * tcount, int * min, int * max, uint64_t * tid_array);

extern void ProcArrayAdd(int index, int tid, pthread_t pid);

//extern void AtStart_ProcArray(TransactionData* td, int index);

extern bool IsTransactionActive(int index, TransactionId tid);
#endif
