/*
 * proc.h
 *
 *  Created on: 2015��11��9��
 *      Author: DELL
 */
/*
 * per process information's structure.
 */
#ifndef PROC_H_
#define PROC_H_

#include<pthread.h>
#include<stdbool.h>

#include "type.h"
#include "transactions.h"
#include "socket.h"

#define MAXPROCS THREADNUM*NODENUM

/*
 * information about process array.
 * maxprocs: max process number.
 */
struct PROCHEAD
{
	int numprocs;
	int maxprocs;
	pthread_mutex_t ilock;
};

typedef struct PROCHEAD PROCHEAD;

struct THREADINFO
{
	int index;//index for process array.
	char* memstart;//start address of current thread's private memory.

	TransactionId curid;
	TransactionId maxid;
};

typedef struct THREADINFO THREAD;

typedef struct terminalArgs
{
	int whse_id;
	int dist_id;
	int type;//'0' for load data, '1' for run transaction.

	//used to wait until all terminals arrive.
	pthread_barrier_t *barrier;

	//used to transactions statistic.
	TransState *StateInfo;
}terminalArgs;

extern pthread_cond_t* cond;

extern pthread_mutex_t* mutex;

extern int* WaitState;

extern size_t PthreadCondSize(void);

extern void InitPthreadCond(void);

extern void InitProcHead(int flag);

extern void ResetProc(void);

//extern void *ProcStart(void* args);

extern int UpdateProcStartId(int index,CommitId cid);

extern int UpdateProcCommitId(int index,StartId sid);

extern int AtRead_UpdateProcId(int index, StartId sid_min);

extern CommitId GetTransactionCidMin(int index);

extern StartId GetTransactionSidMin(int index);

extern StartId GetTransactionSidMax(int index);

extern int IsPairConflict(int index, CommitId cid);

extern void* ServiceProcStart(void* args);

extern void *TransactionProcStart(void* args);

extern void* EMapCleanProcStart(void* args);

#endif /* PROC_H_ */
