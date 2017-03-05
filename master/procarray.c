#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "timestamp.h"
#include "procarray.h"
#include "type.h"
#include "lock.h"

PROC* procbase;

IDMGR* CentIdMgr;

Size ProcArraySize(void)
{
	return sizeof(PROC)*MAXPROCS;
}

void InitProc(void)
{
	Size size;
	int i;
	PROC* proc;
	//initialize the process array.
	size=ProcArraySize();
	procbase=(PROC*)malloc(size);

	if(procbase==NULL)
	{
		printf("memory alloc failed for procarray.\n");
		return;
	}

	memset((char*)procbase,0,ProcArraySize());

	for(i=0;i<MAXPROCS;i++)
	{
		proc=(PROC*)((char*)procbase+i*sizeof(PROC));
		proc->index=i;
	}
}

/*
void ResetProc(void)
{
	int i;
	PROC* proc;

	prohd->maxprocs=MAXPROCS;
	prohd->numprocs=0;
}
*/

/*
 * clean the process array at the end of transaction by index.
 */
void AtEnd_ProcArray(int index)
{
	PROC* proc;
	TransactionId tid;
	proc=procbase+index;
	tid=proc->tid;

	AcquireWrLock(&ProcArrayLock, LOCK_EXCLUSIVE);

	proc->tid=InvalidTransactionId;
	proc->pid=0;

	if(CentIdMgr->latestcompletedId < tid)
		CentIdMgr->latestcompletedId = tid;

	ReleaseWrLock(&ProcArrayLock);
}

void InitTransactionIdAssign(void)
{
	Size size;
	size=sizeof(IDMGR);

	CentIdMgr=(IDMGR*)malloc(size);

	if(CentIdMgr==NULL)
	{
		printf("malloc error for IdMgr.\n");
		return;
	}

	CentIdMgr->curid=1;
	CentIdMgr->latestcompletedId=InvalidTransactionId;
}

TransactionId AssignTransactionId(void)
{
	TransactionId tid;
	pthread_mutex_lock(&CentIdMgr->IdLock);

	tid=CentIdMgr->curid++;

	pthread_mutex_unlock(&CentIdMgr->IdLock);

	if(tid <= CentIdMgr->maxid)
		return InvalidTransactionId;
	return tid;
}

TimeStampTz SetCurrentTransactionStartTimestamp(void)
{
	return GetCurrentTimestamp();
}

TimeStampTz SetCurrentTransactionStopTimestamp(void)
{
	return GetCurrentTimestamp();
}

void GetServerTransactionSnapshot(int index, int * tcount, int * min, int * max, uint64_t * tid_array)
{
	PROC* proc;
	TransactionId tid_min;
	TransactionId tid_max;
	TransactionId tid;
	int i;
    int count;
	//add read-lock here on ProcArray.
	//AcquireWrLock(&ProcArrayLock, LOCK_SHARED);
        AcquireWrLock(&ProcArrayLock, LOCK_EXCLUSIVE);

	tid_max=CentIdMgr->latestcompletedId;
	tid_max+=1;

	tid_min=tid_max;
	count=0;

	for(i=0;i<MAXPROCS;i++)
	//for(i=0;i<=NumTerminals;i++)
	{
		proc=procbase+i;
		tid=proc->tid;
		if(TransactionIdIsValid(tid) && i != index)
		{
			if(tid < tid_min)
				tid_min=tid;

			tid_array[count++]=(uint64_t)tid;
		}
	}

	ReleaseWrLock(&ProcArrayLock);

	*tcount=count;
	*min=tid_min;
	*max=tid_max;
	//printf("PID:%u count:%d min:%d max:%d \n",pthread_self(),count,tid_min,tid_max);
}

void ProcArrayAdd(int index, int tid, pthread_t pid)
{
	PROC* proc;

	proc=procbase+index;

	AcquireWrLock(&ProcArrayLock, LOCK_EXCLUSIVE);

	proc->tid=tid;
	proc->pid=pid;

	ReleaseWrLock(&ProcArrayLock);
}
