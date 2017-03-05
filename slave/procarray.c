/*
 * procarray.c
 *
 *  Created on: May 5, 2016
 *      Author: xiaoxin
 */
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdlib.h>
#include "procarray.h"
#include "shmem.h"
#include "socket.h"
#include "trans.h"
#include "lock.h"

PROC* procarray=NULL;
int procarray_shmid;

size_t ProcArraySize(void)
{
	return sizeof(PROC)*THREADNUM;
}

void InitProcArray(void)
{
	int i;
	size_t size;

	size=ProcArraySize();

	procarray=(PROC*)ShmemAlloc(size);

	if((PROC*)-1==procarray)
	{
		printf("procarray error\n");
		exit(-1);
	}

	memset((char*)procarray, 0, size);

	for(i=0;i<THREADNUM;i++)
		procarray[i].index=i;
}

void setGlobalCidRange(int index, Cid low, Cid up)
{
	AcquireWrLock(ProcArrayLock, LOCK_SHARED);

	procarray[index].global_cid_range_low=low;
	procarray[index].global_cid_range_up=up;

	ReleaseWrLock(ProcArrayLock);
}

void getGlobalCidRange(int index, Cid* low, Cid* up)
{
	AcquireWrLock(ProcArrayLock, LOCK_SHARED);

	*low=procarray[index].global_cid_range_low;
	*up=procarray[index].global_cid_range_up;

	ReleaseWrLock(ProcArrayLock);
}

void setGlobalCidRangeUp(int index, Cid up)
{
	//AcquireWrLock(ProcArrayLock, LOCK_SHARED);

	procarray[index].global_cid_range_up=up;

	//ReleaseWrLock(ProcArrayLock);
}

void AtStart_ProcArray(int index, TransactionId tid, Cid low)
{
	AcquireWrLock(ProcArrayLock, LOCK_SHARED);

	procarray[index].tid=tid;
	procarray[index].global_cid_range_low=low;
	procarray[index].global_cid_range_up=0;

	ReleaseWrLock(ProcArrayLock);
}

void AtEnd_ProcArray(int index)
{
	AcquireWrLock(ProcArrayLock, LOCK_SHARED);

	procarray[index].tid=InvalidTransactionId;
	procarray[index].global_cid_range_low=0;
	procarray[index].global_cid_range_up=0;

	ReleaseWrLock(ProcArrayLock);
}

int getTransactionIdIndex(TransactionId tid)
{
	int index=-1;
	int i;

	for(i=0;i<=THREADNUM;i++)
	{
		if(procarray[i].tid == tid)
		{
			index=i;
			break;
		}
	}

	return index;

}
