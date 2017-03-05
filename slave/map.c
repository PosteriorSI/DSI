/*
 * map.c
 *
 *  Created on: May 2, 2016
 *      Author: xiaoxin
 */
#include <assert.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdlib.h>
#include <unistd.h>
#include "type.h"
#include "map.h"
#include "shmem.h"
#include "socket.h"
#include "config.h"
#include "lock.h"
#include "trans.h"
#include "proc.h"

#define THRESHOLD (int)(EMapNum*0.6)
#define MINUSNUM (int)(EMapNum*0.6)
#define CID_MINUS_TO_CLEAN ((NODENUM*THREADNUM*3 < MINUSNUM) ? MINUSNUM : NODENUM*THREADNUM*3)

int gcid2lcid_shmid;
int gcid2lsnap_shmid;
int readsFromGcid_shmid;

EMap* gcid2lcid=NULL;

EMap* gcid2lsnap=NULL;

LMap* readsFromGcid=NULL;

size_t EMapsize(void)
{
	int size;

	size=EMapNum;

	return size*sizeof(EMap);
}

size_t LMapsize(void)
{
	int size;

	size=THREADNUM;

	return sizeof(LMap)+size*sizeof(TransactionId);
}

void InitEMap(void)
{
	//int shmid;
	size_t size;

	size=EMapsize();

	gcid2lcid=(EMap*)ShmemAlloc(size);

	if(gcid2lcid==NULL)
	{
		printf("gcid2lcid error\n");
		exit(-1);
	}

	gcid2lsnap=(EMap*)ShmemAlloc(size);

	if(gcid2lsnap==NULL)
	{
		printf("gcid2lcid error\n");
		exit(-1);
	}

	memset((char*)gcid2lcid, 0, size);
	memset((char*)gcid2lsnap, 0, size);
}

void InitLMap(void)
{
	//int shmid;
	void* addr;
	size_t size;

	size=LMapsize();

	addr=ShmemAlloc(size);

	memset((char*)addr, 0, size);

	readsFromGcid=(LMap*)addr;

	readsFromGcid->list=(TransactionId*)((char*)addr+sizeof(LMap));
	readsFromGcid->tail=0;
}

int EMapHash(Cid key, int k)
{
	//'size' should be prime number.
	int size;

	size=EMapNum;

	return ((key%size + k * (1 + (((key>>5) +1) % (size - 1)))) % size);
}
/*
 * add a element to the Map.
 */
Cid EMapAdd(Cid key, Cid value, int flag)
{
	Cid IsExist=0;
	int step=0;
	int size;
	int index;
	EMap* map;
	pthread_spinlock_t* latcharray;

	int* Num;
	pthread_spinlock_t* NumLatch;
	bool full;

	if(flag==0)
	{
		map=gcid2lcid;
		latcharray=Gcid2LcidLatch;
		Num=&ShmVariable->LcidNum;
		NumLatch=LcidNumLatch;
	}
	else
	{
		map=gcid2lsnap;
		latcharray=Gcid2LsnapLatch;
		Num=&ShmVariable->LsnapNum;
		NumLatch=LsnapNumLatch;
	}

	size=EMapNum;

	/*
	if(map->num == size)
	{
		printf("map is already full\n");
		return -1;
	}
	*/
	do
	{
		index=EMapHash(key, step);


		AcquireLatch(&latcharray[index]);

		if(map[index].key == 0)
		{
			map[index].key=key;
			map[index].value=value;
			ReleaseLatch(&latcharray[index]);
			break;
		}
		else if(map[index].key == key)
		{
			if(map[index].value != value)
			{
				IsExist=map[index].value;
			}
			ReleaseLatch(&latcharray[index]);
			break;
		}

		ReleaseLatch(&latcharray[index]);

		/*
		if(__sync_bool_compare_and_swap(&map[index].key, 0, key))
		{
			map[index].value=value;
			//map[index].num++;
			break;
		}
		else if(map[index].key == key)
		{
			if(map[index].value != value)
			{
				IsExist=map[index].value;
			}
			break;
		}
		*/
		step++;
	}while(step<size);

	if(step >= size)
	{
		printf("EMap space is full flag=%d\n", flag);
		exit(-1);
	}


	if(IsExist==0)
	{
		AcquireLatch(NumLatch);

		full=(++(*Num) > THRESHOLD)?true:false;

		ReleaseLatch(NumLatch);

		if(full)
		{
			//to clean the EMap.
			if(__sync_bool_compare_and_swap(&WaitState[flag], 1, 2))
			{
				//wake up clean thread.
				//printf("EMap clean wake up flag=%d\n", flag);
				pthread_mutex_lock(&mutex[flag]);
				pthread_cond_signal(&cond[flag]);
				pthread_mutex_unlock(&mutex[flag]);

				//sleep(2);

			}
		}
	}


	return IsExist;
}

/*
 * match the 'key' in the Map and return the corresponding value.
 */
Cid EMapMatch(Cid key, int flag)
{
	Cid value=0;
	int step, index;
	int size;
	EMap* map;
	pthread_spinlock_t* latcharray;

	if(flag==0)
	{
		map=gcid2lcid;
		latcharray=Gcid2LcidLatch;
	}
	else
	{
		map=gcid2lsnap;
		latcharray=Gcid2LsnapLatch;
	}

	size=EMapNum;
	step=0;

	do
	{
		index=EMapHash(key, step);

		AcquireLatch(&latcharray[index]);

		if(map[index].key == 0)
		{
			ReleaseLatch(&latcharray[index]);
			break;
		}
		else if(map[index].key == key)
		{
			value=map[index].value;
			ReleaseLatch(&latcharray[index]);
			break;
		}

		ReleaseLatch(&latcharray[index]);

		step++;
	}while(step<size);

	return value;
}

/*
 * add a tid to the transaction list by 'last_GCid'.
 */
int LMapAdd(LMap* map, Cid last_GCid, TransactionId tid, int index)
{
	int size;

	size=THREADNUM;

	assert(index < size && map->key==last_GCid);

	map->list[index]=tid;

	return 0;
}

void LMapReset(LMap* map, Cid new_key)
{
	map->key=new_key;
	memset((char*)map->list, (TransactionId)0, sizeof(TransactionId)*THREADNUM);
}

void EMapClean(int flag)
{
	//printf("EMap clean begin flag=%d\n", flag);
	int i;
	Cid gcid_min;
	EMap* map;
	pthread_spinlock_t* latcharray;
	int count=0;
	Cid min;
	Cid lastgcid;

	//no need to hold lock here.
	gcid_min=ShmVariable->LAST_GCID;

	min=gcid_min;
	lastgcid=min;

	gcid_min=gcid_min-CID_MINUS_TO_CLEAN;

	gcid_min=gcid_min>0?gcid_min:0;

	if(gcid_min==0)
		return;

	if(flag==0)
	{
		map=gcid2lcid;
		latcharray=Gcid2LcidLatch;
	}
	else
	{
		map=gcid2lsnap;
		latcharray=Gcid2LsnapLatch;
	}

	for(i=0;i<EMapNum;i++)
	{
		AcquireLatch(&latcharray[i]);

		if(map[i].key > 0 && map[i].key < min)
			min=map[i].key;

		if(map[i].key > 0 && map[i].key < gcid_min)
		{
			map[i].key=0;
			map[i].value=0;
			count++;
		}

		ReleaseLatch(&latcharray[i]);
	}

	//EMap clean finished.
	if(flag==0)
	{
		AcquireLatch(LcidNumLatch);

		ShmVariable->LcidNum -= count;

		ReleaseLatch(LcidNumLatch);
	}
	else
	{
		AcquireLatch(LsnapNumLatch);

		ShmVariable->LsnapNum -= count;

		ReleaseLatch(LsnapNumLatch);
	}
	//printf("EMap clean flag=%d, gcid_min=%ld, min=%ld, last_gcid=%ld, minus=%d, count=%d %d %d\n", flag, gcid_min, min, lastgcid, CID_MINUS_TO_CLEAN, count, ShmVariable->LcidNum, ShmVariable->LsnapNum);
}

