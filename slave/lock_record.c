/*
 * lock_record.c
 *
 *  Created on: Nov 23, 2015
 *      Author: xiaoxin
 */
/*
 * interface to manage locks during transaction running which can be unlocked
 * only once transaction committing, such as data-update-lock .
 */
#include<stdbool.h>
#include<stdint.h>
#include"config.h"
#include"trans.h"
#include"communicate.h"
#include"lock_record.h"
#include"mem.h"
#include"thread_global.h"
#include "data.h"
#include "lock.h"

//int LockHash(int table_id, TupleId tuple_id, int k);
int LockHash(int table_id, TupleId tuple_id);

void InitDataLockMemAlloc(void)
{
	Size size;
	char* DataLockMemStart;
	char* memstart;
	THREAD* threadinfo;

	//get start address of current thread's memory.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=MaxDataLockNum*sizeof(DataLock);

	DataLockMemStart=(char*)MemAlloc((void*)memstart,size);

	if(DataLockMemStart == NULL)
	{
		printf("thread memory allocation error for data lock  memory.PID:%d\n",pthread_self());
		return;
	}

	//allocation succeed, set to thread global variable.
	pthread_setspecific(DatalockMemKey,DataLockMemStart);
}

void InitDataLockMem(void)
{
	Size size;
	char* DataLockMemStart;

	DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);
	size=MaxDataLockNum*sizeof(DataLock);
	/*
	char* memstart;
	THREAD* threadinfo;

	//get start address of current thread's memory.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=MaxDataLockNum*sizeof(DataLock);

	DataLockMemStart=(char*)MemAlloc(memstart,size);

	if(DataLockMemStart == NULL)
	{
		printf("thread memory allocation error for data lock  memory.PID:%d\n",pthread_self());
		return;
	}

	//allocation succeed, set to thread global variable.
	pthread_setspecific(DatalockMemKey,DataLockMemStart);
	*/

	memset(DataLockMemStart,0,size);
}

int DataLockInsert(DataLock* lock)
{
	DataLock* lockptr;
	char* DataLockMemStart;
	int index;
	int table_id;
	int tuple_id;
	int flag=0;
	int search=0;

	DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);

	table_id=lock->table_id;
	tuple_id=lock->tuple_id;

	/*
	k=0;
	do
	{
		index=LockHash(table_id, tuple_id, k);

		lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));

		if(lockptr->tuple_id == 0)
		{
			flag=0;
			break;
		}

		if(lockptr->table_id==lock->table_id && lockptr->tuple_id==lock->tuple_id)
		{
			//the lock already exists.
			flag=1;
			break;
		}
		k++;
	}while(k < MaxDataLockNum);

	if(flag==0)
	{
		//succeed in finding free space, so insert it.
		lockptr->table_id=lock->table_id;
		lockptr->tuple_id=lock->tuple_id;
		lockptr->lockmode=lock->lockmode;
		lockptr->ptr=lock->ptr;
		return 1;
	}
	else if(flag==1)
	{
		//already exists.
		//printf("lock already holds.\n");
		return -1;
	}
	else
	{
		//no more free space.
		printf("no more free space for lock.\n");
		return 0;
	}
	*/

	index=LockHash(table_id,tuple_id);
	lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
	search+=1;

	while(lockptr->tuple_id > 0)
	{
		if(search > MaxDataLockNum)
		{
			//there is no free space.
			flag=2;
			break;
		}
		if(lockptr->table_id==lock->table_id && lockptr->tuple_id==lock->tuple_id)
		{
			//the lock already exists.
			flag=1;
			break;
		}
		index=(index+1)%MaxDataLockNum;
		lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
		search++;
	}

	if(flag==0)
	{
		//succeed in finding free space, so insert it.
		lockptr->table_id=lock->table_id;
		lockptr->tuple_id=lock->tuple_id;
		lockptr->lockmode=lock->lockmode;
		lockptr->index=lock->index;
		return 1;
	}

	else if(flag==1)
	{
		//already exists.
		printf("lock_already holds table_id=%d, tuple_id=%ld.\n", lockptr->table_id, lockptr->tuple_id);
		exit(-1);
		return -1;
	}
	else
	{
		//no more free space.
		printf("no more free space for lock.\n");
		exit(-1);
		return 0;
	}
}

//int LockHash(int table_id, TupleId tuple_id, int k)
int LockHash(int table_id, TupleId tuple_id)
{
	//return ((tuple_id + k * (1 + (((tuple_id >> 5) +1) % (MaxDataLockNum - 1)))) % MaxDataLockNum);
	//return ((tuple_id + k * (1 + (((tuple_id) +1) % (MaxDataLockNum - 1)))) % MaxDataLockNum);

	//int value;
	//to change the calculation.
	return (((table_id*10)%MaxDataLockNum+tuple_id%10)%MaxDataLockNum);
	//return value;
}
/*
void DataLockRelease(void)
{
	THREAD* threadinfo;
	int index;
	int index2;
	char* DataLockMemStart;
	DataLock* lockptr;

	threadinfo = (THREAD*)pthread_getspecific(ThreadInfoKey);
	index2 = threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index2);

	//get current transaction's pointer to data-lock memory
	DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);

	//release all locks that current transaction holds.
	for(index=0;index<MaxDataLockNum;index++)
	{
		lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
		//wait to change.
		if(lockptr->tuple_id > 0)
		{
			//printf("unlock PID:%u\n",pthread_self());
            if ((Send3(lindex, lockptr->node_id, cmd_unrwlock, lockptr->table_id, lockptr->index)) == -1)
            	printf("lock send error\n");
            if ((Recv(lindex, lockptr->node_id, 1)) == -1)
            	printf("lock recv error\n");
		}
	}
}
*/

void LocalDataLockRelease(void)
{
	int index, table_index;
	int table_id;
	char* DataLockMemStart;
	DataLock* lockptr;
	TransactionData* td;
	THREAD* threadinfo;

	//get current transaction's pointer to data-lock memory
	DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	//release all locks that current transaction holds.
	for(index=0;index<MaxDataLockNum;index++)
	{
		lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
		table_id=lockptr->table_id;
		table_index=lockptr->index;
		//wait to change.
		if(lockptr->tuple_id > 0)
		{
			ReleaseWrLock(&RecordLock[table_id][table_index]);
			//printf("index=%d, tid=%d, ReleaseWrLock table_id=%d,tuple_id=%ld, %d, %d\n", threadinfo->index, td->tid, table_id, lockptr->tuple_id, table_index, threadinfo->index);
		}
	}
}
/*
 * Is the lock on data (table_id,tuple_id) already exist.
 * @return:'0' for false, '1' for true.
 */
int IsDataLockExist(int table_id, TupleId tuple_id, LockMode mode)
{
	int index,count,flag;
	DataLock* lockptr;
	char* DataLockMemStart;

	//get current transaction's pointer to data-lock memory
	DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);
	/*
	k=0;
	flag=0;
	do
	{
		index=LockHash(table_id, tuple_id, k);

		lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
		if(lockptr->tuple_id > 0 && lockptr->table_id==table_id && lockptr->tuple_id==tuple_id && lockptr->lockmode==mode)
		{
			flag=1;
			break;
		}
		k++;
	}while(k<MaxDataLockNum);
	*/

	index=LockHash(table_id,tuple_id);
	lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));

	count=0;
	flag=0;
	while(lockptr->tuple_id > 0 && count<MaxDataLockNum)
	{
		if(lockptr->table_id==table_id && lockptr->tuple_id==tuple_id && lockptr->lockmode==mode)
		{
			flag=1;
			break;
		}
		index=(index+1)%MaxDataLockNum;
		lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
		count++;
	}

	return flag;
}
/*
 * Is the write-lock on data (table_id,tuple_id) being hold by current transaction.
 * @return:'1' for true,'0' for false.
 */
int IsWrLockHolding(uint32_t table_id, TupleId tuple_id)
{
	if(IsDataLockExist(table_id,tuple_id, LOCK_EXCLUSIVE))
		return 1;
	return 0;
}

/*
 * Is the read-lock on data (table_id,tuple_id) being hold by current transaction.
 * @return:'1' for true,'0' for false.
 */
int IsRdLockHolding(uint32_t table_id, TupleId tuple_id, int nid)
{
	if(IsDataLockExist(table_id,tuple_id, LOCK_SHARED))
		return 1;
	return 0;
}
