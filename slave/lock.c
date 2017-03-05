/*
 * lock.c
 *
 *  Created on: Dec 2, 2015
 *      Author: xiaoxin
 */
/*
 * interface to operations about locks on ProcArray and InvisibleTable.
 */
#include <stdlib.h>
#include "lock.h"
#include "shmem.h"
#include "config.h"

pthread_rwlock_t* MainLockArray=NULL;

pthread_spinlock_t* Gcid2LcidLatch=NULL;

pthread_spinlock_t* Gcid2LsnapLatch=NULL;

size_t MainLockArraySize(void)
{
	return MainLockNum*sizeof(pthread_rwlock_t);
}

void InitMainLockArray(void)
{
	size_t size;
	int i;
	pthread_rwlockattr_t rwlockattr;

	pthread_rwlockattr_init(&rwlockattr);
	pthread_rwlockattr_setpshared(&rwlockattr, PTHREAD_PROCESS_SHARED);

	size=MainLockArraySize();

	MainLockArray=(pthread_rwlock_t*)ShmemAlloc(size);

	if(MainLockArray==NULL)
	{
		printf("Shmvariable error\n");
		exit(-1);
	}

	for(i=0;i<MainLockNum;i++)
	{
		//MainLockArray should be shared between different processes.
		pthread_rwlock_init(&MainLockArray[i], &rwlockattr);
	}
}


/*
 * interface to hold the read-write-lock.
 */
void AcquireWrLock(pthread_rwlock_t* lock, LockMode mode)
{
	if(mode == LOCK_SHARED)
	{
		pthread_rwlock_rdlock(lock);
	}
	else
	{
		pthread_rwlock_wrlock(lock);
	}
}

/*
 * interface to release the read-write-lock.
 */
void ReleaseWrLock(pthread_rwlock_t* lock)
{
	pthread_rwlock_unlock(lock);
}

size_t EMapLatchSize(void)
{
	return (EMapNum+1)*sizeof(pthread_spinlock_t);
}

void InitEMapLatch(void)
{
	int i;
	int size;

	size=EMapLatchSize();

	Gcid2LcidLatch=(pthread_spinlock_t*)ShmemAlloc(size);

	Gcid2LsnapLatch=(pthread_spinlock_t*)ShmemAlloc(size);

	if(Gcid2LcidLatch==NULL || Gcid2LsnapLatch==NULL)
	{
		printf("EMapLock error\n");
		exit(-1);
	}

	for(i=0;i<=EMapNum;i++)
	{
		pthread_spin_init(&Gcid2LcidLatch[i], PTHREAD_PROCESS_SHARED);
		pthread_spin_init(&Gcid2LsnapLatch[i], PTHREAD_PROCESS_SHARED);
	}
}

void AcquireLatch(pthread_spinlock_t* latch)
{
	//printf("before acquire latch\n");
	pthread_spin_lock(latch);
	//printf("after acquire latch\n");

}

void ReleaseLatch(pthread_spinlock_t* latch)
{
	pthread_spin_unlock(latch);
}
