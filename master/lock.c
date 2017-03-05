/*
 * lock.c
 *
 *  Created on: Dec 2, 2015
 *      Author: xiaoxin
 */
/*
 * interface to operations about locks on ProcArray and InvisibleTable.
 */
#include<stdlib.h>
#include"procarray.h"
#include"lock.h"

//to make sure that at most one transaction commits at the same time.
pthread_rwlock_t ProcArrayLock;

pthread_rwlock_t *ProcArrayElemLock;

/*
 * initialize the lock on ProcArray and Invisible.
 */
void InitLock(void)
{
	int i;
	//ProcArray
	ProcArrayElemLock = (pthread_rwlock_t *)malloc (MAXPROCS * sizeof(pthread_rwlock_t));
	pthread_rwlock_init(&ProcArrayLock,NULL);

	for(i=0;i<MAXPROCS;i++)
	{
		pthread_rwlock_init(&ProcArrayElemLock[i],NULL);
	}

	//InvisibleTable

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


