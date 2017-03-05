/*
 * lock.h
 *
 *  Created on: Dec 2, 2015
 *      Author: xiaoxin
 */

#ifndef LOCK_H_
#define LOCK_H_


#include <pthread.h>
#include "type.h"
#include "lock_record.h"

#define LcidNumLatch &Gcid2LcidLatch[EMapNum]
#define LsnapNumLatch &Gcid2LsnapLatch[EMapNum]

extern pthread_rwlock_t* MainLockArray;

extern pthread_spinlock_t* Gcid2LcidLatch;

extern pthread_spinlock_t* Gcid2LsnapLatch;

#define LastGcidLock &MainLockArray[0]

#define LocalTIDLock &MainLockArray[1]

#define LocalCIDLock &MainLockArray[2]

#define ProcArrayLock &MainLockArray[3]

#define MainLockNum 10

extern size_t MainLockArraySize(void);

extern void InitMainLockArray(void);

extern void AcquireWrLock(pthread_rwlock_t* lock, LockMode mode);

extern void ReleaseWrLock(pthread_rwlock_t* lock);

extern size_t EMapLatchSize(void);

extern void InitEMapLatch(void);

extern void AcquireLatch(pthread_spinlock_t* latch);

extern void ReleaseLatch(pthread_spinlock_t* latch);
#endif /* LOCK_H_ */
