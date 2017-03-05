/*
 * shmem.c
 *
 *  Created on: May 4, 2016
 *      Author: xiaoxin
 */
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdlib.h>
#include "shmem.h"
#include "trans.h"
#include "proc.h"
#include "map.h"
#include "procarray.h"
#include "lock.h"

int shmem_shmid;
ShmemHeader* ShmHdr;
void* ShmBase;

ShmemHeader* SharedMemoryCreate(size_t size)
{
	ShmemHeader* hdr=NULL;
	void* MemAddress=NULL;

	shmem_shmid=shmget(IPC_PRIVATE, size, SHM_MODE);
	if(shmem_shmid==-1)
	{
		printf("shmget error\n");
		exit(-1);
	}

	MemAddress=(void*)shmat(shmem_shmid, NULL, 0);
	if(MemAddress==(void*)-1)
	{
		printf("shmat error\n");
		exit(-1);
	}

	hdr=(ShmemHeader*)MemAddress;

	hdr->totalsize=size;
	hdr->freeoffset=sizeof(ShmemHeader);

	return hdr;
}

void CreateShmem(void)
{
	size_t size;

	size=0;

	size += sizeof(ShmemHeader);

	//shmemvariable
	size += ShmemVariableSize();

	size += MainLockArraySize();

	//procarray
	size += ProcArraySize();

	//EMap
	size += EMapsize()*2;

	//EMap-latch
	size += EMapLatchSize()*2;

	//LMap
	size += LMapsize();

	//cond
	size += PthreadCondSize();

	ShmHdr=SharedMemoryCreate(size);
	ShmBase=(void*)ShmHdr;

	//initialize "shmemvariable".
	InitShmemVariable();

	//initialize "MainLockArray".
	InitMainLockArray();

	//initialize "procarray".
	InitProcArray();

	//initialize "ElemMap".
	InitEMap();

	//initialize "ElemeMap-latch"
	InitEMapLatch();

	//initialize "ListMap".
	InitLMap();

	//initialize "cond".
	InitPthreadCond();
}

void* ShmemAlloc(size_t size)
{
	size_t newfree;
	void* addr;
	newfree=ShmHdr->freeoffset+size;

	if(newfree > ShmHdr->totalsize)
	{
		//printf("out of shared memory\n");
		return NULL;
		//exit(-1);
	}

	addr=(void*)((char*)ShmBase+ShmHdr->freeoffset);

	ShmHdr->freeoffset=newfree;

	return addr;
}
