/*
 * proc.c
 *
 *  Created on: 2015��11��9��
 *      Author: DELL
 */
/*
 * process actions are defined here.
 */
#include<malloc.h>
#include<pthread.h>
#include<stdlib.h>

#include "proc.h"
#include "mem.h"
#include "thread_global.h"
#include "trans.h"
#include "lock.h"
#include "util.h"
#include "socket.h"
#include "thread_main.h"
#include "shmem.h"
#include "map.h"

pthread_cond_t* cond;
pthread_mutex_t* mutex;
int* WaitState;

PROCHEAD* prohd;

size_t PthreadCondSize(void)
{
	return 2*sizeof(pthread_cond_t)+2*sizeof(pthread_mutex_t)+2*sizeof(int);
}

void InitPthreadCond(void)
{
	size_t size;
	int i;

	pthread_mutexattr_t mutexattr;
	pthread_condattr_t condattr;

	pthread_mutexattr_init(&mutexattr);
	pthread_mutexattr_setpshared(&mutexattr, PTHREAD_PROCESS_SHARED);
	pthread_mutexattr_settype(&mutexattr, PTHREAD_MUTEX_TIMED_NP);

	pthread_condattr_init(&condattr);
	pthread_condattr_setpshared(&condattr, PTHREAD_PROCESS_SHARED);

	size=2*sizeof(pthread_cond_t);

	cond=(pthread_cond_t*)ShmemAlloc(size);

	if(cond==NULL)
	{
		printf("pthread_cond_t malloc error\n");
		exit(-1);
	}

	size=2*sizeof(pthread_mutex_t);

	mutex=(pthread_mutex_t*)ShmemAlloc(size);

	if(mutex==NULL)
	{
		printf("pthread_cond_t malloc error\n");
		exit(-1);
	}

	size=2*sizeof(int);

	WaitState=(int*)ShmemAlloc(size);

	if(WaitState==NULL)
	{
		printf("wait malloc error\n");
		exit(-1);
	}

	for(i=0;i<2;i++)
	{
		pthread_cond_init(&cond[i], &condattr);
		//pthread_mutex_init(&mutex[i], &attr);
		pthread_mutex_init(&mutex[i], &mutexattr);
		WaitState[i]=0;
	}

}
void InitProcHead(int flag)
{
   //initialize the process array information.
   prohd=(PROCHEAD*)malloc(sizeof(PROCHEAD));
   //in transaction process.
   if(flag==0)
	   prohd->maxprocs=THREADNUM+1;
   //in service process.
   else
	   prohd->maxprocs=NODENUM*THREADNUM+1;
   prohd->numprocs=0;
}

void ResetProc(void)
{
	prohd->maxprocs=THREADNUM;
	prohd->numprocs=0;
}

/*
void *ProcStart(void* args)
{
	int i;
	int j;
	terminalArgs *temp;
	temp = (terminalArgs*) args;
	char* start=NULL;
	THREAD* threadinfo;
	//IDMGR* ProcIdMgr;

	Size size;

	pthread_mutex_lock(&prohd->ilock);
	i=prohd->numprocs++;
	pthread_mutex_unlock(&prohd->ilock);

	start=(char*)MemStart+MEM_PROC_SIZE*i;

	//memset(start, 0, MEM_PROC_SIZE);

	size=sizeof(THREAD);

	threadinfo=(THREAD*)MemAlloc((void*)start,size);

	if(threadinfo==NULL)
	{
		printf("memory alloc error during process running.\n");
		exit(-1);
	}

	pthread_setspecific(ThreadInfoKey,threadinfo);

	//global index for thread
	threadinfo->index=i;
	threadinfo->memstart=(char*)start;

	if (temp->type == 0)
	{
	   InitClient(nodeid, i);
	}

	else
	{
	   // ensure the connect with other nodes in distributed system.
       for (j = 0; j < nodenum; j++)
       {
    	  //printf("j = %d, i = %d\n");
	      InitClient(j, i);
       }
	}
    // ensure the connect with the master node.
    InitMasterClient(i);

	//initialize the transaction ID assignment for per thread.
	//ProcTransactionIdAssign(threadinfo);

	//start running transactions here.
	//SetRandomSeed();
	InitRandomSeed();

	//memory allocation for each transaction data struct.
	InitTransactionStructMemAlloc();

    TransactionRunSchedule(args);

	//printf("PID:%lu start.\n",pthread_self());

	return NULL;
}
*/

/*
 * start function for threads in transaction process.
 */
void *TransactionProcStart(void* args)
{
	int i;
	int j;
	terminalArgs *temp;
	temp = (terminalArgs*) args;
	char* start=NULL;
	THREAD* threadinfo;
	//IDMGR* ProcIdMgr;

	Size size;

	pthread_mutex_lock(&prohd->ilock);
	i=prohd->numprocs++;
	pthread_mutex_unlock(&prohd->ilock);

	start=(char*)MemStart+MEM_PROC_SIZE*i;

	//memset(start, 0, MEM_PROC_SIZE);

	size=sizeof(THREAD);

	threadinfo=(THREAD*)MemAlloc((void*)start,size);

	if(threadinfo==NULL)
	{
		printf("memory alloc error during process running.\n");
		exit(-1);
	}

	pthread_setspecific(ThreadInfoKey,threadinfo);

	//global index for thread
	threadinfo->index=i;
	threadinfo->memstart=(char*)start;

	if (temp->type == 0)
	{
	   InitClient(nodeid, i);
	}

	else
	{
	   // ensure the connect with other nodes in distributed system.
       for (j = 0; j < nodenum; j++)
       {
    	  //printf("j = %d, i = %d\n");
	      InitClient(j, i);
       }
	}
    // ensure the connect with the master node.
    InitMasterClient(i);

	//initialize the transaction ID assignment for per thread.
	//ProcTransactionIdAssign(threadinfo);

	//start running transactions here.
	//SetRandomSeed();
	InitRandomSeed();

	//memory allocation for each transaction data struct.
	InitTransactionStructMemAlloc();

    TransactionRunSchedule(args);

	//printf("PID:%lu start.\n",pthread_self());

	return NULL;
}

void* ServiceProcStart(void* args)
{
	int i;
	//terminalArgs *temp;
	//temp = (terminalArgs*) args;
	server_arg* temp;
	temp=(server_arg*)args;

	char* start=NULL;
	THREAD* threadinfo;

	Size size;

	pthread_mutex_lock(&prohd->ilock);
	i=prohd->numprocs++;
	pthread_mutex_unlock(&prohd->ilock);

	start=(char*)MemStart+MEM_PROC_SIZE*i;

	//memset(start, 0, MEM_PROC_SIZE);

	size=sizeof(THREAD);

	threadinfo=(THREAD*)MemAlloc((void*)start,size);

	if(threadinfo==NULL)
	{
		printf("memory alloc error during process running.\n");
		exit(-1);
	}

	pthread_setspecific(ThreadInfoKey,threadinfo);

	//global index for thread
	threadinfo->index=i;
	threadinfo->memstart=(char*)start;

	InitServiceStructMemAlloc();

	temp->index=i;

	//ready for response.
	Respond((void*)temp);

	return NULL;
}

void* EMapCleanProcStart(void* args)
{
	int flag;

	flag=*((int*)args);

	while(1)
	{

		pthread_mutex_lock(&mutex[flag]);

		WaitState[flag]=1;

		//printf("EMap is waiting flag=%d\n", flag);

		pthread_cond_wait(&cond[flag], &mutex[flag]);

		//printf("EMap wake up flag=%d\n", flag);

		pthread_mutex_unlock(&mutex[flag]);

		if(WaitState[flag] != 2)
			continue;

		WaitState[flag]=0;

		EMapClean(flag);
	}

	pthread_exit(NULL);
	return NULL;
}
