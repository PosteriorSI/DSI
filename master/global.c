/*
 * global.c
 *
 *  Created on: May 2, 2016
 *      Author: xiaoxin
 */
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "type.h"

//used to allocate global-cid for global-transaction.
static Cid Global_Next_CID;

pthread_mutex_t CidLock;

Cid Get_Global_CID(void)
{
	Cid global_cid;

	pthread_mutex_lock(&CidLock);
	global_cid=Global_Next_CID++;
	pthread_mutex_unlock(&CidLock);

	return global_cid;
}

//just get the value of 'global-next-cid'.
Cid Get_Global_Next_CID(void)
{
	Cid global_cid;

	pthread_mutex_lock(&CidLock);
	global_cid=Global_Next_CID;
	pthread_mutex_unlock(&CidLock);

	return global_cid;
}

void Init_Global_CID(void)
{
	Global_Next_CID=1;

	if(pthread_mutex_init(&CidLock, NULL))
	{
		printf("pthread_mutex_init error\n");
		exit(-1);
	}
}

uint64_t TotalDistributedTrasactions(void)
{
	return Global_Next_CID-1;
}

