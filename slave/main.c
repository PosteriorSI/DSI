/*
 * main.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include "mem.h"
#include "thread_main.h"
#include "data_am.h"
#include "transactions.h"
#include "config.h"
#include "proc.h"
#include "socket.h"
#include "shmem.h"

int main(int argc, char *argv[])
{
	/*
	int nthreads=16;
	InitSys();
	ThreadRun(nthreads);
	ExitSys();
	PrintTable();
	*/
	int i,j;
	int commNum[20]={0};
	pid_t pid;
	//InitSys();
	if (argc != 2)
	{
		printf("please enter the configure file's name\n");
	}
    if ((conf_fp = fopen(argv[1], "r")) == NULL)
	{
	   printf("can not open the configure file.\n");
	   fclose(conf_fp);
	   return -1;
    }
	// do some ready work before start the distributed system
	GetReady();

	//initialize the shared memory.
	CreateShmem();

	if ((pid = fork()) < 0)
	{
	   printf("fork error\n");
	}

	else if(pid == 0)
	{
	   //redirection the stdout.

		if(freopen("service_log.txt", "w", stdout)==NULL)
		{
			printf("redirection stdout error\n");
			exit(-1);
		}

		//backend clean thread for gcid2lcid and gcid2lsnap;
		EMapCleanStart();
		// storage process
		InitStorage();

		for(i=1;i<NODENUM*THREADNUM+1;i++)
		{
			for(j=0;j<12;j++)
				commNum[j]+=CommTimes[i][j];
		}

		printf("count for communications times:\n");
		for(i=0;i<12;i++)
			printf("%4d : %d times\n", i, commNum[i]);
      /*
		for (i = 0; i < 9; i++)
		{
		   PrintTable(i);
		}
		*/
		ExitSys();
		//fclose(stdout);
		printf("storage process finished.\n");

	}

	else
	{
	   //backend clean thread for gcid2lcid and gcid2lsnap;
	   //EMapCleanStart();


	   // transaction process
	   InitTransaction();

       // sleep(8);
       dataLoading();

       // wait other slave nodes until all of them have loaded data.
       WaitDataReady();

	   //redirection the stdout.
       /*
		if(freopen("transaction_log.txt", "w", stdout)==NULL)
		{
			printf("redirection stdout error\n");
			exit(-1);
		}
		*/
       // begin run the benchmark.
       RunTerminals(THREADNUM);
	   //dataLoading();
	   //ResetProc();
	   //RunTerminals(NumTerminals);
	   //PrintTable(District_ID);
       //fclose(stdout);
	   printf("transaction process finished.\n");
	}
	return 0;
}

