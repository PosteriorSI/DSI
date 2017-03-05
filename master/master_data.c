#include <stdlib.h>
#include <sys/socket.h>
#include <stdio.h>
#include "type.h"
#include "master.h"
#include "master_data.h"
#include "procarray.h"
#include "global.h"

void ProcessGlobalCID(uint64_t *recv_buffer, int conn, int mindex)
{
	int add;
	Cid global_cid;

	add=(int)recv_buffer[1];

	if(add==0)
	{
		//just get the value of 'Global_Next_CID'.
		global_cid=Get_Global_Next_CID();
	}
	else
	{
		//get the value of 'Global_Next_CID' and 'Global_Next_CID' increases by 1.
		global_cid=Get_Global_CID();
	}

	*(msend_buffer[mindex])=global_cid;

	if (send(conn, msend_buffer[mindex], sizeof(uint64_t), 0) == -1)
		printf("process start transaction send error\n");
}

void ProcessStartTransaction(uint64_t *recv_buffer, int conn, int mindex)
{
   int index;
   int count;
   int max;
   int min;
   pthread_t pid;
   int size;

   size = 3 + 1 + 1 + MAXPROCS;

   TransactionId tid;
   TimeStampTz starttime;

   index = recv_buffer[1];
   pid = recv_buffer[2];

   tid = AssignTransactionId();
   starttime = SetCurrentTransactionStartTimestamp();

   ProcArrayAdd(index, tid, pid);

   GetServerTransactionSnapshot(index, &count, &min, &max, msend_buffer[mindex]+5);

   *(msend_buffer[mindex]) = count;
   *(msend_buffer[mindex]+1) = min;
   *(msend_buffer[mindex]+2) = max;
   *(msend_buffer[mindex]+3) = starttime;
   *(msend_buffer[mindex]+4) = tid;

   if (send(conn, msend_buffer[mindex], size*sizeof(uint64_t), 0) == -1)
	   printf("process start transaction send error\n");
}

void ProcessEndTimestamp(uint64_t *recv_buffer, int conn, int mindex)
{
   TimeStampTz endtime;

   endtime = SetCurrentTransactionStopTimestamp();

   *(msend_buffer[mindex]) = endtime;

   if (send(conn, msend_buffer[mindex], sizeof(uint64_t), 0) == -1)
	   printf("process end time stamp send error\n");
}

void ProcessUpdateProcarray(uint64_t *recv_buffer, int conn, int mindex)
{
	int index;
	int status = 1;

	index = recv_buffer[1];

	AtEnd_ProcArray(index);

	*(msend_buffer[mindex]) = status;

	if (send(conn, msend_buffer[mindex], sizeof(uint64_t), 0) == -1)
	   printf("process end time stamp send error\n");
}
