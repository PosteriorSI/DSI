/*
 * trans.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
/*
 * transaction actions are defined here.
 */
#include<malloc.h>
#include<sys/time.h>
#include<stdlib.h>
#include<assert.h>
#include<sys/socket.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include"trans.h"
#include"thread_global.h"
#include"data_record.h"
#include"lock_record.h"
#include"mem.h"
#include"proc.h"
#include"data_am.h"
#include"transactions.h"
#include"config.h"
#include"thread_main.h"
#include"socket.h"
#include"communicate.h"
#include "shmem.h"
#include "type.h"
#include "map.h"
#include "procarray.h"
#include "lock.h"

ShmemVariable* ShmVariable;

Cid GetGlobalCID(int add);
void AtEnd_TransactionData(void);

size_t ShmemVariableSize(void)
{
	return sizeof(ShmemVariable);
}

void InitShmemVariable(void)
{
	size_t size;

	size=ShmemVariableSize();

	ShmVariable=(ShmemVariable*)ShmemAlloc(size);

	if(ShmVariable==NULL)
	{
		printf("Shmvariable error\n");
		exit(-1);
	}

	ShmVariable->LAST_GCID=0;
	ShmVariable->LOCAL_CID=0;
	ShmVariable->LOCAL_TID=0;

	ShmVariable->LcidNum=0;
	ShmVariable->LsnapNum=0;
}

size_t NodeInfoSize(void)
{
	return sizeof(int)*nodenum;
}

void InitNodeInfoMemAlloc(void)
{
	THREAD* threadinfo;
	char* memstart;
	int* nodeinfo;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=NodeInfoSize();

	nodeinfo=(int*)MemAlloc((void*)memstart, size);

	pthread_setspecific(NodeInfoKey, nodeinfo);
}

void InitNodeInfo(void)
{
	size_t size;
	int* nodeinfo=NULL;

	nodeinfo=(int*)pthread_getspecific(NodeInfoKey);
	size=NodeInfoSize();

	memset((char*)nodeinfo, 0, size);
}

bool nodeFirstAccess(int nid)
{
	bool access;

	int* nodeinfo;

	nodeinfo=(int*)pthread_getspecific(NodeInfoKey);

	access=(nodeinfo[nid] == 0) ? true : false;

	if(access)
		nodeinfo[nid]=1;

	return access;
}

TransactionId AssignLocalTID(void)
{
	TransactionId tid;

	AcquireWrLock(LocalTIDLock, LOCK_EXCLUSIVE);

	ShmVariable->LOCAL_TID++;

	tid=ShmVariable->LOCAL_TID;

	ReleaseWrLock(LocalTIDLock);

	return tid;
}

Cid AssignLocalCID(void)
{
	Cid cid;

	AcquireWrLock(LocalCIDLock, LOCK_EXCLUSIVE);

	ShmVariable->LOCAL_CID++;

	cid=ShmVariable->LOCAL_CID;

	ReleaseWrLock(LocalCIDLock);

	return cid;
}

Cid GetLocalSnapshot(void)
{
	Cid snap;

	AcquireWrLock(LocalCIDLock, LOCK_EXCLUSIVE);

	snap=ShmVariable->LOCAL_CID;

	ReleaseWrLock(LocalCIDLock);

	return snap;
}

Cid AssignGlobalCID(void)
{
	Cid global_cid;

	global_cid=GetGlobalCID(1);

	return global_cid;
}
//get the transaction id, add proc array and get the snapshot from the master node.
/*
void StartTransactionGetData(void)
{
	int index;
	int i;
	//local index
	int lindex;
	pthread_t pid;
	Snapshot* snap;
	THREAD* threadinfo;
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	int size = 3 + 1 + 1 + MAXPROCS;

	index=threadinfo->index;
	lindex = GetLocalIndex(index);
	pid = pthread_self();

	*(send_buffer[lindex]) = cmd_starttransaction;
	*(send_buffer[lindex]+1) = index;
	*(send_buffer[lindex]+2) = pid;

	if (send(server_socket[lindex], send_buffer[lindex], 3*sizeof(uint64_t), 0) == -1)
		printf("start transaction send error\n");
	if (recv(server_socket[lindex], recv_buffer[lindex], size*sizeof(uint64_t), 0) == -1)
		printf("start transaction recv error\n");

	//get the transaction ID
	td->tid = *(recv_buffer[lindex]+4);
	//printf("startTransaction:PID:%u TID:%d index:%d\n",pthread_self(),td->tid,threadinfo->index);
	if(!TransactionIdIsValid(td->tid))
	{
		printf("transaction ID assign error.\n");
		return;
	}

	//td->starttime = *(recv_buffer[lindex]+3);
	//get the snapshot
	snap->tcount = *(recv_buffer[lindex]);
	snap->tid_min =*(recv_buffer[lindex]+1);
	snap->tid_max = *(recv_buffer[lindex]+2);

	for (i = 0; i < MAXPROCS; i++)
		snap->tid_array[i] = *(recv_buffer[lindex]+5+i);
}

TimeStampTz GetTransactionStopTimestamp(void)
{
	THREAD* threadinfo;
	int index;
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);

	*(send_buffer[lindex]) = cmd_getendtimestamp;
	if (send(server_socket[lindex], send_buffer[lindex], sizeof(uint64_t), 0) == -1)
	   printf("get end time stamp send error\n");
	if (recv(server_socket[lindex], recv_buffer[lindex], sizeof(uint64_t), 0) == -1)
	   printf("get end time stamp recv error\n");

	//td->stoptime = *(recv_buffer[lindex]);
	//return (td->stoptime);
	return 0;
}
*/
/*
void UpdateProcArray()
{
   THREAD* threadinfo;
   int index;
   //get the pointer to current thread information.
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

   int lindex;
   lindex = GetLocalIndex(index);

   *(send_buffer[lindex]) = cmd_updateprocarray;
   *(send_buffer[lindex]+1) = index;

   if (send(server_socket[lindex], send_buffer[lindex], 2*sizeof(uint64_t), 0) == -1)
	  printf("update proc array send error\n");
   if (recv(server_socket[lindex], recv_buffer[lindex], sizeof(uint64_t), 0) == -1)
	  printf("update proc array recv error\n");
}
*/
void InitTransactionStructMemAlloc(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	char* memstart;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionData);

	td=(TransactionData*)MemAlloc((void*)memstart,size);

	if(td==NULL)
	{
		printf("memalloc error.\n");
		return;
	}

	pthread_setspecific(TransactionDataKey,td);

	//for_test
	td->ExtendAbort=0;

	//to set node-information memory.
	InitNodeInfoMemAlloc();

	//to set snapshot-data memory.
	//InitTransactionSnapshotDataMemAlloc();

	//to set data memory.
	//InitDataMemAlloc();

	//to set data-lock memory.
	//InitDataLockMemAlloc();
}

void InitServiceStructMemAlloc(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	char* memstart;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionData);

	td=(TransactionData*)MemAlloc((void*)memstart,size);

	if(td==NULL)
	{
		printf("memalloc error.\n");
		return;
	}

	td->state=empty;

	pthread_setspecific(TransactionDataKey,td);

	//to set data memory.
	InitDataMemAlloc();

	//to set data-lock memory.
	InitDataLockMemAlloc();
}

/*
 *start a transaction running environment, reset the
 *transaction's information for a new transaction.
 */
void StartTransaction(void)
{
	LocalBegin();
}

bool CommitTransaction(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	int* nodeinfo;
	Cid global_cid;
	int i;
	int prepare_nodes, response_nodes;
	bool commit=true;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int index;
	int num;

	State response_state;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	nodeinfo=(int*)pthread_getspecific(NodeInfoKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;
	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	prepare_nodes=0;
	response_nodes=0;
	//local commit.
	if(!td->first_remote_access)
	{
		//printf("local commit\n");
		if(nodeinfo[nodeid] > 0)
		{
			//send 'prepare' to node 'nodeid'.
			conn=connect_socket[nodeid][index];
			*(sbuffer)=cmd_singlePrepare;
			num=1;

			Send(conn, sbuffer, num);

			//receive response from node 'nodeid'.
			Receive(conn, rbuffer, num);
			response_state=(State)(*(rbuffer));

			//printf("prepare response\n");
			if(response_state==aborted)
				commit=false;

			if(commit)
			{
				//send 'single commit' to node 'nodeid'.
				*(sbuffer)=cmd_singleCommit;
				num=1;

				Send(conn, sbuffer, num);

				//noreply here.
				num=1;
				Receive(conn, rbuffer, num);
			}
			else
			{
				//send 'single abort' to node 'nodeid'.
				*(sbuffer)=cmd_singleAbort;
				num=1;

				Send(conn, sbuffer, num);

				//noreply here.
				num=1;
				Receive(conn, rbuffer, num);
			}
		}
	}
	//two-phase commit.
	else
	{
		//prepare node one by one to avoid dead lock.
		//printf("two-phase commit index=%d, nodenum=%d, nodeinfo:%d, %d, %d, %d, %d, %d\n", index, NODENUM, nodeinfo[0], nodeinfo[1], nodeinfo[2], nodeinfo[3], nodeinfo[4], nodeinfo[5]);

		for(i=0;i<NODENUM;i++)
		{
			if(nodeinfo[i] > 0)
			{
				//send "prepare" to node "i".
				conn=connect_socket[i][index];

				*(sbuffer)=cmd_prepare;
				*(sbuffer+1)=i;
				num=2;

				Send(conn, sbuffer, num);

				//printf("two phase: send cmd_prepare to node %d, index=%d\n", i, index);

				//receive response from node "i".
				num=1;

				Receive(conn, rbuffer, num);

				response_state=*(rbuffer);

				if(response_state == aborted)
				{
					commit=false;
					break;
				}
			}
		}

		if(commit)
		{
			//commit node synchronously because of no possibility for dead lock in this step.

			//get global-commit-ID.
			global_cid=AssignGlobalCID();

			for(i=0;i<NODENUM;i++)
			{
				if(nodeinfo[i] > 0)
				{
					//send 'global commit' to node 'i'.
					conn=connect_socket[i][index];

					*(sbuffer)=cmd_commit;
					*(sbuffer+1)=global_cid;
					num=2;

					Send(conn, sbuffer, num);
					//printf("send global commit to node %d, index=%d\n", i, index);

					num=1;
					Receive(conn, rbuffer, num);

					//printf("global commit: receive from node %d %ld, index=%d\n", i,*(rbuffer), index);
					if(*(rbuffer)==66)
					{
						printf("ERROR:CommitTransaction node=%d, nodeinfo=%d, index=%d\n", i, nodeinfo[i], index);
						exit(-1);
					}
				}
			}
		}
		else
		{
			//abort node synchronously as commit do.

			for(i=0;i<NODENUM;i++)
			{
				if(nodeinfo[i] > 0)
				{
					//send 'abort' to node 'i'.
					conn=connect_socket[i][index];

					*(sbuffer)=cmd_abort;
					num=1;

					Send(conn, sbuffer, num);

					//printf("send cmd_abort to node %d, index=%d\n", i, index);

					num=1;
					Receive(conn, rbuffer, num);

					//printf("cmd_abort: receive from node %d, %ld, index=%d\n", i, *(rbuffer), index);
				}
			}
		}
	}
	/*
	else
	{
		printf("two-phase commit index=%d, nodenum=%d, nodeinfo:%d,%d\n", index, NODENUM, nodeinfo[0], nodeinfo[1]);
		num=2;
		for(i=0;i<NODENUM;i++)
		{
			if(nodeinfo[i] > 0)
			{
				prepare_nodes++;

				//send 'prepare' to node 'i'.

				conn=connect_socket[i][index];

				*(sbuffer)=cmd_prepare;
				*(sbuffer+1)=i;

				Send(conn, sbuffer, num);

				printf("two phase: send cmd_prepare to node %d, index=%d\n", i, index);
			}
		}

		for(i=0;i<NODENUM;i++)
		{
			if(nodeinfo[i] > 0)
			{
				//receive response from node 'i'.
				conn=connect_socket[i][index];
				num=1;

				printf("ready to receive cmd_prepare from node %d , index=%d\n", i, index);

				Receive(conn, rbuffer, num);
				response_state=(State)(*(rbuffer));

				printf("response_state form node %d state=%d, index=%d\n", i, response_state, index);

				//set 'commit' to 'false' if receive 'abort' message.
				if(response_state == aborted)
					commit=false;

				response_nodes++;

				//if(response_nodes == prepare_nodes)
					//break;
			}
		}

		assert(prepare_nodes == response_nodes);

		if(commit)
		{
			//get global-commit-ID.
			global_cid=AssignGlobalCID();

			for(i=0;i<NODENUM;i++)
			{
				if(nodeinfo[i] > 0)
				{
					//send 'global commit' to node 'i'.
					conn=connect_socket[i][index];

					*(sbuffer)=cmd_commit;
					*(sbuffer+1)=global_cid;
					num=2;

					Send(conn, sbuffer, num);
					printf("send global commit to node %d, index=%d\n", i, index);
				}
			}

			//noreply here.
			num=1;
			for(i=0;i<NODENUM;i++)
			{
				if(nodeinfo[i] > 0)
				{
					conn=connect_socket[i][index];

					Receive(conn, rbuffer, num);

					printf("global commit: receive from node %d %ld, index=%d\n", i, num, *(rbuffer), index);
					if(*(rbuffer)==66)
					{
						printf("ERROR:CommitTransaction node=%d, nodeinfo=%d, index=%d\n", i, nodeinfo[i], index);
						exit(-1);
					}

				}
			}


		}
		else
		{
			for(i=0;i<NODENUM;i++)
			{
				if(nodeinfo[i] > 0)
				{
					//send 'abort' to node 'i'.
					conn=connect_socket[i][index];

					*(sbuffer)=cmd_abort;
					num=1;

					Send(conn, sbuffer, num);

					printf("send cmd_abort to node %d, index=%d\n", i, index);
				}
			}

			//noreply here.
			num=1;
			for(i=0;i<NODENUM;i++)
			{
				if(nodeinfo[i] > 0)
				{
					conn=connect_socket[i][index];

					Receive(conn, rbuffer, num);

					printf("cmd_abort: receive from node %d, %ld, index=%d\n", i, *(rbuffer), index);
				}
			}
		}
	}
	*/

	AtEnd_ProcArray(threadinfo->index);

	AtEnd_TransactionData();

	TransactionMemClean();

	return commit;
}

/*
 * context of transaction, transId identifies the context of
 * the transaction to be executed.
 */
void RunTransaction(int transId)
{

}

/*
 * abort ahead write.
 */
void AbortTransaction(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	int* nodeinfo;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;
	int index;
	int i;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	nodeinfo=(int*)pthread_getspecific(NodeInfoKey);

	index=threadinfo->index;

	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	if(!td->first_remote_access)
	{
		//send 'abort ahead write' to 'nodeid'.
		//printf("Local abort index=%d\n", index);

		if(nodeinfo[nodeid] > 0)
		{
			conn=connect_socket[nodeid][index];

			*(sbuffer)=cmd_abortAheadWrite;
			*(sbuffer+1)=nodeid;
			num=2;

			Send(conn, sbuffer, num);

			num=1;
			Receive(conn, rbuffer, num);
		}
	}
	else
	{
		//printf("Global abort index=%d\n", index);
		num=2;
		for(i=0;i<NODENUM;i++)
		{
			if(nodeinfo[i] > 0)
			{
				conn=connect_socket[i][index];

				*(sbuffer)=cmd_abortAheadWrite;
				*(sbuffer+1)=i;

				Send(conn, sbuffer, num);

				//printf("AbortTransaction: send to node %d\n", i);

				num=1;
				Receive(conn, rbuffer, num);

				if(*(rbuffer)==66)
				{
					printf("ERROR:AbortTransaction node=%d, nodeinfo=%d, index=%d\n", i, nodeinfo[i], index);
					exit(-1);
				}
			}
		}
	}

	AtEnd_ProcArray(threadinfo->index);

	AtEnd_TransactionData();

	TransactionMemClean();
	/*
	TransactionId tid;
	TransactionData* tdata;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//process array clean should be after function 'AbortDataRecord'.
	//AtEnd_ProcArray(index);

	//TransactionContextAbort(tid,index);
	AbortDataRecord(tid, trulynum);

	//once abort, clean the process array after clean updated-data.
	UpdateProcArray();

	DataLockRelease();

	//printf("AbortTransaction: TID: %d abort\n",tdata->tid);
	TransactionMemClean();
	*/
}

void ReleaseConnect(void)
{
	THREAD* threadinfo;

	int conn;
	uint64_t* sbuffer;
	int index;
	int num;
	int i;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;
	sbuffer=send_buffer[index];

	*(sbuffer)=cmd_release;
	num=1;
	for(i=0;i<nodenum;i++)
	{
		conn=connect_socket[i][index];

		Send(conn, sbuffer, num);
	}

	//printf("cmd_release\n");
	conn=server_socket[index];

	*(sbuffer)=cmd_release_master;
	num=1;

	Send(conn, sbuffer, num);
	//printf("cmd_release_master\n");
}

void DataReleaseConnect(void)
{
	int conn;
	int num;
	int index;
	uint64_t* sbuffer;

	index=0;
	conn=connect_socket[nodeid][index];
	sbuffer=send_buffer[index];

	*(sbuffer)=cmd_release;
	num=1;

	Send(conn, sbuffer, num);

	conn=server_socket[index];

	*(sbuffer)=cmd_release_master;
	num=1;

	Send(conn, sbuffer, num);
}

void TransactionLoadData(int i)
{
	int j;
	int result;
	int index;
    for (j = 0; j < 20; j++)
	{
      StartTransaction();
      Data_Insert(2, j+1, j+1, nodeid);
      result=PreCommit(&index);
      if(result == 1)
      {
      	CommitTransaction();
      }
      else
      {
      	  //current transaction has to rollback.
      	  //to do here.
      	  //we should know from which DataRecord to rollback.
      	  AbortTransaction();
      }
	}
    DataReleaseConnect();
    ResetProc();
    ResetMem(0);
}

void TransactionSchedule(int i)
{
	int index;
	int j;
	int result;
	if (i == 1)
	{
		int flag;
		for (j = 0; j < 1000; j++)
		{
	      StartTransaction();
	      Data_Update(2, j%20+1, j+6, nodeid);
	      Data_Update(2, (j+1)%20+1, j+6, (nodeid+1)%nodenum);
	      Data_Update(2, (j+2)%20+1, j+6, (nodeid+2)%nodenum);
	      Data_Update(2, (j+3)%20+1, j+6, (nodeid+3)%nodenum);
	      Data_Read(2, (j+2)%20+1, (nodeid+4)%nodenum, &flag);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction();
	      }
		}
	}

	if (i == 2)
	{
		int flag;
		for (j = 0; j < 1000; j++)
		{
	      StartTransaction();
	      Data_Update(2, j%20+1, j+6, nodeid);
	      Data_Update(2, (j+1)%20+1, j+6, (nodeid+1)%nodenum);
	      Data_Update(2, (j+2)%20+1, j+6, (nodeid+2)%nodenum);
	      Data_Update(2, (j+3)%20+1, j+6, (nodeid+3)%nodenum);
	      Data_Read(2, (j+2)%20+1, (nodeid+4)%nodenum, &flag);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction();
	      }
		}
	}

	if (i == 3)
	{
		int flag;
		for (j = 0; j < 1000; j++)
		{
	      StartTransaction();
	      Data_Update(2, j%20+1, j+6, nodeid);
	      Data_Update(2, (j+1)%20+1, j+6, (nodeid+1)%nodenum);
	      Data_Update(2, (j+2)%20+1, j+6, (nodeid+2)%nodenum);
	      Data_Update(2, (j+3)%20+1, j+6, (nodeid+3)%nodenum);
	      Data_Read(2, (j+2)%20+1, (nodeid+4)%nodenum, &flag);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction();
	      }
		}
	}


	if (i == 4)
	{
		int flag;
		for (j = 0; j < 1000; j++)
		{
	      StartTransaction();
	      Data_Update(2, j%20+1, j+6, nodeid);
	      Data_Update(2, (j+1)%20+1, j+6, (nodeid+1)%nodenum);
	      Data_Update(2, (j+2)%20+1, j+6, (nodeid+2)%nodenum);
	      Data_Update(2, (j+3)%20+1, j+6, (nodeid+3)%nodenum);
	      Data_Read(2, (j+2)%20+1, (nodeid+4)%nodenum, &flag);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction();
	      }
		}
	}

	ReleaseConnect();
/*
	if (*i == 4)
	{
		sleep(3);
		for (j = 0; j < 20; j++)
		{
	      StartTransaction();
	      Data_Update(2, j+1, j+4);
	      result=PreCommit(&index);
	      if(result == 1)
	      {
	      	CommitTransaction();
	      }
	      else
	      {
	      	  //current transaction has to rollback.
	      	  //to do here.
	      	  //we should know from which DataRecord to rollback.
	      	  AbortTransaction(index);
	      }
		}
	}
	*/
	/*
	if (*i == 2)
	{
		sleep(5);
		execTransaction();
	}
	*/
}

void TransactionRunSchedule(void* args)
{
	//printf("run schedule PID:%u.\n",pthread_self());

	//to run transactions according to args.
	int type;
	int rv;
	terminalArgs* param=(terminalArgs*)args;
	type=param->type;

	if(type==0)
	{
		printf("begin LoadData......\n");
		//getchar();
		//LoadData();

		//smallbank
		//LoadBankData();
		//LoadDataTest();

		switch(benchmarkType)
		{
		case TPCC:
			LoadData();
			break;
		case SMALLBANK:
			LoadBankData();
			break;
		default:
			printf("benchmark not specified\n");
		}
		DataReleaseConnect();
		ResetMem(0);
		ResetProc();
	}
	else
	{
		printf("ready to execute transactions...\n");

		rv=pthread_barrier_wait(&barrier);
		if(rv != 0 && rv != PTHREAD_BARRIER_SERIAL_THREAD)
		{
			printf("Couldn't wait on barrier\n");
			exit(-1);
		}


		printf("begin execute transactions...\n");
		//TransactionTest();
		//executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);

	    //smallbank
	   // executeTransactionsBank(transactionsPerTerminal, param->StateInfo);
		switch(benchmarkType)
		{
		case TPCC:
			executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);
			break;
		case SMALLBANK:
			executeTransactionsBank(transactionsPerTerminal, param->StateInfo);
			break;
		default:
			printf("benchmark not specified\n");
		}
		//sleep(1);

		ReleaseConnect();
	}
}

/*
 * get current transaction data, return it's pointer.
 */
/*
PROC* GetCurrentTransactionData(void)
{
	PROC* proc;
	proc=(PROC*)pthread_getspecific(TransactionDataKey);
	return proc;
}
*/

/*
void TransactionContextCommit(TransactionId tid, TimeStampTz ctime, int index)
{
	CommitDataRecord(tid,ctime);

	UpdateProcArray();

	DataLockRelease();
	TransactionMemClean();
}

void TransactionContextAbort(TransactionId tid, int index)
{
	AbortDataRecord(tid, -1);

	//once abort, clean the process array after clean updated-data.
	UpdateProcArray();

	DataLockRelease();
	TransactionMemClean();
}
*/
/*
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int PreCommit(int* index)
{
	char* DataMemStart, *start;
	int num,i,result;
	DataRecord* ptr;
	TransactionData* td;
	THREAD* threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;

	num=*(int*)DataMemStart;

	//sort the data-operation records.
	DataRecordSort((DataRecord*)start, num);
	/*
	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		//printf("%d %ld %d %ld %ld %d\n",i,ptr->index, ptr->table_id,ptr->tuple_id,ptr->value,ptr->type);
	}
	*/
	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));

		switch(ptr->type)
		{
		case DataInsert:
			//printf("PreCommit: insert index=%d,tid=%d, %d %ld %ld %ld.\n",threadinfo->index, td->tid, ptr->table_id,ptr->tuple_id,ptr->value,ptr->index);
			result=TrulyDataInsert(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value);
			//printf("PreCommit: after insert index=%d, table_id=%d, tuple_id=%ld.\n", threadinfo->index, ptr->table_id, ptr->tuple_id);
			break;
		case DataUpdate:
			//printf("PreCommit: update index=%d,tid=%d, %d %ld %ld.\n",threadinfo->index, td->tid, ptr->table_id,ptr->tuple_id, ptr->index);
			result=TrulyDataUpdate(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value);
			//printf("PreCommit: after update index=%d, table_id=%d, tuple_id=%ld.\n", threadinfo->index, ptr->table_id, ptr->tuple_id);
			break;
		case DataDelete:
			//printf("PreCommit: delete index=%d,tid=%d, %d %ld %ld.\n",threadinfo->index, td->tid, ptr->table_id,ptr->tuple_id, ptr->index);
			result=TrulyDataDelete(ptr->table_id, ptr->index, ptr->tuple_id);
			//printf("PreCommit: after delete index=%d, table_id=%d, tuple_id=%ld.\n",threadinfo->index, ptr->table_id, ptr->tuple_id);
			break;
		default:
			printf("PreCommit:shouldn't arrive here.\n");
		}
		if(result == -1)
		{
			//return to rollback.
			//printf("PreCommit: TID: %d rollback.\n",tdata->tid);
			*index=i;
			return -1;
		}
	}

	//printf("PreCommit: TID: %d finished.\n",tdata->tid);
	return 1;
}

int GetNodeId(int index)
{
   return (index/threadnum);
}

int GetLocalIndex(int index)
{
	return (index%threadnum);
}

Cid GetGlobalCID(int add)
{
	Cid global_cid;

	THREAD* threadinfo;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;

	int index;
	int num;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;

	conn=server_socket[index];
	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	*(sbuffer)=cmd_getGlobalCID;
	*(sbuffer+1)=add;
	num=2;

	//get global-next-cid from coordinator here.
	Send(conn, sbuffer, num);

	Receive(conn, rbuffer, num);

	global_cid=(Cid)(*rbuffer);

	return global_cid;
}

/*
Cid GetGlobalNextCID(void)
{
	Cid global_cid;

	THREAD* threadinfo;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;

	int index;
	int num;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;

	conn=server_socket[index];
	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	*(sbuffer)=cmd_getGlobalNextCID;
	num=1;

	//get global-next-cid from coordinator here.
	Send(conn, sbuffer, num);

	Receive(conn, rbuffer, num);

	global_cid=(Cid)(*rbuffer);

	return global_cid;
}
*/

void LocalBegin(void)
{
	TransactionData* td=NULL;
	THREAD* threadinfo=NULL;
	int last_gcid;
	int index;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	index=threadinfo->index;

	//set node-information memory.
	InitNodeInfo();

	td->first_remote_access=false;

	td->tid=AssignLocalTID();

	td->lsnap=GetLocalSnapshot();

	AcquireWrLock(LastGcidLock, LOCK_EXCLUSIVE);

	last_gcid=ShmVariable->LAST_GCID;

	LMapAdd(readsFromGcid, last_gcid, td->tid, index);

	ReleaseWrLock(LastGcidLock);

	td->global_cid_range_low=last_gcid;

	//td->global_cid_range_up=MAXINT;
	td->global_cid_range_up=0;

	td->state=active;

	AtStart_ProcArray(index, td->tid, last_gcid);

	//printf("LocalBegin tid=%d, index=%d\n", td->tid, index);
}

/*
 * extend local transaction to global transaction.
 */
bool LocalExtend(int node_j)
{
	THREAD* threadinfo;
	TransactionData* td;
	int index;
	int num;
	Cid flag=0;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;

	bool success=true;

	Cid gcid_low;
	Cid gcid_up;
	Cid last_gcid;

	Cid lsnap;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	conn=connect_socket[node_j][index];
	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	getGlobalCidRange(index, &gcid_low,  &gcid_up);

	//get the gcid_up from the coordinator.
	if(gcid_up == 0)//ready to change the value to 'MAXINT'.
	{
		gcid_up=GetGlobalCID(0);
	}

	AcquireWrLock(LastGcidLock, LOCK_SHARED);

	last_gcid=ShmVariable->LAST_GCID;

	if(last_gcid < gcid_up && last_gcid > gcid_low)
	{
		gcid_up=last_gcid;
	}

	ReleaseWrLock(LastGcidLock);

	td->global_cid_range_up=gcid_up;

	lsnap=EMapMatch(gcid_up, 1);

	if(lsnap != 0 && lsnap!=td->lsnap)
	{
		//for_test
		success=false;
		//td->lsnap=lsnap;
	}
	else
	{
		flag=EMapAdd(gcid_up, td->lsnap, 1);

		//the key 'gcid_up' already exists and the local-snapshot is not the same.
		if(flag > 0)
			success=false;
	}

	if(success)
	{
		//send 'local_access' to 'node_j'.
		//LocalAccess(node_j);
		assert(nodeFirstAccess(node_j));

		//printf("LocalExtend success tid=%d, gcid_up=%ld\n", td->tid, td->global_cid_range_up);
		*(sbuffer)=cmd_localAccess;
		*(sbuffer+1)=node_j;
		*(sbuffer+2)=gcid_up;
		*(sbuffer+3)=index;
		*(sbuffer+4)=nodeid;
		num=5;

		Send(conn, sbuffer, num);

		//noreply here.

		num=1;

		Receive(conn, rbuffer, num);
	}
	else
	{
		td->ExtendAbort++;
		//printf("LocalExtend fail tid=%d %ld %ld %ld , %ld %ld\n", td->tid, lsnap, td->lsnap, flag, gcid_up, last_gcid);
	}

	return success;
}

/*
 * wait to change according to the abort rate, maybe the snapshot is too old.
 */
Cid LocalFindSnapshot(Cid gcid_up, EMap* map)
{
	Cid snap=0;
	//the max global-cid of those less than 'gcid_up' in the map.
	Cid gcid_min=0;
	int step;
	int size;
	int index;
	pthread_spinlock_t* latcharray=Gcid2LcidLatch;

	size=EMapNum;

	step=0;

	do
	{
		index=EMapHash(gcid_up, step);


		/*
		if(map[index].key==0)
			break;
		else if(map[index].key == gcid_up)
		{
			snap=map[index].value;
			break;
		}
		else if(map[index].key > gcid_min && map[index].key < gcid_up)
		{
			gcid_min=map[index].key;
			snap=map[index].value;
		}
		*/
		AcquireLatch(&latcharray[index]);

		if(map[index].key == gcid_up)
		{
			snap=map[index].value;
			ReleaseLatch(&latcharray[index]);
			break;
		}
		else if(map[index].key > gcid_min && map[index].key < gcid_up)
		{
			gcid_min=map[index].key;
			snap=map[index].value;
		}

		ReleaseLatch(&latcharray[index]);
		step++;
	}while(step < size);

	//snap is 0 if the key 'gcid_up' doesn't exist in the map, also there is no global-cid less than 'gcid_up'.
	return snap;
}

/*
 * remote local-access service.
 */
void LocalAccess(Cid gcid_up)
{
	TransactionData* td=NULL;
	THREAD* threadinfo=NULL;
	int index;
	Cid flag;
	int i;

	Cid lsnap;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	index=threadinfo->index;

	//to set data memory.
	InitDataMem();

	//to set data-lock memory.
	InitDataLockMem();

	td->tid=AssignLocalTID();

	td->global_cid_range_up=gcid_up;

	td->flag=1;

	if(td->state != empty)
	{
		printf("LocalAccess state!=empty index=%d, %d\n", index, td->state);
		exit(-1);
	}

	td->state=active;

	lsnap=EMapMatch(gcid_up, 1);

	if(lsnap != 0)
	{
		td->lsnap=lsnap;
	}
	else
	{
		td->lsnap=LocalFindSnapshot(gcid_up, gcid2lcid);
/*
		if(td->lsnap==0)
		{
			printf("gcid2lcid as follows:\n");
			for(i=0;i<EMapNum;i++)
				printf("(%ld,%ld) ", gcid2lcid[i].key, gcid2lcid[i].value);
			printf("\n");
		}
*/
		flag=EMapAdd(gcid_up, td->lsnap, 1);

		//reset local snapshot to the exists one.
		if(flag > 0)
		{
			//printf("LocalFindSnapshot add conflict tid=%d, %d %d\n", td->tid, td->lsnap, flag);
			td->lsnap=flag;
		}
	}

	//printf("LocalAccess tid=%d, lsnap=%ld, gcid_up=%ld, index=%d\n", td->tid, td->lsnap, gcid_up, index);
}

/*
 * self-access service.
 */
void SelfAccess(TransactionId tid, Cid lsnap)
{
	TransactionData* td=NULL;
	THREAD* threadinfo=NULL;
	int index;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	index=threadinfo->index;

	//to set data memory.
	InitDataMem();

	//to set data-lock memory.
	InitDataLockMem();

	td->tid=tid;

	td->lsnap=lsnap;

	td->flag=1;

	if(td->state != empty)
	{
		printf("LocalAccess state!=empty index=%d, %d\n", index, td->state);
		exit(-1);
	}

	td->state=active;

	//printf("SelfAccess tid=%d, lsnap=%ld\n", tid, lsnap);
}

int LocalPrepare(void)
{
	int index, result;
	Cid local_cid;
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	//printf("LocalPrepare before tid=%d\n", td->tid);
	result=PreCommit(&index);
	//printf("LocalPrepare after tid=%d\n", td->tid);
	//printf("PreCommit finished\n");
	if(result == -1)
	{
		LocalAbort(index);

		td->state=aborted;
	}
	else
	{
		local_cid=AssignLocalCID();

		td->local_cid=local_cid;

		writeCIDInDoubt(local_cid);

		td->state=prepared;
	}

	return result;
}

void LocalCommit(Cid global_cid)
{
	Cid newGcid=global_cid;
	Cid local_cid;

	TransactionId* list;
	Cid last_gcid;
	int num, i;
	TransactionId tid;
	int index;

	TransactionData* td;
	THREAD* threadinfo;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	if(td->state != prepared)
	{
		printf("LocalCommit state!=prepared index=%d, %d\n", threadinfo->index, td->state);
		exit(-1);
	}

	//printf("LocalCommit tid=%d, local_cid=%ld, index=%d\n", td->tid, td->local_cid, threadinfo->index);

	local_cid=td->local_cid;

	EMapAdd(newGcid, local_cid, 0);
	//printf("EMapAdd finished tid=%d, index=%d\n", td->tid, threadinfo->index);

	//consider the atomicity of those operations.
	AcquireWrLock(LastGcidLock, LOCK_EXCLUSIVE);
	//printf("LastGcidLock holds tid=%d, index=%d\n", td->tid, threadinfo->index);

	AcquireWrLock(ProcArrayLock, LOCK_EXCLUSIVE);
	//printf("ProcArrayLock holds tid=%d, index=%d\n", td->tid, threadinfo->index);

	list=readsFromGcid->list;
	//num=readsFromGcid->tail;
	num=threadnum;
	last_gcid=readsFromGcid->key;

	assert(last_gcid == ShmVariable->LAST_GCID);

	for(i=0;i<=num;i++)
	{
		tid=list[i];

		//index=getTransactionIdIndex(tid);
		index=i;
		if(tid != InvalidTransactionId && tid == procarray[index].tid)
		{
			//assert(tid == procarray[index].tid);

			setGlobalCidRangeUp(index, newGcid);
		}
	}

	ShmVariable->LAST_GCID=newGcid;

	LMapReset(readsFromGcid, newGcid);

	ReleaseWrLock(ProcArrayLock);
	//printf("ProcArrayLock release tid=%d, index=%d\n", td->tid, threadinfo->index);

	ReleaseWrLock(LastGcidLock);
	//printf("LastGcidLock release tid=%d, index=%d\n", td->tid, threadinfo->index);

	writeSetVisible();
	//printf("writeSetVisible finished tid=%d, index=%d\n", td->tid, threadinfo->index);

	LocalDataLockRelease();
	//printf("LocalDataLockRelease finished tid=%d, index=%d\n", td->tid, threadinfo->index);

	AtEnd_TransactionData();

	//TransactionMemClean();

	//AtEnd_ProcArray(threadinfo->index);
}

void SingleCommit(void)
{
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	//printf("SingleCommit tid=%d, local_cid=%ld\n", td->tid, td->local_cid);

	writeSetVisible();

	LocalDataLockRelease();

	AtEnd_TransactionData();

	//TransactionMemClean();
}

void LocalAbort(int trulynum)
{
	TransactionId tid;
	TransactionData* tdata;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//printf("LocalAbort tid=%d\n", tid);

	//process array clean should be after function 'AbortDataRecord'.
	//AtEnd_ProcArray(index);

	//TransactionContextAbort(tid,index);
	LocalAbortDataRecord(tid, trulynum);

	LocalDataLockRelease();

	AtEnd_TransactionData();

	//printf("AbortTransaction: TID: %d abort\n",tdata->tid);
	//TransactionMemClean();
}

void SingleAbort(void)
{
	//reset local-transaction's transaction-data here.
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	//printf("SingleAbort tid=%d\n", td->tid);

	td->state=empty;

	td->tid=InvalidTransactionId;
}

void AtEnd_TransactionData(void)
{
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	td->state=empty;

	td->tid=InvalidTransactionId;

	td->flag=-1;
}

/************* interface for service *****************/
void serviceLocalAccess(int conn, uint64_t* buffer)
{
	int node_j;
	Cid gcid_up;

	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;
	TransactionData* td;
	State trans_state;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	trans_state=td->state;

	sbuffer=ssend_buffer[threadinfo->index];

	node_j=(int)(*(buffer+1));
	gcid_up=(Cid)(*(buffer+2));
	td->trans_index=(int)(*(buffer+3));
	td->nid=(int)(*(buffer+4));

	if(trans_state != empty)
	{
		printf("serviceLocalAccess trans_state!=empty index=%d, tid=%d, state=%d, tindex=%d, nid=%d\n", threadinfo->index, td->tid, trans_state, td->trans_index, td->nid);
		exit(-1);
	}

	//printf("serviceLocalAccess %d %ld, tindex=%d, nid=%d\n", node_j, gcid_up, td->trans_index, td->nid);
	assert(node_j == nodeid);

	LocalAccess(gcid_up);

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void serviceSelfAccess(int conn, uint64_t* buffer)
{
	TransactionId tid;
	Cid lsnap;

	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;
	TransactionData* td;
	State trans_state;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	trans_state=td->state;

	sbuffer=ssend_buffer[threadinfo->index];

	tid=(TransactionId)(*(buffer+1));
	lsnap=(Cid)(*(buffer+2));
	td->trans_index=(int)(*(buffer+3));
	td->nid=(int)(*(buffer+4));

	if(trans_state != empty)
	{
		printf("serviceSelfAccess trans_state!=empty index=%d, tid=%d, state=%d, tindex=%d, nid=%d\n", threadinfo->index, td->tid, trans_state, td->trans_index, td->nid);
		exit(-1);
	}
	//printf("serviceSelfAccess %d %ld\n", tid, lsnap);

	SelfAccess(tid, lsnap);

	//printf("serviceSelfAccess tid=%d, tindex=%d, nid=%d\n", tid, td->trans_index, td->nid);

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void serviceLocalPrepare(int conn, uint64_t* buffer)
{
	int result;

	THREAD* threadinfo;
	int index;
	uint64_t* sbuffer;
	int num;
	TransactionData* td;
	State trans_state;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	sbuffer=ssend_buffer[index];

	trans_state=td->state;

	if(trans_state != active)
	{
		printf("serviceLocalPrepare trans_state!=active index=%d, tid=%d, state=%d, tindex=%d, nid=%d\n", index, td->tid, trans_state, td->trans_index, td->nid);
		exit(-1);
	}

	//printf("enter serviceLocalPrepare: index=%d, tindex=%d, nid=%d\n", index, td->trans_index, td->nid);

	result=LocalPrepare();

	//printf("serviceLocalPrepare: index=%d, result=%d, tindex=%d, nid=%d\n", index, result, td->trans_index, td->nid);

	//reply here.
	if(result==-1)
	{
		//send 'abort' message to coordinator.
		*(sbuffer)=aborted;
		num=1;
	}
	else
	{
		//send 'prepared' message to coordinator.
		*(sbuffer)=prepared;
		num=1;
	}

	Send(conn, sbuffer, num);
}

void serviceSingleCommit(int conn, uint64_t* buffer)
{
	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;
	TransactionData* td;
	State trans_state;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	trans_state=td->state;
	sbuffer=ssend_buffer[threadinfo->index];

	if(trans_state != prepared)
	{
		printf("serviceSingleCommit trans_state!=empty index=%d, tid=%d, state=%d, tindex=%d, nid=%d\n", threadinfo->index, td->tid, trans_state, td->trans_index, td->nid);
		exit(-1);
	}
	//printf("serviceSingleCommit index=%d, tindex=%d, nid=%d\n", threadinfo->index, td->trans_index, td->nid);

	SingleCommit();

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void serviceSingleAbort(int conn, uint64_t* buffer)
{
	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;
	TransactionData* td;
	State trans_state;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	trans_state=td->state;
	sbuffer=ssend_buffer[threadinfo->index];

	if(trans_state != aborted)
	{
		printf("serviceSingleAbort trans_state!=empty index=%d, tid=%d, state=%d, tindex=%d, nid=%d\n", threadinfo->index, td->tid, trans_state, td->trans_index, td->nid);
		exit(-1);
	}
	//printf("serviceSingleAbort index=%d, tindex=%d, nid=%d\n", threadinfo->index, td->trans_index, td->nid);

	SingleAbort();

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void serviceLocalCommit(int conn, uint64_t* buffer)
{
	TransactionData* td;
	State trans_state;

	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	sbuffer=ssend_buffer[threadinfo->index];

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	trans_state=td->state;

	if(trans_state != prepared)
	{
		printf("serviceLocalCommit state!=prepared : state=%d, tid=%d, index=%d, flag=%d, tindex=%d, nid=%d\n", trans_state, td->tid, threadinfo->index, td->flag, td->trans_index, td->nid);
		//*(sbuffer)=66;
		//num=1;
		//Send(conn, sbuffer, num);
		exit(-1);
	}

	Cid global_cid;

	global_cid=(Cid)(*(buffer+1));

	//printf("serviceLocalCommit index=%d, tid=%d, %ld, tindex=%d, nid=%d\n", threadinfo->index, td->tid, global_cid, td->trans_index, td->nid);

	LocalCommit(global_cid);

	//printf("LocalCommit finished index=%d, tid=%d, %ld, tindex=%d, nid=%d\n", threadinfo->index, td->tid, global_cid, td->trans_index, td->nid);

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void serviceLocalAbort(int conn, uint64_t* buffer)
{
	TransactionData* td;
	State trans_state;

	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	sbuffer=ssend_buffer[threadinfo->index];

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	trans_state=td->state;

	//printf("serviceLocalAbort index=%d, tindex=%d, nid=%d\n", threadinfo->index, td->trans_index, td->nid);
	if(trans_state == aborted || trans_state == active)
	{
		AtEnd_TransactionData();
	}
	else if(trans_state == prepared)
	{
		LocalAbort(-1);
	}
	else
	{
		printf("transaction state error index=%d, state=%d, tid=%d, tindex=%d, nid=%d\n", threadinfo->index, trans_state, td->tid, td->trans_index, td->nid);
		exit(-1);
	}

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void serviceAbortAheadWrite(int conn, uint64_t* buffer)
{
	int nid;

	TransactionData* td;
	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	sbuffer=ssend_buffer[threadinfo->index];

	nid=(int)(*(buffer+1));

	//printf("serviceAbortAheadWrite index=%d, tindex=%d, nid=%d\n", threadinfo->index, td->trans_index, td->nid);

	assert(nid == nodeid);
	if(td->state != active)
	{
		printf("serviceAbortAheadWrite state!=active : state=%d, tid=%d, index=%d, flag=%d, tindex=%d, nid=%d\n", td->state, td->tid, threadinfo->index, td->flag, td->trans_index, td->nid);
		//*(sbuffer)=66;
		//num=1;
		//Send(conn, sbuffer, num);
		exit(-1);
	}

	//no any writes.
	LocalAbort(0);

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

