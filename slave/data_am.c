/*
 * data_am.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
/*
 * interface for data access method.
 */
#include<pthread.h>
#include<assert.h>
#include<stdbool.h>
#include<sys/socket.h>
#include<assert.h>
#include"config.h"
#include"data_am.h"
#include"data_record.h"
#include"lock_record.h"
#include"thread_global.h"
#include"proc.h"
#include"trans.h"
#include"transactions.h"
#include"data.h"
#include"socket.h"
#include"communicate.h"
#include "lock.h"


static bool IsCurrentTransaction(TransactionId tid, TransactionId cur_tid);

/*
 * @return: '0' to rollback, '1' to go head.
 */
int Data_Insert(int table_id, TupleId tuple_id, TupleId value, int nid)
{
	int result;

	TransactionData* td;
	THREAD* threadinfo;
	bool success=true;
	TransactionId tid;
	Cid lsnap;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;
	int index;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=td->tid;
	lsnap=td->lsnap;

	conn=connect_socket[nid][index];
	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	//remote access.
	if(nid != nodeid)
	{
		if(!td->first_remote_access)
		{
			success=LocalExtend(nid);
		}
		else
		{
			if(nodeFirstAccess(nid))
			{
				//send 'local_access' to 'nid'.

				*(sbuffer)=cmd_localAccess;
				*(sbuffer+1)=nid;
				*(sbuffer+2)=td->global_cid_range_up;
				*(sbuffer+3)=index;
				*(sbuffer+4)=nodeid;
				num=5;

				Send(conn, sbuffer, num);

				//noreply here.
				//to avoid too short interval between two 'Send' operations.
				num=1;
				Receive(conn, rbuffer, num);
			}
		}
	}
	//local access.
	else
	{
		if(nodeFirstAccess(nodeid))
		{
			//send 'self_access' to 'nid'.
			//SelfAccess(tid, lsnap);

			*(sbuffer)=cmd_selfAccess;
			*(sbuffer+1)=tid;
			*(sbuffer+2)=lsnap;
			*(sbuffer+3)=index;
			*(sbuffer+4)=nodeid;
			num=5;

			Send(conn, sbuffer, num);

			//noreply here.
			//to avoid too short interval between two 'Send' operations.
			num=1;
			Receive(conn, rbuffer, num);
		}
	}

	if(!success)
	{
		//return to rollback.
		return 0;
	}

	if(nid != nodeid && !td->first_remote_access)
		td->first_remote_access=true;

	//sleep(1);
	//send 'data_insert' to 'nid'.
	*(sbuffer)=cmd_dataInsert;
	*(sbuffer+1)=nid;
	*(sbuffer+2)=table_id;
	*(sbuffer+3)=tuple_id;
	*(sbuffer+4)=value;
	num=5;

	Send(conn, sbuffer, num);

	//response from 'nid'.

	num=1;
	Receive(conn, rbuffer, num);

	result=(int)*(rbuffer);

	return result;
	/*
	int index;
	int status;
	uint64_t h;
	DataRecord datard;
	THREAD* threadinfo;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;


	 //the node transaction process must to get the data from the storage process in the
	 // node itself or in the other nodes, both use the socket to communicate.


	int lindex;
	lindex = GetLocalIndex(index);
    if ((Send3(lindex, nid, cmd_insert, table_id, tuple_id)) == -1)
       printf("insert send error!\n");
    if ((Recv(lindex, nid, 2)) == -1)
       printf("insert recv error!\n");

    status = *(recv_buffer[lindex]);
    h = *(recv_buffer[lindex]+1);

    if (status == 0)
    	return 0;

	datard.type=DataInsert;
	datard.table_id=table_id;
	datard.tuple_id=tuple_id;
	datard.value=value;
	datard.index=h;
	datard.node_id = nid;
	DataRecordInsert(&datard);
	//printf("Data_Insert: %d.\n",HashTable[h].lcommit);
	return 1;
	*/
}

int LocalDataInsert(int table_id, TupleId tuple_id, TupleId value)
{
	int index=0;
	int h,flag;
	DataRecord datard;
	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the index of 'tuple_id' in table 'table_id'.
	THash HashTable = TableList[table_id];
	h=RecordFindHole(table_id, tuple_id, &flag);

	if(flag==-2)
	{
		//no space for new tuple to insert.
		return 0;
	}
	else if(flag==1 && IsInsertDone(table_id, h))
	{
		//tuple by (table_id, tuple_id) already exists, and
		//insert already done, so return to rollback.
		return 0;
	}

	//record the inserted data 'datard'.
	//get the data pointer.
	datard.type=DataInsert;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.value=value;
	datard.index=h;
	DataRecordInsert(&datard);

	return 1;
}
/*
 * @return:'0' for not found, '1' for success.
 */
int Data_Update(int table_id, TupleId tuple_id, TupleId value, int nid)
{
	int result;

	TransactionData* td;
	THREAD* threadinfo;
	bool success=true;
	TransactionId tid;
	Cid lsnap;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;
	int index;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;
	conn=connect_socket[nid][index];
	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	tid=td->tid;
	lsnap=td->lsnap;
	//remote access.
	if(nid != nodeid)
	{
		if(!td->first_remote_access)
		{
			success=LocalExtend(nid);
		}
		else
		{
			if(nodeFirstAccess(nid))
			{
				//send 'local_access' to 'nid'.
				//LocalAccess(nid);

				*(sbuffer)=cmd_localAccess;
				*(sbuffer+1)=nid;
				*(sbuffer+2)=td->global_cid_range_up;
				*(sbuffer+3)=index;
				*(sbuffer+4)=nodeid;
				num=5;

				Send(conn, sbuffer, num);

				//noreply here.
				//to avoid too short interval between two 'Send' operations.
				num=1;
				Receive(conn, rbuffer, num);
			}
		}
	}
	//local access.
	else
	{
		if(nodeFirstAccess(nodeid))
		{
			//send 'self_access' to 'nid'.
			//SelfAccess(tid, lsnap);

			*(sbuffer)=cmd_selfAccess;
			*(sbuffer+1)=tid;
			*(sbuffer+2)=lsnap;
			*(sbuffer+3)=index;
			*(sbuffer+4)=nodeid;
			num=5;

			Send(conn, sbuffer, num);

			//noreply here.
			//to avoid too short interval between two 'Send' operations.
			num=1;
			Receive(conn, rbuffer, num);

		}
	}

	if(!success)
	{
		//return to rollback.
		return 0;
	}

	if(nid != nodeid && !td->first_remote_access)
		td->first_remote_access=true;

	//sleep(1);
	//printf("send cmd_dataUpdate %d %ld\n", table_id, tuple_id);
	//send 'data_update' to 'nid'.
	*(sbuffer)=cmd_dataUpdate;
	*(sbuffer+1)=nid;
	*(sbuffer+2)=table_id;
	*(sbuffer+3)=tuple_id;
	*(sbuffer+4)=value;
	num=5;

	Send(conn, sbuffer, num);

	//response from 'nid'.

	num=1;
	Receive(conn, rbuffer, num);

	result=(int)*(rbuffer);

	//printf("Data_Update result=%d\n", result);

	return result;
	/*
	int index=0;
	int h;
	DataRecord datard;

    int status;
	THREAD* threadinfo;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);
	if (Send3(lindex, nid, cmd_updatefind, table_id, tuple_id) == -1)
		printf("update find send error\n");
	if (Recv(lindex, nid, 2) == -1)
		printf("update find recv error\n");
	status = *(recv_buffer[lindex]);
	h  = *(recv_buffer[lindex]+1);
    if (status == 0)
    	return 0;

	//record the updated data.
	datard.type=DataUpdate;
	datard.table_id=table_id;
	datard.tuple_id=tuple_id;
	datard.value=value;
    datard.node_id = nid;
	datard.index=h;
	DataRecordInsert(&datard);
	*/
}

int LocalDataUpdate(int table_id, TupleId tuple_id, TupleId value)
{
	int index=0;
	int h;
	DataRecord datard;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the index of 'tuple_id' in table 'table_id'.
	h = RecordFind(table_id, tuple_id);

	//not found.
	if (h < 0)
	{
		//abort transaction outside the function.
		return 0;
	}

	//record the updated data.
	//get the data pointer.

	datard.type=DataUpdate;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.value=value;

	datard.index=h;
	DataRecordInsert(&datard);
	return 1;
}
/*
 * @return:'0' for not found, '1' for success, '-1' for update-conflict-rollback.
 */
int Data_Delete(int table_id, TupleId tuple_id, int nid)
{
	int result;

	TransactionData* td;
	THREAD* threadinfo;
	bool success=true;
	TransactionId tid;
	Cid lsnap;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;
	int index;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;
	conn=connect_socket[nid][index];
	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	tid=td->tid;
	lsnap=td->lsnap;
	//remote access.
	if(nid != nodeid)
	{
		if(!td->first_remote_access)
		{
			success=LocalExtend(nid);
		}
		else
		{
			if(nodeFirstAccess(nid))
			{
				//send 'local_access' to 'nid'.
				//LocalAccess(nid);

				*(sbuffer)=cmd_localAccess;
				*(sbuffer+1)=nid;
				*(sbuffer+2)=td->global_cid_range_up;
				*(sbuffer+3)=index;
				*(sbuffer+4)=nodeid;
				num=5;

				Send(conn, sbuffer, num);

				//noreply here.
				//to avoid too short interval between two 'Send' operations.
				num=1;
				Receive(conn, rbuffer, num);
			}
		}
	}
	//local access.
	else
	{
		if(nodeFirstAccess(nodeid))
		{
			//send 'self_access' to 'nid'.
			//SelfAccess(tid, lsnap);

			*(sbuffer)=cmd_selfAccess;
			*(sbuffer+1)=tid;
			*(sbuffer+2)=lsnap;
			*(sbuffer+3)=index;
			*(sbuffer+4)=nodeid;
			num=5;

			Send(conn, sbuffer, num);

			//noreply here.
			//to avoid too short interval between two 'Send' operations.
			num=1;
			Receive(conn, rbuffer, num);
		}
	}

	if(!success)
	{
		return 0;
	}

	if(nid != nodeid && !td->first_remote_access)
		td->first_remote_access=true;

	//sleep(1);
	//send 'data_update' to 'nid'.
	*(sbuffer)=cmd_dataDelete;
	*(sbuffer+1)=nid;
	*(sbuffer+2)=table_id;
	*(sbuffer+3)=tuple_id;
	num=4;

	Send(conn, sbuffer, num);

	//response from 'nid'.

	num=1;
	Receive(conn, rbuffer, num);

	result=(int)*(rbuffer);

	return result;
	/*
	int index=0;
	int h;
	DataRecord datard;
    int status;
	THREAD* threadinfo;
	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);

	if (Send3(lindex, nid, cmd_updatefind, table_id, tuple_id) == -1)
		printf("delete find send error\n");
	if (Recv(lindex, nid, 2) == -1)
		printf("delete find recv error\n");

	status = *(recv_buffer[lindex]);
	h  = *(recv_buffer[lindex]+1);
    if (status == 0)
    	return 0;

	//record the updated data.
	datard.type=DataDelete;
	datard.table_id=table_id;
	datard.tuple_id=tuple_id;
    datard.node_id = nid;
	datard.index = h;
	DataRecordInsert(&datard);
	*/
}

int LocalDataDelete(int table_id, TupleId tuple_id)
{
	int index=0;
	int h;
	DataRecord datard;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the index of 'tuple_id' in table 'table_id'.
	h = RecordFind(table_id, tuple_id);

	//not found.
	if(h < 0)
	{
		return 0;
	}

	//record the deleted data.
	//get the data pointer.
	datard.type=DataDelete;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.index=h;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * @input:'isupdate':true for reading before updating, false for commonly reading.
 * @return:NULL for read nothing, to rollback or just let it go.
 */
TupleId Data_Read(int table_id, TupleId tuple_id, int nid, int* flag)
{
	TransactionData* td;
	THREAD* threadinfo;
	bool success=true;
	TransactionId tid;
	Cid lsnap;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;
	int index;
	int status;
	TupleId value;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	index=threadinfo->index;
	conn=connect_socket[nid][index];
	sbuffer=send_buffer[index];
	rbuffer=recv_buffer[index];

	tid=td->tid;
	lsnap=td->lsnap;
	//remote access.
	if(nid != nodeid)
	{
		if(!td->first_remote_access)
		{
			success=LocalExtend(nid);
		}
		else
		{
			if(nodeFirstAccess(nid))
			{
				//send 'local_access' to 'nid'.
				//LocalAccess(nid);

				*(sbuffer)=cmd_localAccess;
				*(sbuffer+1)=nid;
				*(sbuffer+2)=td->global_cid_range_up;
				*(sbuffer+3)=index;
				*(sbuffer+4)=nodeid;
				num=5;

				Send(conn, sbuffer, num);

				//noreply here.
				num=1;
				Receive(conn, rbuffer, num);
			}
		}
	}
	//local access.
	else
	{
		if(nodeFirstAccess(nodeid))
		{
			//send 'self_access' to 'nid'.
			//SelfAccess(tid, lsnap);

			*(sbuffer)=cmd_selfAccess;
			*(sbuffer+1)=tid;
			*(sbuffer+2)=lsnap;
			*(sbuffer+3)=index;
			*(sbuffer+4)=nodeid;
			num=5;

			Send(conn, sbuffer, num);

			//noreply here.
			num=1;
			Receive(conn, rbuffer, num);
		}
	}

	if(!success)
	{
		*flag=-1;
		return 0;
	}

	if(nid != nodeid && !td->first_remote_access)
		td->first_remote_access=true;

	//send 'data_read' message to 'nid'.
	*(sbuffer)=cmd_dataRead;
	*(sbuffer+1)=nid;
	*(sbuffer+2)=table_id;
	*(sbuffer+3)=tuple_id;
	num=4;

	Send(conn, sbuffer, num);

	//response from 'nid'.

	num=2;
	Receive(conn, rbuffer, num);

	status=(int)(*(rbuffer));
	value=(TupleId)(*(rbuffer+1));

	//success if flag is '1', fail if flag is '0'.
	*flag=status;
	return value;
}

TupleId LocalDataRead(int table_id, TupleId tuple_id, int* flag)
{
	VersionId i;
	int h;
	int index, visible;
	VersionId newest;
	Cid lsnap;
	char* DataMemStart;

	*flag=1;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the pointer to data-memory.
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	//get pointer to transaction-snapshot-data.
	lsnap=tdata->lsnap;

	THash HashTable = TableList[table_id];
	h = RecordFind(table_id, tuple_id);

	//not found.
	if(h < 0)
	{
		//to rollback.
		*flag=0;
		return 0;
	}

	//the data by 'tuple_id' exist, so read it.
	newest = (HashTable[h].rear + VERSIONMAX -1) % VERSIONMAX;

	visible=IsDataRecordVisible(DataMemStart, table_id, tuple_id);
	if(visible == -1)
	{
		//current transaction has deleted the tuple to read, so return to rollback.
		*flag=-1;
		return 0;
	}
	else if(visible > 0)
	{
		//see own transaction's update.
		return visible;
	}


	pthread_spin_lock(&RecordLatch[table_id][h]);
	//by here, we try to read already committed tuple.
	if(HashTable[h].lcommit >= 0)
	{
		for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i + VERSIONMAX - 1) % VERSIONMAX)
		{
			if (MVCCVisible(&(HashTable[h]), i, lsnap) )
			{
				if(IsMVCCDeleted(&HashTable[h],i))
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					*flag=-2;
					return 0;
				}
				else
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					return HashTable[h].VersionList[i].value;
				}
			}
		}
	}
	pthread_spin_unlock(&RecordLatch[table_id][h]);

	//can see nothing of the data.
	*flag=-3;
	return 0;
}

/*
TupleId LocateData_Read(int table_id, int h, TupleId *id)
{
	VersionId i;
	//int h;
	int index;
	TupleId visible, tuple_id;
	VersionId newread,newest;
	Version* v;
	Snapshot* snap;
	char* DataMemStart;

	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the pointer to data-memory.
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	//get pointer to transaction-snapshot-data.
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

	THash HashTable = TableList[table_id];

	if(HashTable[h].tupleid == InvalidTupleId)
	{
		return 0;
	}

	tuple_id=HashTable[h].tupleid;

	//the data by 'tuple_id' exist, so to read it.
	pthread_spin_lock(&RecordLatch[table_id][h]);
	newest = (HashTable[h].rear + VERSIONMAX -1) % VERSIONMAX;
	//the data by (table_id,tuple_id) is being updated by other transaction.
	if(newest > HashTable[h].lcommit)
	{
		//to do nothing here.
	}

	visible=IsDataRecordVisible(DataMemStart, table_id, tuple_id);
	if(visible == -1)
	{
		//current transaction has deleted the tuple to read, so return to rollback.
		pthread_spin_unlock(&RecordLatch[table_id][h]);
		return 0;
	}
	else if(visible > 0)
	{
		//see own transaction's update.
		pthread_spin_unlock(&RecordLatch[table_id][h]);
		//return (void*)&HashTable[h].VersionList[0];
		//return 1;
		*id=tuple_id;
		return visible;
	}

	//by here, we try to read already committed tuple.
	if(HashTable[h].lcommit >= 0)
	{
		for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i + VERSIONMAX -1) % VERSIONMAX)
		{
			//
			if (MVCCVisible(&(HashTable[h]), i, snap) )
			{
				if(IsMVCCDeleted(&HashTable[h],i))
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					return 0;
				}
				else
				{
					pthread_spin_unlock(&RecordLatch[table_id][h]);
					//return (void*)&HashTable[h].VersionList[i];
					//return 1;
					*id=tuple_id;
					return HashTable[h].VersionList[i].value;
				}
			}
		}
	}
	pthread_spin_unlock(&RecordLatch[table_id][h]);

	return 0;


}
*/

bool IsCurrentTransaction(TransactionId tid, TransactionId cur_tid)
{
	return tid == cur_tid;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataInsert(int table_id, int index, TupleId tuple_id, TupleId value)
{
	int tindex;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	THREAD* threadinfo;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	tindex=threadinfo->index;

	THash HashTable=TableList[table_id];
	//printf("index=%d, tid=%d:before lock %ld, %d\n", tindex, tid, tuple_id, index);
	AcquireWrLock(&(RecordLock[table_id][index]), LOCK_EXCLUSIVE);
	//printf("index=%d, tid=%d, AcquireWrLock table_id=%d,tuple_id=%ld, %d\n", tindex, tid, table_id, tuple_id, index);

	if(IsInsertDone(table_id, index))
	{
		//other transaction has inserted the tuple.
		ReleaseWrLock(&(RecordLock[table_id][index]));
		//printf("index=%d, tid=%d, ReleaseWrLock table_id=%d,tuple_id=%ld, %d\n", tindex, tid, table_id, tuple_id, index);
		return -1;
	}

	//by here, we can insert the tuple.
	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.index=index;
	DataLockInsert(&lockrd);

	//int lindex;
	//lindex = GetLocalIndex(index);

	pthread_spin_lock(&RecordLatch[table_id][index]);

	assert(tuple_id != InvalidTupleId);
	assert(isEmptyQueue(&HashTable[index]));

	HashTable[index].tupleid = tuple_id;
	EnQueue(&HashTable[index],tid,value);

	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataUpdate(int table_id, int index, TupleId tuple_id, TupleId value)
{
	int i, old;
	bool firstadd=false;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	Cid lsnap;
	THREAD* threadinfo;
	int tindex;

	THash HashTable=TableList[table_id];

	//int lindex;
	//lindex = GetLocalIndex(index2);

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;
	tindex=threadinfo->index;

	//get the pointer to transaction-snapshot-data.
	lsnap=tdata->lsnap;

	//printf("before lock %ld\n", tuple_id);
	//to void repeatedly add lock.
	if(!IsWrLockHolding(table_id,tuple_id))
	{
		//the first time to hold the wr-lock on data (table_id,tuple_id).
		//pthread_rwlock_wrlock(&(RecordLock[table_id][index]));
		//printf("index=%d, tid=%d:before lock %ld, %d\n", tindex, tid, tuple_id, index);
		AcquireWrLock(&RecordLock[table_id][index], LOCK_EXCLUSIVE);
		firstadd=true;
		//printf("index=%d, tid=%d, AcquireWrLock table_id=%d,tuple_id=%ld, %d %d\n", tindex, tid, table_id, tuple_id, index, threadinfo->index);
	}

	//by here, we have hold the write-lock.

	//current transaction can't update the data.
	if(!IsUpdateConflict(&HashTable[index],tid,lsnap))
	{
		//release the write-lock and return to rollback.
		if(firstadd)
		{
			ReleaseWrLock(&(RecordLock[table_id][index]));
			//printf("index=%d, tid=%d, ReleaseWrLock table_id=%d,tuple_id=%ld, %d %d\n", tindex, tid, table_id, tuple_id, index, threadinfo->index);
		}
		return -1;

	}

	//by here, we are sure that we can update the data.
	//record the lock.
	if(firstadd)
	{
		lockrd.table_id=table_id;
		lockrd.tuple_id=tuple_id;
		lockrd.index = index;
		lockrd.lockmode=LOCK_EXCLUSIVE;
		DataLockInsert(&lockrd);
	}

	//here are the place we truly update the data.
	pthread_spin_lock(&RecordLatch[table_id][index]);
	//assert(!isEmptyQueue(&HashTable[index]));

	EnQueue(&HashTable[index],tid,value);
	if (isFullQueue(&(HashTable[index])))
	{
	   old = (HashTable[index].front +  VERSIONMAX/3) % VERSIONMAX;
	   for (i = HashTable[index].front; i != old; i = (i+1) % VERSIONMAX)
	   {
			//HashTable[index].VersionList[i].committime = InvalidTimestamp;
			HashTable[index].VersionList[i].tid = 0;
			HashTable[index].VersionList[i].deleted = false;
			HashTable[index].VersionList[i].cid=0;

			HashTable[index].VersionList[i].value=0;
		}
		HashTable[index].front = old;
	}
	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
int TrulyDataDelete(int table_id, int index, TupleId tuple_id)
{
	int i, old;
	int newest;
	bool firstadd=false;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	Cid lsnap;
	THREAD* threadinfo;
	int tindex;

	THash HashTable=TableList[table_id];

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;
	tindex=threadinfo->index;

	//get the pointer to transaction-snapshot-data.
	lsnap=tdata->lsnap;

	//to void repeatedly add lock.
	if(!IsWrLockHolding(table_id,tuple_id))
	{
		//the first time to hold the wr-lock on data (table_id,tuple_id).
		//printf("index=%d, tid=%d:before lock %ld, %d\n", tindex, tid, tuple_id, index);
		AcquireWrLock(&(RecordLock[table_id][index]), LOCK_EXCLUSIVE);
		//printf("index=%d, tid=%d, AcquireWrLock table_id=%d,tuple_id=%ld, %d %d\n", tindex, tid, table_id, tuple_id, index, threadinfo->index);
		firstadd=true;
	}

	//by here, we have hold the write-lock.

	//current transaction can't update the data.
	if(!IsUpdateConflict(&HashTable[index],tid,lsnap))
	{
		//release the write-lock and return to rollback.
		if(firstadd)
		{
			ReleaseWrLock(&(RecordLock[table_id][index]));
			//printf("index=%d, tid=%d, ReleaseWrLock table_id=%d,tuple_id=%ld, %d %d\n", tindex, tid, table_id, tuple_id, index, threadinfo->index);
		}
		return -1;

	}

	//by here, we are sure that we can update the data.
	//record the lock.
	if(firstadd)
	{
		lockrd.table_id=table_id;
		lockrd.tuple_id=tuple_id;
		lockrd.index = index;
		lockrd.lockmode=LOCK_EXCLUSIVE;
		DataLockInsert(&lockrd);
	}

	//here are the place we truly update the data.
	pthread_spin_lock(&RecordLatch[table_id][index]);
	assert(!isEmptyQueue(&HashTable[index]));

	EnQueue(&HashTable[index],tid,0);
	newest = (HashTable[index].rear + VERSIONMAX -1) % VERSIONMAX;
	HashTable[index].VersionList[newest].deleted = true;

	if (isFullQueue(&(HashTable[index])))
	{
	   old = (HashTable[index].front +  VERSIONMAX/3) % VERSIONMAX;
	   for (i = HashTable[index].front; i != old; i = (i+1) % VERSIONMAX)
	   {
		    //HashTable[index].VersionList[i].committime = InvalidTimestamp;
			HashTable[index].VersionList[i].tid = 0;
			HashTable[index].VersionList[i].deleted = false;
			HashTable[index].VersionList[i].cid=0;

			HashTable[index].VersionList[i].value=0;
		}
		HashTable[index].front = old;
	}

	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

void PrintTable(int table_id)
{
	int i,j,k;
	THash HashTable;
	Record* rd;
	char filename[10];

	FILE* fp;

	memset(filename,'\0',sizeof(filename));

	filename[0]=(char)(table_id+'0');
        filename[1]=(char)('+');
        filename[2]=(char)(nodeid+'0');
	strcat(filename, ".txt");

	if((fp=fopen(filename,"w"))==NULL)
	{
		printf("file open error\n");
		exit(-1);
	}
	i=table_id;

	HashTable=TableList[i];

	printf("num=%d\n", RecordNum[i]);
	for(j=0;j<RecordNum[i];j++)
	{
		rd=&HashTable[j];
		fprintf(fp,"%d: %ld",j,rd->tupleid);
		for(k=0;k<VERSIONMAX;k++)
			fprintf(fp,"(%2d %ld %ld %2d)",rd->VersionList[k].tid,rd->VersionList[k].cid,rd->VersionList[k].value,rd->VersionList[k].deleted);
		fprintf(fp,"\n");
	}
	printf("\n");
}

/*************** interface for service *******************/
void serviceDataInsert(int conn, uint64_t* buffer)
{
	int nid;
	int table_id;
	TupleId tuple_id;
	TupleId value;
	int result;

	TransactionData* td;
	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	sbuffer=ssend_buffer[threadinfo->index];

	nid=(int)*(buffer+1);
	table_id=(int)*(buffer+2);
	tuple_id=(TupleId)*(buffer+3);
	value=(TupleId)*(buffer+4);

	//printf("serviceDataInsert %d %ld %ld %d index=%d\n", table_id, tuple_id, value, nid, threadinfo->index);

	assert(nid == nodeid);

	if(td->state != active)
	{
		printf("serviceDataInsert state!=active : state=%d, tid=%d, index=%d\n", td->state, td->tid, threadinfo->index);
		exit(-1);
	}

	result=LocalDataInsert(table_id, tuple_id, value);

	//reply here.

	*(sbuffer)=result;
	num=1;

	Send(conn, sbuffer, num);
}

void serviceDataUpdate(int conn, uint64_t* buffer)
{
	int nid;
	int table_id;
	TupleId tuple_id;
	TupleId value;
	int result;

	TransactionData* td;
	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	sbuffer=ssend_buffer[threadinfo->index];

	nid=(int)*(buffer+1);
	table_id=(int)*(buffer+2);
	tuple_id=(TupleId)*(buffer+3);
	value=(TupleId)*(buffer+4);

	//printf("serviceDataUpdate nid=%d, table_id=%d, tuple_id=%ld, value=%ld, index=%d\n", nid, table_id, tuple_id, value, threadinfo->index);

	assert(nid == nodeid);

	if(td->state != active)
	{
		printf("serviceDataUpdate state!=active : state=%d, tid=%d, index=%d\n", td->state, td->tid, threadinfo->index);
		exit(-1);
	}
	result=LocalDataUpdate(table_id, tuple_id, value);

	//reply here.

	*(sbuffer)=result;
	num=1;

	//printf("serviceDataUpdate result=%d\n", result);

	Send(conn, sbuffer, num);
}

void serviceDataDelete(int conn, uint64_t* buffer)
{
	int nid;
	int table_id;
	TupleId tuple_id;
	int result;

	TransactionData* td;
	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	sbuffer=ssend_buffer[threadinfo->index];

	nid=(int)*(buffer+1);
	table_id=(int)*(buffer+2);
	tuple_id=(TupleId)*(buffer+3);

	//printf("serviceDataDelete nid=%d, table_id=%d, tuple_id=%ld, index=%d\n", nid, table_id, tuple_id, threadinfo->index);

	assert(nid == nodeid);

	if(td->state != active)
	{
		printf("serviceDataDelete state!=active : state=%d, tid=%d, index=%d\n", td->state, td->tid, threadinfo->index);
		exit(-1);
	}

	result=LocalDataDelete(table_id, tuple_id);

	//reply here.

	*(sbuffer)=result;
	num=1;

	Send(conn, sbuffer, num);
}

void serviceDataRead(int conn, uint64_t* buffer)
{
	int nid;
	int table_id;
	TupleId tuple_id;
	int flag;
	int status;
	TupleId value;

	int num;
	THREAD* threadinfo;
	uint64_t* sbuffer;
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	sbuffer=ssend_buffer[threadinfo->index];

	nid=(int)(*(buffer+1));
	table_id=(int)(*(buffer+2));
	tuple_id=(TupleId)(*(buffer+3));

	//printf("serviceDataRead nid=%d, table_id=%d, tuple_id=%ld, index=%d\n", nid, table_id, tuple_id, threadinfo->index);

	assert(nid == nodeid);

	if(td->state != active)
	{
		printf("serviceDataRead state!=active : state=%d, tid=%d, index=%d\n", td->state, td->tid, threadinfo->index);
		exit(-1);
	}

	value=LocalDataRead(table_id, tuple_id, &flag);

	switch(flag)
	{
	case 1:
		status=1;
		break;
	case 0:
	case -1:
	case -2:
	case -3:
		status=0;
		break;
	default:
		break;
	}

	*(sbuffer)=status;
	*(sbuffer+1)=value;
	num=2;

	Send(conn, sbuffer, num);
}


