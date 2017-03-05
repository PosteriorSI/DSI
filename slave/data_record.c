/*
 * data_record.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */
#include<assert.h>
#include"data_record.h"
#include"mem.h"
#include"thread_global.h"
#include"data_am.h"
#include"trans.h"
#include "communicate.h"


#define DataMemMaxSize 128*1024

/*
void GetPosition(Record * r, int * tableid, int * h)
{
	Record * base = &(TableList[0][0]);
	*tableid = (r - base) / RECORDNUM;
	*h = (r - base) % RECORDNUM;
}
*/
/*
void CommitInsertData(int table_id, int index, TimeStampTz ctime, int nid)
{
	THREAD* threadinfo;
	int index2;
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index2);

	if((Send4(lindex, nid, cmd_commitinsert, table_id, index, ctime)) == -1)
		printf("commit insert send error!\n");
	if((Recv(lindex, nid, 1)) == -1)
		printf("commit insert recv error!\n");
}

void CommitUpdateData(int table_id, int index, TimeStampTz ctime, int nid)
{
	int index2;
	THREAD* threadinfo;
    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index2);

	if (Send4(lindex, nid, cmd_commitupdate, table_id, index, ctime) == -1)
		printf("commit update send error\n");
    if((Recv(lindex, nid, 1)) == -1)
    	printf("commit update recv error\n");
}

//void CommitDeleteData(void * data, TimeStampTz ctime)
void CommitDeleteData(int table_id, int index, TimeStampTz ctime, int nid)
{
	int index2;
	THREAD* threadinfo;
    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index2);

	if (Send4(lindex, nid, cmd_commitupdate, table_id, index, ctime) == -1)
		printf("commit delete send error\n");
    if((Recv(lindex, nid, 1)) == -1)
    	printf("commit delete recv error\n");
}

//void AbortInsertData(void * data)
void AbortInsertData(int table_id, int index, int nid)
{
	int index2;
	THREAD* threadinfo;
    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index2);

	if (Send3(lindex, nid, cmd_abortinsert, table_id, index) == -1)
		printf("abort insert send error\n");
    if((Recv(lindex, nid, 1)) == -1)
    	printf("abort insert recv error\n");
}

//void AbortUpdateData(void * data)
void AbortUpdateData(int table_id, int index, int nid)
{
	bool isdelete = false;
	int index2;
	THREAD* threadinfo;
    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index2);

	if (Send4(lindex, nid, cmd_abortupdate, table_id, index, isdelete) == -1)
		printf("abort update send error\n");
    if((Recv(lindex, nid, 1)) == -1)
    	printf("abort update recv error\n");
}

void AbortDeleteData(int table_id, int index, int nid)
{
	bool isdelete = true;
	int index2;
	THREAD* threadinfo;
    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index2=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index2);

	if (Send4(lindex, nid, cmd_abortupdate, table_id, index, isdelete) == -1)
		printf("abort delete send error\n");
    if((Recv(lindex, nid, 1)) == -1)
    	printf("abort delete recv error\n");
}
*/
/*
 * insert a data-update-record.
 */
void DataRecordInsert(DataRecord* datard)
{
	int num;
	char* start;
	char* DataMemStart;
	DataRecord* ptr;

	//get the thread's pointer to data memory.
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	start=(char*)((char*)DataMemStart+DataNumSize);
	num=*(int*)DataMemStart;

	if(DataNumSize+(num+1)*sizeof(DataRecord) > DataMemSize())
	{
		printf("data memory out of space. PID: %lu\n",pthread_self());
		return;
	}

	*(int*)DataMemStart=num+1;

	//start address for record to insert.
	start=start+num*sizeof(DataRecord);

	//insert the data record here.
	ptr=(DataRecord*)start;
	ptr->type=datard->type;

	ptr->table_id=datard->table_id;
	ptr->tuple_id=datard->tuple_id;

	ptr->value=datard->value;

	ptr->index=datard->index;
}

Size DataMemSize(void)
{
	return DataMemMaxSize;
}

void InitDataMemAlloc(void)
{
	char* DataMemStart=NULL;
	char* memstart;
	THREAD* threadinfo;
	Size size;

	//get start address of current thread's memory.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=DataMemSize();
	DataMemStart=(char*)MemAlloc((void*)memstart,size);

	if(DataMemStart==NULL)
	{
		printf("thread memory allocation error for data memory.PID:%lu\n",pthread_self());
		return;
	}

	//set global variable.
	pthread_setspecific(DataMemKey,DataMemStart);
}

void InitDataMem(void)
{
	char* DataMemStart=NULL;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	/*
	char* memstart;
	THREAD* threadinfo;
	Size size;

	//get start address of current thread's memory.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=DataMemSize();
	DataMemStart=(char*)MemAlloc((void*)memstart,size);

	if(DataMemStart==NULL)
	{
		printf("thread memory allocation error for data memory.PID:%d\n",pthread_self());
		return;
	}

	//set global variable.
	pthread_setspecific(DataMemKey,DataMemStart);
	*/

	memset(DataMemStart,0,DataMemSize());
}

/*
 * write the commit time of current transaction to every updated tuple.
 */
/*
void CommitDataRecord(TransactionId tid, TimeStampTz ctime)
{
	int num;
	int i;
	char* DataMemStart=NULL;
	char* start;
	DataRecord* ptr;
	int table_id;
	int nid;
	uint64_t index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;
	//printf("PID:%u,datamemstart=%d\n",pthread_self(),start);

	num=*(int*)DataMemStart;
	//printf("datarecord num: %d\n",num);

	//deal with data-update record one by one.
	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		table_id=ptr->table_id;
		index=ptr->index;
		nid = ptr->node_id;
		switch(ptr->type)
		{
		case DataInsert:
			//interface to do here.
			//get the address of inserted tuple and write the cid.
			//then update the tailer.

			CommitInsertData(table_id, index, ctime, nid);
			break;
		case DataUpdate:
			//interface to do here.
			//get the address of inserted and deleted tuple, and
			//write the cid, then update the tailer.

			CommitUpdateData(table_id, index, ctime, nid);
			break;
		case DataDelete:
			//interface to do here.
			//get the address of deleted tuple and write the cid.
			//then update the tailer.

			CommitDeleteData(table_id, index, ctime, nid);
			break;
		default:
			printf("shouldn't arrive here.\n");
		}
	}
}
*/
void InsertDataInDoubt(int table_id, int index, Cid CID)
{
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

    pthread_spin_lock(&RecordLatch[table_id][index]);
	r->commitInDoubt = (r->commitInDoubt + 1) % VERSIONMAX;
	r->VersionList[r->commitInDoubt].cid = CID;
	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void UpdateDataInDoubt(int table_id, int index, Cid CID)
{
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

    pthread_spin_lock(&RecordLatch[table_id][index]);
	r->commitInDoubt = (r->commitInDoubt + 1) % VERSIONMAX;
	r->VersionList[r->commitInDoubt].cid = CID;
    pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void DeleteDataInDoubt(int table_id, int index, Cid CID)
{
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

    pthread_spin_lock(&RecordLatch[table_id][index]);
	r->commitInDoubt = (r->commitInDoubt + 1) % VERSIONMAX;
	r->VersionList[r->commitInDoubt].cid = CID;

	pthread_spin_unlock(&RecordLatch[table_id][index]);
}
void writeCIDInDoubt(Cid CID)
{
	int num;
	int i;
	char* DataMemStart=NULL;
	char* start;
	DataRecord* ptr;
	int table_id;
	int nid;
	uint64_t index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;
	//printf("PID:%u,datamemstart=%d\n",pthread_self(),start);

	num=*(int*)DataMemStart;

	//deal with data-update record one by one.
	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		table_id=ptr->table_id;
		index=ptr->index;
		switch(ptr->type)
		{
		case DataInsert:
			//interface to do here.
			//get the address of inserted tuple and write the cid.
			//then update the tailer.

			/*
			 * interface:CommitInsertData(data,cid);
			 */
			//CommitInsertData(table_id, index, ctime, nid);
			InsertDataInDoubt(table_id, index, CID);
			break;
		case DataUpdate:
			//interface to do here.
			//get the address of inserted and deleted tuple, and
			//write the cid, then update the tailer.

			/*
			 * interface:CommitUpdateData(data,cid);
			 */
			//CommitUpdateData(table_id, index, ctime, nid);
			UpdateDataInDoubt(table_id, index, CID);
			break;
		case DataDelete:
			//interface to do here.
			//get the address of deleted tuple and write the cid.
			//then update the tailer.

			/*
			 * interface:CommitDeleteData(data,cid);
			 */
			//CommitDeleteData(table_id, index, ctime, nid);
			DeleteDataInDoubt(table_id, index, CID);
			break;
		default:
			printf("shouldn't arrive here.\n");
		}
	}
}

void setDataVisible(int table_id, int index)
{
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

    pthread_spin_lock(&RecordLatch[table_id][index]);

    r->lcommit=r->commitInDoubt;

	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void writeSetVisible(void)
{
	int num;
	int i;
	char* DataMemStart=NULL;
	char* start;
	DataRecord* ptr;
	int table_id;
	uint64_t index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;
	//printf("PID:%u,datamemstart=%d\n",pthread_self(),start);

	num=*(int*)DataMemStart;

	if(num > 0)
	{
		ptr=(DataRecord*)start;
		table_id=ptr->table_id;
		index=ptr->index;

		for(i=0;i<num;i++)
		{
			ptr=(DataRecord*)(start+i*sizeof(DataRecord));

			if(table_id == ptr->table_id && index == ptr->index)
				continue;

			setDataVisible(table_id, index);

			table_id=ptr->table_id;
			index=ptr->index;
		}

		//deal with the last record.
		setDataVisible(table_id, index);
	}
}

/*
 * rollback all updated tuples by current transaction.
 */
/*
void AbortDataRecord(TransactionId tid, int trulynum)
{
	int num;
	int i;
	char* DataMemStart=NULL;
	char* start;
	DataRecord* ptr;
	int table_id;
	int nid;
	uint64_t index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;
	//num=*(int*)DataMemStart;
	//transaction abort in function CommitTransaction.
	if(trulynum==-1)
		num=*(int*)DataMemStart;
	//transaction abort in function AbortTransaction.
	else
		num=trulynum;

	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		table_id=ptr->table_id;
		index=ptr->index;
		nid = ptr->node_id;
		switch(ptr->type)
		{
		case DataInsert:
			//to do here.
			//get the address of inserted tuple and free it's space.

			AbortInsertData(table_id, index, nid);
			break;
		case DataUpdate:
			//to do here.
			//get the address of inserted and deleted tuple, and free the
			//space of inserted, reset the space of deleted to original.

			AbortUpdateData(table_id, index, nid);
			break;
		case DataDelete:
			//to do here.
			//get the address of deleted tuple and reset the space to
			//original.

			AbortDeleteData(table_id, index, nid);
			break;
		default:
			printf("shouldn't get here.\n");
		}
	}
}
*/
void LocalAbortInsertData(int table_id, int index)
{
	VersionId newest;
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

	pthread_spin_lock(&RecordLatch[table_id][index]);
	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;

	r->tupleid=InvalidTupleId;
	r->rear=0;
	r->front=0;
	r->lcommit=-1;
	r->commitInDoubt=-1;

	r->VersionList[newest].tid = InvalidTransactionId;

	r->VersionList[newest].value = 0;

	r->VersionList[newest].cid=0;

	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void LocalAbortUpdateData(int table_id, int index)
{
	VersionId newest;
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

	pthread_spin_lock(&RecordLatch[table_id][index]);
	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
	r->VersionList[newest].tid =InvalidTransactionId;
	r->VersionList[newest].cid=0;
	r->VersionList[newest].value=0;
	r->rear = newest;
	r->commitInDoubt=r->lcommit;
	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void LocalAbortDeleteData(int table_id, int index)
{
	VersionId newest;
	THash HashTable;

	HashTable=TableList[table_id];

	Record * r = (Record *) &HashTable[index];

	pthread_spin_lock(&RecordLatch[table_id][index]);
	newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
	r->VersionList[newest].tid = InvalidTransactionId;
	r->VersionList[newest].deleted = false;
	r->VersionList[newest].cid=0;
	r->VersionList[newest].value=0;
	r->rear = newest;
	r->commitInDoubt=r->lcommit;
	pthread_spin_unlock(&RecordLatch[table_id][index]);
}

void LocalAbortDataRecord(TransactionId tid, int trulynum)
{
	int num;
	int i;
	char* DataMemStart=NULL;
	char* start;
	DataRecord* ptr;
	int table_id;
	uint64_t index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;
	//num=*(int*)DataMemStart;
	//transaction abort in function CommitTransaction.
	if(trulynum==-1)
		num=*(int*)DataMemStart;
	//transaction abort in function AbortTransaction.
	else
		num=trulynum;

	for(i=num-1;i>=0;i--)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		table_id=ptr->table_id;
		index=ptr->index;
		switch(ptr->type)
		{
		case DataInsert:
			//to do here.
			//get the address of inserted tuple and free it's space.

			/*
			 * interface:AbortInsertData(data);
			 */
			//AbortInsertData(table_id, index, nid);
			LocalAbortInsertData(table_id, index);
			break;
		case DataUpdate:
			//to do here.
			//get the address of inserted and deleted tuple, and free the
			//space of inserted, reset the space of deleted to original.

			/*
			 * interface:AbortUpdateData(data);
			 */
			//AbortUpdateData(table_id, index, nid);
			LocalAbortUpdateData(table_id, index);
			break;
		case DataDelete:
			//to do here.
			//get the address of deleted tuple and reset the space to
			//original.

			/*
			 * interface:AbortDeleteData(data);
			 */
			//AbortDeleteData(table_id, index, nid);
			LocalAbortDeleteData(table_id, index);
			break;
		default:
			printf("shouldn't get here.\n");
		}
	}
}
/*
 * sort the transaction's data-record to avoid dead lock between different
 * update transactions.
 * @input:'dr':the start address of data-record, 'num': number of data-record.
 */
void DataRecordSort(DataRecord* dr, int num)
{
	//sort according to the table_id and tuple_id;
	DataRecord* ptr1, *ptr2;
	DataRecord* startptr=dr;
	DataRecord temp;
	int i,j;

	for(i=0;i<num-1;i++)
		for(j=0;j<num-i-1;j++)
		{
			ptr1=startptr+j;
			ptr2=startptr+j+1;
			//if((ptr1->node_id > ptr2->node_id) || ((ptr1->node_id == ptr2->node_id) && (ptr1->table_id > ptr2->table_id)) || ((ptr1->node_id == ptr2->node_id) && (ptr1->table_id == ptr2->table_id) && (ptr1->tuple_id > ptr2->tuple_id)))
			if(ptr1->table_id > ptr2->table_id || (ptr1->table_id == ptr2->table_id && ptr1->tuple_id > ptr2->tuple_id))
			{
				temp.table_id=ptr1->table_id;
				temp.tuple_id=ptr1->tuple_id;
				temp.type=ptr1->type;
				temp.value=ptr1->value;
				temp.index=ptr1->index;

				ptr1->table_id=ptr2->table_id;
				ptr1->tuple_id=ptr2->tuple_id;
				ptr1->type=ptr2->type;
				ptr1->value=ptr2->value;
				ptr1->index=ptr2->index;

				ptr2->table_id=temp.table_id;
				ptr2->tuple_id=temp.tuple_id;
				ptr2->type=temp.type;
				ptr2->value=temp.value;
				ptr2->index=temp.index;
			}
		}
}

/*
 * @return:'1' for visible inner own transaction, '-1' for invisible inner own transaction,
 * '0' for first access the tuple inner own transaction.
 */
TupleId IsDataRecordVisible(char* DataMemStart, int table_id, TupleId tuple_id)
{
	int num,i;
	char* start=DataMemStart+DataNumSize;
	num=*(int*)DataMemStart;
	DataRecord* ptr;

	for(i=num-1;i>=0;i--)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));
		if(ptr->tuple_id == tuple_id && ptr->table_id == table_id)
		{
			if(ptr->type == DataDelete)
			{
				//the tuple has been deleted by current transaction.
				return -1;
			}
			else
			{
				//insert or update.
				//return 1;
				return ptr->value;
			}
		}
	}

	//first access the tuple.
	return 0;
}



