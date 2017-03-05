#ifndef DATA_H_
#define DATA_H_
/* add by yu the data structure for the tuple */

#include<stdbool.h>
#include"type.h"
#include"timestamp.h"
//#define RECORDNUM 1000
//#define TABLENUM  9

//smallbank
//#define TABLENUM 3
#define VERSIONMAX 20

#define InvalidTupleId (TupleId)(0)


// Version is used for store a version of a record
typedef struct {
	TransactionId tid;
	//TimeStampTz committime;

	Cid cid;

	bool deleted;

	//to stock other information of each version.
	TupleId value;
} Version;

// Record is a multi-version tuple structure
typedef struct {
	TupleId tupleid;
	int rear;
	int front;
	int lcommit;

	int commitInDoubt;

	Version VersionList[VERSIONMAX];
} Record;

// THash is pointer to a hash table for every table
typedef Record * THash;

typedef int VersionId;

extern int TABLENUM;

// the lock in the tuple is used to verify the atomic operation of transaction
//extern pthread_rwlock_t* RecordLock[TABLENUM];
extern pthread_rwlock_t** RecordLock;

// just use to verify the atomic operation of a short-time
//extern pthread_spinlock_t* RecordLatch[TABLENUM];
extern pthread_spinlock_t** RecordLatch;

// every table will have a separated HashTable
//extern Record* TableList[TABLENUM];
extern Record** TableList;

//extern int BucketNum[TABLENUM];
//extern int BucketSize[TABLENUM];

//extern int RecordNum[TABLENUM];

extern int* BucketNum;
extern int* BucketSize;

extern int* RecordNum;

extern bool MVCCVisible(Record * r, VersionId v, Cid lsnap);

extern bool IsInsertDone(int table_id, int index);
extern bool isEmptyQueue(Record * r);
extern void EnQueue(Record * r, TransactionId tid, TupleId value);

extern bool IsUpdateConflict(Record * r, TransactionId tid, Cid snap);
extern bool isFullQueue(Record * r);

extern int RecordFind(int table_id, TupleId r);

extern bool IsMVCCDeleted(Record * r, VersionId v);

extern int RecordFindHole(int table_id, TupleId r, int *flag);


//extern int Data_Insert(int table_id, int tuple_id);
#endif
