/*
 * trans.h
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */

#ifndef TRANS_H_
#define TRANS_H_

#include "type.h"
#include "proc.h"
#include "timestamp.h"

#define InvalidTransactionId ((TransactionId)0)

typedef enum
{
	empty,
	active,
	prepared,
	committed,
	aborted
}State;

struct TransactionData
{
	TransactionId tid;

	Cid lsnap;

	Cid global_cid_range_up;

	Cid global_cid_range_low;

	Cid local_cid;

	State state;

	//false for not yet, true for already.
	bool first_remote_access;

	int flag;

	//for test
	int trans_index;
	int nid;
	int ExtendAbort;
};

typedef struct TransactionData TransactionData;


typedef struct ShmemVariable
{
	Cid LOCAL_CID;
	TransactionId LOCAL_TID;
	Cid LAST_GCID;

	int LcidNum;
	int LsnapNum;
}ShmemVariable;

#define TransactionIdIsValid(tid) (tid != InvalidTransactionId)

extern ShmemVariable* ShmVariable;

extern void InitTransactionStructMemAlloc(void);

extern void TransactionLoadData(int i);

extern void TransactionRunSchedule(void* args);

//extern PROC* GetCurrentTransactionData(void);

//extern void TransactionContextCommit(TransactionId tid, TimeStampTz ctime, int index);

//extern void TransactionContextAbort(TransactionId tid, int index);

extern void StartTransaction(void);

extern bool CommitTransaction(void);

extern void AbortTransaction(void);

extern int PreCommit(int* index);

extern int GetNodeId(int index);

extern int GetLocalIndex(int index);

extern size_t NodeInfoSize(void);

extern TransactionId AssignLocalTID(void);

extern Cid AssignLocalCID(void);

extern Cid GetLocalSnapshot(void);

extern int PreCommit(int* index);

extern void LocalAccess(Cid gcid_up);

extern void LocalAbort(int trulynum);

extern void LocalBegin(void);

extern bool LocalExtend(int node_j);

extern bool nodeFirstAccess(int nid);

extern void serviceLocalAccess(int conn, uint64_t* buffer);

extern void serviceSelfAccess(int conn, uint64_t* buffer);

extern void serviceLocalPrepare(int conn, uint64_t* buffer);

extern void serviceSingleCommit(int conn, uint64_t* buffer);

extern void serviceSingleAbort(int conn, uint64_t* buffer);

extern void serviceLocalCommit(int conn, uint64_t* buffer);

extern void serviceLocalAbort(int conn, uint64_t* buffer);

extern void serviceAbortAheadWrite(int conn, uint64_t* buffer);

extern size_t ShmemVariableSize(void);

extern void InitShmemVariable(void);

extern void InitServiceStructMemAlloc(void);

#endif /* TRANS_H_ */
