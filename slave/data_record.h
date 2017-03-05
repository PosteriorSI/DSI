/*
 * data_record.h
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */

#ifndef DATA_RECORD_H_
#define DATA_RECORD_H_

#include "type.h"
#include "timestamp.h"

#define DataNumSize sizeof(int)
/*
 * type definitions for data update.
 */
typedef enum UpdateType
{
	//data insert
	DataInsert,
	//data update
	DataUpdate,
	//data delete
	DataDelete
}UpdateType;

struct DataRecord
{
	UpdateType type;

	int table_id;
	TupleId tuple_id;

	//other information attributes.
	TupleId value;

	//index in the table.
	uint64_t index;
};
typedef struct DataRecord DataRecord;

extern void InitDataMem(void);

extern void InitDataMemAlloc(void);

//extern void InsertRecord(void* data);

//extern void UpdateRecord(void* olddata,void* newdata);

//extern void DeleteRecord(void* data);

extern void DataRecordInsert(DataRecord* datard);

extern Size DataMemSize(void);

//extern void CommitDataRecord(TransactionId tid, TimeStampTz ctime);

//extern void AbortDataRecord(TransactionId tid, int trulynum);

extern void DataRecordSort(DataRecord* dr, int num);

extern TupleId IsDataRecordVisible(char* DataMemStart, int table_id, TupleId tuple_id);

extern void writeSetVisible(void);

extern void writeCIDInDoubt(Cid CID);

extern void LocalAbortDataRecord(TransactionId tid, int trulynum);

#endif /* DATA_RECORD_H_ */
