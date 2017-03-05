/*
 * procarray.h
 *
 *  Created on: May 5, 2016
 *      Author: xiaoxin
 */

#ifndef PROCARRAY_H_
#define PROCARRAY_H_
#include "type.h"

struct PROC
{
	TransactionId tid;
	Cid global_cid_range_low;
	Cid global_cid_range_up;
	int index;//the index for per thread.
};

typedef struct PROC PROC;

extern PROC* procarray;

extern size_t ProcArraySize(void);

extern void InitProcArray(void);

extern void AtStart_ProcArray(int index, TransactionId tid, Cid low);

extern void getGlobalCidRange(int index, Cid* low, Cid* up);

extern int getTransactionIdIndex(TransactionId tid);

extern void AtEnd_ProcArray(int index);

extern void setGlobalCidRangeUp(int index, Cid up);

#endif /* PROCARRAY_H_ */
