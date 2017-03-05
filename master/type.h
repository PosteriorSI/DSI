#ifndef TYPE_H_
#define TYPE_H_

#include<stdint.h>

typedef uint32_t TransactionId;
typedef uint64_t Size;

typedef uint64_t Cid;

#define InvalidTransactionId ((TransactionId)0)
#define TransactionIdIsValid(tid) (tid != InvalidTransactionId)
#endif /* TYPE_H_ */
