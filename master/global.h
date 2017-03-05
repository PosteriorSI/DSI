/*
 * global.h
 *
 *  Created on: May 2, 2016
 *      Author: xiaoxin
 */

#ifndef GLOBAL_H_
#define GLOBAL_H_

#include "type.h"

extern void Init_Global_CID(void);

extern Cid Get_Global_CID(void);

extern Cid Get_Global_Next_CID(void);

extern uint64_t TotalDistributedTrasactions(void);

#endif /* GLOBAL_H_ */
