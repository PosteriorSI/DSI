/*
 * map.h
 *
 *  Created on: May 2, 2016
 *      Author: xiaoxin
 */
#ifndef MAP_H_
#define MAP_H_

#include "type.h"


typedef struct ElemMap
{
	//the number of element in the Map.
	//int num;
	Cid key;
	Cid value;
}EMap;

typedef struct ListMap
{
	int tail;
	Cid key;
	TransactionId *list;
}LMap;

extern EMap* gcid2lcid;

extern EMap* gcid2lsnap;

extern LMap* readsFromGcid;

extern Cid EMapAdd(Cid key, Cid value, int flag);

extern Cid EMapMatch(Cid key, int flag);

extern int LMapAdd(LMap* map, Cid last_GCid, TransactionId tid, int index);

extern void LMapReset(LMap* map, Cid new_key);

extern void InitEMap(void);

extern void InitLMap(void);

extern int EMapHash(Cid key, int k);

extern size_t EMapsize(void);

extern size_t LMapsize(void);

extern void EMapClean(int flag);

#endif /* MAP_H_ */
