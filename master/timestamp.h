/*
 * timestamp.h
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */

#ifndef TIMESTAMP_H_
#define TIMESTAMP_H_

#include<sys/time.h>
#include<stdint.h>

#define DATEBASE 2000

#define SECS_PER_DAY 86400

#define USECS_PER_SEC 1000000

typedef int64_t TimeStampTz;

#define InvalidTimestamp (TimeStampTz)0

extern TimeStampTz GetCurrentTimestamp(void);

extern int64_t GetCurrentInterTimestamp(void);


#endif /* TIMESTAMP_H_ */
