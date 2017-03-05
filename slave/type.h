/*
 * type.h
 *
 *  Created on: 2015��11��9��
 *      Author: DELL
 */
/*
 * data type is defined here.
 */
#ifndef TYPE_H_
#define TYPE_H_

#include<stdio.h>
#include<stdint.h>
#include<string.h>
#include<stdbool.h>

typedef uint32_t TransactionId;

typedef uint32_t StartId;

typedef uint32_t CommitId;

typedef uint64_t Size;

typedef uint64_t TupleId;

typedef uint64_t Cid;

#define MAXINTVALUE 1<<30

#define MAXINT (Cid)(1<<62)

/*
 * type of the command from the client, and the server process will get the different result
 * respond to different command to the client.
 */
typedef enum command
{
   cmd_release=1,
   cmd_localAccess,
   cmd_selfAccess,
   cmd_dataInsert,
   cmd_dataUpdate,
   cmd_dataDelete,
   cmd_dataRead,
   cmd_prepare,
   cmd_abort,
   cmd_commit,

   cmd_singlePrepare,
   cmd_singleAbort,
   cmd_singleCommit,

   cmd_abortAheadWrite
} command;

typedef enum server_command
{
   cmd_starttransaction,
   cmd_getendtimestamp,
   cmd_updateprocarray,

   cmd_release_master,

   cmd_getGlobalCID,
} master_command;

#endif /* TYPE_H_ */
