#ifndef MASTER_H_
#define MASTER_H_

#include <stdint.h>
#include <pthread.h>

#define THREADNUM threadnum
#define NODENUM nodenum

#define MSEND_BUFFER_MAXSIZE 1000
#define MRECV_BUFFER_MAXSIZE 8

#define LINEMAX 20

#define LISTEN_QUEUE 500

typedef enum server_command
{
   cmd_starttransaction,
   cmd_getendtimestamp,
   cmd_updateprocarray,

   cmd_release_master,

   cmd_getGlobalCID,
} master_command;

typedef struct master_arg
{
   int index;
   int conn;
} master_arg;

extern void InitMasterBuffer(void);
extern void InitMessage(void);
extern void InitParam(void);
extern void InitRecord(void);
extern void InitMaster(void);
extern void InitNetworkParam(void);

extern int oneNodeWeight;
extern int twoNodeWeight;
extern int redo_limit;

//hotspot control
extern int HOTSPOT_PERCENTAGE;
extern int HOTSPOT_FIXED_SIZE;

//duration control
extern int extension_limit;

//random read control
extern int random_read_limit;

extern int nodenum;
extern int threadnum;
extern int client_port;
extern int master_port;
extern int message_port;
extern int param_port;
extern int record_port;
extern char master_ip[20];

extern uint64_t ** msend_buffer;
extern uint64_t ** mrecv_buffer;
extern pthread_t * master_tid;

#endif
