#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "proc.h"
#include "socket.h"
#include "type.h"
#include "data.h"
#include "thread_main.h"
#include "trans.h"
#include "data_am.h"

void* Respond(void *sockp);

uint32_t** CommTimes;

int oneNodeWeight;
int twoNodeWeight;

int redo_limit;

int nodenum;
int threadnum;

int nodeid;
int recordfd;

int master_port;
int message_port;
int param_port;
int record_port;
int port_base;
int ip_suffix;
FILE * conf_fp;
char master_ip[20];
char local_ip[20];

//the number of nodes should not be larger than NODE NUMBER MAX
char node_ip[NODENUMMAX][20];

//send buffer and receive buffer for client
uint64_t ** send_buffer;
uint64_t ** recv_buffer;

//send buffer for server respond.
uint64_t ** ssend_buffer;
uint64_t ** srecv_buffer;

/*
  * this array is used to save the connect with other nodes in the distributed system.
  * the connect with other slave node should be maintained until end of the process.  */

int connect_socket[NODENUMMAX][THREADNUMMAX];
int server_socket[THREADNUMMAX];

int message_socket;
int param_socket;

pthread_t* server_tid=NULL;

void InitCommTimes(void)
{
	int size;
	int i,j;
	int num=20;

	size=sizeof(uint32_t*)*(NODENUM*THREADNUM+1);
	CommTimes=(uint32_t**)malloc(size);

	size=sizeof(uint32_t)*num;
	for(i=0;i<NODENUM*THREADNUM+1;i++)
	{
		CommTimes[i]=(uint32_t*)malloc(size);
		memset(CommTimes[i], (uint32_t)0, size);
	}
}


// read the configure parameters from the configure file.
int ReadConfig(char * find_string, char * result)
{
   int i;
   int j;
   int k;
   char buffer[30];
   char * p;

   for (i = 0; i < LINEMAX; i++)
   {
      if (fgets(buffer, sizeof(buffer), conf_fp) == NULL)
         continue;
      for (p = find_string, j = 0; *p != '\0'; p++, j++)
      {
         if (*p != buffer[j])
            break;
      }

      if (*p != '\0' || buffer[j] != ':')
      {
         continue;
      }

      else
      {
         k = 0;
         //jump over the character ':' and the space character.
         j = j + 2;
         while (buffer[j] != '\0')
         {
            *(result+k) = buffer[j];
            k++;
            j++;
         }
         *(result+k) = '\0';
         rewind(conf_fp);
         return 1;
      }
   }
   rewind(conf_fp);
   printf("can not find the configure you need\n");
   return -1;
}

void InitClientBuffer(void)
{
   int i;

   send_buffer = (uint64_t **) malloc ((THREADNUM+1) * sizeof(uint64_t *));
   recv_buffer = (uint64_t **) malloc ((THREADNUM+1) * sizeof(uint64_t *));

   if (send_buffer == NULL || recv_buffer == NULL)
	   printf("client buffer pointer malloc error\n");

   for (i = 0; i < THREADNUM+1; i++)
   {
	   send_buffer[i] = (uint64_t *) malloc (SEND_BUFFER_MAXSIZE * sizeof(uint64_t));
	   recv_buffer[i] = (uint64_t *) malloc (RECV_BUFFER_MAXSIZE * sizeof(uint64_t));
	   if ((send_buffer[i] == NULL) || recv_buffer[i] == NULL)
		   printf("client buffer malloc error\n");
   }
}

void InitServerBuffer(void)
{
   int i;
   ssend_buffer = (uint64_t **) malloc ((NODENUM*THREADNUM+1) * sizeof(uint64_t *));
   srecv_buffer = (uint64_t **) malloc ((NODENUM*THREADNUM+1) * sizeof(uint64_t *));

   if (ssend_buffer == NULL || srecv_buffer == NULL)
	   printf("server buffer pointer malloc error\n");

   for (i = 0; i < NODENUM*THREADNUM+1; i++)
   {
      ssend_buffer[i] = (uint64_t *) malloc (SSEND_BUFFER_MAXSIZE * sizeof(uint64_t));
      srecv_buffer[i] = (uint64_t *) malloc (SRECV_BUFFER_MAXSIZE * sizeof(uint64_t));
	   if ((ssend_buffer[i] == NULL) || srecv_buffer[i] == NULL)
		   printf("server buffer malloc error\n");
   }
}

void InitParamClient(void)
{
	int slave_sockfd;
	slave_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int port = param_port;
    struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_addr.s_addr = inet_addr(master_ip);
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);

	if (connect(slave_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) < 0)
	{
		printf("client master param connect error!\n");
	    exit(1);
	}

	param_socket = slave_sockfd;
}

void InitMessageClient(void)
{
	int slave_sockfd;
	slave_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int port = message_port;
    struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_addr.s_addr = inet_addr(master_ip);
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);

	if (connect(slave_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) < 0)
	{
		printf("client master message connect error!\n");
	    exit(1);
	}

	message_socket = slave_sockfd;
}

void InitMasterClient(int threadid)
{
	int slave_sockfd;
	slave_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int port = master_port;
    struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_addr.s_addr = inet_addr(master_ip);
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);

	if (connect(slave_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) < 0)
	{
		printf("client master connect error!\n");
	    exit(1);
	}

	server_socket[threadid] = slave_sockfd;
}

void InitClient(int nid, int threadid)
{
	int slave_sockfd;
	slave_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int port = port_base + nid;
    struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_addr.s_addr = inet_addr(node_ip[nid]);
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);

	if (connect(slave_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) < 0)
	{
		printf("client connect error! nid=%d, threadid=%d\n", nid, threadid);
	    exit(1);
	}

	connect_socket[nid][threadid] = slave_sockfd;
}

void InitRecordClient(void)
{
	int slave_sockfd;
	slave_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int port = record_port;
    struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_addr.s_addr = inet_addr(master_ip);
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);

	if (connect(slave_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) < 0)
	{
		printf("client record connect error\n");
	    exit(1);
	}
	recordfd = slave_sockfd;
}

void InitServer(int nid)
{
	int status;
	int conn;
	void * pstatus;

	server_tid = (pthread_t *) malloc ((NODENUM*THREADNUM+1)*sizeof(pthread_t));
	server_arg *argu = (server_arg *)malloc((NODENUM*THREADNUM+1)*sizeof(server_arg));

	int master_sockfd;
    int port = port_base + nid;
	// use the TCP protocol
	master_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	//bind
	struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);
	mastersock_addr.sin_addr.s_addr = INADDR_ANY;

	if (bind(master_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) == -1)
	{
		printf("bind error!\n");
		exit(1);
	}

	//listen
    if(listen(master_sockfd, LISTEN_QUEUE) == -1)
    {
        printf("listen error!\n");
        exit(1);
    }

    // force the transaction process waiting for the completion of the server's initialization.
    sem_post(wait_server);

    printf("server %d is ready\n", nodeid);
    //receive or transfer data
	socklen_t slave_length;
	struct sockaddr_in slave_addr;
	slave_length = sizeof(slave_addr);
   	int i = 0;

    while(i < NODENUM*THREADNUM+1)
    {
    	conn = accept(master_sockfd, (struct sockaddr*)&slave_addr, &slave_length);
    	argu[i].conn = conn;
    	argu[i].index = i;
    	if(conn < 0)
    	{
    		printf("server accept connect error!\n");
    	    exit(1);
    	}
    	status = pthread_create(&server_tid[i], NULL, ServiceProcStart, &(argu[i]));
    	if (status != 0)
    		printf("create thread %d error %d!\n", i, status);
    	i++;
    }

    // wait the child threads to complete.
    for (i = 0; i < NODENUM*THREADNUM+1; i++)
       pthread_join(server_tid[i], &pstatus);
}

void* Respond(void *pargu)
{
	int conn;
	int index;
	server_arg * temp;
	temp = (server_arg *) pargu;
	conn = temp->conn;
	index = temp->index;
	command type;
	int end=0;

	uint64_t* rbuffer;

	rbuffer=srecv_buffer[index];
	memset(rbuffer, 0, SRECV_BUFFER_MAXSIZE*sizeof(uint64_t));
	int ret;

	/*
	int i;
	for(i=0;i<6;i++)
	{
		printf("waiting for message %d\n", i);
		recv(conn, srecv_buffer[index],sizeof(uint64_t), 0);
		printf("i=%d,%d\n", i, *(srecv_buffer[index]));
	}

	sleep(10);
	*/

    do
    {
    	//printf("waiting for message index=%d\n", index);
   	    ret = recv(conn, rbuffer, SRECV_BUFFER_MAXSIZE*sizeof(uint64_t), 0);
    	//ret=recv(conn, rbuffer, sizeof(uint64_t), 0);

   	    if (ret == -1)
   	    {
   	    	printf("server receive error!\n");
   	    	exit(-1);
   	    }

   	    type = (command)(*(rbuffer));

   	    //printf("deal with message index=%d, type=%d\n", index, type);
   	    switch(type)
   	    {
   	    case cmd_localAccess:
   	    	serviceLocalAccess(conn, rbuffer);
			CommTimes[index][0]++;
   	    	break;
   	    case cmd_selfAccess:
   	    	serviceSelfAccess(conn, rbuffer);
			CommTimes[index][1]++;
   	    	break;
   	    case cmd_dataInsert:
   	    	serviceDataInsert(conn, rbuffer);
			CommTimes[index][2]++;
   	    	break;
   	    case cmd_dataUpdate:
   	    	serviceDataUpdate(conn, rbuffer);
			CommTimes[index][3]++;
   	    	break;
   	    case cmd_dataDelete:
   	    	serviceDataDelete(conn, rbuffer);
			CommTimes[index][4]++;
   	    	break;
   	    case cmd_dataRead:
   	    	serviceDataRead(conn, rbuffer);
			CommTimes[index][5]++;
   	    	break;
   	    case cmd_singlePrepare:
   	    	serviceLocalPrepare(conn, rbuffer);
			CommTimes[index][6]++;
   	    	break;
   	    case cmd_singleCommit:
   	    	serviceSingleCommit(conn, rbuffer);
			CommTimes[index][7]++;
   	    	break;
   	    case cmd_singleAbort:
   	    	serviceSingleAbort(conn, rbuffer);
			CommTimes[index][8]++;
   	    	break;
   	    case cmd_prepare:
   	    	serviceLocalPrepare(conn, rbuffer);
			CommTimes[index][9]++;
   	    	break;
   	    case cmd_commit:
   	    	serviceLocalCommit(conn, rbuffer);
			CommTimes[index][10]++;
   	    	break;
   	    case cmd_abort:
   	    	serviceLocalAbort(conn, rbuffer);
			CommTimes[index][11]++;
   	    	break;
   	    case cmd_abortAheadWrite:
   	    	serviceAbortAheadWrite(conn, rbuffer);
			CommTimes[index][12]++;
   	    	break;
	    case cmd_release:
		    printf("enter release the connect %d\n", index);
		    break;
	    default:
		    printf("error route, never here cmd=%d, index=%d!\n", type, index);
		    break;
   	    }
    }while (type != cmd_release);

    //printf("respond end %d\n", index);

   	close(conn);
   	pthread_exit(NULL);
   	return (void*)NULL;
}

void InitNetworkParam(void)
{
   char buffer[5];

   ReadConfig("paramport", buffer);
   param_port = atoi(buffer);

   ReadConfig("masterip", master_ip);

   ReadConfig("localip", local_ip);
   
   fclose(conf_fp);
}

void GetParam(void)
{
   int i;
   int param_send_buffer[1];
   int param_recv_buffer[10+NODENUMMAX];

   // register local IP to master node && get by other nodes in the system
   in_addr_t help = inet_addr(local_ip);
   if (help == INADDR_NONE)
   {
       printf("inet addr error\n");
       exit(-1);
   }
   param_send_buffer[0] = (uint32_t)help;

   if (send(param_socket, param_send_buffer, sizeof(param_send_buffer), 0) == -1)
	   printf("get param send error\n");
   if (recv(param_socket, param_recv_buffer, sizeof(param_recv_buffer), 0) == -1)
	   printf("get param recv error\n");

   nodenum = param_recv_buffer[0];
   threadnum = param_recv_buffer[1];
   port_base = param_recv_buffer[2];
   message_port = param_recv_buffer[3];
   master_port = param_recv_buffer[4];
   record_port = param_recv_buffer[5];
   nodeid = param_recv_buffer[6];

   oneNodeWeight = param_recv_buffer[7];
   twoNodeWeight = param_recv_buffer[8];
   redo_limit = param_recv_buffer[9];

   // hotspot control
   HOTSPOT_PERCENTAGE = param_recv_buffer[10];;
   HOTSPOT_FIXED_SIZE = param_recv_buffer[11];;

   // duration control
   extension_limit = param_recv_buffer[12];
       
   // random read control
   random_read_limit = param_recv_buffer[13];


   for (i = 0; i < nodenum; i++)
   {
      struct in_addr help;
      help.s_addr = param_recv_buffer[14+i];
      char * result = inet_ntoa(help);
      int k;
      for (k = 0; result[k] != '\0'; k++)
      {
    	  node_ip[i][k] = result[k];
      }
      node_ip[i][k] = '\0';
   }
}

void WaitDataReady(void)
{
	int wait_buffer[1];
	wait_buffer[0] = 999;

	if (send(message_socket, wait_buffer, sizeof(wait_buffer), 0) == -1)
		printf("wait data send error\n");
	if (recv(message_socket, wait_buffer, sizeof(wait_buffer), 0) == -1)
		printf("wait data recv error\n");
}
