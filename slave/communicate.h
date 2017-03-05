#ifndef COMMUNICATE_H_
#define COMMUNICATE_H_

#include "type.h"

extern int Send(int conn, uint64_t* buffer, int num);

extern int Receive(int conn, uint64_t* buffer, int num);

#endif
