#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

//This structure holds all arguments of BigQ constructor
typedef struct sortingArgs {
	Pipe* inPipe;
	Pipe* outPipe;
	OrderMaker* sortOrder;
	int runLen;
}sSortingArgs;

class BigQ {

private:
	//Need to create a structure to pass to worker thread
	sSortingArgs args;
	//Worker thread
	pthread_t workerThread;

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	//void* sortInput (void* args);
	bool sortComparator(Record& left, Record& right);
};

#endif
