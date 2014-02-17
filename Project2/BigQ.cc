#include "BigQ.h"

using namespace std;

void *sortInput (void* ptr) {
	sSortingArgs* args = (sSortingArgs*) ptr;

	int pageIndex = 0,runNum = 0;

	// read data from in pipe sort them into runlen pages
	Record in;
	bool isRemaining = true;

	Page *pageArr;
	pageArr = new Page[args->runLen];

	while(args->inPipe->Remove(&in)){

		//if appended = 0 that means page is full
		if(!pageArr[pageIndex].Append(&in)) {
			//now that page if full we need to create a new page
			//Also we need to check that page size is not equal to runLen
			if(pageIndex + 1 != args->runLen) {
#ifdef verbose
				cout << "pushing " << p->numRecs <<" record in page " << pageIndex << endl;
#endif
				pageIndex++;
#ifdef verbose
				cout << "creating new page = " << pageIndex <<endl;
#endif
				pageArr[pageIndex].Append(&in);
				isRemaining = true;
			}
			else {
				isRemaining = false;
				runNum++;
				cout << "ELSE run num = " << runNum << endl;

				Record temp;

				for(int j= 0;j <= pageIndex; j++) {
					while(pageArr[j].GetFirst(&temp)) {
						args->outPipe->Insert(&temp);
					}
				}
				delete[] pageArr;
				pageArr = new Page[args->runLen];
				pageIndex = 0;
				pageArr[pageIndex].Append(&in);
				isRemaining = true;
			}
		}
	}

	if(isRemaining) {
		isRemaining = false;
		runNum++;
#ifdef verbose
		cout << "run num = " << runNum << endl;
#endif

		Record temp;

		for(int j= 0;j <= pageIndex; j++) {
			while(pageArr[j].GetFirst(&temp)) {
				args->outPipe->Insert(&temp);
			}
		}
	}
	// construct priority queue over sorted runs and dump sorted data
	// into the out pipe

	// finally shut down the out pipe
	args->outPipe->ShutDown();
	return 0;
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	//Copy constructor args in sSortingArgs structure
	args.inPipe = &in;
	args.outPipe = &out;
	args.sortOrder = &sortorder;
	args.runLen = runlen;

	//spawn a worker thread and return from the constructor
	pthread_create( &workerThread, NULL, sortInput, (void*)&args);

}

BigQ::~BigQ () {
}



bool BigQ::sortComparator(Record& left, Record& right) {
	ComparisonEngine compEngine;
	compEngine.Compare(&left, &right, args.sortOrder);

	return false;
}
