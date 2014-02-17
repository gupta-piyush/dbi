#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <iostream>
#include <stdlib.h>

DBFile::DBFile () {
	pageBuffer = new Page();
	pageOffset = 0;
	filePtr = new File();
	if (filePtr == NULL || pageBuffer == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}
}

DBFile::~DBFile () {
	delete filePtr;
	if(pageBuffer != NULL)
		delete pageBuffer;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {

	if(f_type == heap) {
		//create the file using file class
		// '0' in first argument means open
		filePtr->Open(0,f_path);
		/* Heap specific code here */
		return 1;
	}
	else if(f_type == sorted) {
		/* sorted specific code here */
	}
	else if(f_type == tree) {
		/* tree specific code here */
	}
	else {
		cout << "Don't know the file type. EXIT !!!\n";
		return 0;
	}
	return 1;
}

//Function assumes file already exist
int DBFile::Open (char *f_path) {
	//Any value except 0 will just open the file
	filePtr->Open(1,f_path);
	return 1;
}

int DBFile::Close () {
	filePtr->Close();
	return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	FILE * pFile;
	pFile = fopen (loadpath,"r");

	int pageOffset = 0;

	Record *recordPtr = new Record();
	Page *pagePtr = new Page();

	while(recordPtr->SuckNextRecord(&f_schema,pFile)) {
		if(!pagePtr->Append(recordPtr)) {
			//Came here because can't append more.
			//Have to add the current records to file now
#ifdef verbose
			cout << "adding page = " << pageOffset << endl;
#endif
			filePtr->AddPage(pagePtr,pageOffset++);
			//Page is written. don't need content it anymore
			pagePtr->EmptyItOut();
			pagePtr->Append(recordPtr);
		}
	}
#ifdef verbose
	cout << "adding page last = " << pageOffset << endl;
#endif
	filePtr->AddPage(pagePtr,pageOffset);

	delete recordPtr;
	delete pagePtr;
}


void DBFile::MoveFirst () {
	filePtr->GetPage(pageBuffer,pageOffset);
#ifdef verbose
	cout << "file length = " << filePtr->GetLength() << endl << flush;
#endif
}

void DBFile::Add (Record &rec) {
	// Get the length the file
	// second argument i.e. whichPage is gonna be incremented by 1 in first stmt
	// and offset should less than length. Hence '-2'
	filePtr->GetPage(pageBuffer,filePtr->GetLength()-2);
	if(!pageBuffer->Append(&rec)) {
		//Append didn't succeed. pageBuffer is full. need to empty and create new page to add
		pageBuffer->EmptyItOut();
		pageBuffer->Append(&rec);
		//Incrementing page - hence '-1' now
		filePtr->AddPage(pageBuffer,filePtr->GetLength()-1);
	}
	filePtr->AddPage(pageBuffer,filePtr->GetLength()-2);
}

int DBFile::GetNext (Record &fetchme) {
	if(pageBuffer == NULL)
		return 0;
	if(pageBuffer->GetFirst(&fetchme))
		return 1;

	// Case where pageBuffer is empty so need to do offset++
	// Check if length is in range of total number of pages.
	if(pageOffset+1 < (filePtr->GetLength()-1))
		pageOffset++;
	else
		return 0;

#ifdef verbose
	cout << "reading page offset = " << pageOffset << endl;
#endif
	//Get the new page and return first record
	filePtr->GetPage(pageBuffer,pageOffset);
	return pageBuffer->GetFirst(&fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine compEngine;

	while( GetNext(fetchme) ){
		if(compEngine.Compare(&fetchme,&literal,&cnf)) {
			return 1;
		}
	}
	return 0;
}
