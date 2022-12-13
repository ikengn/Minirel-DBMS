#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	cout << "Doing QU_Delete" << endl;

	Status status;
	AttrDesc currAttr;

	// Create HeapFileScan with the given information
	HeapFileScan deleteScan(relation, status);
	if (status != OK) {return status;}
	
	// Check if the input parameter is NULL
	if (attrName=="") {

		status = deleteScan.startScan(0, 0, STRING, NULL, op);
		if (status != OK) { return status; }
	
	}else{
		// Look up attribute info
		status =  attrCat->getInfo(relation, attrName, currAttr);
		if (status != OK) { return status; }
		

		// Get correct attribute type and start scan with correct type
		if (type == INTEGER) {

			int currValue;
			char* currFilter;
			currValue = atoi(attrValue);
			currFilter = (char*) &currValue;

			status = deleteScan.startScan(currAttr.attrOffset, 
					currAttr.attrLen, type, currFilter, op);

			

		} else if (type == FLOAT) {

			float currValue;
			char* currFilter;
			currValue = atof(attrValue);
			currFilter = (char*) &currValue;

			status = deleteScan.startScan(currAttr.attrOffset, 
					currAttr.attrLen, type, currFilter, op);
			

		} else {

			status = deleteScan.startScan(currAttr.attrOffset, 
					currAttr.attrLen, type, attrValue, op);

		}
	}

	RID scanRID;

	// For each value that satisfies the scan, remove from relation
	while (deleteScan.scanNext(scanRID) == OK) {

		// Delete record
		status = deleteScan.deleteRecord();
		if (status != OK) { return status;}

	}


// part 6
return OK;



}


