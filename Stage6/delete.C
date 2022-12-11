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

	Status status;
	AttrDesc currAttr;

	// Check if the input parameter is NULL
	if (attrName[0] == '\0') {

		return BADCATPARM;

	}

	// Look up attribute info
	status =  attrCat->getInfo(relation, attrName, currAttr);
	if (status != OK) { return status; }


	// Create HeapFileScan with the given information
	HeapFileScan deleteScan(relation, status);
	if (status != OK) {return status;}

	// TODO: Figure out how to cast filter to a certain type
	status = deleteScan.startScan(currAttr.attrOffset, 
				currAttr.attrLen, type, attrValue, op);

	// Get correct attribute type and start scan with correct type
	if (type == INTEGER) {

		int currValue;
		currValue = atoi(attrValue);
		

	} else if (type == FLOAT) {

		float currValue;
		currValue = atof(attrValue);
		

	}

	RID scanRID;

	// For each value that satisfies the scan, remove from relation
	while (deleteScan.scanNext(scanRID) == OK) {

		// Delete record
		// Is this what you need to do/all you need to do???
		deleteScan.deleteRecord();

	}


// part 6
return OK;



}


