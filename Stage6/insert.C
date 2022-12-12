#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	// make sure attrCount corresponds to # of attr in relation
	RelDesc currRelation;
	Status status = relCat->getInfo(relation, currRelation);
	if (status != OK) {
		return status;
	}
	if (currRelation.attrCnt != attrCnt) {
		return OK;
	}

	// Ensure no nulls are present before we try to insert, and figure out the total size of the record to insert
	int recSize = 0;
	for (int i = 0; i < attrCnt; i++) {
		if (attrList[i].attrValue == NULL) {
			return ATTRNOTFOUND;
		}
		recSize += attrList[i].attrLen;
	}
	
	// create Record to hold all the data in the proper order
	Record recToInsert;
	recToInsert.length = recSize;
	void *recData = malloc(recSize);
	recToInsert.data = recData;
	
	// fetch the data and put it in recData
	for (int i = 0; i < attrCnt; i++) {
		AttrDesc currAttrDesc;
		status = attrCat->getInfo(relation, attrList[i].attrName, currAttrDesc);
		if (status != OK) {
			free(recData);
			return status;
		}
		// copy data into appropiate spot in recData
		memcpy(((char *)recData + currAttrDesc.attrOffset), attrList[i].attrValue, attrList[i].attrLen); // TODO is this the proper way to incorporate the offset for recData?
	}

	// insert into heapfile for the relation, which is done by creating a new InsertFileScan object
	// TODO unsure on if it's supposed to create a new object here... that's the only way I can think of to do it, b/c I can't find any already-created heapfile object for the relation
	InsertFileScan *ifs = new InsertFileScan(relation, status);
	RID recRID;
	status = ifs->insertRecord(recToInsert, recRID);
	if (status != OK) {
		free(recData);
		delete ifs;
		return status;
	}

	// deallocate mem
	free(recData);
	delete ifs;
	return OK;
}

