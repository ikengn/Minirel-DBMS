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
	cout << "Doing QU_Insert" << endl;

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
		AttrDesc currAttrDesc;
		status = attrCat->getInfo(relation, attrList[i].attrName, currAttrDesc);
		if (status != OK) {
			return status;
		}
		recSize += currAttrDesc.attrLen;
	}
	
	// create Record to hold all the data in the proper order
	Record recToInsert;
	recToInsert.length = recSize;
	char *recData;
	recData = (char *)malloc(recSize);
	if (recData == NULL) {
		cout << "NULL returned by malloc" << endl;
		return OK;
	}
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
		if (currAttrDesc.attrType == INTEGER) {
			int temp;
			temp = atoi((char*)attrList[i].attrValue);
			memcpy(recData + currAttrDesc.attrOffset, &temp, currAttrDesc.attrLen);
		}
		else if (currAttrDesc.attrType == FLOAT) {
			float temp;
			temp = atof((char*)attrList[i].attrValue);
			memcpy(recData + currAttrDesc.attrOffset, &temp, currAttrDesc.attrLen);
		}
		else {
			memcpy(recData + currAttrDesc.attrOffset, attrList[i].attrValue, currAttrDesc.attrLen);
		}
	}

	// insert into heapfile for the relation, which is done by creating a new InsertFileScan object
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

