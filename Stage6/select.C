#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result,
               const int projCnt,
               const attrInfo projNames[],
               const attrInfo *attr,
               const Operator op,
               const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	int reclen = 0;
	AttrDesc names[projCnt];
	Status status = OK;
	const char* filter;

    for (int i = 0; i< projCnt; i++){
		// get the AttrDesc value according to attrInfo
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, names[i]);
		if (status != OK){
			return status;
		}

		// increment the total record length by the length of this attribute
		reclen += names[i].attrLen;

		//debug output
        //cout<< names[i].relName << " " << names[i].attrName<<" "<< names[i].attrType << " " << names[i].attrLen<< " "<< names[i].attrOffset<<endl;
    }

    AttrDesc* attrDesc = new AttrDesc();
    if (attr == NULL){
        attrDesc = NULL;
		
		// call the ScanSelect method to do the actual work
		status = ScanSelect(result, projCnt, names, attrDesc, op, filter,  reclen);
		if (status != OK){
			return status;
		}
    }else{

		// // Get correct attribute type
		int intVal;
        float floatVal;
        switch (attr->attrType) {
            case INTEGER:
                intVal = atoi(attrValue);
                filter = (char*)&intVal;
                break;
            case FLOAT:
                floatVal = atof(attrValue);
                filter = (char*)&floatVal;
                break;
            case STRING:
                filter = attrValue;
                break;
		}

		// get the AttrDesc value according to attrInfo
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
		if (status != OK){
			return status;
		}

		//debug output
		//cout<< attrDesc->relName << " " << attrDesc->attrName<<" "<< attrDesc->attrType << " " << attrDesc->attrLen<< " "<< attrDesc->attrOffset<<endl;
		
		// call the ScanSelect method to do the actual work
		status = ScanSelect(result, projCnt, names, attrDesc, op, filter,  reclen);
		if (status != OK){
			return status;
		}
	}
    return OK;
}

const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	
	// Temporarary record for output table
	RID rid;
	Record rec;
	Status status;

	// open current table (to be scanned) as a HeapFileScan object
	string rel_name = projNames[0].relName;
	HeapFileScan *heap_scan = NULL;

	heap_scan = new HeapFileScan(rel_name, status); 
	// check if an unconditional scan is required
	if (attrDesc != NULL){
		if (attrDesc->attrType == INTEGER) {
			heap_scan->startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, filter, op);
		}
		else if (attrDesc->attrType == FLOAT) {
			heap_scan->startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, filter, op);
		}
		else {
			heap_scan->startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
		}
	}

	if (status != OK) {
		return status;
	}

	// open "result" as an InsertFileScan object
	InsertFileScan out_table(result, status);
	if (status != OK) {
		return status;
	}

	// scan the current table
	while (heap_scan->scanNext(rid) == OK) {

		heap_scan->HeapFile::getRecord(rid, rec);
		char *data = new char[reclen];
		Record temp_rec = {data, reclen};
		int off_set = 0;

		// if find a record, then copy stuff over to the temporary record
		for (int i = 0; i < projCnt; i++) {
			char *n_place = data + off_set;
			char *place = ((char*)rec.data) + projNames[i].attrOffset;
			int len = projNames[i].attrLen;
			memcpy(n_place, place, len);
			off_set += len;
		}

		// insert into the output table
		RID temp_rid;
		status = out_table.insertRecord(temp_rec, temp_rid);
		if (status != OK) {
			return status;
		}
	}

	return status;

}
