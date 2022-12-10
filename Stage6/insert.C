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
	// Ensure no nulls are present before we try to insert
	for (int i = 0; i < attrCnt; i++) {
		if (attrList[i].attrValue == NULL) {
			return ATTRNOTFOUND;
		}
	}

	// 
	return OK;
}

