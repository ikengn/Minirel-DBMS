#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
// Harrison
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		status = db.createFile(fileName);
        if (status != OK)
            return status;
        
        // now that we have the file, open it TODO do we need to do this? I think so, bc otherwise we wouldn't be able to call allocPage?
        status = db.openFile(fileName, file);
        if (status != OK)
            return status;
        
        // allocate an empty header page
        Page* hdrPageUncasted;
        status = bufMgr->allocPage(file, hdrPageNo, hdrPageUncasted);
        if (status != OK)
            return status;
        hdrPage = (FileHdrPage *) hdrPageUncasted;

        // Initialize the header page values
        strcpy(hdrPage->fileName, fileName.c_str()); // TODO might be a better way to do this (I'm used to C)
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;
        hdrPage->firstPage = -1;
        hdrPage->lastPage = -1;
		
		// allocate the first data page of the heapfile
        status = bufMgr->allocPage(file, newPageNo, newPage);
		if (status != OK)
            return status;

		// initialize page contents
        newPage->init(newPageNo);
		
		// update relevant header page data
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt++;
		
		// unpin pages, mark as dirty
        bufMgr->unPinPage(file, hdrPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);
		
		// close file
		status = db.closeFile(file);
        if (status != OK)
            return status;
        
        return OK;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;
    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // initialize header page number 
		status = filePtr->getFirstPage(headerPageNo);
        if ( status != OK){
            returnStatus = status;
            return;
        }
        
        // initialize header page based on the header page number.
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if ( status != OK){
            returnStatus = status;
            return;
        }
        headerPage = (FileHdrPage*) pagePtr;
        
        // this content is initialized in the earlier createFile call
        cout << "##############################" <<endl;
        cout << headerPage->fileName << endl;
        cout << headerPage->pageCnt << endl;
        cout << headerPage->recCnt << endl;
        cout << headerPage->firstPage << endl;
        cout << headerPage->lastPage << endl;
        cout << "##############################" <<endl;

        // set the dirty flag
        hdrDirtyFlag = false;
		
        // set current page number to first page number
        curPageNo = headerPage->firstPage;

        cout << curPageNo << endl;

        // read in the first page as current page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if ( status != OK){
            returnStatus = status;
            return;
        }

        // set the dirty flag
        curDirtyFlag = false;

        //set current record ID
        curRec = NULLRID;

        // set the return status to OK
        returnStatus = OK;
        cout << "constructor returned OK." << endl;
        cout << "headerPageNo: " << headerPageNo << endl;
        cout << "curPageNo: " << curPageNo << endl;
        cout << "##############################" <<endl;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    if (curPage == NULL) {
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK)
            return status;
        // set appropriate flags for reading in a new page
        curPageNo = rid.pageNo;
        curDirtyFlag = 0;
        curRec = rid;
    }
    if (rid.pageNo == curPageNo) { // this IF is taken if the first IF was taken
        status = curPage->getRecord(rid, rec);
        return status;
    }

    // curPage is not the page we want (but wasn't null)
    status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    if (status != OK)
        return status;
    
    status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
    if (status != OK)
        return status;
    curPageNo = rid.pageNo;
    curDirtyFlag = 0;
    curRec = rid;

    status = curPage->getRecord(rid, rec);
    return status;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;
    Status recStat;

    // checking if a marked scan exists
    if (markedPageNo == 0) {
        nextPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        curPageNo = nextPageNo;
        recStat = curPage->firstRecord(nextRid);
    } else {

        // else start from the beginning
        nextPageNo = markedPageNo;
        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        curPageNo = nextPageNo;
        recStat = curPage->nextRecord(markedRec, nextRid);
    }

    if (status != OK)
        return status;

    // looping through all the pages
    while (status == OK) {

        // looping through records within a pages
        while (recStat != NORECORDS && recStat != ENDOFPAGE) {
            
            tmpRid = nextRid;
            recStat = curPage->getRecord(tmpRid, rec);

            // comparing the record with the filter
            if (recStat == OK && matchRec(rec)) {
                outRid = tmpRid;
                curRec = tmpRid;
                markScan();
                return OK;
            }

            recStat = curPage->nextRecord(tmpRid, nextRid);
        }    

        // moving to the next page and getting the first record
        curPage->getNextPage(nextPageNo);    
        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        if (status == OK) {
            curPageNo = nextPageNo;
            recStat = curPage->firstRecord(nextRid);
        } else {
            return status;
        }
        
        if (status == FILEEOF) 
            return FILEEOF;
    }
	return FILEEOF;
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // Check if the current page is NULL
    if (curPage == NULL) {
        // Get the information from the last page, from headerPAge
        curPageNo = headerPage->lastPage;
        
        // Read this page into the buffer
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            return status;
        }
    }

    // Attempt to insert the record
    status = curPage->insertRecord(rec, outRid);

    // If the record is inserted, then do the BOOKKEEPING
    if (status == OK) {
        // Do BOOKKEEPING
        // New record
        headerPage->recCnt++;
        // Header updated (above)
        hdrDirtyFlag = true;
        // New rid
        curRec = outRid;
        // Page updated with new record?
        curDirtyFlag = true;
        return OK;
    }

    // If not inserted into current page, create a new page to insert into
    // Create pointer for the last page
    Page* lastPage = NULL;

    // Get pointer to last page
    filePtr->readPage(headerPage->lastPage, lastPage);

    // Create a new page
    bufMgr->allocPage(filePtr, newPageNo, newPage);
    // Initialize it
    newPage->init(newPageNo);

    // modify content of the header page
    headerPage->pageCnt++;

    lastPage->setNextPage(newPageNo);
    headerPage->lastPage = newPageNo;

    // Update the current page information
    curPage = newPage;
    curPageNo = newPageNo;

    // Try to insert the new record once more
    status = curPage->insertRecord(rec, outRid);

    // If successful do the bookkeeping
    if (status == OK) {
        // Do BOOKKEEPING
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curRec = outRid;
        curDirtyFlag = true;
    }

    // If not simply return the status
    return status;
        
}


