#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

#define OKORRETURN(s) { if(s != OK) return s; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/*
This method, basing on the clock algorithm, attempts to allocate a free(unvalid or evictable) frame in the buffer pool.

frame: a reference for a frame index which will be set with the index of a available frame if there are any;
Status: return "OK" if successful, otherwise return error prompts.

This method can not set the frame description in advance. The caller following may get failed even there is a free frame in buffer pool. In that way, the found free frame
will be in idle forever.
*/
const Status BufMgr::allocBuf(int & frame) 
{
    Status status {OK};

    // advance clockHand to find a evictable frame, you need to traverse the bufTable up to two laps
    for(int i = 0; i < 2*numBufs; advanceClock(), i++) {
        BufDesc* curFrame = bufTable + clockHand;
        // check whether current frame is valid, if no, just take it
        if(curFrame->valid == false)  {
            frame = clockHand;
            return status;
        }

        // check whether current frame is pinned by any upper layer application
        if(curFrame->pinCnt != 0) continue;

        // if this page is unpinned, check its refbit
        if(curFrame->refbit) {
            curFrame->refbit = false;
            continue;
        }

        // finally get a avaiable frame and give its index back
        frame = clockHand;

        // if dirty, flush this page into disk(unix file)
        if(curFrame->dirty == true)
            status = curFrame->file->writePage(curFrame->pageNo, bufPool + curFrame->frameNo);
        
        return status;
    }

    //if traverse twice and can't find one available, which means there is no evictable frame
    return status = BUFFEREXCEEDED;
}

/*
This method is to read a specific page of a certain file from buffer pool or disk.

file: a pointer to the objective file;
PageNo: page number in the target file;
page: a reference for a Page pointer which point to the address of target page if read successfully;
Status: return "OK" if successful otherwise return error prompts.

When the target page is in the buffer pool, we need to renew both pinCnt and refbit.
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    Status status {OK};
    int frameNo;
    int freeFrameNo;

    // check if this page is in the buffer pool
    status = hashTable->lookup(file, PageNo, frameNo);
    if(status == OK) {
        page = bufPool + frameNo;
        bufTable[frameNo].pinCnt++;
        bufTable[frameNo].refbit = true;
        return status;
    }

    // if target page is crrently not in the buffer pool 
    // alloc a free frame in the buffer pool
    status = allocBuf(freeFrameNo);
    OKORRETURN(status);


    // read the page from the disk(unix file)
    status = file->readPage(PageNo, bufPool + freeFrameNo);
    OKORRETURN(status);

    // remove the page in hashTable if it used to storage a page
    if(bufTable[freeFrameNo].valid == true) {
        status = hashTable->remove(bufTable[freeFrameNo].file, bufTable[freeFrameNo].pageNo);
        OKORRETURN(status);
    }

    // set the corresponding Buf Description in bufTable
    bufTable[freeFrameNo].Set(file, PageNo);

    // insert the new page into hashTable
    status = hashTable->insert(file, PageNo, freeFrameNo);
    OKORRETURN(status);

    // assign the address of target page in the buffer pool and give back to upper layer caller
    page = bufPool + freeFrameNo;

    return status;
}

/*
This method is used to unpin a page in the buffer pool when a caller want to declaim that it no longer use this page.

file: a pointer to the objective file;
PageNo: page number in the target file;
dirty: a flag to show if this page has been modified by this caller;
Status: return "OK" if successful otherwise return error prompts.

Test wether this page is in the buffer pool prior to other setps.
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    Status status {OK};
    int frameNo;

    // check if this page is in the buffer pool
    status = hashTable->lookup(file, PageNo, frameNo);
    OKORRETURN(status);

    // if this page is dirty?
    if(bufTable[frameNo].dirty == 0)
        bufTable[frameNo].dirty = dirty;

    // decrease the pinCnt of this page in the buffer pool
    if(bufTable[frameNo].pinCnt == 0)
        return status = PAGENOTPINNED;
    else
        bufTable[frameNo].pinCnt--;

    return status;
}

/*
This method is to allocate a new page in the disk for a file and allocate a frame in the buffer pool for this page.

file: a pointer to the objective file;
PageNo: page number for this new page in the target file;
page: a reference for a Page pointer which point to the address of target page if read successfully;
Status: return "OK" if successful otherwise return error prompts.
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status {OK};

    // allocate a new page in the disk(unix file) and obtain the page number
    status = file->allocatePage(pageNo);
    if(status != OK)    return status;

    // allocate a frame for this new page
    int freeFrameNo {-1};
    status = allocBuf(freeFrameNo);
    if(status != OK)    return status;

    // read this page from disk(unix file) and update the page given back
    status = file->readPage(pageNo, bufPool + freeFrameNo);
    if(status != OK)    return status;
    page = bufPool + freeFrameNo;

    // if this frame is a valid frame(used to keep a page), then remove this page from the hashtable
    if(bufTable[freeFrameNo].valid == true) {
        status = hashTable->remove(bufTable[freeFrameNo].file, bufTable[freeFrameNo].pageNo);
        if(status != OK)    return status;
    }

    // update the description of this allocated frame
    bufTable[freeFrameNo].Set(file, pageNo);

    // insert a new entry into hashtable for this new page
    status = hashTable->insert(file, pageNo, freeFrameNo);

    return status;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    cout << endl << "clockHand: " << clockHand << "\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt << "\tdirty: " << tmpbuf->dirty;
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}




