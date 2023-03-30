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

#define DEBUGBUF true;

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs) {
	numBufs = bufs;

	bufTable = new BufDesc[bufs];
	memset(bufTable, 0, bufs * sizeof(BufDesc));
	for (int i = 0; i < bufs; i++) {
		bufTable[i].frameNo = i;
		bufTable[i].valid = false;
	}

	bufPool = new Page[bufs];
	memset(bufPool, 0, bufs * sizeof(Page));

	int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
	hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

	clockHand = bufs - 1;
}

BufMgr::~BufMgr() {

	// flush out all unwritten pages
	for (int i = 0; i < numBufs; i++) {
		BufDesc *tmpbuf = &bufTable[i];
		if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
			cout << "flushing page " << tmpbuf->pageNo << " from frame " << i
					<< endl;
#endif

			tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
		}
	}

	delete[] bufTable;
	delete[] bufPool;
}

/**
 * Method 1.
 *
 */
const Status BufMgr::allocBuf(int &frame) {
	int count = 0;

	while (true) {
		advanceClock();

		if (bufTable[clockHand].valid == true) {
			if (count == numBufs) {
				return BUFFEREXCEEDED;
			}

			if (bufTable[clockHand].pinCnt > 0) {
				count++;
			}

			if (bufTable[clockHand].refbit == true) {
				bufTable[clockHand].refbit = false;
				continue;

			} else if (bufTable[clockHand].refbit == false
					&& bufTable[clockHand].pinCnt > 0) {
				continue;

			} else if (bufTable[clockHand].refbit == false
					&& bufTable[clockHand].pinCnt <= 0
					&& bufTable[clockHand].dirty == true) {
				flushFile(bufTable[clockHand].file);
				bufTable[clockHand].dirty = false;
			}

			if (bufTable[clockHand].valid == true) {
				hashTable->remove(bufTable[clockHand].file,
						bufTable[clockHand].pageNo);
			}
		}

		bufTable[clockHand].Clear();
		frame = clockHand;
		return OK;
	}
}

/**
 * Method 2.
 *
 */
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page) {
	int frameNo;
	int newFrameNo;
	Status status = hashTable->lookup(file, PageNo, frameNo);

	if (status != HASHNOTFOUND && status != OK) {
		return status;
	}

	if (status == HASHNOTFOUND) {
		status = allocBuf(newFrameNo);

		if (status != OK) {
			return status;
		}

		status = hashTable->insert(file, PageNo, newFrameNo);
		if (status != OK) {
			return status;
		}

		bufTable[newFrameNo].Set(file, PageNo);
		bufTable[newFrameNo].frameNo = newFrameNo;
		status = file->readPage(PageNo, &bufPool[newFrameNo]);
		if (status != OK) {
			disposePage(file, PageNo);
			return status;
		}
		page = &bufPool[newFrameNo];
	} else {
		bufTable[frameNo].refbit = true;
		bufTable[frameNo].pinCnt++;
		page = &bufPool[frameNo];
	}

	return OK;
}

/**
 * Method 3.
 *
 */
const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty) {
	int frameNo;
	Status status = hashTable->lookup(file, PageNo, frameNo);

	if (status != OK) {
		return status;
	}

	if (bufTable[frameNo].pinCnt == 0) {
		status = PAGENOTPINNED;
		return status;
	}

	bufTable[frameNo].pinCnt--;
	if (dirty == true) {
		bufTable[frameNo].dirty = true;
	}
	return OK;

}

/**
 * Method 4.
 *
 */
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page) {
	int frameNo;
	Status status = file->allocatePage(pageNo);

	if (status != OK) {
		return status;
	}

	status = allocBuf(frameNo);
	if (status != OK) {
		return status;
	}

	status = hashTable->insert(file, pageNo, frameNo);
	if (status != OK) {
		return status;
	}

	bufTable[frameNo].Set(file, pageNo);
	bufTable[frameNo].frameNo = frameNo;
	page = &bufPool[frameNo];

	return OK;

}

const Status BufMgr::disposePage(File *file, const int pageNo) {
	// see if it is in the buffer pool
	Status status = OK;
	int frameNo = 0;
	status = hashTable->lookup(file, pageNo, frameNo);
	if (status == OK) {
		// clear the page
		bufTable[frameNo].Clear();
	}
	status = hashTable->remove(file, pageNo);

	// deallocate it in the file
	return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file) {
	Status status;

	for (int i = 0; i < numBufs; i++) {
		BufDesc *tmpbuf = &(bufTable[i]);
		if (tmpbuf->valid == true && tmpbuf->file == file) {

			if (tmpbuf->pinCnt > 0)
				return PAGEPINNED;

			if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
				cout << "flushing page " << tmpbuf->pageNo << " from frame "
						<< i << endl;
#endif
				if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
						&(bufPool[i]))) != OK)
					return status;

				tmpbuf->dirty = false;
			}

			hashTable->remove(file, tmpbuf->pageNo);

			tmpbuf->file = NULL;
			tmpbuf->pageNo = -1;
			tmpbuf->valid = false;
		}

		else if (tmpbuf->valid == false && tmpbuf->file == file)
			return BADBUFFER;
	}

	return OK;
}

void BufMgr::printSelf(void) {
	BufDesc *tmpbuf;

	cout << endl << "Print buffer...\n";
	for (int i = 0; i < numBufs; i++) {
		tmpbuf = &(bufTable[i]);
		cout << i << "\t" << (char*) (&bufPool[i]) << "\tpinCnt: "
				<< tmpbuf->pinCnt;

		if (tmpbuf->valid == true)
			cout << "\tvalid\n";
		cout << endl;
	};
}

