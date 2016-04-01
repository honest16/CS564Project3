/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright(c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 
/*
 * Constructs a buffer of size bufs. 
 * Initializes metadata information in bufDescTable.
 * Creates the buffer hash table.
 */
BufMgr::BufMgr(std::uint32_t bufs) 
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for(FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/*
 * Destructor for the BufMgr class.
 * Flushes out all valid dirty pages before deleting buffer pool 
 */	
BufMgr::~BufMgr() {
  
  //Flushing out all valid, dirty pages
  for(std::uint32_t i = 0; i < numBufs; i++) { 
  	if(bufDescTable[i].dirty && bufDescTable[i].valid) {
		bufDescTable[i].file->writePage(bufPool[i]);
		bufDescTable[i].dirty = false;
  	}
  }

  //Deallocating the buffer pool
  delete [] bufPool;
 
  //Deallocating the BufDesc table
  delete [] bufDescTable;
}

/*
 * Advances the clockhand one space in the buffer pool ahead (with wrap-around).
 * Used in the allocBuf to find a valid frame.
 */
void BufMgr::advanceClock()
{
	// clockHand is a pointer into the bufPool and bufDescTable and both of these should be changed
	clockHand = (clockHand + 1) % numBufs;
}

/*
 * Finds a free frame in the buffer pool using the clock algorithm.
 * Returns the result by reference in frame variable
 */
void BufMgr::allocBuf(FrameId& frame) 
{
	//implement the clock algorithm here
	
	//walk through the bufDescTable starting from where ever the current clockHand is
	std::uint32_t numPinned = 0;
	advanceClock();
	
	//keep going until we find an invalid page
	while(bufDescTable[clockHand].valid) {
		
		//reset the refbit if necessary then move onto the next frame
		if(bufDescTable[clockHand].refbit) {
			bufDescTable[clockHand].refbit = false;
			advanceClock();
			continue;
		}

		//cant use this page because it is pinned. If every page is pinned, throw an exception
		if(bufDescTable[clockHand].pinCnt > 0) {
			if(++numPinned == numBufs) throw BufferExceededException();
			advanceClock();
			continue;
		}

		//if we get to this point we know the page is valid and not pinned
		//so write it if it is dirty then use the page (break from the loop as we are done searching)
		if(bufDescTable[clockHand].dirty) {
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;
		}
		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
		break;
	}
	//once we break from the loop we know we found a free page where the clockHand is so use it!
	//Set is called in readPage() and allocPage() when we have the file and pageNo
	bufDescTable[clockHand].Clear();
	
	frame = clockHand;
}

/*
 * Get page pageNo from file and return the result in page variable by reference
 */	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try {
		//is the page in the buffer pool? If not this function will throw a HashNotFoundException
		hashTable->lookup(file, pageNo, frameNo);

		//if that line doesn't throw an exception then the page was already in the buffer
		page = &bufPool[frameNo];

		//this page was just referenced and someone is using it so increase the count
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
	} catch(HashNotFoundException& e) {
		try {
			allocBuf(frameNo);
			Page tempPage = file->readPage(pageNo);
			bufPool[frameNo] = tempPage;
			
			hashTable->insert(file, pageNo, frameNo);
			bufDescTable[frameNo].Set(file, pageNo);

			//tempPage will lose scope and we need to update page to point to the newly allocated page
			page = &bufPool[frameNo];
		} catch(BufferExceededException& e) {
			//what to do here!? PANIC!!
		}
	}
}

/*
 * Decrease the pin count for the specified page in the specified file.
 * If the page is dirty then we need to write that page to disk.
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try {
		//try to lookup the page in the hashtable
		hashTable->lookup(file, pageNo, frameNo);
		
		//cant unpin a page that isnt pinned
		if(bufDescTable[frameNo].pinCnt == 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}
		else {
			//else decrease the count and update the dirty bit if that page was dirty
			bufDescTable[frameNo].pinCnt--;
			if(dirty) {
				bufDescTable[frameNo].dirty = true;
			}
		}
	//if the page isnt there we dont need to worry about unpinning it
	} catch(HashNotFoundException& e) {
		//what to do here!? PANIC!!
	}
}

/*
 * Flush out all pages belonging to specified file.
 */
void BufMgr::flushFile(const File* file) 
{
	for(FrameId i = 0; i < numBufs; i++) {
		//only looking for pages that belong to the file
		if(bufDescTable[i].file == file) {
			//throw exceptions
			
			//we dont want to write invalid data
			if(!bufDescTable[i].valid) {
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
				continue;
			}
			//and we dont want to write pages that still pinned
			if(bufDescTable[i].pinCnt > 0) {
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
				continue;
			}
			
			//write if dirty
			if(bufDescTable[i].dirty) {
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			
			//remove the page from the hashtable
			hashTable->remove(file, bufDescTable[i].pageNo);
			
			//clear the metedata 
			bufDescTable[i].Clear();
		}	
	}
}

/*
 * Looks for a frame in which to allocate a page for the specified file. 
 * Returns by reference the page number and pointer to the actual page in the buffer pool
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frameNo;

	//allocate a new page for the file and set the pageNo
	Page newPage = file->allocatePage();
	pageNo = newPage.page_number();

	//put it in the buffer and have it set the frameNo
	allocBuf(frameNo);

	//insert this new page into the hashTable and the buffer pool at whatever frame it gave us
	hashTable->insert(file, pageNo, frameNo);
	bufPool[frameNo] = newPage;

	//update the metadata for the frame that now contains a newly allocated page
	bufDescTable[frameNo].Set(file, pageNo);

	//return the page to the caller
	page = &bufPool[frameNo]; 
}

/*
 * Gets rid of the specified page according to the page number from the specified file
 */
void BufMgr::disposePage(File* file, const PageId pageNo)
{
	FrameId frameNo;
	
	//delete the page from the file
	file->deletePage(pageNo);
	
	try {
		//remove the page from the hashTable if it exists
		hashTable->lookup(file, pageNo, frameNo);
		hashTable->remove(file, pageNo);

		//update the metadata
		bufDescTable[frameNo].Clear();
	} catch(HashNotFoundException& e) {
		//what to do here!? PANIC!!
		
	} 
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for(std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if(tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
