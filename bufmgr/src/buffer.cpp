/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
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

BufMgr::BufMgr(std::uint32_t bufs) 
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  
  //Flushing out all dirty pages
  for (FrameId i = 0; i < numBufs; i++) 
  { 
  	if (bufDescTable[i].dirty) {
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;
  	}
  }

  //Deallocating the buffer pool
  delete[] bufPool;
 
  //Deallocating the BufDesc table
  delete[] bufDescTable;
  
}

void BufMgr::advanceClock()
{
	// clockHand is a pointer into the bufPool and bufDescTable and both of these should be changed
	
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId& frame) 
{
	//implement the clock algorithm here
	
	//walk through the bufDescTable starting from where ever the current clockHand is

	//*
	std::uint32_t numPinned = 0;

	//keep going until we find an invalid page
	while (bufDescTable[clockHand].valid) {
		
		//reset the refbit if necessary then move onto the next frame
		if (bufDescTable[clockHand].refbit) {
			bufDescTable[clockHand].refbit = false;
			advanceClock();
			continue;
		}

		//cant use this page because it is pinned. If every page is pinned, throw an exception
		if (bufDescTable[clockHand].pinCnt > 0) {
			advanceClock();
			if (++numPinned == numBufs) throw BufferExceededException();
			continue;
		}

		//if we get to this point we know the page is valid and not pinned
		//so write it if it is dirty then use the page (break from the loop as we are done searching)
		if (bufDescTable[clockHand].dirty) {
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;
		}
		break;
	}
	//once we break from the loop we know we found a free page where the clockHand is so use it!
	//call set() on frame here or in readPage()/allocPage() after this is called?
	frame = clockHand;
	//*/
}

	
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
	} catch (const HashNotFoundException& e) {
		try {
			allocBuf(frameNo);
			Page tempPage = file->readPage(pageNo);
			bufPool[frameNo] = tempPage;
			
			hashTable->insert(file, pageNo, frameNo);
			bufDescTable[frameNo].Set(file, pageNo);

			//tempPage will lose scope and we need to update page to point to the newly allocated page
			page = &tempPage;
		} catch(BufferExceededException& e) {
			//what to do here!? PANIC!!
		}
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try {
		hashTable->lookup(file, pageNo, frameNo);
		if (bufDescTable[frameNo].pinCnt == 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}
		else {
			bufDescTable[frameNo].pinCnt--;
			if (dirty) {
				bufDescTable[frameNo].dirty = true;
			}
		}
	} catch (HashNotFoundException& e) {
		//what to do here!? PANIC!!
	}
}

void BufMgr::flushFile(const File* file) 
{
 //Scanning bufTable for pages belonging to the file 
 for (FrameId i = 0; i < numBufs; i++) 
  { 
    //For each page encountered:
  	if (bufDescTable[i].file == file) {
			
			try{
				try {
					// If the page is dirty, 
					if(bufDescTable[i].dirty){
						//call file->writePage() to flush the page to disk and then set the dirty bit for the page to false
						bufDescTable[i].file->writePage(bufPool[i]);
						bufDescTable[i].dirty = false;
					}
					// Remove the page from the hashtable (whether the page is clean or dirty)
					hashTable->remove(file, bufDescTable[i].pageNo);

					//update the metadata
					bufDescTable[i].valid = false;
		  
					// Invoke the Clear() method of BufDesc for the page frame
					bufDescTable[i].Clear();
				} catch (PagePinnedException e) {
					//what to do here!? PANIC!!
				}
			} catch (BadBufferException e1) {
				//what to do here!? PANIC!!
			}
			
  	}
  }
 
 

 //Throws BadBuffer-Exception if an invalid page belonging to the file is encountered.
}

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

void BufMgr::disposePage(File* file, const PageId pageNo)
{
	FrameId frameNo;
	try {
		//remove the page from the hashTable if it exists
		hashTable->lookup(file, pageNo, frameNo);
		hashTable->remove(file, pageNo);

		//update the metadata
		bufDescTable[frameNo].valid = false;
		bufDescTable[frameNo].Clear();

		//delete the page from the file
		file->deletePage(pageNo);
	} catch (HashNotFoundException& e) {
		//what to do here!? PANIC!!
	}
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
