/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
		: bufMgr(bufMgrIn)
		, attributeType(attrType)
		, attrByteOffset(attrByteOffset)
		, leafOccupancy(INTARRAYLEAFSIZE)
		, nodeOccupancy(INTARRAYNONLEAFSIZE)
		, scanExecuting(false)
		, nextEntry(-1)
		, currentPageNum(Page::INVALID_NUMBER)
		, currentPageData(nullptr)
{
	// only supports badgerdb::Integer as attributeType!

	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str();  // index file name
	outIndexName = indexName;  // return the index file name via reference

	Page *headerPage;  // header page
	Page *rootPage;    // root page

	if (!File::exists(outIndexName))
	{
		// create a new index file if it doesn't exist
		file = new BlobFile(outIndexName, true);

		// allocate the header page and root page
		bufMgr->allocPage(file, headerPageNum, headerPage);
		bufMgr->allocPage(file, rootPageNum, rootPage);

		// set the index meta information
		auto *indexMetaInfoPtr = (IndexMetaInfo *)headerPage;
		// TODO: what if the name is more than 20 characters?
		outIndexName.copy(indexMetaInfoPtr->relationName, 20);
		indexMetaInfoPtr->attrByteOffset = attrByteOffset;
		indexMetaInfoPtr->attrType = attributeType;
		indexMetaInfoPtr->rootPageNo = rootPageNum;

		// set the root node information
		auto *rootIntPtr = (NonLeafNodeInt *)rootPage;
		rootIntPtr->level = 1;
		std::fill_n(rootIntPtr->keyArray, nodeOccupancy, 0);
		std::fill_n(rootIntPtr->pageNoArray, nodeOccupancy + 1, (PageId)Page::INVALID_NUMBER);

		// unpin with modification
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);

		// TODO: insertion happens here

		// flush the file
		bufMgr->flushFile(file);
	}
	else
	{
		// otherwise, open the existing index file
		file = new BlobFile(outIndexName, false);

		// retrieve the header page
		headerPageNum = file->getFirstPageNo();
		bufMgr->readPage(file, headerPageNum, headerPage);
		auto *indexMetaInfoPtr = (IndexMetaInfo *)headerPage;

		// throw an exception if the information doesn't match
		if (attributeType != indexMetaInfoPtr->attrType
				|| attrByteOffset != indexMetaInfoPtr->attrByteOffset
				|| outIndexName.compare(indexMetaInfoPtr->relationName) != 0)
		{
			// unpin without modification
			bufMgr->unPinPage(file, headerPageNum, false);
			
			throw BadIndexInfoException(outIndexName);
		}

		// get the root page number
		rootPageNum = indexMetaInfoPtr->rootPageNo;

		// unpin without modification
		bufMgr->unPinPage(file, headerPageNum, false);
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// end the last scanning
  if (scanExecuting)
	{
		endScan();
	}

	// flush the file before the deletion
	bufMgr->flushFile(file);

	delete file;
	file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	// throw an exception if the opcodes are bad
	if ((lowOpParm != GT && lowOpParm != GTE)
	    || (highOpParm != LT && highOpParm != LTE))
	{
		throw BadOpcodesException();
	}

	// throw an exception if the search range is bad
	if (*(int *)lowValParm > *(int *)highValParm)
	{
		throw BadScanrangeException();
	}

	// end the last scan
	if (scanExecuting)
	{
		endScan();
	}

	// set the scanning information
	scanExecuting = true;
	lowValInt = *(int *)lowValParm;
	lowOp = lowOpParm;
	highValInt = *(int *)highValParm;
	highOp = highOpParm;
	
	// find the first entry
	// throw an exception if no such key is found
	if (!findFirstEntry(rootPageNum, 0)) {
		throw NoSuchKeyFoundException();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	// throw an exception if no scan has been initialized
	if (!scanExecuting)
	{
		throw ScanNotInitializedException();
	}

	// throw an exception if no more satisfying record
	if (currentPageNum == Page::INVALID_NUMBER)
	{
		throw IndexScanCompletedException();
	}

	// return the record id via reference
	auto *leafIntPtr = (LeafNodeInt *)currentPageData;
	outRid = leafIntPtr->ridArray[nextEntry];

	bool isEnd = false;  // true if need to change page but reach the end

	// update for the next scan
	++nextEntry;
	if (nextEntry >= leafOccupancy || leafIntPtr->ridArray[nextEntry].page_number == Page::INVALID_NUMBER)
	{
		// need to change the page
		// get the page number of the right sibling
		PageId rightSibPageNum = leafIntPtr->rightSibPageNo;

		// unpin without modification
		bufMgr->unPinPage(file, currentPageNum, false);

		isEnd = rightSibPageNum == Page::INVALID_NUMBER;

		// change the page correspondingly
		if (!isEnd)
		{
			currentPageNum = rightSibPageNum;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			nextEntry = 0;

			leafIntPtr = (LeafNodeInt *)currentPageData;
		}
	}

	// see if the next record does not satisfy the scan criteria
	if (isEnd || !compareOp(leafIntPtr->keyArray[nextEntry], highValInt, highOp))
	{
		// unpin without modification
		if (!isEnd)
		{
			bufMgr->unPinPage(file, currentPageNum, false);
		}

		currentPageNum = Page::INVALID_NUMBER;
		currentPageData = nullptr;
		nextEntry = -1;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
	// throw an exception if no scan has been initialized.
	if (!scanExecuting)
	{
		throw ScanNotInitializedException();
	}

	// unpin without modification
	if (currentPageNum != Page::INVALID_NUMBER)
	{
		bufMgr->unPinPage(file, currentPageNum, false);
	}

	// reset correspondingly
	scanExecuting = false;
	nextEntry = -1;
	currentPageNum = Page::INVALID_NUMBER;
	currentPageData = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::findFirstEntry
// -----------------------------------------------------------------------------
//
bool BTreeIndex::findFirstEntry(PageId curPageNum, int prvLevel)
{
	bool found = false;  // found or not

	// read the current page
	Page *curPage;
	bufMgr->readPage(file, curPageNum, curPage);

	// check if the current page is a leaf
	if (prvLevel == 1)
	{
		auto *curLeafIntPtr = (LeafNodeInt *)curPage;  // current leaf pointer
		for (int i = 0; 
				i < leafOccupancy && curLeafIntPtr->ridArray[i].page_number != Page::INVALID_NUMBER;
				++i)
		{
			// skip if the key is too small
			if (!compareOp(curLeafIntPtr->keyArray[i], lowValInt, lowOp))
			{
				continue;
			}

			// break if the key is too large
			// since it cannot be found later
			if (!compareOp(curLeafIntPtr->keyArray[i], highValInt, highOp))
			{
				break;
			}

			// found if the key is within the range
			// return directly without unpinning
			nextEntry = i;
			currentPageNum = curPageNum;
			currentPageData = curPage;
			return true;
		}
	}
	else
	{
		auto *curNodeIntPtr = (NonLeafNodeInt *)curPage;  // current node pointer

		// find in every valid children page
		for (int i = 0; 
				i < nodeOccupancy + 1 && curNodeIntPtr->pageNoArray[i] != Page::INVALID_NUMBER;
				++i)
		{
			// the keys of the entries are less than or equal to the right key
			// skip if the low value cannot be in this children page
			if (i + 1 != nodeOccupancy + 1
					&& curNodeIntPtr->pageNoArray[i + 1] != Page::INVALID_NUMBER
					&& !compareOp(curNodeIntPtr->keyArray[i], lowValInt, lowOp))
			{
				continue;
			}

			found = findFirstEntry(curNodeIntPtr->pageNoArray[i], curNodeIntPtr->level);

			// break once found
			if (found) break;
		}
	}

	// unpin the current page
	bufMgr->unPinPage(file, curPageNum, false);

	return found;
}

}
