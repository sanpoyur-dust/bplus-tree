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
	
	// read the root page
	Page *rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);

	// find the first entry
	// throw an exception if no such key is found
	if (!findFirstEntry(rootPage, 0)) {
		// unpin without modification
		bufMgr->unPinPage(file, rootPageNum, false);

		throw NoSuchKeyFoundException();
	}

	// unpin without modification
	bufMgr->unPinPage(file, rootPageNum, false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

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
	bufMgr->unPinPage(file, currentPageNum, false);

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
bool BTreeIndex::findFirstEntry(Page *curPage, int prvLevel)
{
	// check if the current page is a leaf
	if (prvLevel == 1)
	{
		auto *curLeafPtr = (LeafNodeInt *)curPage;  // current leaf pointer
		for (int i = 0; 
				i < leafOccupancy && curLeafPtr->ridArray[i].page_number != Page::INVALID_NUMBER;
				++i)
		{
			// skip if the key is too small
			if (!compareOp(curLeafPtr->keyArray[i], lowValInt, lowOp))
			{
				continue;
			}

			// not found if the key is too large
			// since it cannot be found later
			if (!compareOp(curLeafPtr->keyArray[i], highValInt, highOp))
			{
				return false;
			}

			// found if the key is within the range
			nextEntry = i;
			currentPageNum = curLeafPtr->ridArray[i].page_number;
			currentPageData = curPage;
			return true;
		}

		return false;
	}

	auto *curNodePtr = (NonLeafNodeInt *)curPage;  // current node pointer
	Page *nxtPage;  // next page

	// find in every valid children page
	for (int i = 0; 
			i < nodeOccupancy + 1 && curNodePtr->pageNoArray[i] != Page::INVALID_NUMBER;
			++i)
	{
		// the keys of the entries are less than or equal to the right key
		// skip if the low value cannot be in this children page
		if (i + 1 != nodeOccupancy + 1
				&& curNodePtr->pageNoArray[i + 1] != Page::INVALID_NUMBER
				&& !compareOp(curNodePtr->keyArray[i], lowValInt, lowOp))
		{
			continue;
		}

		// set this page as the next page to find
		bufMgr->readPage(file, curNodePtr->pageNoArray[i], nxtPage);
		bool found = findFirstEntry(nxtPage, curNodePtr->level);
		if (found)
		{
			// if found, unpin only if the page is not a leaf
			if (curNodePtr->level == 0)
			{
				bufMgr->unPinPage(file, curNodePtr->pageNoArray[i], false);
			}
			return true;
		}
	}

	return false;
}

}
