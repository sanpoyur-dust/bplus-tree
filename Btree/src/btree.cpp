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

		FileScan fscan = FileScan(relationName, bufMgr);
		try
		{
			RecordId scanRid;
			while(1)
			{
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				const void *key = record + attrByteOffset;
				insertEntry(key, scanRid);
			}
		}
		catch(const EndOfFileException &e)
		{}

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
	int val = *(int *)key;

	// find the leaf where the entry should belong
	PageId leafPageNum = findLeafPageNum(val, GTE);
	Page *leafPage;
	bufMgr->readPage(file, leafPageNum, leafPage);

	auto *leafIntPtr = (LeafNodeInt *)leafPage;

	// determine the number of valid entries in the leaf
	int m = 0;
	while (m < leafOccupancy
			&& leafIntPtr->ridArray[m].page_number != Page::INVALID_NUMBER)
	{
		++m;
	}

	// locate an entry with a larger key in the leaf
	int pos = m;  // the position to insert the entry
	for (int i = 0; i < m; ++i)
	{
		if (leafIntPtr->keyArray[i] > val)
		{
			pos = i;
			break;
		}
	}

	// check if there is enough space
	if (m < leafOccupancy)
	{
		RIDKeyPair<int> rk;
		rk.set(rid, val);
		insertRIDKeyPair(leafIntPtr, rk, pos, m);
	}
	else
	{
		std::cout << "TODO: there is no enough space..." << std::endl;
		throw std::exception();
	}

	// unpin with modification
	bufMgr->unPinPage(file, leafPageNum, true);
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
	
	// find the leftmost entry with a key that lies within the search bound
	currentPageNum = findLeafPageNum(lowValInt, lowOp);
	if (currentPageNum != Page::INVALID_NUMBER) {
		bufMgr->readPage(file, currentPageNum, currentPageData);
		nextEntry = -1;

		// initialize the actual current page and next entry
		if (findScanEntry())
		{
			// return if found
			return;
		}

		// otherwise, be ready to throw an exception
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = Page::INVALID_NUMBER;
	}
	
	// throw an exception if no such entry is found
	throw NoSuchKeyFoundException();
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
	if (nextEntry == -1)
	{
		throw IndexScanCompletedException();
	}

	findScanEntry();
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
// BTreeIndex::findLeafPageNum
// -----------------------------------------------------------------------------
//
PageId BTreeIndex::findLeafPageNum(int val, Operator op)
{
	// read the root page
	PageId curPageNum = rootPageNum;
	Page *curPage;
	bufMgr->readPage(file, curPageNum, curPage);

	int level;  // previous level

	do
	{
		auto *curNodeIntPtr = (NonLeafNodeInt *)curPage;
		level = curNodeIntPtr->level;

		// find the leftmost valid children page
		for (int i = 0; 
				i < nodeOccupancy + 1 && curNodeIntPtr->pageNoArray[i] != Page::INVALID_NUMBER;
				++i)
		{
			// check if the upper bound doesn't exist
			// or it is at least or greater than the given value
			if (i + 1 == nodeOccupancy + 1
					|| curNodeIntPtr->pageNoArray[i + 1] == Page::INVALID_NUMBER
					|| compareOp(curNodeIntPtr->keyArray[i], val, op))
			{
				// unpin the current page without modification
				PageId nxtPageNum = curNodeIntPtr->pageNoArray[i];
				bufMgr->unPinPage(file, curPageNum, false);

				// check if the next page is a leaf
				if (level == 1)
				{
					return nxtPageNum;
				}

				// change the current page to the next
				curPageNum = nxtPageNum;
				bufMgr->readPage(file, nxtPageNum, curPage);
				break;
			}
		}
	} while (true);

	return Page::INVALID_NUMBER;
}

// -----------------------------------------------------------------------------
// BTreeIndex::findScanEntry
// -----------------------------------------------------------------------------
//
bool BTreeIndex::findScanEntry()
{
	auto *curLeafIntPtr = (LeafNodeInt *)currentPageData;  // current leaf pointer
	do
	{
		++nextEntry;

		// check if at the end of the current page
		if (nextEntry >= leafOccupancy
				|| curLeafIntPtr->ridArray[nextEntry].page_number == Page::INVALID_NUMBER)
		{
			// need to change the page
			// get the page number of the right sibling
			PageId rightSibPageNum = curLeafIntPtr->rightSibPageNo;

			// unpin the current page without modification
			bufMgr->unPinPage(file, currentPageNum, false);

			// check if at the end of all leaves
			if (rightSibPageNum == Page::INVALID_NUMBER)
			{
				// reset correspondingly
				currentPageNum = Page::INVALID_NUMBER;
				currentPageData = nullptr;
				nextEntry = -1;

				return false;
			}

			// chenge the page correspondingly
			currentPageNum = rightSibPageNum;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			curLeafIntPtr = (LeafNodeInt *)currentPageData;

			nextEntry = 0;
		}

		// skip if the key is too small
		if (!compareOp(curLeafIntPtr->keyArray[nextEntry], lowValInt, lowOp))
		{
			continue;
		}

		// break if the key is too large
		// since it cannot be found later
		if (!compareOp(curLeafIntPtr->keyArray[nextEntry], highValInt, highOp))
		{
			break;
		}

		// found if the key is within the range
		// return directly without unpinning
		return true;
	} while (true);

	// unpin the current page
	bufMgr->unPinPage(file, currentPageNum, false);

	// reset correspondingly
	currentPageNum = Page::INVALID_NUMBER;
	currentPageData = nullptr;
	nextEntry = -1;

	return false;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertRIDKeyPair
// -----------------------------------------------------------------------------
//
template <class T>
void BTreeIndex::insertRIDKeyPair(LeafNodeInt *leafIntPtr, RIDKeyPair<T> &rk, int pos, int m)
{
	for (int i = m - 1; i >= pos; --i)
	{
		leafIntPtr->ridArray[i + 1] = leafIntPtr->ridArray[i];
		leafIntPtr->keyArray[i + 1] = leafIntPtr->keyArray[i];
	}

	leafIntPtr->ridArray[pos] = rk.rid;
	leafIntPtr->keyArray[pos] = rk.key;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertPageKeyPair
// -----------------------------------------------------------------------------
//
template <class T>
void BTreeIndex::insertPageKeyPair(NonLeafNodeInt *nodeIntPtr, PageKeyPair<T> &pk, int pos, int m)
{
	for (int i = m - 1; i >= pos; --i)
	{
		nodeIntPtr->pageNoArray[i + 1] = nodeIntPtr->pageNoArray[i];
		nodeIntPtr->keyArray[i + 1] = nodeIntPtr->keyArray[i];
	}

	nodeIntPtr->pageNoArray[pos] = pk.pageNo;
	nodeIntPtr->keyArray[pos] = pk.key;
}

}
