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
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str();  // index file name
	outIndexName = indexName;  // return the index file name via reference

	Page *headerPage;  // header page
	Page *rootPage;    // root page

	// the first leaf page
	PageId leafPageNum;
	Page *leafPage;

	if (!File::exists(outIndexName))
	{
		// create a new index file if it doesn't exist
		file = new BlobFile(outIndexName, true);

		// allocate the header page, root page
		bufMgr->allocPage(file, headerPageNum, headerPage);
		bufMgr->allocPage(file, rootPageNum, rootPage);

		// set the index meta information
		auto *indexMetaInfoPtr = (IndexMetaInfo *)headerPage;
		// the name should be less than 20 characters
		outIndexName.copy(indexMetaInfoPtr->relationName, 20);
		indexMetaInfoPtr->attrByteOffset = attrByteOffset;
		indexMetaInfoPtr->attrType = attributeType;
		indexMetaInfoPtr->rootPageNo = rootPageNum;

		// set the root node information
		auto *rootIntPtr = (NonLeafNodeInt *)rootPage;
		clearNode(rootIntPtr, 1, 0, nodeOccupancy);

		// allocate the leaf page
		bufMgr->allocPage(file, leafPageNum, leafPage);
		auto *leafPagePtr = (LeafNodeInt *)leafPage;
		clearLeaf(leafPagePtr, Page::INVALID_NUMBER, 0, leafOccupancy);

		rootIntPtr->pageNoArray[0] = leafPageNum;

		// unpin with modification
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);
		bufMgr->unPinPage(file, leafPageNum, true);

		// insert the records in the relation
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
	// read the root page
	Page *rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);
	auto *rootIntPtr = (NonLeafNodeInt *)rootPage;

	// construct the data entry to insert
	RIDKeyPair<int> inserted;
	inserted.set(rid, *(int *)key);
	PageKeyPair<int> pushed;

	if (!insertEntryAux(rootIntPtr, inserted, pushed))
	{
		// insert the pushed up key from the old root in a new root
		bufMgr->unPinPage(file, rootPageNum, true);

		// update the root
		PageId oldRootPageNum = rootPageNum;
		bufMgr->allocPage(file, rootPageNum, rootPage);
		rootIntPtr = (NonLeafNodeInt *)rootPage;
		clearNode(rootIntPtr, 0, 0, nodeOccupancy);

		// set the pushed up key and children pages for the new root
		rootIntPtr->pageNoArray[0] = oldRootPageNum;
		insertPageKeyPairAux(rootIntPtr, 0, pushed, 0);
	}

	bufMgr->unPinPage(file, rootPageNum, true);
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
		// read the starting page possibly having the first entry
		bufMgr->readPage(file, currentPageNum, currentPageData);

		// this will be incremented later
		nextEntry = -1;

		// find the actual current page and first entry
		// the starting page will be unpinned unless it is what we found
		if (updateScanEntry())
		{
			// return if found
			return;
		}
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

	// return the next record ID via reference
	outRid = ((LeafNodeInt *)currentPageData)->ridArray[nextEntry];

	// update the next record
	updateScanEntry();
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
// BTreeIndex::clearNode
// -----------------------------------------------------------------------------
//
void BTreeIndex::clearNode(NonLeafNodeInt *nodeIntPtr, int level, int st, int ed)
{
	nodeIntPtr->level = level;
	for (int i = st; i < ed; ++i)
	{
		nodeIntPtr->keyArray[i] = 0;
		nodeIntPtr->pageNoArray[i] = Page::INVALID_NUMBER;
	}
	nodeIntPtr->pageNoArray[ed] = Page::INVALID_NUMBER;
}

// -----------------------------------------------------------------------------
// BTreeIndex::clearLeaf
// -----------------------------------------------------------------------------
//
void BTreeIndex::clearLeaf(LeafNodeInt *leafIntPtr, PageId rightSibPageNo, int st, int ed)
{
	leafIntPtr->rightSibPageNo = rightSibPageNo;
	for (int i = st; i < ed; ++i)
	{
		leafIntPtr->keyArray[i] = 0;
		leafIntPtr->ridArray[i] = {Page::INVALID_NUMBER, Page::INVALID_SLOT, 0};
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertPageKeyPairAux
// -----------------------------------------------------------------------------

template <class T>
void BTreeIndex::insertPageKeyPairAux(NonLeafNodeInt *nodeIntPtr, int m, const PageKeyPair<T> &pk, int pos)
{
	for (int i = m - 1; i >= pos; --i)
	{
		nodeIntPtr->pageNoArray[i + 2] = nodeIntPtr->pageNoArray[i + 1];
		nodeIntPtr->keyArray[i + 1] = nodeIntPtr->keyArray[i];
	}

	nodeIntPtr->pageNoArray[pos + 1] = pk.pageNo;
	nodeIntPtr->keyArray[pos] = pk.key;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertRIDKeyPairAux
// -----------------------------------------------------------------------------

template <class T>
void BTreeIndex::insertRIDKeyPairAux(LeafNodeInt *leafIntPtr, int m, const RIDKeyPair<T> &rk, int pos)
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
// BTreeIndex::findPageNumInNode
// -----------------------------------------------------------------------------

PageId BTreeIndex::findPageNumInNode(NonLeafNodeInt *nodeIntPtr, int val, Operator op)
{
	if (nodeIntPtr->pageNoArray[1] == Page::INVALID_NUMBER)
	{
		// the node has no key
		return nodeIntPtr->pageNoArray[0];
	}
	else
	{
		// find the leftmost valid children page
		for (int i = 0; 
				i < nodeOccupancy + 1 && nodeIntPtr->pageNoArray[i] != Page::INVALID_NUMBER;
				++i)
		{
			// found if the upper bound doesn't exist or is at least the given value
			if (i + 1 == nodeOccupancy + 1
					|| nodeIntPtr->pageNoArray[i + 1] == Page::INVALID_NUMBER
					|| compareOp(nodeIntPtr->keyArray[i], val, op))
			{
				return nodeIntPtr->pageNoArray[i];
			}
		}
	}

	return Page::INVALID_NUMBER;
}

// -----------------------------------------------------------------------------
// BTreeIndex::findLeafPageNum
// -----------------------------------------------------------------------------

PageId BTreeIndex::findLeafPageNum(int val, Operator op)
{
	// start from the root page
	PageId curPageNum = rootPageNum;
	Page *curPage;
	bufMgr->readPage(file, curPageNum, curPage);

	int curLevel;       // current level
	PageId nxtPageNum;  // next page id

	while (true)
	{
		auto *curNodeIntPtr = (NonLeafNodeInt *)curPage;
		curLevel = curNodeIntPtr->level;

		// find the child page to go into
		nxtPageNum = findPageNumInNode(curNodeIntPtr, val, op);

		bufMgr->unPinPage(file, curPageNum, false);

		if (curLevel == 1 || nxtPageNum == Page::INVALID_NUMBER)
		{
			// directly return if the next page is a leaf
			// or if the next page is not found
			return nxtPageNum;
		}

		// change the current page to the next
		curPageNum = nxtPageNum;
		bufMgr->readPage(file, nxtPageNum, curPage);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::updateScanEntry
// -----------------------------------------------------------------------------

bool BTreeIndex::updateScanEntry()
{
	auto *curLeafIntPtr = (LeafNodeInt *)currentPageData;

	while (true)
	{
		++nextEntry;

		// check if at the end of the current page
		if (nextEntry >= leafOccupancy
				|| curLeafIntPtr->ridArray[nextEntry].page_number == Page::INVALID_NUMBER)
		{
			// break if at the end of all leaves
			if (curLeafIntPtr->rightSibPageNo == Page::INVALID_NUMBER)
			{
				break;
			}

			// unpin and change the page to the right sibling page
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = curLeafIntPtr->rightSibPageNo;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			curLeafIntPtr = (LeafNodeInt *)currentPageData;

			// it should always has at least one entry
			nextEntry = 0;
		}

		// check if the key lies within the bound

		// skip to the next one if the key is too small
		// this only happens when a scan starts and the first entry is to be initialized
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
	}

	// not found
	// unpin the current page and reset information correspondingly
	bufMgr->unPinPage(file, currentPageNum, false);
	currentPageNum = Page::INVALID_NUMBER;
	currentPageData = nullptr;
	nextEntry = -1;
	return false;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntryAux
// -----------------------------------------------------------------------------

template <class T>
bool BTreeIndex::insertEntryAux(NonLeafNodeInt *nodeIntPtr, const RIDKeyPair<T> &rk, PageKeyPair<T> &pk)
{
	// read the child page to go into
	PageId nxtPageNum = findPageNumInNode(nodeIntPtr, rk.key, GT);
	Page *nxtPage;
	bufMgr->readPage(file, nxtPageNum, nxtPage);

	// possible pushed or copied up entry from a full child page
	PageKeyPair<int> pushedOrCopied;  

	bool ok;  // whether the insertion completes without split or not

	// check if the child page is a leaf
	if (nodeIntPtr->level == 1)
	{
		// insert the <rid, key> pair in the leaf node
		auto *nxtLeafIntPtr = (LeafNodeInt *)nxtPage;
		ok = insertRIDKeyPair(nxtLeafIntPtr, rk, pushedOrCopied);
	}
	else
	{
		// insert in the non leaf node recursively
		auto *nxtNodeIntPtr = (NonLeafNodeInt *)nxtPage;
		ok = insertEntryAux(nxtNodeIntPtr, rk, pushedOrCopied);
	}

	bufMgr->unPinPage(file, nxtPageNum, true);
	if (ok)
	{
		// no need to perform extra insertion in the current node
		return true;
	}

	int m = nodeOccupancy;    // number of pages in the node
	int pos = nodeOccupancy;  // position to insert
	for (int i = 0; i < nodeOccupancy; ++i)
	{
		if (nodeIntPtr->pageNoArray[i + 1] == Page::INVALID_NUMBER)
		{
			m = i;
			if (pos == nodeOccupancy)
			{
				pos = m;
			}
			break;
		}
		if (pos == nodeOccupancy && nodeIntPtr->keyArray[i] > pushedOrCopied.key)
		{
			pos = i;
		}
	}

	// insert in the current node
	if (m != nodeOccupancy)
	{
		insertPageKeyPairAux(nodeIntPtr, m, pushedOrCopied, pos);
	}
	else
	{
		int mid = (m + 1) >> 1;  // median position

		// allocate a newly split node page
		PageId splitPageNum;
		Page *splitPage;
		bufMgr->allocPage(file, splitPageNum, splitPage);

		auto *splitNodeIntPtr = (NonLeafNodeInt *)splitPage;
		clearNode(splitNodeIntPtr, nodeIntPtr->level, 0, m);

		// copy the right half to the split node
		int cnt = 0;
		splitNodeIntPtr->pageNoArray[0] = nodeIntPtr->pageNoArray[mid];
		for (int i = mid; i < m; ++i)
		{
			splitNodeIntPtr->keyArray[cnt] = nodeIntPtr->keyArray[i];
			splitNodeIntPtr->pageNoArray[cnt + 1] = nodeIntPtr->pageNoArray[i + 1];
			++cnt;
		}

		if (pos <= mid)
		{
			// insert in the left half
			insertPageKeyPairAux(nodeIntPtr, mid + 1, pushedOrCopied, pos);
		}
		else
		{
			// insert in the split node
			insertPageKeyPairAux(splitNodeIntPtr, m - mid - 1, pushedOrCopied, pos - mid);
		}

		// push up the median
		pk = {splitPageNum, nodeIntPtr->keyArray[mid]};

		// clear the right half
		clearNode(nodeIntPtr, nodeIntPtr->level, mid, m);

		bufMgr->unPinPage(file, splitPageNum, true);
	}

	return m != nodeOccupancy;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertRIDKeyPair
// -----------------------------------------------------------------------------

template <class T>
bool BTreeIndex::insertRIDKeyPair(LeafNodeInt *leafIntPtr, const RIDKeyPair<T> &rk, PageKeyPair<T> &pk)
{
	int m = leafOccupancy;    // number of pages in the leaf
	int pos = leafOccupancy;  // position to insert
	for (int i = 0; i < leafOccupancy; ++i)
	{
		if (leafIntPtr->ridArray[i].page_number == Page::INVALID_NUMBER)
		{
			m = i;
			if (pos == leafOccupancy)
			{
				pos = m;
			}
			break;
		}
		if (pos == leafOccupancy && leafIntPtr->keyArray[i] > rk.key)
		{
			pos = i;
		}
	}

	if (m != leafOccupancy)
	{
		// the leaf node is not full
		insertRIDKeyPairAux(leafIntPtr, m, rk, pos);
		return true;
	}
	else
	{
		// if full, split the leaf node
		int mid = (m + 1) >> 1;  // median position

		// allocate a newly split page
		PageId splitPageNum;
		Page *splitPage;
		bufMgr->allocPage(file, splitPageNum, splitPage);

		auto *splitLeafIntPtr = (LeafNodeInt *)splitPage;
		// set the right sibling of the split leaf
		clearLeaf(splitLeafIntPtr, leafIntPtr->rightSibPageNo, 0, m);

		// copy the right half (including the median) to the split leaf
		// if inserted in the left, the median will change
		// the old median will shift one position right
		// index 0 will be left empty in this case to store the new median
		int splitIndex = pos <= mid;
		for (int i = mid; i < m; ++i)
		{
			splitLeafIntPtr->keyArray[splitIndex] = leafIntPtr->keyArray[i];
			splitLeafIntPtr->ridArray[splitIndex] = leafIntPtr->ridArray[i];
			++splitIndex;
		}

		if (pos <= mid)
		{
			// insert in the left half of the original leaf
			// after this, the left half (0 to mid - 1) will be used
			// the key at index mid will be the new median
			// which will be stored at index 0 of the split leaf
			// TODO: what?!
			insertRIDKeyPairAux(leafIntPtr, mid, rk, pos);
			splitLeafIntPtr->keyArray[0] = leafIntPtr->keyArray[mid];
			splitLeafIntPtr->ridArray[0] = leafIntPtr->ridArray[mid];
		}
		else
		{
			// insert in the split node
			insertRIDKeyPairAux(splitLeafIntPtr, m - mid, rk, pos - mid);
		}

		// copy up the median at index 0 of the split leaf
		pk = {splitPageNum, splitLeafIntPtr->keyArray[0]};

		// clear the right half (starting from mid) of the original leaf
		clearLeaf(leafIntPtr, splitPageNum, mid, m);

		bufMgr->unPinPage(file, splitPageNum, true);
		return false;
	}
}

}
