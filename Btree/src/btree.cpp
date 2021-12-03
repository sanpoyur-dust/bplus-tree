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
{
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str();  // index file name
	outIndexName = indexName;  // return the index file name via reference

	Page *headerPage;  // header page
	Page *rootPage;    // root page

	if (!File::exists(indexName))
	{
		// create a new index file if it doesn't exist
		file = new BlobFile(indexName, true);

		// allocate the header page and root page
		bufMgr->allocPage(file, headerPageNum, headerPage);
		bufMgr->allocPage(file, rootPageNum, rootPage);

		// set the index meta information
		auto *indexMetaInfo = (IndexMetaInfo *)headerPage;
		// TODO: what if the name is more than 20 characters?
		indexName.copy(indexMetaInfo->relationName, 20);
		indexMetaInfo->attrByteOffset = attrByteOffset;
		indexMetaInfo->attrType = attributeType;
		indexMetaInfo->rootPageNo = rootPageNum;

		// TODO: figure out what Piazza @466 means

		// unpin correspondingly
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);

		// TODO: insertion happens here
	}
	else
	{
		// TODO: otherwise, open the existing index file
		file = new BlobFile(indexName, false);
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	delete file;
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

}

}
