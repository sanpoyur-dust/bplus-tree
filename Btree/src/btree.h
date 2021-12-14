/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
enum Datatype
{
	INTEGER = 0,
	DOUBLE = 1,
	STRING = 2
};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{ 
	LT, 	/* Less Than */
	LTE,	/* Less Than or Equal to */
	GTE,	/* Greater Than or Equal to */
	GT		/* Greater Than */
};

/**
 * Compare x and y using the given operator.
 * @param x Left value
 * @param y Right value
 * @param op Operator (LT/LTE/GTE/GT)
 * @return whether x op y or not
 */
inline bool compareOp(int x, int y, Operator op)
{
  switch (op)
  {
  case LT:
    return x < y;
  case LTE:
    return x <= y;
  case GTE:
    return x >= y;
  case GT:
    return x > y;
  default:
    return false;
  }
}

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key               rid
const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo                  key       pageNo
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( PageId ) );

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that 
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
template <class T>
class RIDKeyPair{
public:
	RecordId rid;
	T key;
	void set( RecordId r, T k)
	{
		rid = r;
		key = k;
	}
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
template <class T>
class PageKeyPair{
public:
	PageId pageNo;
	T key;
	void set( int p, T k)
	{
		pageNo = p;
		key = k;
	}
};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
template <class T>
bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
{
	if( r1.key != r2.key )
		return r1.key < r2.key;
	else
		return r1.rid.page_number < r2.rid.page_number;
}

/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
	char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in pages.
   */
	int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
	Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
	PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of 
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes 
at this level are just above the leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
struct NonLeafNodeInt{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ INTARRAYNONLEAFSIZE + 1 ];
};


/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
struct LeafNodeInt{
  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ INTARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};


/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
	File		*file;

  /**
   * Buffer Manager Instance.
   */
	BufMgr	*bufMgr;

  /**
   * Page number of meta page.
   */
	PageId	headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
	PageId	rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
	Datatype	attributeType;

  /**
   * Offset of attribute, over which index is built, inside records. 
   */
	int 		attrByteOffset;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
	int			leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int			nodeOccupancy;


	// MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
	bool		scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
	int			nextEntry;

  /**
   * Page number of current page being scanned.
   */
	PageId	currentPageNum;

  /**
   * Current Page being scanned.
   */
	Page		*currentPageData;

  /**
   * Low INTEGER value for scan.
   */
	int			lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
	double	lowValDouble;

  /**
   * Low STRING value for scan.
   */
	std::string	lowValString;

  /**
   * High INTEGER value for scan.
   */
	int			highValInt;

  /**
   * High DOUBLE value for scan.
   */
	double	highValDouble;

  /**
   * High STRING value for scan.
   */
	std::string highValString;
	
  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
	Operator	lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
	Operator	highOp;

  /**
   * Clear the non leaf node with the specified information.
   * It resets the level, keys, and the pages.
   * Keys will be cleared from index st to ed - 1.
   * Pages will be cleared from index st to ed.
   * @param nodeIntPtr Non leaf node to clear
   * @param level Level to reset
   * @param st (Inclusive) lower bound to clear
   * @param ed (Exclusive) upper bound to clear 
   */
  void clearNode(NonLeafNodeInt *nodeIntPtr, int level, int st, int ed);

  /**
   * Clear the leaf node with the specified information.
   * It resets the right sibling page ID, keys, and the record IDs.
   * Keys will be cleared from index st to ed - 1.
   * Record IDs will be cleared from index st to ed - 1.
   * @param leafIntPtr Leaf node to clear
   * @param rightSibPageNo Right sibling page ID to reset
   * @param st (Inclusive) lower bound to clear
   * @param ed (Exclusive) upper bound to clear 
   */
  void clearLeaf(LeafNodeInt *leafIntPtr, PageId rightSibPageNo, int st, int ed);

  /**
   * Find the page ID for the leftmost page with keys possibly GT/GTE the given value.
   * This happens if the upper bound of the page is GT/GTE the given value.
   * In the special case when the node has no key, the first page ID is returned.
   * @param nodeIntPtr Non leaf node to find in
   * @param val A given key value
   * @param op Operator (GT/GTE)
   * @return the satisfying page ID
   */
  PageId findPageNumInNode(NonLeafNodeInt *nodeIntPtr, int val, Operator op);

  /**
   * Find the page ID for the leftmost leaf page with keys possibly GT/GTE the given value.
   * This happens if the upper bounds of the recursively found pages are GT/GTE the given value.
   * In the special case when the root has no key, the first leaf page ID is returned.
   * @param val A given key value
   * @param op Operator (GT/GTE)
   * @return the satisfying leaf page ID
   */
  PageId findLeafPageNum(int val, Operator op);

  /**
   * Update the next entry with a key that lies within the search bound.
   * The corresponding current page and page ID will be updated as well.
   * A starting current page should possibly contain keys satisfying the lower search bound.
   * The current page will be set to invalid if such entry does not exist.
   * For convenience, the invalid next entry has an index -1.
   * @return whether such entry exist or not.
   */
	bool updateScanEntry();

  /**
   * Auxiliary method of insertPageKeyPair.
   * Insert the specified <pid, key> pair into the non leaf node at the position.
   * It assumes the non leaf node to have enough space.
   * Note that the key corresponds to the upper bound of the page.
   * @param nodeIntPtr Non leaf node to insert into
   * @param m Number of valid keys
   * @param pk <pid, key> pair to insert
   * @param pos Insert position
   */
  template<class T>
  void insertPageKeyPairAux(NonLeafNodeInt *nodeIntPtr, int m, const PageKeyPair<T> &pk, int pos);

  /**
   * Auxiliary method of insertRIDKeyPair.
   * Insert the specified <rid, key> pair into the leaf node at the postion.
   * It assumes the leaf node to have enough space.
   * @param leafIntPtr Leaf node to insert into
   * @param m Number of valid keys
   * @param rk <rid, key> pair to insert
   * @param pos Insert position
   */
  template <class T>
  void insertRIDKeyPairAux(LeafNodeInt *leafIntPtr, int m, const RIDKeyPair<T> &rk, int pos);

  /**
   * Insert the specified <pid, key> pair into the non leaf node.
   * If the non leaf node is full, it will be split with a retrned pushed up <pid, key> pair.
   * @param nodeIntPtr Non leaf node to insert into
   * @param pk1 <pid, key> pair to insert
   * @param pk2 <pid, key> pair to copy up
   * @return whether the insertion completes without split or not
   */
  template <class T>
  bool insertPageKeyPair(NonLeafNodeInt *nodeIntPtr, const PageKeyPair<T> &pk1, PageKeyPair<T> &pk2);

  /**
   * Insert the specified <rid, key> pair into the leaf node.
   * If the leaf node is full, it will be split with a retrned copied up <pid, key> pair.
   * @param leafIntPtr Leaf node to insert into
   * @param rk <rid, key> pair to insert
   * @param pk <pid, key> pair to copy up
   * @return whether the insertion completes without split or not
   */
  template <class T>
  bool insertRIDKeyPair(LeafNodeInt *leafIntPtr, const RIDKeyPair<T> &rk, PageKeyPair<T> &pk);

  /**
   * Auxiliary method of insertEntry.
   * Insert the specified <rid, key> pair recursively starting from the non leaf node.
   * If the non leaf node is full, it will be split with a retrned pushed up <pid, key> pair.
   * @param nodeIntrPtr	non leaf node to start from
   * @param rk <rid, key> pair to insert
   * @param pk <pid, key> pair to pushed up
   */
  template <class T>
  bool insertEntryAux(NonLeafNodeInt *nodeIntrPtr, const RIDKeyPair<T> &rk, PageKeyPair<T> &pk);

 public:

  /**
   * BTreeIndex Constructor. 
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   */
	BTreeIndex(const std::string & relationName, std::string & outIndexName,
						BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType);
	

  /**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
	~BTreeIndex();


  /**
	 * Insert a new entry using the pair <value,rid>. 
	 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
	 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
	 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
	 * Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char string
   * @param rid			Record ID of a record whose entry is getting inserted into the index.
	**/
	void insertEntry(const void* key, const RecordId rid);


  /**
	 * Begin a filtered scan of the index.  For instance, if the method is called 
	 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
	 * greater than "a" and less than or equal to "d".
	 * If another scan is already executing, that needs to be ended here.
	 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
	 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
   * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
	void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);


  /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
	void scanNext(RecordId& outRid);  // returned record id


  /**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
	void endScan();
	
};

}
