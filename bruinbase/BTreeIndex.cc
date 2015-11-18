/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <string.h>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
	treeHeight = 0;
	memset(metadata, 0, PageFile::PAGE_SIZE);
}

void BTreeIndex::printAll()
{

}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    RC error;
	
	// open the pagefile
	if ( error = pf.open(indexname, mode) )
		return error;	
	
	// if we haven't written anything to the pagefile, make sure we're in write mode
	if (pf.endPid() == 0 && mode == 'r')
	{
		close();
		return -1;
	}
	
	// read in the metadata into our rootPid and treeHeight variables		
	if ( error = pf.read(0, metadata) )
		return error;
	
	memcpy(&rootPid, metadata, sizeof(PageId));
	memcpy(&treeHeight, metadata + sizeof(PageId), sizeof(int));
	
	// if the variables have odd data, reinitialize to default values 	
	if (rootPid <= 0 || treeHeight < 0 || pf.endPid() == 0)
	{
		rootPid = -1;
		treeHeight = 0;
	}
	
	return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    RC error;
	
	// copy the temp data into the metadata buffer
	memcpy(metadata, &rootPid, sizeof(PageId));
	memcpy(metadata + sizeof(PageId), &treeHeight, sizeof(int));
	
	// write metadata to the pagefile
	if ( error = pf.write(0, metadata) )
		return error;
	
	// close the pagefile
    return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	int overflow_key = -1;
	int overflow_pid = -1;

	// First insert. Create a leaf node as root
	if (treeHeight == 0) {
		BTLeafNode leaf;
		leaf.insert(key, rid);

		// Page 0 is reserved for metadata. 
		// Start writing from Page 1
		if (pf.endPid()) {
			rootPid = pf.endPid();
		}
		else {
			rootPid = 1;
		}

		// Now root exists, so treeHeight is 1
		treeHeight++; 

		return leaf.write(rootPid, pf);
	}

	// More than one level. Recurse to find position to insert
    return recursiveInsert(key, rid, 1, rootPid, overflow_key, overflow_pid);
}

RC BTreeIndex::recursiveInsert(int key, const RecordId& rid, int height, 
	PageId pid, int& overflow_key, PageId& overflow_pid)
{
	overflow_key = -1;
	overflow_pid = -1;

	// We are at a leaf node
	if (height == treeHeight) {
		// Get information of this leaf node
		BTLeafNode leaf;
		leaf.read(pid, pf);

		// If did not overflow, then insert was successful. Done.
		if (leaf.insert(key, rid) == 0) {
			return leaf.write(pid, pf);
		}

		// Overflow. Split leaf node
		else {
			BTLeafNode sibling;
			// int siblingKey;
			RC error;

			// Insert and split. Return immediately if error
			if (error = leaf.insertAndSplit(key, rid, sibling, overflow_key))
				return error;

			overflow_pid = pf.endPid();

			leaf.setNextNodePtr(overflow_pid);

			// Write new sibling key to disk. Return immediately if error
			if (error = sibling.write(overflow_pid, pf))
				return error;

			return leaf.write(pid, pf);
		}

		// Leaf root was split. Create a nonleaf to point to the two leaves
		if (treeHeight == 1) {
			BTNonLeafNode root;
			root.initializeRoot(pid, overflow_key, overflow_pid);
			rootPid = pf.endPid();
			treeHeight++;
			return root.write(rootPid, pf);
		}

		return 0;
	}

	// We are not at a leaf node. Non leaf node.
	else {
		BTNonLeafNode node;
		PageId child;
		RC overflow;

		node.read(pid, pf);
		node.locateChildPtr(key, child);

		overflow = recursiveInsert(key, rid, height+1, child, overflow_key, overflow_pid);

		// There was overflow. Insert new pointers
		if (overflow == 1) {
			// If did not overflow, then insert was successful. Done.
			if (node.insert(overflow_key, overflow_pid) == 0) {
				return node.write(pid, pf);
			}

			// Overflow. Split non-leaf node.
			else {
				BTNonLeafNode sibling;
				int midKey;
				RC error;

				// Insert and split. Return immediately if error
				if (error = node.insertAndSplit(overflow_key, overflow_pid, sibling, midKey))
					return error;
				overflow_key = midKey;
				overflow_pid = pf.endPid();

				// Write new sibling key to disk. Return immediately if error
				if (error = sibling.write(overflow_pid, pf))
					return error;

				return node.write(pid, pf);
			}
		}

		// Either 0 or error code
		return overflow;
	}
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	return recursiveLocate(searchKey, cursor, rootPid, 1);
}

RC BTreeIndex::recursiveLocate(int searchKey, IndexCursor& cursor, PageId pid, int depth_count)
{
	RC error;
	
	// we've reached a leaf node
	if (depth_count == treeHeight)
	{	
		cursor.pid = pid;	
		BTLeafNode ln;
		
		if ( error = ln.read(pid, pf) )
			return error;
		
		int eid;
		error = ln.locate(searchKey, eid);
		cursor.eid = eid; // should be fine either way
		return error;	
	}
	
	// we're in a non-leaf node
	BTNonLeafNode nln;
	
	if ( error = nln.read(pid, pf) )
		return error;
	
	PageId next_pid;
	
	if ( error = nln.locateChildPtr(searchKey, next_pid) )
		return error;
	
	return recursiveLocate(searchKey, cursor, next_pid, depth_count + 1);
}


/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	RC error;
	BTLeafNode ln;
	
	if ( error = ln.read(cursor.pid, pf) )
		return error;
	
	error = ln.readEntry(cursor.eid, key, rid);
	
	// move the cursor forward by 1
	cursor.eid++;
	
	// if we've just moved past the end of the node,
	// move the cursor to the next node
	if ( cursor.eid == ln.getKeyCount() )
	{
		cursor.pid = ln.getNextNodePtr();
		cursor.eid = 0;
	}
	
    return error;
}
