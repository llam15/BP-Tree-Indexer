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
#include <queue>
#include <iostream>

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

void BTreeIndex::test() 
{
	RC rc;
	int key; 
	RecordId rid;
	RecordId insert_rid; // for inserting
	IndexCursor cursor;
	
	
	rc = open("test_index", 'w');
	cerr << "Created index successfully: " << (rc == 0) << endl;
	cerr << "Initially, the rootPid is -1 and treeHeight is 0: "
	     << (treeHeight == 0 && rootPid == -1) << endl;
	
	insert_rid.pid = 2;  insert_rid.sid = 4;
	rc = insert(10, insert_rid);
	cerr << "\n--Inserting key 10 with pid=2, sid=4--\n\n";
	cerr << "Successfully inserted first element: " << (rc == 0) << endl;
	cerr << "The root pid is now 1: " << (rootPid == 1) << endl;
	cerr << "The tree height is now 1: " << (treeHeight == 1) << endl;
	
	rc = locate(10, cursor);
	cerr << "Obtained a cursor pointing to the key=10: " << (rc == 0) << endl;
	cerr << "cursor.eid is 0: " << (cursor.eid == 0) << endl;
	cerr << "cursor.pid is 1: " << (cursor.pid == 1) << endl;
	
	rc = readForward(cursor, key, rid);
	cerr << "Was able to read the cursor pointing to the key: " << (rc == 0) << endl;
	cerr << "Was able to read cursor to get appropriate key and record: " 
	     << (key == 10 && rid.pid == 2 && rid.sid == 4) << endl;
	
	rc = readForward(cursor, key, rid);
	cerr << "Moving the cursor forward returns an error: " 
	     << (rc != 0) << endl;
	
	insert_rid.pid = 5;  insert_rid.sid = 1;
	insert(7, insert_rid);
	cerr << "\n--Inserting key 7 with pid=5, sid=1--\n\n";
	
	rc = locate(7, cursor);
	cerr << "Obtained a cursor pointing to the key=7: " << (rc == 0) << endl;
	readForward(cursor, key, rid);
	cerr << "Was able to read cursor to get appropriate key and record: "
		 << (key == 7 && rid.pid == 5 && rid.sid == 1) << endl;
	
	readForward(cursor, key, rid);
	cerr << "Moving the cursor forward once reads the key=1: "
		 << (key == 10 && rid.pid == 2 && rid.sid == 4) << endl;
		 
	rc = readForward(cursor, key, rid);
	cerr << "Reading the cursor forward once more makes it invalid: "
		 << (rc != 0) << endl;
	
	cerr << "\n--Inserting 82 more keys: 11 through 92 --\n\n";
	
	for (int i = 11; i <= 92; i++)
	{
		insert_rid.pid = i + 1;
		insert_rid.sid = i - 1;
		insert(i, insert_rid);
	}

	locate(7, cursor);
	
	cerr << "Reading forward through the node...\n";
	
	for (int i = 0; i < 84; i++)
	{
		rc = readForward(cursor, key, rid);

		if ( i > 1 && key != (i + 9) && rid.pid != (i + 10) && rid.sid != (i + 8) )
		{
			cerr << "0";
		}
	}
	
	rc = readForward(cursor, key, rid);
	
	cerr << "Reading past the last entry throws an error: " << (rc != 0) << endl;
	insert_rid.pid = 10;
	insert_rid.sid = 1;
	insert(8, insert_rid);
	for (int i = 93; i <= 135; i++)
	{
		insert_rid.pid = i + 1;
		insert_rid.sid = i - 1;
		insert(i, insert_rid);
	}

	for (int i = -10000; i <= 6; i++)
	{
		insert_rid.pid = 13000 + (i + 1);
		insert_rid.sid = 13000 + (i - 1);
		insert(i, insert_rid);
	}

	rc = locate(-9675, cursor);
	cerr << "Obtained a cursor pointing to the key=-9675: " << (rc == 0) << endl;
	readForward(cursor, key, rid);
	cerr << "Was able to read cursor to get appropriate key and record: "
		 << (key == -9675 && rid.pid == 3326 && rid.sid == 3324) << endl;

	rc = locate(92, cursor);

	cerr << "Obtained a cursor pointing to the key=92: " << (rc == 0) << endl;
	cerr << "Cursor.pid = " << cursor.pid << endl;

	int num_keys = printAll(false);
	cout << num_keys << endl;
	close();
}

void BTreeIndex::test2() 
{
	cerr << "===================" << endl;
	RC rc;
	int key; 
	RecordId rid;
	RecordId insert_rid; // for inserting
	IndexCursor cursor;

	cerr << "treeHeight is 0 and rootPid is -1  before opening: " << (treeHeight == 0 && rootPid == -1) << endl;
	rc = open("test_index", 'w');
	cerr << "Opened index successfully for writing: " << (rc == 0) << endl;
	cerr << "rootPid: " << rootPid << endl;
	cerr <<  "treeHeight: " << treeHeight << endl;

	rc = locate(92, cursor);
	cerr << "Obtained a cursor pointing to the key=92: " << (rc == 0) << endl;
	readForward(cursor, key, rid);
	cerr << "Was able to read cursor to get appropriate key and record: "
		 << (key == 92 && rid.pid == 93 && rid.sid == 91) << endl;

	readForward(cursor, key, rid);
	cerr << "Was able to read cursor to get appropriate key and record: "
		 << (key == 93 && rid.pid == 94 && rid.sid == 92) << endl;

	insert_rid.pid = 9999;
	insert_rid.sid = 1;
	insert(9, insert_rid);
	rc = locate(9, cursor);
	cerr << "Obtained a cursor pointing to the key=9: " << (rc == 0) << endl;
	readForward(cursor, key, rid);
	cerr << "Was able to read cursor to get appropriate key and record: "
		 << (key == 9 && rid.pid == 9999 && rid.sid == 1) << endl;
	close();
}


int BTreeIndex::printAll(bool print)
{
	queue<BTNode> nodes;
	int totalKeys = 0;

	if (treeHeight == 0) {
		cout << "Empty tree" << endl;
		return totalKeys;
	}
	else {
		cout << "Tree Height: " << treeHeight << endl;
		cout << "===" << endl;
	}

	BTNode root;
	root.height = 1;
	root.pid = rootPid;
	if (root.height == treeHeight) {
		root.leaf_type = true;
	}
	else {
		root.leaf_type = false;
	}

	nodes.push(root);

	while (!nodes.empty()) {
		BTNode next = nodes.front();
		nodes.pop();

		if (!next.leaf_type) {
			BTNonLeafNode nonleaf;
			nonleaf.read(next.pid, pf);
			if (print)
				nonleaf.printAll();

			PageId children[128];
			int num_children = nonleaf.getChildren(children);

			for (int i = 0; i < num_children; i++) {
				BTNode child_node;
				child_node.height = next.height + 1;
				if (child_node.height == treeHeight)
					child_node.leaf_type = true;
				else child_node.leaf_type = false;
				child_node.pid = children[i];
				nodes.push(child_node);
			}
		}
		else {
			BTLeafNode leaf;
			leaf.read(next.pid, pf);
			if (print)
				leaf.printAll();
			totalKeys += leaf.getKeyCount();
		}
	}
	return totalKeys;
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

			if (error = leaf.write(pid, pf))
				return error;

			// Leaf root was split. Create a nonleaf to point to the two leaves
			if (treeHeight == 1) {
				BTNonLeafNode root;
				root.initializeRoot(pid, overflow_key, overflow_pid);
				rootPid = pf.endPid();
				treeHeight++;
				if (error = root.write(rootPid, pf))
					return error;
			}
			return 1;
		}
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

				if (error = node.write(pid, pf))
					return error;

				// Root was split. Create a nonleaf to point to the two nonleaves
				if (height == 1) {
					BTNonLeafNode root;
					root.initializeRoot(pid, overflow_key, overflow_pid);
					rootPid = pf.endPid();
					treeHeight++;
					if (error = root.write(rootPid, pf))
						return error;
				}
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

	if (cursor.pid == 0)
		return RC_INVALID_CURSOR;
	
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
