#include "BTreeNode.h"
#include <string.h>
#include <stdlib.h>
#include <cassert>
#include <iostream>

using namespace std;

//////////////////////////////////////////////////////////////
//                      BT LEAF NODE                        //
//////////////////////////////////////////////////////////////

/*
 * The structure of a BT LEAF NODE:
 *  ----------------------------------------------
 * | num_keys | KR_Pair | KR_Pair | ... | nextPID |
 *  ----------------------------------------------
 */

/*
 * Constructor. Initialize buffer to all 0's.
 */
BTLeafNode::BTLeafNode()
{
	memset(buffer, 0, PageFile::PAGE_SIZE); 
}

void BTLeafNode::printAll(bool keys_only) {
	int *keycount = (int *) buffer;
	cout << "[" << *keycount << "] | ";

	KRPair *kr = (KRPair *) (buffer + BTLeafNode::BEGINNING_OFFSET);
	for (int i = 0; i < *keycount; i++) {
		if (keys_only)
			cout << (kr + i)->key << " | ";
		else
			cout << (kr + i)->key << ", (" << (kr + i)->rid.pid << ", " << (kr + i)->rid.sid << ") | ";
	}
	cout << getNextNodePtr() << " | ";
	cout << endl << "---" << endl;
}

void BTLeafNode::test()
{
	int eid, key;
	RecordId rid, insert_rid;
	
	// no keys initially
	assert ( getKeyCount() == 0 );
	
	// the key 0 is not in the node
	assert ( locate(0, eid) == RC_NO_SUCH_RECORD );
	
	// we can't read entries in the empty node
	for (int i = 0; i < 84; i++)
		assert( readEntry(i, key, rid) == RC_INVALID_CURSOR );
	
	setNextNodePtr(25);
	
	insert_rid.pid = 11; insert_rid.sid = 1;
	insert( 12, insert_rid  );
	insert_rid.pid = 0;  insert_rid.sid = 12;
	insert( -4,  insert_rid  );
	insert_rid.pid = 9;  insert_rid.sid = 5;
	insert( 0,  insert_rid  );
	insert_rid.pid = 7;  insert_rid.sid = 8;
	insert( 7,  insert_rid  );
		
	// 4 keys now in the node
	assert ( getKeyCount() == 4 );
	
	// next node ptr has PageID 25
	assert ( getNextNodePtr() == 25 );
	
	// the key 13 isn't in the node
	assert ( locate(13, eid) == RC_NO_SUCH_RECORD );
	
	// the key 7 is in the node
	assert ( locate(7,  eid) == 0);
	
	// 7 is the key of the third entry in the node
	assert ( eid == 2 );

	// there is no fifth entry in the node
	assert ( readEntry(4, key, rid) == RC_INVALID_CURSOR );
	
	// the first entry in the node has a key of 0 and RecordID of (9, 5)
	assert ( readEntry(0, key, rid) == 0 );
	assert ( key == -4 && rid.pid == 0 && rid.sid == 12);
	
	for (int i = 20; i < 100; i++)
	{
		insert_rid.pid = i;
		insert_rid.sid = 1;
		insert( 120 - i, insert_rid );			
	}

	/* This should be the key layout right now:
	 * 0 4 7 12 21 22 23 24 25 ... 100
	*/
	// we now have 84 keys
	assert ( getKeyCount() == 84 );
	
	// can't insert any more keys
	insert_rid.pid = 2; insert_rid.sid = 2;
	assert ( insert(200, insert_rid ) == RC_NODE_FULL );
	assert ( locate(200, eid) == RC_NO_SUCH_RECORD );
	
	locate(100, eid);
	
	// eid of last key is 83
	assert ( eid == 83 );

	// the sixth entry has a key of 22 and a RecordID of (98, 1)
	assert ( readEntry(5, key, rid) == 0 );
	assert ( key == 22 && rid.pid == 98 && rid.sid == 1 );
	
	BTLeafNode new_sibling;
	int first_key_in_sibling;
	
	// try to insert the key 20 and RecordID (4, 10)
	
	//PageID saved_next = getNextNodePtr();
	insert_rid.pid = 4; insert_rid.sid = 10;
	assert  ( insertAndSplit(20, insert_rid, new_sibling, first_key_in_sibling) == 0 );
	
	// left node has 43 keys, right node has 42 keys
	assert  ( getKeyCount() == 43 && new_sibling.getKeyCount() == 42);
	
	// left node points to right node
	//assert  ( getNextNodePtr() == new_sibling.getPID() );
	
	// right node points to what left node pointed to before the insert
	//assert  ( new_sibling.getNextNodePtr() == saved_next );
	// the last entry in the left node has a key of 58 after the split
	assert  ( locate(58, eid) == 0 );
	assert  ( eid == 42 );
	
	// the entry with key=59 is not in the left node. it is the first key in the right node
	assert  ( locate(59, eid) == RC_NO_SUCH_RECORD );
	assert  ( first_key_in_sibling == 59 );
	assert  ( new_sibling.locate(59, eid) == 0 );
	assert  ( eid == 0 );
	
	// the second key in the right node is 60
	assert  ( new_sibling.locate(60, eid) == 0 );
	assert  ( eid == 1 );
	
	// the last key in the new sibling is 100 with a RecordID (20, 1)
	assert  ( new_sibling.readEntry(41, key, rid) == 0 );
	assert  ( key == 100 && rid.pid == 20 && rid.sid == 1 );
	
	
	cerr << "All tests successful.\n";	
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	//setPID(pid);
	return pf.read(pid, buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	// Traverse through buffer in 12 byte increments
	// If the key = 0, then we have reached the end of 
	// entries. Otherwise keep going until MAX_LEAF_KEYS
	// int keycount = 0;
	// KRPair *cur = (KRPair *) buffer;
	// while (cur->key != 0 && keycount < BTLeafNode::MAX_LEAF_KEYS) {
	// 	keycount++;
	// 	cur++;
	// }
	// return keycount; 

	// Key count stored as first element in buffer.
	int *count = (int *) buffer;
	return *count;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	int num_keys = getKeyCount();
	int eid;
	int offset;
	KRPair insert_pair;
	PageId next_node = getNextNodePtr();

	// If no space, return error code
	if (num_keys >= BTLeafNode::MAX_LEAF_KEYS) {
		return RC_NODE_FULL;
	}

	// There is space. Insert new key, RecordID pair.
	insert_pair.key = key;
	insert_pair.rid = rid;
	locate(key, eid);

	// eid now contains index entry number to insert into)
	char *temp_buffer = (char *) malloc(PageFile::PAGE_SIZE * sizeof(char));

	// Copy the first eid KRPairs into the temp buffer.
	// The element to insert will come after these.
	// Then copy the rest of the buffer after the element
	offset = BTLeafNode::BEGINNING_OFFSET + (sizeof(KRPair) * eid);
	memcpy(temp_buffer, buffer, offset);
	memcpy(temp_buffer + offset, &insert_pair, sizeof(KRPair));
	memcpy(temp_buffer + offset + sizeof(KRPair), buffer + offset, 
		num_keys * sizeof(KRPair) - (sizeof(KRPair) * eid));

	// Move the newly constructed buffer back into the buffer variable
	memcpy(buffer, temp_buffer, PageFile::PAGE_SIZE);

	// Free temp_buffer to prevent memory leak
	free(temp_buffer);

	// Increase num_keys.
	num_keys++;
	setKeyCount(num_keys);
	setNextNodePtr(next_node);
	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	KRPair insert_pair;
	int eid;
	int num_keys = getKeyCount();
	int pivot; // Contains the eid of the pair at which we are splitting
	int offset;
	int side = 0; // 0 = left, 1 = right
	int left_keys;
	int right_keys;

	// Check that sibling is EMPTY
	if (sibling.getKeyCount() != 0)
		return RC_INVALID_ATTRIBUTE;

	// Only split if the current node is FULL
	if (num_keys < BTLeafNode::MAX_LEAF_KEYS)
		return RC_INVALID_ATTRIBUTE;

	locate(key, eid);
	// Split consistently, such that left node has more keys.
	if (eid <= (num_keys/2)) {
		pivot = num_keys/2;
	} 
	else {
		pivot = num_keys/2 + 1;
		side = 1;
	}
	left_keys = pivot;
	right_keys = num_keys - pivot;

	offset = BTLeafNode::BEGINNING_OFFSET + (sizeof(KRPair) * pivot);

	// Clear sibling anyways. 
	memset(sibling.buffer, 0, PageFile::PAGE_SIZE);
	// Move second half of buffer to sibling.
	memcpy(sibling.buffer + BTLeafNode::BEGINNING_OFFSET, buffer + offset, 
		PageFile::PAGE_SIZE - sizeof(PageId) - offset);
	// Sibling points to the next node that the original node was pointing to
	sibling.setNextNodePtr(getNextNodePtr());
	sibling.setKeyCount(right_keys);

	// Clear second half of buffer.
	memset(buffer + offset, 0, PageFile::PAGE_SIZE - sizeof(PageId) - offset);
	setKeyCount(left_keys);
	// Set this node to point to the new sibling
	//setNextNodePtr(sibling.getPID());

	// Insert new key, rid pair into correct leaf node.
	if (side) {
		sibling.insert(key, rid);
	}
	else {
		insert(key, rid);
	}

	// siblingKey = first key in sibling node.
	siblingKey = ((KRPair *) (sibling.buffer + BTLeafNode::BEGINNING_OFFSET))->key;
	return 0; 
}

/**
 * Set the key count in the buffer. The key count is contained in the first
 * four bytes of the buffer.
 * @param new_count[IN] The new key count
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setKeyCount(int new_count)
{
	if (new_count < 0) {
		return RC_INVALID_ATTRIBUTE;
	}
	int *cur_count = (int*) buffer;
	*cur_count = new_count;
	return 0;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	// cur = beginning of buffer. keycount = number of keys in node
	KRPair *cur = (KRPair *) (buffer + BTLeafNode::BEGINNING_OFFSET);
	int keycount = getKeyCount();

	// Search through the buffer. 
	// If the current key is the searchKey, then stop and return that eid.
	// Else if the current key is greater than the searchKey, then the 
	// searchKey does not exist. Stop and return the eid of the first larger element.
	for (eid = 0; eid < keycount; eid++) {
		if ((cur + eid)->key == searchKey) {
			return 0;
		}
		else if ((cur + eid)->key > searchKey) {
			return RC_NO_SUCH_RECORD;
		}
		// cur++;
	}
	return RC_NO_SUCH_RECORD; 
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	KRPair *target;

	// eid is 0-indexed. Check between 0 and current number of entries
	if (eid < 0 || eid >= getKeyCount()) {
		return RC_INVALID_CURSOR;
	}

	// Get target pair. Put key and rid into respective variables
	target = ((KRPair *) (buffer + BTLeafNode::BEGINNING_OFFSET))  + eid;
	key = target->key;
	rid = target->rid;

	return 0; 
}

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	// PID is contained in the last 4 bytes of buffer.
	PageId pid = 0;
	memcpy (&pid, buffer + PageFile::PAGE_SIZE - sizeof(PageId), sizeof(PageId));
	return pid;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	// Check for valid PID
	if (pid < 0) {
		return RC_INVALID_PID;
	}

	// Copy PID into last 4 bytes of buffer.
	memcpy (buffer + PageFile::PAGE_SIZE - sizeof(PageId), &pid, sizeof(PageId));
	return 0; 
}

//////////////////////////////////////////////////////////////
//                      BT NONLEAF NODE                     //
//////////////////////////////////////////////////////////////

/*
 * The structure of a BT NON LEAF NODE:
 *  ------------------------------------------------
 * | num_keys | first_PID | KP_Pair | KP_Pair | ... |
 *  ------------------------------------------------
 */

/*
 * Constructor. Initialize buffer to all 0's.
 */
BTNonLeafNode::BTNonLeafNode()
{
	memset(buffer, 0, PageFile::PAGE_SIZE); 
}

void BTNonLeafNode::printAll(bool keys_only) {
	int *beginning = (int *) buffer;
	cout << "[" << *beginning << "] | ";

	cout << *(beginning + 1) << " | ";

	KPPair *kp = (KPPair *) (buffer + BTNonLeafNode::BEGINNING_OFFSET);
	for (int i = 0; i < *beginning; i++) {
		if (keys_only)
			cout << (kp + i)->key << " | ";
		else
			cout << (kp + i)->key << ", " << (kp + i)->pid << " | ";
	}
	cout << endl << "---" << endl;
}

void BTNonLeafNode::test()
{
	int eid;
	
	// no keys initially
	assert ( getKeyCount() == 0 );
	
	// the key 0 is not in the node
	assert ( locate(0, eid) == RC_NO_SUCH_RECORD );
	initializeRoot(1, 3, 4);
	insert( 1,  11 );
	insert( 2,  5  );
	insert( 0,  9  );
		
	// 4 keys now in the node
	assert ( getKeyCount() == 4 );
	
	// the key 13 isn't in the node
	assert ( locate(13, eid) == RC_NO_SUCH_RECORD );
	
	// the key 2 is in the node
	assert ( locate(2,  eid) == 0);
	
	// 2 is the key of the third entry in the node
	assert ( eid == 2 );
	
	// the first entry in the node has a key of 0
	assert ( locate(0, eid) == 0 );
	assert ( eid == 0 );
	
	for (int i = 10; i < BTNonLeafNode::MAX_NON_KEYS + 6; i++)
	{
		if (i == 69) {
			insert (5, 95);
		}
		else{
			insert( i, 100-i );
		}
	}
	/* This should be the key layout right now:
	 * 0 1 2 3 10 11 12 ... 132
	*/
	
	// we now have 127 keys
	assert ( getKeyCount() == BTNonLeafNode::MAX_NON_KEYS );
	
	// can't insert any more keys
	assert ( insert(200, 3 ) == RC_NODE_FULL );
	assert ( locate(200, eid) == RC_NO_SUCH_RECORD );
	
	// eid of last key is 126
	locate(132, eid);
	assert ( eid == 126 );

	// the sixth entry has a key of 11 
	// assert ( locate(11, eid)  == 0 );
	// assert ( eid == 5 );
	
	BTNonLeafNode sibling;
	int midkey;
	
	// try to insert the key 8 and PageId 9
	printAll();
	assert  ( insertAndSplit(69, 9, sibling, midkey) == 0 );
	printAll();
	sibling.printAll();
	cout << midkey << endl;
	// left node has 64 keys, right node has 63 keys
	assert  ( getKeyCount() == 64 && sibling.getKeyCount() == 63 );
	
	// the middle key is 69
	assert  ( midkey == 69 );
	
	// the last key in the left node is 68
	assert  ( locate(68, eid) == 0 );
	assert  ( eid == 63 );
	
	// the key 69 is not in the left node or right node
	assert  ( locate(69, eid) == RC_NO_SUCH_RECORD && sibling.locate(69, eid) == RC_NO_SUCH_RECORD );

	// the key 70 is not in the left node.  it is the first key in the right node.
	assert  ( locate(70, eid) == RC_NO_SUCH_RECORD );
	assert  ( sibling.locate(70, eid) == 0 );
	assert  ( eid  == 0 );
	
	// the second key in the right node is 71
	assert  ( sibling.locate(71, eid) == 0 );
	assert  ( eid == 1 );
	
	// the last key in the right node is 132
	assert  ( sibling.locate(132, eid) == 0 );
	assert  ( eid == 62 );
	
	
	cerr << "All tests successful.\n";	
}


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
	//setPID(pid);
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ 
	// Store key count as first element in buffer.
	int *count = (int *) buffer;
	return *count;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	int num_keys = getKeyCount();
	int eid;
	int offset;
	KPPair insert_pair;

	// If no space, return error code
	if (num_keys >= BTNonLeafNode::MAX_NON_KEYS) {
		return RC_NODE_FULL;
	}

	// There is space. Insert new key, RecordID pair.
	insert_pair.key = key;
	insert_pair.pid = pid;
	locate(key, eid);

	// eid now contains index entry number to insert into)
	char *temp_buffer = (char *) malloc(PageFile::PAGE_SIZE * sizeof(char));

	// Copy the first eid KRPairs into the temp buffer.
	// The element to insert will come after these.
	// Then copy the rest of the buffer after the element
	offset = BTNonLeafNode::BEGINNING_OFFSET + (sizeof(KPPair) * eid);
	memcpy(temp_buffer, buffer, offset);
	memcpy(temp_buffer + offset, &insert_pair, sizeof(KPPair));
	memcpy(temp_buffer + offset + sizeof(KPPair), buffer + offset, 
		num_keys * sizeof(KPPair) - (sizeof(KPPair) * eid));

	// Move the newly constructed buffer back into the buffer variable
	memcpy(buffer, temp_buffer, PageFile::PAGE_SIZE);

	// Free temp_buffer to prevent memory leak
	free(temp_buffer);

	// Increase num_keys.
	num_keys++;
	setKeyCount(num_keys);
	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	KPPair insert_pair;
	KPPair mid_pair;
	int eid;
	int num_keys = getKeyCount();
	int pivot; // Contains the eid of the pair at which we are splitting
	int offset;
	int side = 0; // 0 = left, 1 = right
	int left_keys;
	int right_keys;

	// Check that sibling is EMPTY
	if (sibling.getKeyCount() != 0)
		return RC_INVALID_ATTRIBUTE;

	// Only split if the current node is FULL
	if (num_keys < BTNonLeafNode::MAX_NON_KEYS)
		return RC_INVALID_ATTRIBUTE;

	locate(key, eid);
	// Split consistently, such that left node has more keys.
	if (eid <= (num_keys/2)) {
		pivot = num_keys/2;
	} 
	else {
		pivot = num_keys/2 + 1;
		side = 1;
	}

	offset = BTNonLeafNode::BEGINNING_OFFSET + (sizeof(KPPair) * pivot);

	// Inserted value is middle key
	if (eid == pivot) {
		left_keys = pivot;
		right_keys = num_keys - pivot;
		mid_pair.key = key;
		mid_pair.pid = pid;
	}
	else {
		left_keys = pivot;
		right_keys = num_keys - pivot - 1;
		mid_pair = *((KPPair *) (buffer + offset));
		// Skip over middle key. Do not copy to sibling
		offset += sizeof(KPPair);
	}

	memcpy(&midKey, &mid_pair, sizeof(int));

	// Clear sibling anyways. 
	memset(sibling.buffer, 0, PageFile::PAGE_SIZE);
	memcpy(sibling.buffer + sizeof(int), &mid_pair.pid, sizeof(PageId));
	// Move second half of buffer to sibling.
	memcpy(sibling.buffer + BTNonLeafNode::BEGINNING_OFFSET, buffer + offset, 
		PageFile::PAGE_SIZE - offset);

	sibling.setKeyCount(right_keys);

	// Clear second half of buffer.
	memset(buffer + offset, 0, PageFile::PAGE_SIZE - offset);
	setKeyCount(left_keys);
	// Set this node to point to the new sibling
	//setNextNodePtr(sibling.getPID());

	if (eid != pivot) {
		// Insert new key, rid pair into correct leaf node.
		if (side) {
			sibling.insert(key, pid);
		}
		else {
			insert(key, pid);
		}
	}
	return 0; 
}

/**
 * Set the key count in the buffer. The key count is contained in the first
 * four bytes of the buffer.
 * @param new_count[IN] The new key count
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::setKeyCount(int new_count)
{
	if (new_count < 0) {
		return RC_INVALID_ATTRIBUTE;
	}
	int *cur_count = (int*) buffer;
	*cur_count = new_count;
	return 0;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTNonLeafNode::locate(int searchKey, int& eid)
{ 
	// cur = beginning of buffer. keycount = number of keys in node
	KPPair *cur = (KPPair *) (buffer + BTNonLeafNode::BEGINNING_OFFSET);
	int keycount = getKeyCount();

	// Search through the buffer. 
	// If the current key is the searchKey, then stop and return that eid.
	// Else if the current key is greater than the searchKey, then the 
	// searchKey does not exist. Stop and return the eid of the first larger element.
	for (eid = 0; eid < keycount; eid++) {
		if (cur->key == searchKey) {
			return 0;
		}
		else if (cur->key > searchKey) {
			return RC_NO_SUCH_RECORD;
		}
		cur++;
	}
	return RC_NO_SUCH_RECORD; 
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	KPPair *target;
	int num_keys = getKeyCount();
	int i;
	
	target = (KPPair *) (buffer + BTNonLeafNode::BEGINNING_OFFSET);
	for (i = 0; i < num_keys; i++) {
		if (searchKey < (target + i)->key) {
			pid = (target + i - 1)->pid;
			return 0;
		}

	}
	pid = (target + i - 1)->pid;
	return 0; 
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
	if (pid1 < 0 || pid2 < 0) {
		return RC_INVALID_PID;
	}
	memset(buffer, 0, PageFile::PAGE_SIZE);
	memcpy(buffer + sizeof(int), &pid1, sizeof(PageId));
	insert(key, pid2);
	return 0;
}

int BTNonLeafNode::getChildren(PageId *children)
{
	KPPair *target;
	int num_keys = getKeyCount();
	int num_children = 0;
	int i;

	if (num_keys == 0)
		return 0;

	target = (KPPair *) (buffer + BTNonLeafNode::BEGINNING_OFFSET);
	for (i = 0; i <= num_keys; i++) {
		children[i] = (target + i - 1)->pid;
		num_children++;
	}
	return num_children;
}
