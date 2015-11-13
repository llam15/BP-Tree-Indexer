#include "BTreeNode.h"
#include <string.h>
#include <stdlib.h>

using namespace std;

//////////////////////////////////////////////////////////////
//						 BT LEAF NODE						//
//////////////////////////////////////////////////////////////

/*
 * Constructor. Initialize buffer to all 0's.
 */
BTLeafNode::BTLeafNode()
{
	memset(buffer, 0, PageFile::PAGE_SIZE); // this is \0??
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
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
	// entries. Otherwise keep going until MAX_KEYS
	int keycount = 0;
	KRPair *cur = (KRPair *) buffer;
	while (cur->key != 0 && keycount < BTLeafNode::MAX_KEYS) {
		keycount++;
		cur++;
	}
	return keycount; 
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	int eid;
	int offset;
	KRPair insert_pair;

	// If no space, return error code
	if (getKeyCount() >= BTLeafNode::MAX_KEYS) {
		return RC_NODE_FULL;
	}

	// There is space. Insert new key, RecordID pair.
	insert_pair.key = key;
	insert_pair.rid = rid;
	locate(key, eid);

	// eid now contains index entry number to insert into)
	char *temp_buffer = (char *) malloc(PageFile::PAGE_SIZE);

	// Copy the first eid KRPairs into the temp buffer.
	// The element to insert will come after these.
	// Then copy the rest of the buffer after the element
	offset = sizeof(KRPair) * eid;
	memcpy(temp_buffer, buffer, offset);
	memcpy(temp_buffer + offset, &insert_pair, sizeof(KRPair));
	memcpy(temp_buffer + offset + sizeof(KRPair), buffer + offset, 
		PageFile::PAGE_SIZE - offset);

	// Move the newly constructed buffer back into the buffer variable
	memcpy(buffer, temp_buffer, PageFile::PAGE_SIZE);

	// Free temp_buffer to prevent memory leak
	free(temp_buffer);
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
	// Contains the eid of the pair that will be the first of the new sibling
	int pivot = (num_keys + 1)/2;
	int offset = sizeof(KRPair) * pivot;

	// Check that sibling is EMPTY
	if (sibling.getKeyCount() != 0)
		return RC_INVALID_ATTRIBUTE;

	// Only split if the current node is FULL
	if (num_keys < BTLeafNode::MAX_KEYS)
		return RC_INVALID_ATTRIBUTE;

	// Clear sibling anyways. 
	memset(sibling.buffer, 0, PageFile::PAGE_SIZE);
	// Move second half of buffer to sibling.
	memcpy(sibling.buffer, buffer + offset, 
		PageFile::PAGE_SIZE - sizeof(PageId) - offset);
	// sibling points to the next node that the original node was pointing to
	sibling.setNextNodePtr(getNextNodePtr());

	// Clear second half of buffer.
	memset(buffer + offset, 0, PageFile::PAGE_SIZE - sizeof(PageId) - offset);

	// Insert new key, rid pair into correct leaf node.
	locate(key, eid);
	if (eid >= pivot) {
		sibling.insert(key, rid);
	}
	else {
		insert(key, rid);
	}

	siblingKey = ((KRPair *) sibling.buffer)->key;

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
	KRPair *cur = (KRPair *) buffer;
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
	target = (KRPair *) buffer + eid;
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
//						BT NONLEAF NODE						//
//////////////////////////////////////////////////////////////

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return 0; }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return 0; }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ return 0; }


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ return 0; }

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
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
