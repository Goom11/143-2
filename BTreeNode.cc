#include "BTreeNode.h"

using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf) {
    return pf.read(pid, (void *) &buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf) {
    return pf.write(pid, (const void *) &buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount() {
    return buffer.numKeyRecords;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid) {
    if (getKeyCount() == MAX_KEY_RECORDS) {
        return RC_NODE_FULL;
    }
    BTNodeKeyRecord keyRecordToInsert = { key, rid };

    int i;
    for(i = 0; i < getKeyCount() && key > buffer.keyRecords[i].key; i++) {
    }

    BTNodeKeyRecord temp[MAX_KEY_RECORDS];
    memcpy(
        temp,
        buffer.keyRecords + i * sizeof(BTNodeKeyRecord),
        (MAX_KEY_RECORDS - i) * sizeof(BTNodeKeyRecord)
    );
    buffer.keyRecords[i] = keyRecordToInsert;
    memcpy(
        buffer.keyRecords + (i + 1) * sizeof(BTNodeKeyRecord),
        temp,
        (MAX_KEY_RECORDS - (i + 1)) * sizeof(BTNodeKeyRecord)
    );

    buffer.numKeyRecords++;
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
                              BTLeafNode& sibling, int& siblingKey) {
    if (getKeyCount() == 0) {
        return insert(key, rid);
    }
    if (getKeyCount() == 1) {
        if (key > buffer.keyRecords[0].key) {
            siblingKey = key;
            return sibling.insert(key, rid);
        }
        BTNodeKeyRecord greaterKeyRecord = buffer.keyRecords[0];
        buffer.keyRecords[0] = { key, rid };
        siblingKey = greaterKeyRecord.key;
        return sibling.insert(greaterKeyRecord.key, greaterKeyRecord.rid);
    }
    int half = getKeyCount() / 2;
    int halfKey = buffer.keyRecords[half].key;
    siblingKey = buffer.keyRecords[half + 1].key;
    for (int i = half + 1; i < getKeyCount(); i++) {
        BTNodeKeyRecord keyRecord = buffer.keyRecords[i];
        RC error = sibling.insert(keyRecord.key, keyRecord.rid);
        if (error != 0) {
            return error;
        }
    }
    buffer.numKeyRecords = half + 1;
    return key > halfKey ? sibling.insert(key, rid) : insert(key, rid);
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
RC BTLeafNode::locate(int searchKey, int& eid) {
    for(eid = 0;
        eid < getKeyCount() && searchKey < buffer.keyRecords[eid].key;
        eid++) { }
    return searchKey == buffer.keyRecords[eid].key ? 0 : RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid) {
    if (eid < 0 || eid >= getKeyCount()) {
        return RC_NO_SUCH_RECORD;
    }
    key = buffer.keyRecords[eid].key;
    rid = buffer.keyRecords[eid].rid;
    return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr() {
    return buffer.nextLeaf;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid) {
    buffer.nextLeaf = pid;
    return 0;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf) {
    return pf.read(pid, (void *) &buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf) {
    return pf.write(pid, (const void *) &buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount() {
    return buffer.numKeys;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid) {
    int i;
    for(i = 0; i < getKeyCount() && key > buffer.keys[i]; i++) {
    }

    int temp[MAX_KEYS];
    memcpy(
        temp,
        buffer.keys + i * sizeof(int),
        (MAX_KEYS - i) * sizeof(int)
    );
    buffer.keys[i] = key;
    memcpy(
            buffer.keys + (i + 1) * sizeof(int),
            temp,
            (MAX_KEYS - (i + 1)) * sizeof(int)
    );
    buffer.numKeys++;

    i++;
    PageId tempPageIds[MAX_KEYS + 1];
    memcpy(
            tempPageIds,
            buffer.pageIds + i * sizeof(PageId),
            ((MAX_KEYS + 1) - i) * sizeof(PageId)
    );
    buffer.pageIds[i] = pid;
    memcpy(
            buffer.pageIds + (i + 1) * sizeof(PageId),
            tempPageIds,
            ((MAX_KEYS + 1) - (i + 1)) * sizeof(PageId)
    );

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
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey) {
    // Add special cases for numbers less than 4
    if (getKeyCount() <= 1) {
        return RC_INVALID_ATTRIBUTE;
    }

    if (getKeyCount() == 2) {
        if (key < buffer.keys[0]) {
            midKey = buffer.keys[0];
            int error = sibling.initializeRoot(buffer.pageIds[1], buffer.keys[1], buffer.pageIds[2]);
            buffer.keys[0] = key;
            buffer.pageIds[1] = pid;
            buffer.numKeys = 1;
            return error;

        } else if (key > buffer.keys[1]) {
            midKey = buffer.keys[1];
            int error = sibling.initializeRoot(buffer.pageIds[2], key, pid);
            buffer.numKeys = 1;
            return error;
        } else {
            midKey = key;
            int error = sibling.initializeRoot(pid, buffer.keys[1], buffer.pageIds[2]);
            buffer.numKeys = 1;
            return error;
        }

    }

    int half = getKeyCount() / 2;
    midKey = buffer.keys[half];

    if (buffer.keys[half + 1] > key && buffer.keys[half] < key) {
        sibling.initializeRoot(buffer.pageIds[half + 1], key, pid);
        for (int i = half + 1; i < getKeyCount(); i++) {
            int error = sibling.insert(buffer.keys[i], buffer.pageIds[i + 1]);
            if (error != 0) {
                return error;
            }
        }
    } else {
        sibling.initializeRoot(buffer.pageIds[half + 1], buffer.keys[half + 1], buffer.pageIds[half + 2]);
        sibling.insert(key, pid);
        for (int i = half + 2; i < getKeyCount(); i++) {
            int error = sibling.insert(buffer.keys[i], buffer.pageIds[i + 1]);
            if (error != 0) {
                return error;
            }
        }
    }

    buffer.numKeys = half;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid) {
    if (buffer.numKeys == 0)
        return RC_NO_SUCH_RECORD;

    int i;
    for(i = 0; i < getKeyCount(); i++) {
        if (searchKey < buffer.keys[i]) {
            pid = buffer.pageIds[i];
            return 0;
        }
    }
    pid = buffer.pageIds[i];
    return  0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2) {
    buffer.numKeys = 1;
    buffer.keys[0] = key;
    buffer.pageIds[0] = pid1;
    buffer.pageIds[1] = pid2;
    return 0;
}
