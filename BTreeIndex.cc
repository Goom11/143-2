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

using namespace std;

const int RC_SPLIT = 401;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
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
    RC pfRC = pf.open(indexname, mode);

    if (pf.endPid() == 0) {
        TreeIndexMetadata buffer;
        buffer.rootPid = 1;
        buffer.treeHeight = 1;
        int error = pf.write(0, (void *) &buffer);
        if (error != 0)
            return error;

        rootPid = buffer.rootPid;
        BTLeafNode leaf;
        return leaf.write(rootPid, pf);
    } else {
        if (pfRC != 0) {
            return pfRC;
        }
        TreeIndexMetadata buffer;
        pfRC = pf.read(0, (void *) &buffer);
        if (pfRC != 0) {
            return pfRC;
        }
        rootPid = buffer.rootPid;
        treeHeight = buffer.treeHeight;
        return 0;
    }
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    TreeIndexMetadata buffer;
    buffer.rootPid = rootPid;
    buffer.treeHeight = treeHeight;
    RC pfRC = pf.write(0, (void *) &buffer);
    if (pfRC != 0) {
        return pfRC;
    }
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
    BTLeafNode leaf;
    BTNonLeafNode nonLeaf;

    if (leaf.read(rootPid, pf) == 0) {
        BTLeafNode sibling;
        int siblingKey;
        int error = leafInsert(leaf, key, rid, sibling, siblingKey);
        int writeError;

        // If there is a split
        if (error == RC_SPLIT) {

            // Write the sibling with the end pid, make sure no errors
            int siblingPid = pf.endPid();
            writeError = sibling.write(siblingPid, pf);
            if (writeError != 0)
                return writeError;

            sibling.setNextNodePtr(leaf.getNextNodePtr());
            leaf.setNextNodePtr(siblingPid);
            int oldRoot = rootPid;
            writeError = leaf.write(oldRoot, pf);
            if (writeError != 0)
                return writeError;

            // Get the next last pid, write the index with that pid
            rootPid = pf.endPid();
            nonLeaf.initializeRoot(oldRoot, siblingKey, siblingPid);
            return nonLeaf.write(rootPid, pf);

        } else if (error == 0) {
            // Otherwise, write the updated leaf
            return leaf.write(rootPid, pf);
        } else {
            return error;
        }


    } else if (nonLeaf.read(rootPid, pf) == 0) {
        BTNonLeafNode sibling;
        int midkey;
        int error = indexInsert(nonLeaf, rootPid, key, rid, sibling, midkey);

        if (error == RC_SPLIT) {
            int writeError;

            // Write the sibling
            int siblingPid = pf.endPid();
            writeError = sibling.write(siblingPid, pf);
            if (writeError != 0)
                return writeError;

            // Save oldRoot's pid, and write the new nonLeaf
            int oldRoot = rootPid;
            writeError = nonLeaf.write(oldRoot, pf);
            if (writeError != 0)
                return  writeError;

            // Update rootPid and save the new root
            rootPid = pf.endPid();
            BTNonLeafNode root;
            root.initializeRoot(oldRoot, midkey, siblingPid);
            return root.write(rootPid, pf);
        } else if (error == 0) {
            return nonLeaf.write(rootPid, pf);
        }
        return error;

    } else {
        // Flag was incorrectly set, therefore must be an invalid pid
        return RC_INVALID_PID;
    }
}

RC BTreeIndex::indexInsert(BTNonLeafNode& index, PageId pid, int key, const RecordId& rid, BTNonLeafNode& sibling, int& midKey)
{
    int newPid;
    int error;
    error = index.locateChildPtr(key, newPid);
    if (error != 0) {
        return error;
    }


    BTLeafNode leaf;
    BTNonLeafNode nonLeaf;

    if (leaf.read(newPid, pf) == 0) {
        BTLeafNode leafS;
        int siblingKey;
        error = leafInsert(leaf, key, rid, leafS, siblingKey);

        // If the leaf splits, write the leaf and its sibling
        if (error == RC_SPLIT) {
            int siblingPid = pf.endPid();
            leafS.setNextNodePtr(leaf.getNextNodePtr());
            leaf.setNextNodePtr(siblingPid);
            leafS.write(siblingPid, pf);
            leaf.write(newPid, pf);

            int nonLeafError = index.insert(siblingKey, siblingPid);
            if (nonLeafError == RC_NODE_FULL) {
                index.insertAndSplit(siblingKey, siblingPid, sibling, midKey);
                return RC_SPLIT;
            } else if (nonLeafError == 0){
                return 0;
            } else {
                return nonLeafError;
            }
        } else if (error == 0) {
            leaf.write(newPid, pf);
            return 0;
        }
        return error;

    } else if (nonLeaf.read(newPid, pf) == 0) {
        BTNonLeafNode nonLeafSibling;
        int siblingKey;
        error = indexInsert(nonLeaf, newPid, key, rid, nonLeafSibling, siblingKey);

        if (error == RC_SPLIT) {
            nonLeaf.write(newPid, pf);
            int siblingPid = pf.endPid();
            nonLeafSibling.write(siblingPid, pf);

            int nonLeafError = index.insert(siblingKey, siblingPid);
            if (nonLeafError == RC_NODE_FULL) {
                index.insertAndSplit(siblingKey, siblingPid, sibling, midKey);
                return RC_SPLIT;
            } else if (nonLeafError == 0){
                return 0;
            } else {
                return nonLeafError;
            }
        }
        return error;
    } else {
        // Flag was incorrectly set, therefore must be an invalid pid
        return RC_INVALID_PID;
    }
}

RC BTreeIndex::leafInsert(BTLeafNode& leaf, int key, const RecordId& rid, BTLeafNode& sibling, int& siblingKey)
{
    RC error = leaf.insert(key, rid);

    if (error == RC_NODE_FULL) {
        error = leaf.insertAndSplit(key, rid, sibling, siblingKey);
        return error == 0 ? RC_SPLIT : error;
    }

    return error;
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
    return locateFull(searchKey, cursor, rootPid);
}

RC BTreeIndex::locateFull(int searchKey, IndexCursor& cursor, PageId curPid) {
    BTLeafNode leaf;
    BTNonLeafNode node;

    if (leaf.read(curPid, pf) == 0) {
        cursor.pid = curPid;
        return leaf.locate(searchKey, cursor.eid);
    } else if (node.read(curPid, pf) == 0) {
        PageId nextPid;
        node.locateChildPtr(searchKey, nextPid);
        return locateFull(searchKey, cursor, nextPid);
    }
    return RC_INVALID_PID;
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
    if (cursor.pid == -1) {
        return RC_END_OF_TREE;
    }
    BTLeafNode leaf;
    RC leafRC = leaf.read(cursor.pid, pf);
    if (leafRC != 0) {
        return leafRC;
    }
    leafRC = leaf.readEntry(cursor.eid, key, rid);
    if (leafRC != 0) {
        return leafRC;
    }
    leafRC = leaf.getNextCursor(cursor);
    return leafRC;
}
