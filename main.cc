/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeNode.h"
#include "RecordFile.h"
#include <cstdio>

int main()
{
  printf("%lu\n", sizeof(BTNodeKeyRecord));
  printf("%lu\n", sizeof(BTLeafNodeBuffer));
  printf("%lu\n", sizeof(BTNonLeafNodeBuffer));

  BTLeafNode btLeafNode;
  RecordId rid;
  rid.pid = 2;
  rid.sid = 3;
  for (int i = 90; i > 0; i--) {
    if (i != 55) {
      btLeafNode.insert(i, rid);
    }
  }
  int eid;

  BTLeafNode sibling;
  int siblingKey;
  btLeafNode.insertAndSplit(55, rid, sibling, siblingKey);
  btLeafNode.locate(80, eid);
  int key;
  RecordId ridrecv;
  btLeafNode.readEntry(eid, key, ridrecv);

  BTNonLeafNode root;
  root.initializeRoot(-1, -1, 0);

  for (int i = 300; i > 0; i--) {
    if (i != 55) {
      root.insert(i, i);
    }
  }

  BTNonLeafNode siblingnl;
  int mid;
  root.insertAndSplit(55, 55, siblingnl, mid);

  // run the SQL engine taking user commands from standard input (console).
  SqlEngine::run(stdin);

  return 0;
}
