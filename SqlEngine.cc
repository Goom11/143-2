/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <climits>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }



    // If the tree index
  BTreeIndex bti;
  if (bti.open(table + ".idx", 'r') == 0) {
      count = 0;

      // Get the minimum value to search for initially in the tree
      int minVal = INT_MIN;
      for (int i = 0; i < cond.size(); i++) {
          if (cond[i].attr == 1 && (cond[i].comp == SelCond::EQ ||
                                    cond[i].comp == SelCond::GT ||
                                    cond[i].comp == SelCond::GE)) {
              if (minVal < atoi(cond[i].value))
                  minVal = atoi(cond[i].value);
          }

      }

      // Locate the minVal
      IndexCursor cursor;
      bti.locate(minVal, cursor);
      bool readmore = true;

      while (readmore) {

          readmore = true;
          bool printOrCount = true;
          bool valueSetForThisRow = false;

          // read the tuple
          rc = bti.readForward(cursor, key, rid);
          if ((rc < 0)) {
              if (rc == RC_END_OF_TREE) {
                  if (attr == 4) {
                      goto print_count;
                  }
                  goto exit_select;
              }

              fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
              goto exit_select;
          }

          // check the conditions on the tuple
          for (unsigned i = 0; i < cond.size(); i++) {
              // compute the difference between the tuple value and the condition value
              switch (cond[i].attr) {
                  case 1:
                      diff = key - atoi(cond[i].value);
                      break;
                  case 2:
                      if (!valueSetForThisRow) {
                          if ((rc = rf.read(rid, key, value)) < 0) {
                              fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
                              goto exit_select;
                          }
                          valueSetForThisRow = true;
                      }

                      diff = strcmp(value.c_str(), cond[i].value);
                      break;
              }

              // skip the tuple if any condition is not met
              switch (cond[i].comp) {
                  case SelCond::EQ:
                      if (diff != 0) {
                          readmore = false;
                          printOrCount = false;
                      }
                      break;
                  case SelCond::NE:
                      if (diff == 0)
                          printOrCount = false;
                      break;
                  case SelCond::GT:
                      if (diff <= 0)
                          printOrCount = false;
                      break;
                  case SelCond::LT:
                      if (diff >= 0) {
                          readmore = false;
                          printOrCount = false;
                      }
                      break;
                  case SelCond::GE:
                      if (diff < 0)
                          printOrCount = false;
                      break;
                  case SelCond::LE:
                      if (diff >= 0) {
                          readmore = false;
                          printOrCount = false;
                      }
                      break;
              }
          }

          if (printOrCount) {
              // the condition is met for the tuple.
              // increase matching tuple counter
              count++;

              // print the tuple if it needs to be printed
              switch (attr) {
                  case 1:  // SELECT key
                      fprintf(stdout, "%d\n", key);
                      break;
                  case 2:  // SELECT value
                      if (!valueSetForThisRow) {
                          if ((rc = rf.read(rid, key, value)) < 0) {
                              fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
                              goto exit_select;
                          }
                          valueSetForThisRow = true;
                      }

                      fprintf(stdout, "%s\n", value.c_str());
                      break;
                  case 3:  // SELECT *
                      if (!valueSetForThisRow) {
                          if ((rc = rf.read(rid, key, value)) < 0) {
                              fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
                              goto exit_select;
                          }
                          valueSetForThisRow = true;
                      }

                      fprintf(stdout, "%d '%s'\n", key, value.c_str());
                      break;
              }
          }
      }

      goto exit_select;
  }

  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
  while (rid < rf.endRid()) {
    // read the tuple
    if ((rc = rf.read(rid, key, value)) < 0) {
      fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
      goto exit_select;
    }

    // check the conditions on the tuple
    for (unsigned i = 0; i < cond.size(); i++) {
      // compute the difference between the tuple value and the condition value
      switch (cond[i].attr) {
      case 1:
	diff = key - atoi(cond[i].value);
	break;
      case 2:
	diff = strcmp(value.c_str(), cond[i].value);
	break;
      }

      // skip the tuple if any condition is not met
      switch (cond[i].comp) {
      case SelCond::EQ:
	if (diff != 0) goto next_tuple;
	break;
      case SelCond::NE:
	if (diff == 0) goto next_tuple;
	break;
      case SelCond::GT:
	if (diff <= 0) goto next_tuple;
	break;
      case SelCond::LT:
	if (diff >= 0) goto next_tuple;
	break;
      case SelCond::GE:
	if (diff < 0) goto next_tuple;
	break;
      case SelCond::LE:
	if (diff > 0) goto next_tuple;
	break;
      }
    }

    // the condition is met for the tuple. 
    // increase matching tuple counter
    count++;

    // print the tuple 
    switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

    // move to the next tuple
    next_tuple:
    ++rid;
  }

    print_count:
    // print matching tuple count if "select count(*)"
    if (attr == 4) {
        fprintf(stdout, "%d\n", count);
    }
    rc = 0;

    // close the table file and return
    exit_select:
    rf.close();
    return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
    // Opens the RecordFile in write mode, and opens the loadfile.
    RecordFile rf(table + ".tbl", 'w');
    ifstream tableFile(loadfile.c_str());


    BTreeIndex bti;
    if (index) {
        bti.open(table + ".idx", 'w');
    }

    if (tableFile.is_open())
    {
        string tuple;

        // Reads every line of the loadfile into the tuple string.
        while ( getline (tableFile,tuple) )
        {
            int key;
            string value;

            // If there is no parse error, write the values to the RecordFile
            int resVal = parseLoadLine(tuple, key, value);
            if (resVal == 0) {
                RecordId rid;
                rf.append(key, value, rid);
                if (index) {
                    bti.insert(key, rid);
                }

            } else {
                cout << "Error code: " << resVal << endl;
                exit(1);
            }

        }
        tableFile.close();

        if (index) {
            bti.close();
        }
    }
    else {
        cout << "Error loading from file: " << loadfile << endl;
    }

    rf.close();

  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
