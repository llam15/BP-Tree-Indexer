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
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include <climits>

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);

int optimizeQuery(const vector<SelCond> &conditions, int &start_key, int &end_key, bool &use_tree)
{
  start_key = INT_MIN;
  end_key = INT_MAX;
  use_tree = false;
  
  int eq_count = 0;
  int eq_key;
  
  vector<int> ne_keys;
  
  for (int i = 0; i < conditions.size(); i++)
  {
    if (conditions[i].attr == 1) // key attribute
    {
      int key = atoi(conditions[i].value);
      switch(conditions[i].comp)
      {
        case SelCond::EQ: 
          start_key = key;
          end_key = key;
          if (key != eq_key)
            eq_count++;
          eq_key = key;
          use_tree = true;
          break;
        case SelCond::NE:
          ne_keys.push_back(key);
          break;
        case SelCond::LT:
          if (key < end_key)
            end_key = key - 1;
          use_tree = true;
          break;
        case SelCond::LE:
          if (key <= end_key)
            end_key = key;
          use_tree = true;
          break;
        case SelCond::GT:
          if (key > start_key)
            start_key = key + 1;
          use_tree = true;
          break;
        case SelCond::GE:
          if (key >= start_key)
            start_key = key;
          use_tree = true;
          break;
        default:
          break;
      }
      
    }
  }
  
  // Error if the beginning of our range is greater than the end of our range.
  // i.e. a query like "key >= 20 and key < 9"
  // Error if we specifiy equality on different keys
    // i.e. a query like "key=12 and key=14"
  // Error if the equal key does not fall within the range 
  // i.e. a query like "key=9 and key > 12 and key < 100"
  if (start_key > end_key || eq_count > 1 || (eq_count == 1 && !(start_key <= eq_key && eq_key <= end_key) ))
  {
    return -1;
  } 
  
  // Error if the equal key is the same as any of the not equal keys
  // i.e. a query like "key=9 and key<>9"
  for (int i = 0; i < ne_keys.size(); i++)
  {
    if (eq_key == ne_keys[i])
    {
      return -1;
    }
  } 
    
  return 0;

}

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

  BTreeIndex index;
  IndexCursor cursor;
  int start_key;
  int end_key;
  bool use_tree;
  bool io_flag;
  string index_file = table + ".idx";
  int index_error = index.open(index_file, 'r');
  int optimize;
  // optimizeQuery checks start_key < end_key, etc. 
  // Returns -1 if an invalid query. If invalid, go to exit.
  if (optimize = optimizeQuery(cond, start_key, end_key, use_tree)) {
    goto exit_select;
  }

  // No index, so just read normally.
  if (index_error || (!use_tree && attr != 4)) {
    // fprintf(stderr, "NOT USING INDEX\n");
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
  }

  // Use BTreeIndex
  else {
    // fprintf(stderr, "USING INDEX\n");
    count = 0;

    // Getting count(*) with no select conditions. Return index's keyCount
    if (cond.size() == 0 && attr == 4) {
      count = index.getKeyCount();
      goto exit_while;
    }

    index.locate(start_key, cursor);

    while (index.readForward(cursor, key, rid) == 0) {
      io_flag = false;

      if (key > end_key)
        goto exit_while;

      for (unsigned i = 0; i < cond.size(); i++) {
        if (cond[i].attr == 1 &&
            cond[i].comp == SelCond::NE &&
            atoi(cond[i].value)) {
          goto cursor_forward;
        }

        else if (cond[i].attr == 2) {
          // read the tuple
          if (!io_flag) {
            if ((rc = rf.read(rid, key, value)) < 0) {
              fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
              goto exit_select;
            }
          }

          io_flag = true;
          diff = strcmp(value.c_str(), cond[i].value);

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
      }

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      if ((attr == 2 || attr == 3) && !io_flag) {
          // read the tuple
          if ((rc = rf.read(rid, key, value)) < 0) {
            fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
            goto exit_select;
          }
      }

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

      cursor_forward:
      ;
    }
  }

  exit_while:
  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  if (!index_error) {
    index.close();
  }
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  string tablename = table + ".tbl";
  RecordFile rf;
  RecordId   rid;  
  RC     rc;
  BTreeIndex b;
   
  // create index if necessary 
  if (index)
  {
    string indexname = table + ".idx";
    b.open(indexname, 'w'); 
  }
  
  rc = rf.open(tablename, 'w');
  
  ifstream fin;
  fin.open(loadfile.c_str());
  string line;
  
  while ( getline(fin, line) )
  {
    int key;
    string value;
    parseLoadLine(line, key, value);
    rf.append(key, value, rid);
    if (index)
    {
      b.insert(key, rid);
    }
  }
  
  rf.close();
  fin.close();
  // close index if necessary 
  if (index)
  {
    b.close();
  }
  
  return rc;
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
