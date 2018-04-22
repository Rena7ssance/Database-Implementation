#ifndef OPTIMIZER_CC
#define OPTIMIZER_CC

#include <iostream>
#include "Optimizer.h"
#include "RelAlgExpr.h"
using namespace std;

Optimizer :: Optimizer(map <string, MyDB_TablePtr> &allTablesIn,
		map <string, MyDB_TableReaderWriterPtr> &allTableReaderWritersIn,
		map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWritersIn,
		vector <ExprTreePtr> &valuesToSelectIn,
		vector <pair <string, string>> &tablesToProcessIn,
		vector <ExprTreePtr> &allDisjunctionsIn,
		vector <ExprTreePtr> &groupingClausesIn):
		allTables(allTablesIn),
		allTableReaderWriters(allTableReaderWritersIn),
		allBPlusReaderWriters(allBPlusReaderWritersIn),
		valuesToSelect(valuesToSelectIn),
		tablesToProcess(tablesToProcessIn),
		allDisjunctions(allDisjunctionsIn),
		groupingClauses(groupingClausesIn) {
}
#endif