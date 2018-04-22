#ifndef OPTIMIZER
#define OPTIMIZER

#include "ExprTree.h"
#include "RelAlgExpr.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_BPlusTreeReaderWriter.h"


class Optimizer {
public:
	Optimizer (map <string, MyDB_TablePtr> &allTables,
		map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters,
		vector <ExprTreePtr> &valuesToSelect,
		vector <pair <string, string>> &tablesToProcess,
		vector <ExprTreePtr> &allDisjunctions,
		vector <ExprTreePtr> &groupingClauses);

	MyDB_TableReaderWriterPtr opt(vector <ExprTreePtr> &allDisjunctions, 
		vector <pair <string, string>> &tablesToProcess,
		vector <ExprTreePtr> &valuesToSelect);

	MyDB_TableReaderWriterPtr getTable (string name);

private:
	map <string, MyDB_TablePtr> &allTables;
	map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters;
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters;
	
	vector <ExprTreePtr> &valuesToSelect;
	vector <pair <string, string>> &tablesToProcess;
	vector <ExprTreePtr> &allDisjunctions;
	vector <ExprTreePtr> &groupingClauses;
};

#endif