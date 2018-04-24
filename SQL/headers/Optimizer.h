#ifndef OPTIMIZER
#define OPTIMIZER

#include "RelAlgExpr.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_BPlusTreeReaderWriter.h"


class Optimizer {
public:
	Optimizer (map <string, MyDB_TablePtr> allTables,
		map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr> allBPlusReaderWriters,
		vector <ExprTreePtr> valuesToSelect,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> allDisjunctions,
		vector <ExprTreePtr> groupingClauses);


	void execute();
	pair <RelAlgExprPtr, int> optimize(vector <ExprTreePtr> newValuesToSelect);

	MyDB_TableReaderWriterPtr getTable(string tableName);

private:

	// private fields
	// map <string, MyDB_TablePtr> &allTables;
	// map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters;
	// map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters;	
	// vector <ExprTreePtr> &valuesToSelect;
	// vector <pair <string, string>> &tablesToProcess;
	// vector <ExprTreePtr> &allDisjunctions;
	// vector <ExprTreePtr> &groupingClauses;
	map <string, MyDB_TablePtr> allTables;
	map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters;
	map <string, MyDB_BPlusTreeReaderWriterPtr> allBPlusReaderWriters;	
	vector <ExprTreePtr> valuesToSelect;
	vector <pair <string, string>> tablesToProcess;
	vector <ExprTreePtr> allDisjunctions;
	vector <ExprTreePtr> groupingClauses;
};

#endif