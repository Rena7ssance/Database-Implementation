#ifndef OPTIMIZER_CC
#define OPTIMIZER_CC

#include "Optimizer.h"
#include "ExprTree.h"
#include "RelAlgExpr.h"
#include <iostream>

#define OUTPUT_PATH "my_db_output.txt"

using namespace std;

Optimizer :: Optimizer(map <string, MyDB_TablePtr> allTablesIn,
		map <string, MyDB_TableReaderWriterPtr> allTableReaderWritersIn,
		map <string, MyDB_BPlusTreeReaderWriterPtr> allBPlusReaderWritersIn,
		vector <ExprTreePtr> valuesToSelectIn,
		vector <pair <string, string>> tablesToProcessIn,
		vector <ExprTreePtr> allDisjunctionsIn,
		vector <ExprTreePtr> groupingClausesIn) {

			// allTables(allTablesIn),
			// allTableReaderWriters(allTableReaderWritersIn),
			// allBPlusReaderWriters(allBPlusReaderWritersIn),
			// valuesToSelect(valuesToSelectIn),
			// tablesToProcess(tablesToProcessIn),
			// allDisjunctions(allDisjunctionsIn),
			// groupingClauses(groupingClausesIn)

		allTables= allTablesIn;
		allTableReaderWriters = allTableReaderWritersIn;
		allBPlusReaderWriters = allBPlusReaderWritersIn;
		valuesToSelect = valuesToSelectIn;
		tablesToProcess = tablesToProcessIn;
		allDisjunctions = allDisjunctionsIn;
		groupingClauses = groupingClausesIn;

}

void Optimizer :: execute() {

	cout << "Optimizer.execute()" << endl;
	cout << endl;

	// verify the attributes among the selection clause 
	// and verify the aggregation computations
	vector <ExprTreePtr> newValuesToSelect;
	vector < pair <pair <MyDB_AggType, string>, MyDB_AttTypePtr>> aggsToCompute; // e.g. MyDB_AggType :: avg, "*([r_suppkey], double[1.0])"
	for (auto a : valuesToSelect) {
		a->getAtt(newValuesToSelect);

		if (a->isAggregateAtt()) {
			pair <string, string> agg = a->getAggregateAtt();
			string aggType = agg.first;
			string aggToCompute = agg.second;

			if (aggType == "sum") {
				aggsToCompute.push_back(make_pair(make_pair(MyDB_AggType :: sumType, aggToCompute), a->getAttType()));
				cout << "shit: " + aggToCompute << endl;
			} else if (aggType == "avg") {
				aggsToCompute.push_back(make_pair(make_pair(MyDB_AggType :: avgType, aggToCompute), a->getAttType()));
			} else {
				cout << "[Error. Optimizer.execute()]: " << a->toString() << " unknown aggType" << endl;
				exit(-1);
			}
		}
	}

	cout << "[Optimizer.execute(): the size of aggsToCompute is: " + to_string(aggsToCompute.size()) + "]"<< endl; 
	cout << "[Optimizer.execute(): the list of attributes in the selection are: ]" << endl;
	for (auto a : newValuesToSelect) {
		cout << a->toString() << endl;
	}
	cout << endl;


	// reduce the number of  
	pair <RelAlgExprPtr, int> reducedRA = optimize(newValuesToSelect);
	RelAlgExprPtr expr = reducedRA.first;

	if (aggsToCompute.size() != 0) { // compute aggregation
		expr = make_shared <AggregateSelection> (expr, aggsToCompute, groupingClauses);
	}

	
	MyDB_TableReaderWriterPtr output = expr->run();

	// MyDB_Schema schemeOut = output->getTable()->getSchema();
	vector< pair<string, MyDB_AttTypePtr>> tempOutputAtts = 
			output->getTable()->getSchema()->getAtts(); // (g0)(g1)...(gn)(a0)...(an)

	// reconstruct the attribute to be select
	newValuesToSelect.clear();
	int i = groupingClauses.size();

	cout << "-------" << endl;
	string tableName = "";
	for (auto a : valuesToSelect) { 
		if (a->isAggregateAtt()) {
			pair <string, MyDB_AttTypePtr> outputAtt = tempOutputAtts[i++];
			string attName = outputAtt.first;
			cout << attName << endl;
			ExprTreePtr newAttToBeSelect = make_shared <Identifier> (tableName, attName);
			newAttToBeSelect->setAttType(outputAtt.second);
			newValuesToSelect.push_back(newAttToBeSelect);
		} else {
			newValuesToSelect.push_back(a);
		}
	}
	cout << "-------" << endl;

	cout <<  "[Optimizer.execute(): the list of attributes in the reselection are: ]" << endl;
	for (auto a : newValuesToSelect) {
		cout << a->toString() << endl;
	}
	cout << endl;


	RelAlgExprPtr reTableIn = make_shared <Table> (output, "");
	expr = make_shared <SingleSelection> (reTableIn, newValuesToSelect, vector <ExprTreePtr>());

	cout << "[Optimizer.execute(): rerun]" << endl;
	cout << endl;

	output = expr->run();
	output->writeIntoTextFile (OUTPUT_PATH);
	MyDB_RecordPtr temp = output->getEmptyRecord ();
	MyDB_RecordIteratorAltPtr myIter = output->getIteratorAlt ();
	int record_num = 0;
	cout << "The first 30 records: " << endl;
	while (myIter->advance ()) {
		myIter->getCurrent (temp);
		if (record_num < 30) {
			cout << temp << "\n";
		}
		++ record_num;
	}
	cout << "Total number of records: " << record_num << endl;
	//*/
}

pair <RelAlgExprPtr, int> Optimizer :: optimize(vector <ExprTreePtr> newValuesToSelect) {

	cout << "Optimizer.optimize()" << endl;

	if (tablesToProcess.size() == 1) { // Singele Selection
		string tableName = tablesToProcess[0].first;
		string tableAlais = tablesToProcess[0].second;
		MyDB_TableReaderWriterPtr table = Optimizer :: getTable(tableName);
		if (table == nullptr) {
			cout << "[optimize().Error] : cannot find the MyDB_TableReaderWriterPtr " + tableName << endl;
			exit(-1);
		}
		RelAlgExprPtr tableIn = make_shared <Table> (table, tableAlais);
		RelAlgExprPtr op = make_shared <SingleSelection> (tableIn, newValuesToSelect, allDisjunctions);
		return make_pair (op, 0);
	} else {

	}

	return make_pair(nullptr, 0);
}

MyDB_TableReaderWriterPtr Optimizer :: getTable(string tableName) {
	if (allTables[tableName]->getFileType() == "heap") {
		return allTableReaderWriters[tableName];
	} else if (allTables[tableName]->getFileType() == "bplustree") {
		return allBPlusReaderWriters[tableName];
	} else {
		cout << "Optimizer.cc getTabel() : cannot find table " << tableName << endl;
		return nullptr;
 	}
}

#endif