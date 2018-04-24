#ifndef RELATIONAL_ALGEBRA_EXPRESSIONS_CC
#define RELATIONAL_ALGEBRA_EXPRESSIONS_CC

#include "RelAlgExpr.h"
#include <vector>

/*	
	initialized the static variables
*/

vector <int> RelAlgExpr :: availableIds;
int RelAlgExpr :: tableId = 0;

/*
	------------------------
	Redefine the input table 
	------------------------
*/
Table :: Table(MyDB_TableReaderWriterPtr tableIn, string tableAlaisIn) {	
	table = tableIn;
	tableAlais = tableAlaisIn;
}

MyDB_TableReaderWriterPtr Table :: run() {

	// define the output record format
	MyDB_SchemaPtr schemaOut = make_shared <MyDB_Schema> ();
	vector <string> projections;
	for (auto a : table->getTable()->getSchema()->getAtts()) {
		if (tableAlais != "") {
			schemaOut->appendAtt(make_pair(tableAlais + "_" + a.first, a.second));
		} else {
			schemaOut->appendAtt(make_pair(a.first, a.second));
		}
		projections.push_back("[" + a.first + "]");
	}

	// get the current output table name
	string tableOutName = "table" + to_string(RelAlgExpr :: getId());
	
	// define the output table and tableReaderWriter (ptr)
	MyDB_TablePtr tableOut = 
		make_shared <MyDB_Table> (tableOutName, tableOutName + ".bin", schemaOut);
	MyDB_TableReaderWriterPtr output = 
		make_shared <MyDB_TableReaderWriter> (tableOut, table->getBufferMgr());

	RegularSelection op (table, output, "bool[true]", projections);
	op.run();

	return output;
}

/*
	---------------
	SingleSelection
	---------------
*/
SingleSelection :: SingleSelection(RelAlgExprPtr tableIn, 
		vector <ExprTreePtr> valuesToSelectIn,
		vector <ExprTreePtr> allDisjunctionsIn) {

	table = tableIn;
	valuesToSelect = valuesToSelectIn;
	allDisjunctions = allDisjunctionsIn;
}


MyDB_TableReaderWriterPtr SingleSelection :: run() {

	cout << "RelAlgExpr.cc : SingleSelection.run()" << endl;
	cout << endl;

	string selectionPredicate = "bool[true]";
	for (auto a : allDisjunctions) {
		selectionPredicate = "&& (" + a->toString() + ", " + selectionPredicate + ")";
	}

	cout << "RelAlgExpr.cc : SingleSelection.run() CNF clauses are" << endl;
	cout << selectionPredicate << endl;
	cout << endl;

	// get the current output table name
	string tableOutName = "table" + to_string(RelAlgExpr :: getId());

	// define the output record format
	MyDB_SchemaPtr schemaOut = make_shared <MyDB_Schema> ();
	int i = 0;
	for (auto a : valuesToSelect) {
		MyDB_AttTypePtr type = a->getAttType();
		if (type == nullptr) {
			cout << "RelAlgExpr.cc : Error to get " << a->toString() << " MyDB_AttTypePtr"  << endl;
			exit(-1);
		}

		string colName = "";
		if (a->isIdentifierAtt()) {
			string name = a->toString();
			colName = name.substr(1, name.length()-2);
		} else {
			colName = tableOutName + "_att_" + to_string(i++);
		}

		if (colName == "") {
			cout << "[Error SingleSelection.run()] cannot find the colName of " + a->toString() << endl;
			exit(-1);
		}
		// (attribute1, type1), (attribute2, type2) ...
		schemaOut->appendAtt(make_pair(colName, type));
	}

	cout << "SingleSelection.run() output schema is : " << schemaOut << endl;
	cout << endl;
	
	// get the input table
	MyDB_TableReaderWriterPtr input = table->run();

	// define the output table and tableReaderWriter (ptr)
	MyDB_TablePtr tableOut = 
		make_shared <MyDB_Table> (tableOutName, tableOutName + ".bin", schemaOut);
	MyDB_TableReaderWriterPtr output = 
		make_shared <MyDB_TableReaderWriter> (tableOut, input->getBufferMgr());


	vector <string> projections;
	for (auto a : valuesToSelect) {
		projections.push_back(a->toString());
	}

	// selection run
	RegularSelection op (input, output, selectionPredicate, projections);
	op.run();

	// retur
	return output;
}

string SingleSelection :: toString() {

}



/*
	---------------
	Aggregation for selection
	---------------
*/
AggregateSelection :: AggregateSelection(RelAlgExprPtr tableIn,
 		vector <pair< pair<MyDB_AggType, string>, MyDB_AttTypePtr>> aggsToComputeIn,
 		vector <ExprTreePtr> groupingClausesIn) {
	table = tableIn;
	aggsToCompute = aggsToComputeIn;
	groupingClauses = groupingClausesIn;
}

MyDB_TableReaderWriterPtr AggregateSelection :: run() {

	cout << "RelAlgExpr.cc : AggregationSelection.run()" << endl;
	cout << endl;

	vector <pair<MyDB_AggType, string>> newAggsToCompute;
	vector <string> newGroupingClauses;

	// get the current output table name
	string tableOutName = "table" + to_string(RelAlgExpr :: getId());

	// define the output record format
	MyDB_SchemaPtr schemaOut = make_shared <MyDB_Schema> ();
	int i = 0;
	for (auto a : groupingClauses) {

		newGroupingClauses.push_back(a->toString());

		MyDB_AttTypePtr type = a->getAttType();
		if (type == nullptr) {
			cout << "[Error AggregateSelection.run() ]" << a->toString() << " MyDB_AttTypePtr"  << endl;
			exit(-1);
		}
		string colName = "";
		if (a->isIdentifierAtt()) {
			string name = a->toString();
			colName = name.substr(1, name.length()-2);
		} else {
			colName = tableOutName + "_att_" + to_string(i++);
		}

		if (colName == "") {
			cout << "[Error SingleSelection.run()] cannot find the colName of " + a->toString() << endl;
			exit(-1);
		}
		// (attribute1, type1), (attribute2, type2) ...
		schemaOut->appendAtt(make_pair(colName, type));
	}

	cout << "[AggregateSelection.run():] schemaOut is " << endl;
	cout << schemaOut << endl;
	cout << endl;

	for (auto a : aggsToCompute) {
		newAggsToCompute.push_back(a.first);
		schemaOut->appendAtt(make_pair(tableOutName + "_att_" + to_string(i++), a.second));
	}

	cout << "[AggregateSelection.run():] schemaOut with aggregate is " << endl;
	cout << schemaOut << endl;
	cout << endl;

	// get the input table
	MyDB_TableReaderWriterPtr input = table->run();
	// define the output table and tableReaderWriter (ptr)
	MyDB_TablePtr tableOut = 
		make_shared <MyDB_Table> (tableOutName, tableOutName + ".bin", schemaOut);
	MyDB_TableReaderWriterPtr output = 
		make_shared <MyDB_TableReaderWriter> (tableOut, input->getBufferMgr());

	Aggregate op (input, output, newAggsToCompute, newGroupingClauses, "bool[true]");
	op.run();

	return output;
}

string AggregateSelection :: toString() {

}


int RelAlgExpr :: getId() {

	int id;
	if (availableIds.empty()) {
		id = tableId++;
	} else {
		id = availableIds.back();
		availableIds.pop_back();
	}
	return id;
}
#endif
