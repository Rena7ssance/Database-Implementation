#ifndef RELATIONAL_ALGEBRA_EXPRESSIONS_H
#define RELATIONAL_ALGEBRA_EXPRESSIONS_H


#include "MyDB_TableReaderWriter.h"
#include "MyDB_Schema.h"
#include "ExprTree.h"

#include "RegularSelection.h"
#include "Aggregate.h"

using namespace std;

class RelAlgExpr;
typedef shared_ptr <RelAlgExpr> RelAlgExprPtr;


class RelAlgExpr {

public :
	virtual string toString() = 0;
	virtual MyDB_TableReaderWriterPtr run() = 0;
	virtual ~RelAlgExpr() {}

	// attributes to generate the output file id
	static vector <int> availableIds;
	static int tableId;
	static int getId ();
};

class Table : public RelAlgExpr {

public :
	Table (MyDB_TableReaderWriterPtr tableIn, string tableAlaisIn);
	MyDB_TableReaderWriterPtr run();

	string toString() {
		return table->getTable()->getName();
	}

	~Table () {}

private :
	MyDB_TableReaderWriterPtr table;
	string tableAlais;
};


class SingleSelection : public RelAlgExpr {

public:
	// constructor
	SingleSelection (RelAlgExprPtr tableIn, 
		vector <ExprTreePtr> valuesToSelectIn,
		vector <ExprTreePtr> allDisjunctionsIn);

	// return the sql run out table
	MyDB_TableReaderWriterPtr run();

	string toString();

	~ SingleSelection() {}

private:
	// private fields
	RelAlgExprPtr table;
	vector <ExprTreePtr> valuesToSelect;
	vector <ExprTreePtr> allDisjunctions;
};


class AggregateSelection : public RelAlgExpr {

public :
 	// constructor
 	AggregateSelection (RelAlgExprPtr tableIn,
 		vector <pair< pair<MyDB_AggType, string>, MyDB_AttTypePtr>> aggsToComputeIn,
 		vector <ExprTreePtr> groupingClausesIn);

 	// return the sql run out table
	MyDB_TableReaderWriterPtr run();

	string toString();

	~ AggregateSelection() {}

private :
	RelAlgExprPtr table;
 	vector <pair< pair<MyDB_AggType, string>, MyDB_AttTypePtr>> aggsToCompute;
 	vector <ExprTreePtr> groupingClauses;
};
#endif