#ifndef RELATIONAL_ALGEBRA_EXPRESSIONS_H
#define RELATIONAL_ALGEBRA_EXPRESSIONS_H


#include "MyDB_TableReaderWriter.h"
#include "ExprTree.h"

using namespace std;

class RelAlgExpr;
typedef shared_ptr <RelAlgExpr> RelAlgExprPtr;

struct stats {
	size_t T;
	vector < pair <string, size_t>> V;
};



class RelAlgExpr {

public :
	virtual string toString() = 0;
	virtual MyDB_TableReaderWriterPtr run() = 0;
	virtual ~RelAlgExpr() {}

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

#endif