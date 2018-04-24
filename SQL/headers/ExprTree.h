
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include "MyDB_Catalog.h"
#include <string>
#include <vector>
#include <algorithm>


// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

enum ExprType {
	NUMBER_TYPE, 
	BOOL_TYPE, 
	STRING_TYPE, 
	ERROR_TYPE
};


// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree : public enable_shared_from_this <ExprTree> {

public:
	virtual string toString () = 0;
	virtual ~ExprTree () {}

	// Roy7wt
	virtual bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses 
		) = 0;

	virtual ExprType getType() = 0;

	virtual void getAtt (vector <ExprTreePtr> &vec) {};
	virtual bool isIdentifierAtt() {return false;}
	virtual bool isAggregateAtt() {return false;}
	virtual pair <string, string> getAggregateAtt() {return make_pair("", "");};


	MyDB_AttTypePtr getAttType() {
		return attType;
	}

	void setAttType(MyDB_AttTypePtr attTypeIn) {
		attType = attTypeIn;
	}

	void setAttType () {
		switch (getType()) {
			case NUMBER_TYPE : {
				attType = make_shared <MyDB_DoubleAttType> ();
				break;
			}
			case STRING_TYPE : {
				attType = make_shared <MyDB_StringAttType> ();
				break;
			} 
			case BOOL_TYPE : {
				attType = make_shared <MyDB_BoolAttType> ();
				break;
			} 
			default: {
				return;
			}
		}
	}

	string type2Str() {
		switch (getType()) {
			case NUMBER_TYPE : {
				return "number";
			}
			case STRING_TYPE: {
				return "string";
			} 
			case BOOL_TYPE : {
				return "bool";
			} 
			default: {
				return "nullptr";
			}
		}
	}

protected :
	MyDB_AttTypePtr attType;
};

class BoolLiteral : public ExprTree {

private:
	bool myVal;
public:
	
	BoolLiteral (bool fromMe) {
		myVal = fromMe;
	}

	string toString () {
		if (myVal) {
			return "bool[true]";
		} else {
			return "bool[false]";
		}
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		// attType = make_shared <MyDB_BoolAttType> ();
		setAttType();

		return true;
	}

	ExprType getType() {
		return BOOL_TYPE;
	}
};

class DoubleLiteral : public ExprTree {

private:
	double myVal;
public:

	DoubleLiteral (double fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "double[" + to_string (myVal) + "]";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		// attType = make_shared <MyDB_DoubleAttType> ();
		setAttType();

		return true;
	}

	ExprType getType() {
		return NUMBER_TYPE;
	}

	~DoubleLiteral () {}
};

// this implement class ExprTree
class IntLiteral : public ExprTree {

private:
	int myVal;
public:

	IntLiteral (int fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "int[" + to_string (myVal) + "]";
	}

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		// attType = make_shared <MyDB_IntAttType> ();
		setAttType();

		return true;
	}

	ExprType getType() {
		return NUMBER_TYPE;
	}

	~IntLiteral () {}
};

class StringLiteral : public ExprTree {

private:
	string myVal;
public:

	StringLiteral (char *fromMe) {
		fromMe[strlen (fromMe) - 1] = 0;
		myVal = string (fromMe + 1);
	}

	string toString () {
		return "string[" + myVal + "]";
	}

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		// attType = make_shared <MyDB_StringAttType> ();
		setAttType();

		return true;
	}

	ExprType getType() {
		return STRING_TYPE;
	}
	~StringLiteral () {}
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
	string attType;
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
		attType = string ("");
	}

	Identifier (string tableNameIn, string attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
		attType = string ("");
	}

	string toString () {
		if (tableName == "") {
			return "[" + attName + "]";
		} else {
		 	return "[" + tableName + "_" + attName + "]";
		}
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		vector<string> tables;
		catalog->getStringList("tables", tables);

		string tableRealName;
		for (auto table : tablesToProcess) {
			if (table.second == tableName) {

				// get the real name of table
				tableRealName = table.first; 
				break;
			}
		}

		if (tableRealName == "") { // the alias of table name is not in the catalog
			cout << "[SemanticError: table alias (" << tableName << ")does not exist in the database]" << endl;
			return false;
		}

		// check the attributes
		vector<string> atts;
		string key = tableRealName + ".attList";
		catalog->getStringList(key, atts);
		
		if (std :: find(atts.begin(), atts.end(), attName) != atts.end()) {
			/* attName contains in the table*/
			attType = "";
			string attKey = tableRealName + "." + attName + ".type";
			catalog->getString(attKey, attType);
		} else {
			/* attName does not contain in the table*/
			cout << "[SemanticError: Attributes (" << attName << ") does not exist in (" << tableRealName << ") table]"  << endl;
			return false;
		}

		// check for the group when the identifier is in the valuesToSelect
		if (!groupingClauses.empty()) {
			/* this is an aggregation query*/

			bool attInGroupAtt = false;
			for (auto groupingClause : groupingClauses) {
				if (groupingClause->toString() == toString()) {
					attInGroupAtt = true;
					break;
				}
			}

			if (!attInGroupAtt) {
				// in the case of an aggregation query, the att is not functions of grouping attributes
				cout << "[SemanticError: In the case of an aggregation query, the selected attribute " << toString() << " is not functions of grouping attributes]" << endl;
				return false;
			}
		}

		setAttType();
		return true;
	}

	ExprType getType() {
		if (attType == "int" || attType == "double") return NUMBER_TYPE;
		if (attType == "string") return STRING_TYPE;
		if (attType == "bool") return BOOL_TYPE;
		return ERROR_TYPE;
	}
 	
 	bool isIdentifierAtt() {
 		return true;
 	}
 	void getAtt(vector <ExprTreePtr> &vec) {
 		vec.push_back(shared_from_this());
 	}

	~Identifier () {}
};

class MinusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	MinusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			setAttType();
			return true;
		} else if (lhsType == NUMBER_TYPE && rhsType != NUMBER_TYPE) {
			cout << "[SemanticError: type mismatches that " << rhs->toString() << " is not number type]" << endl;
			return false;
		} else if (lhsType != NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			cout << "[SemanticError: type mismatches that " << lhs->toString() << " is not number type]" << endl;
			return false;
		} else {
			cout << "[SemanticError: type mismatches that " << lhs->toString() << " and " << rhs->toString() << " are neither number type]" << endl;
			return false;
		}
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();
		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			return NUMBER_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~MinusOp () {}
};

class PlusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	PlusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {
		
		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			setAttType();
			return true;
		}
		if (lhsType == STRING_TYPE && rhsType == STRING_TYPE) {
			setAttType();
			return true;
		}

		cout << "[SemanticError: type mismatches that " << lhs->toString() << " and " << rhs->toString() << "]" << endl;
		return false;
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			return NUMBER_TYPE;
		} else if (lhsType == STRING_TYPE && rhsType == STRING_TYPE) {
			return STRING_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~PlusOp () {}
};

class TimesOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	TimesOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "* (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {
		
		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			setAttType();
			return true;
		} else if (lhsType == NUMBER_TYPE && rhsType != NUMBER_TYPE) {
			cout << "[SemanticError: type mismatches that " << rhs->toString() << " is not number type]" << endl;
			return false;
		} else if (lhsType != NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			cout << "[SemanticError: type mismatches that " << lhs->toString() << " is not number type]" << endl;
			return false;
		} else {
			cout << "[SemanticError: type mismatches that " << lhs->toString() << " and " << rhs->toString() << " are neither number type]" << endl;
			return false;
		}
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();
		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			return NUMBER_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~TimesOp () {}
};

class DivideOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	DivideOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "/ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {
		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			setAttType();
			return true;
		} else if (lhsType == NUMBER_TYPE && rhsType != NUMBER_TYPE) {
			cout << "[SemanticError: type mismatches that " << rhs->toString() << " is not number type]" << endl;
			return false;
		} else if (lhsType != NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			cout << "[SemanticError: type mismatches that " << lhs->toString() << " is not number type]" << endl;
			return false;
		} else {
			cout << "[SemanticError: type mismatches that " << lhs->toString() << " and " << rhs->toString() << " are neither number type]" << endl;
			return false;
		}
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();
		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			return NUMBER_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~DivideOp () {}
};

class GtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	GtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "> (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {
		
		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			setAttType();
			return true;
		}
		if (lhsType == STRING_TYPE && rhsType == STRING_TYPE) {
			setAttType();
			return true;
		}

		cout << "[SemanticError: type mismatches that " << lhs->toString() << " and " << rhs->toString() << "]" << endl;
		return false;
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			return BOOL_TYPE;
		} else if (lhsType == STRING_TYPE && rhsType == STRING_TYPE) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~GtOp () {}
};

class LtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	LtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "< (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {
		
		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			setAttType();
			return true;
		}
		if (lhsType == STRING_TYPE && rhsType == STRING_TYPE) {
			setAttType();
			return true;
		}

		cout << "[SemanticError: type mismatches that " << lhs->toString() << " and " << rhs->toString() << "]" << endl;
		return false;
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == NUMBER_TYPE && rhsType == NUMBER_TYPE) {
			return BOOL_TYPE;
		} else if (lhsType == STRING_TYPE && rhsType == STRING_TYPE) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~LtOp () {}
};

class NeqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	NeqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {
		
		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();
		if (lhsType != rhsType) {
			cout << "[SemanticError: type mismatches that " << lhs->toString() << " and " << rhs->toString() << "]" << endl;
			return false;
		}
		setAttType();
		return true;
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();
		if (lhsType == rhsType) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~NeqOp () {}
};

class OrOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	OrOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "|| (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType != BOOL_TYPE) {
			cout << "[SemanticError: the left expression of OR that " << lhs->toString() << " is not a bool type]" << endl;
			return false;
		}else if (rhsType != BOOL_TYPE) {
			cout << "[SemanticError: the right expression of OR that " << rhs->toString() << " is not a bool type]" << endl;
			return false;
		}
		setAttType();
		return true;
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();

		if (lhsType == BOOL_TYPE && rhsType == BOOL_TYPE) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~OrOp () {}
};

class EqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	EqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "== (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		// check for identifiers
		if (!lhs->checkFunc(catalog, tablesToProcess, groupingClauses) || !rhs->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();
		if (lhsType != rhsType) {
			cout << "[SemanticError: type mismatches that " << lhs->toString() << " and " << rhs->toString() << "]" << endl;
			return false;
		}
		setAttType();
		return true;
	}

	ExprType getType() {
		ExprType lhsType = lhs->getType();
		ExprType rhsType = rhs->getType();
		if (lhsType == rhsType) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		lhs->getAtt(vec);
		rhs->getAtt(vec);
 	}

	~EqOp () {}
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "!(" + child->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		// check for identifiers
		if (!child->checkFunc(catalog, tablesToProcess, groupingClauses)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType childType = child->getType();
		if (childType != BOOL_TYPE) {
			cout << "[SemanticError: the expression " << child->toString() << " is not bool type]" << endl;
			return false;
		}
		setAttType();
		return true;
	}

	ExprType getType() {
		ExprType childType = child->getType();
		if (childType == BOOL_TYPE) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		child->getAtt(vec);
 	}

	~NotOp () {}
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "sum(" + child->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {

		vector <ExprTreePtr> emptyVec;
		// check for identifiers
		if (!child->checkFunc(catalog, tablesToProcess, emptyVec)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType childType = child->getType();
		if (childType != NUMBER_TYPE) {
			cout << "[SemanticError: expression " << child->toString() << " is not number type, failing to sum]" << endl;
			return false;
		}
		setAttType();
		return true;
	}

	ExprType getType() {
		ExprType childType = child->getType();
		if (childType == NUMBER_TYPE) {
			return NUMBER_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		child->getAtt(vec);
 	}

 	bool isAggregateAtt() {
 		return true;
 	}

 	pair <string, string> getAggregateAtt() {
 		return make_pair("sum", child->toString());
 	}

	~SumOp () {}
};

class AvgOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	AvgOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "avg(" + child->toString () + ")";
	}	

	bool checkFunc(
		MyDB_CatalogPtr catalog,
		vector <pair <string, string>> tablesToProcess,
		vector <ExprTreePtr> groupingClauses ) {
		
		vector <ExprTreePtr> emptyVec;
		// check for identifiers
		if (!child->checkFunc(catalog, tablesToProcess, emptyVec)) {
			// one of the identifiers is wrong
			return false;
		}

		ExprType childType = child->getType();
		if (childType != NUMBER_TYPE) {
			cout << "[SemanticError: expression " << child->toString() << " is not number type, failing to sum]" << endl;
			return false;
		}
		setAttType();
		return true;
	}

	ExprType getType() {
		ExprType childType = child->getType();
		if (childType == NUMBER_TYPE) {
			return NUMBER_TYPE;
		} else return ERROR_TYPE;
	}

	void getAtt(vector <ExprTreePtr> &vec) {	
		child->getAtt(vec);
 	}

 	bool isAggregateAtt() {
 		return true;
 	}

 	pair <string, string> getAggregateAtt() {
 		return make_pair("avg", child->toString());
 	}

	~AvgOp () {}
};

#endif
