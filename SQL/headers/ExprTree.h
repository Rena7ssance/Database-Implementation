
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include <string>
#include <vector>

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;


// Roy7wt
enum ExprType
{
	INT_TYPE,
	DOUBLE_TYPE,
	STRING_TYPE,
	BOOL_TYPE,
	ERROR_TYPE
};

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {


public:
	virtual string toString () = 0;
	virtual ~ExprTree () {}

	// Roy7wt
	virtual ExprType getExprType() = 0;

	// void getAttType() {
	// 	return attType;
	// }
	// void setAttType (MyDB_AttTypePtr attTypeIn) {
	// 	attType = attTypeIn;
	// }
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

	ExprType getExprType() {
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

	ExprType getExprType() {
		return DOUBLE_TYPE;
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

	ExprType getExprType() {
		return INT_TYPE;	
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

 	ExprType getExprType() {
		return STRING_TYPE;	
	}

	~StringLiteral () {}
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
	}

	string toString () {
		return "[" + tableName + "_" + attName + "]";
	}	

	ExprType getExprType() {
		if (attName == "int") {
			return INT_TYPE;
		} else if (attName == "double") {
			return DOUBLE_TYPE;
		} else if (attName == "string") {
			return STRING_TYPE;
		} else if (attName == "bool") {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == INT_TYPE && rt == INT_TYPE) {
			return INT_TYPE;
		} else if (lt == DOUBLE_TYPE && rt == DOUBLE_TYPE) {
			return DOUBLE_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == INT_TYPE && rt == INT_TYPE) {
			return INT_TYPE;
		} else if (lt == DOUBLE_TYPE && rt == DOUBLE_TYPE) {
			return DOUBLE_TYPE;
		} else if (lt == STRING_TYPE && rt == STRING_TYPE) {
			return STRING_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == INT_TYPE && rt == INT_TYPE) {
			return INT_TYPE;
		} else if (lt == DOUBLE_TYPE && rt == DOUBLE_TYPE) {
			return DOUBLE_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == INT_TYPE && rt == INT_TYPE) {
			return INT_TYPE;
		} else if (lt == DOUBLE_TYPE && rt == DOUBLE_TYPE) {
			return DOUBLE_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == rt) {
			if (lt == BOOL_TYPE || lt == ERROR_TYPE) {
				return ERROR_TYPE;
			} else return BOOL_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == rt) {
			if (lt == BOOL_TYPE || lt == ERROR_TYPE) {
				return ERROR_TYPE;
			} else return BOOL_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == rt) {
			if (lt == BOOL_TYPE || lt == ERROR_TYPE) {
				return ERROR_TYPE;
			} else return BOOL_TYPE;
		} else return ERROR_TYPE;
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


	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == BOOL_TYPE && rt == BOOL_TYPE) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType lt = lhs->getExprType();
		ExprType rt = rhs->getExprType();
		if (lt == BOOL_TYPE && rt == BOOL_TYPE) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType ct = child->getExprType();
		if (ct == BOOL_TYPE) {
			return BOOL_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType ct = child->getExprType();
		if (ct == INT_TYPE) {
			return INT_TYPE;
		} else if (ct == DOUBLE_TYPE) {
			return DOUBLE_TYPE;
		} else return ERROR_TYPE;
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

	ExprType getExprType() {
		ExprType ct = child->getExprType();
		if (ct == INT_TYPE) {
			return INT_TYPE;
		} else if (ct == DOUBLE_TYPE) {
			return DOUBLE_TYPE;
		} else return ERROR_TYPE;
	}

	~AvgOp () {}
};

#endif
