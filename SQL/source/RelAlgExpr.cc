#ifndef RELATIONAL_ALGEBRA_EXPRESSIONS_CC
#define RELATIONAL_ALGEBRA_EXPRESSIONS_CC

#include "RelAlgExpr.h"
#include "MyDB_Schema.h"
#include "RegularSelection.h"



// --------------------
Table :: Table(MyDB_TableReaderWriterPtr tableIn, string tableAlaisIn) {	
	table = tableIn;
	tableAlais = tableAlaisIn;
}

MyDB_TableReaderWriterPtr Table :: run() {

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

	MyDB_TablePtr tableOut = make_shared <MyDB_Table> ("roy7wt", "roy7wt.bin", schemaOut);
	MyDB_TableReaderWriterPtr output = make_shared <MyDB_TableReaderWriter> (tableOut, table->getBufferMgr());


	RegularSelection op (table, output, "bool[true]", projections);
	op.run();

	return output;
}

#endif
