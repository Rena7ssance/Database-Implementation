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

	MyDB_TablePtr tableOut = make_shared <MyDB_Table> ("asdfasdf", "asdfasdf.bin", schemaOut);
	MyDB_TableReaderWriterPtr output = make_shared <MyDB_TableReaderWriter> (tableOut, table->getBufferMgr());


	RegularSelection op (table, output, "&& (&& (== ([l_shipdate], string[1994-05-12]), == ([l_commitdate], string[1994-05-22])), == ([l_receiptdate], string[1994-06-10]))", projections);
	op.run();

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

	return output;
}

#endif
