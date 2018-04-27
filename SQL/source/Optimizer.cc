#ifndef OPTIMIZER_CC
#define OPTIMIZER_CC

#include "Optimizer.h"
#include "ExprTree.h"
#include "RelAlgExpr.h"
#include <iostream>
#include <math.h>       /* pow */

#define OUTPUT_PATH "my_db_output.txt"

using namespace std;

Optimizer :: Optimizer(map <string, MyDB_TablePtr> allTablesIn,
		map <string, MyDB_TableReaderWriterPtr> allTableReaderWritersIn,
		map <string, MyDB_BPlusTreeReaderWriterPtr> allBPlusReaderWritersIn,
		vector <ExprTreePtr> valuesToSelectIn,
		vector <pair <string, string>> tablesToProcessIn,
		vector <ExprTreePtr> allDisjunctionsIn,
		vector <ExprTreePtr> groupingClausesIn) {

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
	pair <RelAlgExprPtr, int> reducedRA = optimize(allDisjunctions, tablesToProcess, newValuesToSelect);
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

	cout << "[Optimizer.execute(): re-run]" << endl;
	cout << expr->toString() << endl;

	output = expr->run();
	output->writeIntoTextFile (OUTPUT_PATH);
	MyDB_RecordPtr rec = output->getEmptyRecord ();
	MyDB_RecordIteratorAltPtr iter = output->getIteratorAlt ();
	int count = 0;
	while (iter->advance ()) {
		iter->getCurrent (rec);
		if (count++ < 30) {
			cout << rec << endl;
		}
	}
	cout << "The number of query result record is " << count << endl;
}

pair <RelAlgExprPtr, int> Optimizer :: optimize(
	vector <ExprTreePtr> cnf, // allDisjunctions
	vector <pair <string, string>> tablesToProcess,
	vector <ExprTreePtr> newValuesToSelect) {

	// cout << "Optimizer.optimize()" << endl;

	if (tablesToProcess.size() == 1) { // Singele Selection
		string tableName = tablesToProcess[0].first;
		string tableAlais = tablesToProcess[0].second;
		MyDB_TableReaderWriterPtr table = Optimizer :: getTableByName(tableName);

		if (table == nullptr) {
			cout << "[optimize().Error] : cannot find the MyDB_TableReaderWriterPtr " + tableName << endl;
			exit(-1);
		}

		RelAlgExprPtr tableIn = make_shared <Table> (table, tableAlais);
		RelAlgExprPtr op = make_shared <SingleSelection> (tableIn, newValuesToSelect, cnf);
		return make_pair (op, 0);
	} else { // in this case there is more than one table
		
		int bestCost = -1; // bestCost is huge
		RelAlgExprPtr bestPlan = nullptr; // bestPlan is empty

		// loop through every break into a LHS and a RHS
		vector< pair<vector <pair <string, string>>, vector <pair <string, string>>>> subsets = getAllSubsets(tablesToProcess);
		for (auto subset : subsets) {
			vector <pair <string, string>> left = subset.first; // left = T'
			vector <pair <string, string>> right = subset.second; // right = T-T'

			// get the CNF expressions : left, right and top
			vector <ExprTreePtr> leftCNF;
			vector <ExprTreePtr> rightCNF;
			vector <ExprTreePtr> topCNF;

			for (auto disjunction : cnf) {
				bool referToLeft = false;
				for (int i = 0; i < left.size(); i++) {
					referToLeft = referToLeft || disjunction->isReferToTable(left[i].second);
				}

				bool referToRight = false;
				for (int i = 0; i < right.size(); i++) {
					referToRight = referToRight || disjunction->isReferToTable(right[i].second);
				}

				if (referToLeft && !referToRight) {
					leftCNF.push_back(disjunction);
				} else if (!referToLeft && referToRight) {
					rightCNF.push_back(disjunction);
				} else if (referToLeft && referToRight) {
					topCNF.push_back(disjunction);
				} else continue;
			}


			// figure out what atts we need from the left and the right
			vector <ExprTreePtr> leftAtts;
			vector <ExprTreePtr> rightAtts;

			vector <pair <string, MyDB_AttTypePtr>> tempLeftAtts; // atts(left)
			for (auto leftTablePair : left) {
				auto atts = Optimizer :: getTableByName(leftTablePair.first)->getTable()->getSchema()->getAtts();
				for (auto att : atts) {
					string attName = leftTablePair.second + "_" + att.first;
					tempLeftAtts.push_back(make_pair(attName, att.second));
				}
			}

			vector <pair <string, MyDB_AttTypePtr>> tempRightAtts; // atts(right)
			for (auto rightTablePair : right) {
				auto atts = Optimizer :: getTableByName(rightTablePair.first)->getTable()->getSchema()->getAtts();
				for (auto att : atts) {
					string attName = rightTablePair.second + "_" + att.first;
					tempRightAtts.push_back(make_pair(attName, att.second));
				}
			}

			vector <ExprTreePtr> attsTopCNf; // atts (topCNF)
			for (auto disjunction : topCNF) {
			 	disjunction->getAtt(attsTopCNf);
			}


			vector <ExprTreePtr> nextValuesToSelect;
			for (auto a : newValuesToSelect) {
				a->getAtt(nextValuesToSelect);
			}

			 // Let leftAtts be atts (left) N (A U atts (topCNF))
			for (auto tempLeftAtt : tempLeftAtts) { 
				string AttName2Str = "[" + tempLeftAtt.first + "]";
				for (auto attTopCnf : attsTopCNf) {
					if (attTopCnf->toString() == AttName2Str) {
						leftAtts.push_back(attTopCnf);
					}
				}
				for (auto attA : nextValuesToSelect) {
					if (attA->toString() == AttName2Str) {
						leftAtts.push_back(attA);
					}
				}
			}

			// Let rightAtts be atts (right) N (A U atts (topCNF))
			for (auto tempRightAtt : tempRightAtts) { 
				string AttName2Str = "[" + tempRightAtt.first + "]";
				for (auto attTopCnf : attsTopCNf) {
					if (attTopCnf->toString() == AttName2Str) {
						rightAtts.push_back(attTopCnf);
					}
				}
				for (auto attA : nextValuesToSelect) {
					if (attA->toString() == AttName2Str) {
						rightAtts.push_back(attA);
					}
				}
			}

			// cout << "-------------------------" << endl;
			// for (auto a : leftCNF) {
			// 	cout << a->toString() << endl;
			// }
			// for (auto a : left) {
			// 	cout << a.first << endl;
			// }
			// for (auto a : leftAtts) {
			// 	cout << a->toString() << endl;
			// }
			// cout << endl;
			// for (auto a : rightCNF) {
			// 	cout << a->toString() << endl;
			// }
			// for (auto a : right) {
			// 	cout << a.first << endl;
			// }
			// for (auto a : rightAtts) {
			// 	cout << a->toString() << endl;
			// }
			// cout << "-------------------------" << endl;

			// recursively optimize the left and right plans
			pair <RelAlgExprPtr, int> leftOpt = optimize(leftCNF, left, leftAtts);
			pair <RelAlgExprPtr, int> rightOpt = optimize(rightCNF, right, rightAtts);

			int leftCost = leftOpt.second;
			int rightCost = rightOpt.second;
			if (leftCost + rightCost > bestCost) {
				bestCost = leftCost + rightCost;
				bestPlan = make_shared <JoinSelection> (leftOpt.first, 
					rightOpt.first, 
					nextValuesToSelect,
					topCNF);
			}
		}
		return make_pair (bestPlan, bestCost + static_pointer_cast<JoinSelection>(bestPlan)->getTopCNFSize());
	}
}


vector< pair<vector <pair <string, string>>, vector <pair <string, string>>>> Optimizer :: getAllSubsets(vector <pair <string, string>> tablesToProcess) {

	// geeksforgeeks search the generation of subsets
	int tableSize = tablesToProcess.size();
	vector < pair<vector <pair <string, string>>, vector <pair <string, string>>>> ret;
	int powSetZize = (int) pow(2, tableSize);
	for (int i = 1; i < powSetZize/2; i++) {
		vector <pair <string, string>> former;
		vector <pair <string, string>> latter;
		for (int j = 0; j < tableSize; j++) {
			if ((i & (1 << j))) {
				former.push_back(tablesToProcess[j]);
			} else {
				latter.push_back(tablesToProcess[j]);
			}
		}
		ret.push_back(make_pair(former, latter));
	}
	// 	// Comment : test the generation
	// 	cout << "-----------------" << endl;
	// 	cout << "left:";
	// 	for (auto a : former) {
	// 		cout << a.first << ",";
	// 	}
	// 	cout << endl;
	// 	cout << "right:";
	// 	for (auto a : latter) {
	// 		cout << a.first << ",";
	// 	}
	// 	cout << endl;
	// 	cout << "-----------------" << endl;
	// 	cout << endl;
	// }

	return ret;
}


MyDB_TableReaderWriterPtr Optimizer :: getTableByName(string tableName) {
	if (allTables[tableName]->getFileType() == "heap") {
		return allTableReaderWriters[tableName];
	} else if (allTables[tableName]->getFileType() == "bplustree") {
		return allBPlusReaderWriters[tableName];
	} else {
		cout << "Optimizer.ccgetTableByName() : cannot find table " << tableName << endl;
		return nullptr;
 	}
}

#endif