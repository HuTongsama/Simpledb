#pragma once
#include "SimpleDbTestBase.h"
#include"SystemTestUtil.h"
#include"JoinPredicate.h"
#include"Join.h"
#include<map>
using namespace std;
using namespace Simpledb;
class SysJoinTest :public SimpleDbTestBase {
protected:
	const int COLUMNS = 2;
	void validateJoin(int table1ColumnValue, int table1Rows, int table2ColumnValue,
		int table2Rows) {
        map<int, int> columnSpecification;
        columnSpecification[0] = table1ColumnValue;
        vector<vector<int>> t1Tuples;
        shared_ptr<HeapFile> table1 = SystemTestUtil::createRandomHeapFile(
            COLUMNS, table1Rows, columnSpecification, t1Tuples);
        EXPECT_EQ(t1Tuples.size(), table1Rows);

        columnSpecification[0] = table2ColumnValue;
        vector<vector<int>> t2Tuples;
        shared_ptr<HeapFile> table2 = SystemTestUtil::createRandomHeapFile(
            COLUMNS, table2Rows, columnSpecification, t2Tuples);
        EXPECT_EQ(t2Tuples.size(), table2Rows);

        // Generate the expected results
        vector<vector<int>> expectedResults;
        for (auto& t1 : t1Tuples) {
            for (auto& t2 : t2Tuples) {
                // If the columns match, join the tuples
                if (t1.at(0) == t2.at(0)) {
                    vector<int> out(t1);
                    out.insert(out.end(), t2.begin(), t2.end());
                    expectedResults.push_back(out);
                }
            }
        }

        // Begin the join
        shared_ptr<TransactionId> tid = make_shared<TransactionId>();
        shared_ptr<SeqScan> ss1 = make_shared<SeqScan>(tid, table1->getId(), "");
        shared_ptr<SeqScan> ss2 = make_shared<SeqScan>(tid, table2->getId(), "");
        shared_ptr<JoinPredicate> p = make_shared<JoinPredicate>(0, Predicate::Op::EQUALS, 0);
        shared_ptr<Join> joinOp = make_shared<Join>(p, ss1, ss2);

        // test the join results
        SystemTestUtil::matchTuples(joinOp, expectedResults);

        joinOp->close();
        Database::getBufferPool()->transactionComplete(tid);
	}
};

TEST_F(SysJoinTest, TestSingleMatch) {
    validateJoin(1, 1, 1, 1);
}
TEST_F(SysJoinTest, TestNoMatch) {
    validateJoin(1, 2, 2, 10);
}

TEST_F(SysJoinTest, TestMultipleMatch) {
    validateJoin(1, 3, 1, 3);
}
