#pragma once
#include"SimpleDbTestBase.h"
#include"SystemTestUtil.h"
#include "HeapFile.h"
#include<map>
using namespace Simpledb;
using namespace std;
class FilterBase :public SimpleDbTestBase {
protected:
	const int COLUMNS = 3;
	const int ROWS = 1097;
	vector<vector<int>> _createdTuples;

	/** Should apply the predicate to table. This will be executed in transaction tid. */
	virtual int applyPredicate(
		shared_ptr<HeapFile> table,
		shared_ptr<TransactionId> tid,
		shared_ptr<Predicate> predicate) {
		return -1;
	}
	void validateAfter(shared_ptr<HeapFile> table) {}

	int runTransactionForPredicate(shared_ptr<HeapFile> table, shared_ptr<Predicate> predicate){
		shared_ptr<TransactionId> tid = make_shared<TransactionId>();
		int result = applyPredicate(table, tid, predicate);
		Database::getBufferPool()->transactionComplete(tid);
		return result;
	}

	void validatePredicate(int column, int columnValue, int trueValue, int falseValue,
		Predicate::Op operation){
			// Test the true value
			shared_ptr<HeapFile> f = createTable(column, columnValue);
			shared_ptr<Predicate> predicate = make_shared<Predicate>(column, operation, make_shared<IntField>(trueValue));
			EXPECT_EQ(ROWS, runTransactionForPredicate(f, predicate));
			f = Utility::openHeapFile(COLUMNS, f->getFile());
			validateAfter(f);

			// Test the false value
			f = createTable(column, columnValue);
			predicate = make_shared<Predicate>(column, operation, make_shared<IntField>(falseValue));
			EXPECT_EQ(0, runTransactionForPredicate(f, predicate));
			f = Utility::openHeapFile(COLUMNS, f->getFile());
			validateAfter(f);
	}

	shared_ptr<HeapFile> createTable(int column, int columnValue){
		map<int, int> columnSpecification;
		columnSpecification[column] = columnValue;
		return SystemTestUtil::createRandomHeapFile(
			COLUMNS, ROWS, columnSpecification, _createdTuples);
	}
};

TEST_F(FilterBase, TestEquals) {
	validatePredicate(0, 1, 1, 2, Predicate::Op::EQUALS);
}
TEST_F(FilterBase, TestLessThan) {
	validatePredicate(1, 1, 2, 1, Predicate::Op::LESS_THAN);
}
TEST_F(FilterBase, TestLessThanOrEq) {
	validatePredicate(2, 42, 42, 41, Predicate::Op::LESS_THAN_OR_EQ);
}
TEST_F(FilterBase, TestGreaterThan) {
	validatePredicate(2, 42, 41, 42, Predicate::Op::GREATER_THAN);
}
TEST_F(FilterBase, TestGreaterThanOrEq) {
	validatePredicate(2, 42, 42, 43, Predicate::Op::GREATER_THAN_OR_EQ);
}