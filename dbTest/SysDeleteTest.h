#pragma once
#include"FilterBase.h"
#include"Delete.h"
#include"HeapFile.h"
#include"Filter.h"
using namespace Simpledb;

class SysDeleteTest : public FilterBase {
protected:
	int applyPredicate(shared_ptr<HeapFile> table, shared_ptr<TransactionId> tid, shared_ptr<Predicate> predicate)override {
        shared_ptr<SeqScan> ss = make_shared<SeqScan>(tid, table->getId(), "");
        shared_ptr<Filter> filter = make_shared<Filter>(predicate, ss);
        shared_ptr<Delete> deleteOperator = make_shared<Delete>(tid, filter);
        //Query q = new Query(deleteOperator, tid);
        //q.start();
        deleteOperator->open();
        bool hasResult = false;
        int result = -1;
        while (deleteOperator->hasNext()) {
            Tuple* t = deleteOperator->next();
            EXPECT_FALSE(hasResult);
            hasResult = true;
            EXPECT_TRUE(SystemTestUtil::SINGLE_INT_DESCRIPTOR->equals(*t->getTupleDesc()));
            result = dynamic_pointer_cast<IntField>(t->getField(0))->getValue();
        }
        EXPECT_TRUE(hasResult);

        deleteOperator->close();

        // As part of the same transaction, scan the table
        if (result == 0) {
            // Deleted zero tuples: all tuples still in table
            _expectedTuples = _createdTuples;
        }
        else {
            EXPECT_EQ(result, _createdTuples.size());
            _expectedTuples = vector<vector<int>>();
        }
        SystemTestUtil::matchTuples(table, tid, _expectedTuples);
        return result;
	}

    void validateAfter(shared_ptr<HeapFile> table)override{
        // As part of a different transaction, scan the table
        SystemTestUtil::matchTuples(table, _expectedTuples);
    }

	vector<vector<int>> _expectedTuples;
};

TEST_F(SysDeleteTest, TestEquals) {
    validatePredicate(0, 1, 1, 2, Predicate::Op::EQUALS);
}
TEST_F(SysDeleteTest, TestLessThan) {
    validatePredicate(1, 1, 2, 1, Predicate::Op::LESS_THAN);
}
TEST_F(SysDeleteTest, TestLessThanOrEq) {
    validatePredicate(2, 42, 42, 41, Predicate::Op::LESS_THAN_OR_EQ);
}
TEST_F(SysDeleteTest, TestGreaterThan) {
    validatePredicate(2, 42, 41, 42, Predicate::Op::GREATER_THAN);
}
TEST_F(SysDeleteTest, TestGreaterThanOrEq) {
    validatePredicate(2, 42, 42, 43, Predicate::Op::GREATER_THAN_OR_EQ);
}