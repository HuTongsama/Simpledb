#pragma once
#include"FilterBase.h"
#include"Filter.h"
class SysFilterTest :public FilterBase {
protected:
	int applyPredicate(shared_ptr<HeapFile> table,
		shared_ptr<TransactionId> tid,
		shared_ptr<Predicate> predicate)override {
        shared_ptr<SeqScan> ss = make_shared<SeqScan>(tid, table->getId(), "");
        shared_ptr<Filter> filter = make_shared<Filter>(predicate, ss);
        filter->open();

        int resultCount = 0;
        while (filter->hasNext()) {
            EXPECT_NO_THROW(filter->next());
            resultCount += 1;
        }

        filter->close();
        return resultCount;
	}
};

TEST_F(SysFilterTest, TestEquals) {
    validatePredicate(0, 1, 1, 2, Predicate::Op::EQUALS);
}
TEST_F(SysFilterTest, TestLessThan) {
    validatePredicate(1, 1, 2, 1, Predicate::Op::LESS_THAN);
}
TEST_F(SysFilterTest, TestLessThanOrEq) {
    validatePredicate(2, 42, 42, 41, Predicate::Op::LESS_THAN_OR_EQ);
}
TEST_F(SysFilterTest, TestGreaterThan) {
    validatePredicate(2, 42, 41, 42, Predicate::Op::GREATER_THAN);
}
TEST_F(SysFilterTest, TestGreaterThanOrEq) {
    validatePredicate(2, 42, 42, 43, Predicate::Op::GREATER_THAN_OR_EQ);
}