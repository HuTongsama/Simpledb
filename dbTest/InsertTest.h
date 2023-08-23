#pragma once
#include"SimpleDbTestBase.h"
#include"TestUtil.h"
#include"Insert.h"
using namespace Simpledb;
class InsertTest :public SimpleDbTestBase, public TestUtil::CreateHeapFile {
protected:
	void SetUp()override {
		SimpleDbTestBase::SetUp();
		TestUtil::CreateHeapFile::SetUp();

        _scan1 = TestUtil::createTupleList(2,
            vector<int> { 1, 2,
            1, 4,
            1, 6,
            3, 2,
            3, 4,
            3, 6,
            5, 7 });
        _tid = make_shared<TransactionId>();
	}

	shared_ptr<OpIterator> _scan1;
	shared_ptr<TransactionId> _tid;
};


/**
 * Unit test for Insert.getTupleDesc()
 */
TEST_F(InsertTest, GetTupleDesc) {
    Insert op(_tid, _scan1, _empty->getId());
    shared_ptr<TupleDesc> expected = Utility::getTupleDesc(1);
    shared_ptr<TupleDesc> actual = op.getTupleDesc();
    EXPECT_TRUE(expected->equals(*actual));
}

/**
 * Unit test for Insert.getNext(), inserting elements into an empty file
 */
TEST_F(InsertTest, GetNext) {
    Insert op(_tid, _scan1, _empty->getId());
    op.open();
    EXPECT_TRUE(TestUtil::compareTuples(
        *Utility::getHeapTuple(7, 1),// the length of scan1
        *op.next()));

    // we should fit on one page
    EXPECT_EQ(1, _empty->numPages());
}