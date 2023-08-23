#pragma once
#include"SimpleDbTestBase.h"
#include"TestUtil.h"
#include"Filter.h"
class FilterTest : public SimpleDbTestBase {
protected:
	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_scan = make_shared<TestUtil::MockScan>(-5, 5, _testWidth);
	}
	shared_ptr<OpIterator> _scan;
	int _testWidth = 3;
};

TEST_F(FilterTest, GetTupleDesc) {
	shared_ptr<Predicate> pred = make_shared<Predicate>(0, Predicate::Op::EQUALS, TestUtil::getField(0));
	Filter op(pred, _scan);
	shared_ptr<TupleDesc> expected = Utility::getTupleDesc(_testWidth);
	shared_ptr<TupleDesc> actual = op.getTupleDesc();
	EXPECT_TRUE(expected->equals(*actual));
}

TEST_F(FilterTest, Rewind) {
	shared_ptr<Predicate> pred = make_shared<Predicate>(0, Predicate::Op::EQUALS, TestUtil::getField(0));
	Filter op(pred, _scan);
	op.open();
	EXPECT_TRUE(op.hasNext());
	EXPECT_NO_THROW(op.next());
	EXPECT_TRUE(TestUtil::checkExhausted(op));

	op.rewind();
	shared_ptr<Tuple> expected = Utility::getHeapTuple(0, _testWidth);
	Tuple* actual = op.next();
	EXPECT_TRUE(TestUtil::compareTuples(*expected, *actual));
	op.close();
}

TEST_F(FilterTest, FilterSomeLessThan) {
	shared_ptr<Predicate> pred = make_shared<Predicate>(0, Predicate::Op::LESS_THAN, TestUtil::getField(2));
	Filter op(pred, _scan);
	TestUtil::MockScan expectedOut(-5, 2, _testWidth);
	op.open();
	TestUtil::compareDbIterators(op, expectedOut);
	op.close();
}

TEST_F(FilterTest, FilterAllLessThan) {
	shared_ptr<Predicate> pred = make_shared<Predicate>(0, Predicate::Op::LESS_THAN, TestUtil::getField(-5));
	Filter op(pred, _scan);
	op.open();
	EXPECT_TRUE(TestUtil::checkExhausted(op));
	op.close();
}

TEST_F(FilterTest, FilterEqual) {
	_scan = make_shared<TestUtil::MockScan>(-5, 5, _testWidth);
	shared_ptr<Predicate> pred = make_shared<Predicate>(0, Predicate::Op::EQUALS, TestUtil::getField(-5));
	shared_ptr<Filter> op = make_shared<Filter>(pred, _scan);
	op->open();
	EXPECT_TRUE(TestUtil::compareTuples(*Utility::getHeapTuple(-5, _testWidth), *(op->next())));
	op->close();

	_scan = make_shared<TestUtil::MockScan>(-5, 5, _testWidth);
	pred = make_shared<Predicate>(0, Predicate::Op::EQUALS, TestUtil::getField(0));
	op = make_shared<Filter>(pred, _scan);
	op->open();
	EXPECT_TRUE(TestUtil::compareTuples(*Utility::getHeapTuple(0, _testWidth),*(op->next())));
	op->close();

	_scan = make_shared<TestUtil::MockScan>(-5, 5, _testWidth);
	pred = make_shared<Predicate>(0, Predicate::Op::EQUALS, TestUtil::getField(4));
	op = make_shared<Filter>(pred, _scan);
	op->open();
	EXPECT_TRUE(TestUtil::compareTuples(*Utility::getHeapTuple(4, _testWidth), *(op->next())));
	op->close();
}

TEST_F(FilterTest, FilterEqualNoTuples) {
	shared_ptr<Predicate> pred = make_shared<Predicate>(0, Predicate::Op::EQUALS, TestUtil::getField(5));
	Filter op(pred, _scan);
	op.open();
	TestUtil::checkExhausted(op);
	op.close();
}