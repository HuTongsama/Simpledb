#pragma once
#include"SimpleDbTestBase.h"
#include"Aggregate.h"
#include"TestUtil.h"
using namespace Simpledb;
class AggregateTest : public SimpleDbTestBase {
protected:
	void SetUp()override {
        _scan1 = TestUtil::createTupleList(_width1,
            vector<int>{ 1, 2,
            1, 4,
            1, 6,
            3, 2,
            3, 4,
            3, 6,
            5, 7 });
        _scan2 = TestUtil::createTupleList(_width1,
            vector<TestUtil::TupData>{
                TestUtil::TupData(Int_Type::INT_TYPE(), "", 1), TestUtil::TupData(String_Type::STRING_TYPE(), "a"),
                TestUtil::TupData(Int_Type::INT_TYPE(), "", 1), TestUtil::TupData(String_Type::STRING_TYPE(), "a"),
                TestUtil::TupData(Int_Type::INT_TYPE(), "", 1), TestUtil::TupData(String_Type::STRING_TYPE(), "a"),
                TestUtil::TupData(Int_Type::INT_TYPE(), "", 3), TestUtil::TupData(String_Type::STRING_TYPE(), "a"),
                TestUtil::TupData(Int_Type::INT_TYPE(), "", 3), TestUtil::TupData(String_Type::STRING_TYPE(), "a"),
                TestUtil::TupData(Int_Type::INT_TYPE(), "", 3), TestUtil::TupData(String_Type::STRING_TYPE(), "a"),
                TestUtil::TupData(Int_Type::INT_TYPE(), "", 5), TestUtil::TupData(String_Type::STRING_TYPE(), "a"), });
        _scan3 = TestUtil::createTupleList(_width1,
            vector<TestUtil::TupData>{
                TestUtil::TupData(String_Type::STRING_TYPE(), "a"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 2),
                TestUtil::TupData(String_Type::STRING_TYPE(), "a"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 4),
                TestUtil::TupData(String_Type::STRING_TYPE(), "a"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 6),
                TestUtil::TupData(String_Type::STRING_TYPE(), "b"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 2),
                TestUtil::TupData(String_Type::STRING_TYPE(), "b"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 4),
                TestUtil::TupData(String_Type::STRING_TYPE(), "b"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 6),
                TestUtil::TupData(String_Type::STRING_TYPE(), "c"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 7), });

        _sum = TestUtil::createTupleList(_width1,
            vector<int> { 1, 12,
            3, 12,
            5, 7 });
        _sumstring = TestUtil::createTupleList(_width1,
            vector<TestUtil::TupData>{
                TestUtil::TupData(String_Type::STRING_TYPE(), "a"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 12),
                TestUtil::TupData(String_Type::STRING_TYPE(), "b"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 12),
                TestUtil::TupData(String_Type::STRING_TYPE(), "c"), TestUtil::TupData(Int_Type::INT_TYPE(), "", 7), });

        _avg = TestUtil::createTupleList(_width1,
            vector<int> { 1, 4,
            3, 4,
            5, 7 });
        _min = TestUtil::createTupleList(_width1,
            vector<int> { 1, 2,
            3, 2,
            5, 7 });
        _max = TestUtil::createTupleList(_width1,
            vector<int> { 1, 6,
            3, 6,
            5, 7 });
        _count = TestUtil::createTupleList(_width1,
            vector<int> { 1, 3,
            3, 3,
            5, 1 });
	}

	int _width1 = 2;
	shared_ptr<OpIterator> _scan1;
	shared_ptr<OpIterator> _scan2;
	shared_ptr<OpIterator> _scan3;

	shared_ptr<OpIterator> _sum;
	shared_ptr<OpIterator> _sumstring;

	shared_ptr<OpIterator> _avg;
	shared_ptr<OpIterator> _max;
	shared_ptr<OpIterator> _min;
	shared_ptr<OpIterator> _count;
};

/**
   * Unit test for Aggregate.getTupleDesc()
   */
TEST_F(AggregateTest, GetTupleDesc) {
    // Int, Int TupleDesc
    shared_ptr<Aggregate> op = make_shared<Aggregate>(_scan1, 0, 0, Aggregator::Op::MIN);
    shared_ptr<TupleDesc> expected = Utility::getTupleDesc(2);
    shared_ptr<TupleDesc> actual = op->getTupleDesc();
    EXPECT_TRUE(expected->equals(*actual));

    // Int, String TupleDesc
    // We group by the String field, returning <String, Count> tuples.
    op = make_shared<Aggregate>(_scan2, 0, 1, Aggregator::Op::COUNT);
    expected = make_shared<TupleDesc>(vector<shared_ptr<Type>>{ String_Type::STRING_TYPE(), Int_Type::INT_TYPE() });
    actual = op->getTupleDesc();
    EXPECT_TRUE(expected->equals(*actual));
}


/**
 * Unit test for Aggregate.rewind()
 */
TEST_F(AggregateTest, Rewind) {

    shared_ptr<Aggregate> op = make_shared<Aggregate>(_scan1, 1, 0, Aggregator::Op::MIN);
    op->open();
    while (op->hasNext()) {
        EXPECT_NO_THROW(op->next(), runtime_error);
    }
    EXPECT_TRUE(TestUtil::checkExhausted(*op));

    op->rewind();
    _min->open();
    TestUtil::matchAllTuples(_min, op);
}

/**
 * Unit test for Aggregate.getNext() using a count aggregate with string types
 */
TEST_F(AggregateTest, CountStringAggregate) {
    shared_ptr<Aggregate> op = make_shared<Aggregate>(_scan2, 1, 0,
        Aggregator::Op::COUNT);
    op->open();
    _count->open();
    TestUtil::matchAllTuples(_count, op);
}

/**
 * Unit test for Aggregate.getNext() using a count aggregate with string types
 */
TEST_F(AggregateTest, SumStringGroupBy) {
    shared_ptr<Aggregate> op = make_shared<Aggregate>(_scan3, 1, 0,
        Aggregator::Op::SUM);
    op->open();
    _sumstring->open();
    TestUtil::matchAllTuples(_sumstring, op);
}

/**
 * Unit test for Aggregate.getNext() using a sum aggregate
 */
TEST_F(AggregateTest, SumAggregate) {
    shared_ptr<Aggregate> op = make_shared<Aggregate>(_scan1, 1, 0,
        Aggregator::Op::SUM);
    op->open();
    _sum->open();
    TestUtil::matchAllTuples(_sum, op);
}
/**
 * Unit test for Aggregate.getNext() using an avg aggregate
 */
TEST_F(AggregateTest, AvgAggregate) {
    shared_ptr<Aggregate> op = make_shared<Aggregate>(_scan1, 1, 0,
        Aggregator::Op::AVG);
    op->open();
    _avg->open();
    TestUtil::matchAllTuples(_avg, op);
}

/**
 * Unit test for Aggregate.getNext() using a max aggregate
 */
TEST_F(AggregateTest, MaxAggregate) {
    shared_ptr<Aggregate> op = make_shared<Aggregate>(_scan1, 1, 0,
        Aggregator::Op::MAX);
    op->open();
    _max->open();
    TestUtil::matchAllTuples(_max, op);
}

/**
 * Unit test for Aggregate.getNext() using a min aggregate
 */
TEST_F(AggregateTest, MinAggregate) {
    shared_ptr<Aggregate> op = make_shared<Aggregate>(_scan1, 1, 0,
        Aggregator::Op::MIN);
    op->open();
    _min->open();
    TestUtil::matchAllTuples(_min, op);
}