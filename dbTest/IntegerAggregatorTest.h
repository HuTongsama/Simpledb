#pragma once
#include"SimpleDbTestBase.h"
#include"IntegerAggregator.h"
#include"TestUtil.h"
using namespace Simpledb;
class IntegerAggregatorTest : public SimpleDbTestBase {
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

        // verify how the results progress after a few merges
        _sum = vector<vector<int>>{
          { 1, 2 },
          { 1, 6 },
          { 1, 12 },
          { 1, 12, 3, 2 }
        };

        _min = vector<vector<int>>{
          { 1, 2 },
          { 1, 2 },
          { 1, 2 },
          { 1, 2, 3, 2 }
        };

        _max = vector<vector<int>>{
          { 1, 2 },
          { 1, 4 },
          { 1, 6 },
          { 1, 6, 3, 2 }
        };

        _avg = vector<vector<int>>{
          { 1, 2 },
          { 1, 3 },
          { 1, 4 },
          { 1, 4, 3, 2 }
        };
	}

	int _width1 = 2;
	shared_ptr<OpIterator> _scan1;
	vector<vector<int>> _sum;
	vector<vector<int>> _min;
	vector<vector<int>> _max;
	vector<vector<int>> _avg;
};

/**
 * Test IntegerAggregator.mergeTupleIntoGroup() and iterator() over a sum
 */
TEST_F(IntegerAggregatorTest, MergeSum) {
    _scan1->open();
    IntegerAggregator agg(0, Int_Type::INT_TYPE(), 1, Aggregator::Op::SUM);

    for (auto& step : _sum) {
        agg.mergeTupleIntoGroup(_scan1->next());
        shared_ptr<OpIterator> it = agg.iterator();
        it->open();
        TestUtil::matchAllTuples(TestUtil::createTupleList(_width1, step), it);
    }
}

/**
 * Test IntegerAggregator.mergeTupleIntoGroup() and iterator() over a min
 */
TEST_F(IntegerAggregatorTest, MergeMin) {
    _scan1->open();
    IntegerAggregator agg(0, Int_Type::INT_TYPE(), 1, Aggregator::Op::MIN);

    for (auto& step : _min) {
        agg.mergeTupleIntoGroup(_scan1->next());
        shared_ptr<OpIterator> it = agg.iterator();
        it->open();
        TestUtil::matchAllTuples(TestUtil::createTupleList(_width1, step), it);
    }
}

/**
 * Test IntegerAggregator.mergeTupleIntoGroup() and iterator() over a max
 */
TEST_F(IntegerAggregatorTest, MergeMax) {
    _scan1->open();
    IntegerAggregator agg(0, Int_Type::INT_TYPE(), 1, Aggregator::Op::MAX);
    for (auto& step : _max) {
        agg.mergeTupleIntoGroup(_scan1->next());
        shared_ptr<OpIterator> it = agg.iterator();
        it->open();
        TestUtil::matchAllTuples(TestUtil::createTupleList(_width1, step), it);
    }
}

/**
 * Test IntegerAggregator.mergeTupleIntoGroup() and iterator() over an avg
 */
TEST_F(IntegerAggregatorTest, MergeAvg) {
    _scan1->open();
    IntegerAggregator agg(0, Int_Type::INT_TYPE(), 1, Aggregator::Op::AVG);
    for (auto& step : _avg) {
        agg.mergeTupleIntoGroup(_scan1->next());
        shared_ptr<OpIterator> it = agg.iterator();
        it->open();
        TestUtil::matchAllTuples(TestUtil::createTupleList(_width1, step), it);
    }
}

/**
 * Test IntegerAggregator.iterator() for OpIterator behaviour
 */
TEST_F(IntegerAggregatorTest, TestIterator) {
    // first, populate the aggregator via sum over scan1
    _scan1->open();
    IntegerAggregator agg(0, Int_Type::INT_TYPE(), 1, Aggregator::Op::SUM);
    try {
        while (true)
            agg.mergeTupleIntoGroup(_scan1->next());
    }
    catch (const std::exception&) {
        // explicitly ignored
    }
    shared_ptr<OpIterator> it = agg.iterator();
    it->open();

    // verify it has three elements
    int count = 0;
    try
    {
        while (true) {
            it->next();
            count++;
        }
    }
    catch (const std::exception&){
        // explicitly ignored
    }
    EXPECT_EQ(3, count);
    // rewind and try again
    it->rewind();
    count = 0;
    try {
        while (true) {
            it->next();
            count++;
        }
    }
    catch (const std::exception&) {
        // explicitly ignored
    }
    EXPECT_EQ(3, count);
    // close it and check that we don't get anything
    it->close();
    EXPECT_THROW(it->next(), runtime_error);

}
