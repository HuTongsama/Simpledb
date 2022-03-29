#pragma once
#include"SimpleDbTestBase.h"
#include"StringAggregator.h"
#include"TestUtil.h"
using namespace Simpledb;
class StringAggregatorTest : public SimpleDbTestBase {
protected:
	void SetUp()override {
        _scan1 = TestUtil::createTupleList(_width1,
            vector<TestUtil::TupData>{
                TestUtil::TupData(Int_Type::INT_TYPE,"",1), TestUtil::TupData(String_Type::STRING_TYPE, "a"),
                TestUtil::TupData(Int_Type::INT_TYPE, "", 1), TestUtil::TupData(String_Type::STRING_TYPE, "b"),
                TestUtil::TupData(Int_Type::INT_TYPE, "", 1), TestUtil::TupData(String_Type::STRING_TYPE, "c"),
                TestUtil::TupData(Int_Type::INT_TYPE, "", 3), TestUtil::TupData(String_Type::STRING_TYPE, "d"),
                TestUtil::TupData(Int_Type::INT_TYPE, "", 3), TestUtil::TupData(String_Type::STRING_TYPE, "e"),
                TestUtil::TupData(Int_Type::INT_TYPE, "", 3), TestUtil::TupData(String_Type::STRING_TYPE, "f"),
                TestUtil::TupData(Int_Type::INT_TYPE, "", 5), TestUtil::TupData(String_Type::STRING_TYPE, "g")});

        // verify how the results progress after a few merges
        _count = vector<vector<int>>{
          { 1, 1 },
          { 1, 2 },
          { 1, 3 },
          { 1, 3, 3, 1 }
        };
	}


	int _width1 = 2;
	shared_ptr<OpIterator> _scan1;
	vector<vector<int>> _count;
};

/**
   * Test String.mergeTupleIntoGroup() and iterator() over a COUNT
   */
TEST_F(StringAggregatorTest, MergeCount) {
    _scan1->open();
    StringAggregator agg(0, Int_Type::INT_TYPE, 1, Aggregator::Op::COUNT);

    for (auto& step : _count) {
        agg.mergeTupleIntoGroup(_scan1->next());
        shared_ptr<OpIterator> it = agg.iterator();
        it->open();
        TestUtil::matchAllTuples(TestUtil::createTupleList(_width1, step), it);
    }
}

/**
 * Test StringAggregator.iterator() for OpIterator behaviour
 */
TEST_F(StringAggregatorTest, TestIterator) {
    // first, populate the aggregator via sum over scan1
    _scan1->open();
    StringAggregator agg(0, Int_Type::INT_TYPE, 1, Aggregator::Op::COUNT);
    try
    {
        while (true)
            agg.mergeTupleIntoGroup(_scan1->next());
    }
    catch (const std::exception&)
    {
        // explicitly ignored
    }

    shared_ptr<OpIterator> it = agg.iterator();
    it->open();

    // verify it has three elements
    int count = 0;
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
