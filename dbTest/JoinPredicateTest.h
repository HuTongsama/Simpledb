#pragma once
#include"SimpleDbTestBase.h"
#include"JoinPredicate.h"
#include"Utility.h"
using namespace Simpledb;
class JoinPredicateTest :public SimpleDbTestBase {};

TEST_F(JoinPredicateTest, FilterVaryingVals) {
    vector<int> vals = { -1, 0, 1 };

    for (int i : vals) {
        JoinPredicate p(0, Predicate::Op::EQUALS, 0);
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i - 1)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i + 1)));

    }

    for (int i : vals) {
        JoinPredicate p(0, Predicate::Op::GREATER_THAN, 0);
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i - 1)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i + 1)));
    }

    for (int i : vals) {
        JoinPredicate p(0, Predicate::Op::GREATER_THAN_OR_EQ, 0);
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i - 1)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i + 1)));
    }

    for (int i : vals) {
        JoinPredicate p(0, Predicate::Op::LESS_THAN, 0);
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i - 1)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i + 1)));
    }

    for (int i : vals) {
        JoinPredicate p(0, Predicate::Op::LESS_THAN_OR_EQ, 0);
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i - 1)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i), Utility::getHeapTuple(i + 1)));
    }
}