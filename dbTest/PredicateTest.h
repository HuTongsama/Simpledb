#pragma once
#include"SimpleDbTestBase.h"
#include"Utility.h"
#include"Predicate.h"
#include"TestUtil.h"
using namespace Simpledb;
class PredicateTest :public SimpleDbTestBase {};

TEST_F(PredicateTest, Filter) {
    vector<int> vals = { -1, 0, 1 };

    for (int i : vals) {
        Predicate p(0, Predicate::Op::EQUALS, TestUtil::getField(i));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i - 1)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i + 1)));
    }

    for (int i : vals) {
        Predicate p(0, Predicate::Op::GREATER_THAN, TestUtil::getField(i));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i - 1)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i + 1)));
    }

    for (int i : vals) {
        Predicate p(0, Predicate::Op::GREATER_THAN_OR_EQ, TestUtil::getField(i));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i - 1)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i + 1)));
    }

    for (int i : vals) {
        Predicate p(0, Predicate::Op::LESS_THAN, TestUtil::getField(i));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i - 1)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i + 1)));
    }

    for (int i : vals) {
        Predicate p(0, Predicate::Op::LESS_THAN_OR_EQ, TestUtil::getField(i));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i - 1)));
        EXPECT_TRUE(p.filter(Utility::getHeapTuple(i)));
        EXPECT_FALSE(p.filter(Utility::getHeapTuple(i + 1)));
    }
}