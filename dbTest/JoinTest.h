#pragma once
#include"SimpleDbTestBase.h"
#include"Join.h"
#include"TestUtil.h"
using namespace Simpledb;
class JoinTest :public SimpleDbTestBase {
protected:
    void SetUp()override {
        _scan1 = TestUtil::createTupleList(_width1,
            vector<int>{ 1, 2,
            3, 4,
            5, 6,
            7, 8 });
        _scan2 = TestUtil::createTupleList(_width2,
            vector<int> { 1, 2, 3,
            2, 3, 4,
            3, 4, 5,
            4, 5, 6,
            5, 6, 7 });
        _eqJoin = TestUtil::createTupleList(_width1 + _width2,
            vector<int>{ 1, 2, 1, 2, 3,
            3, 4, 3, 4, 5,
            5, 6, 5, 6, 7 });
        _gtJoin = TestUtil::createTupleList(_width1 + _width2,
            vector<int> {3, 4, 1, 2, 3, // 1, 2 < 3
            3, 4, 2, 3, 4,
            5, 6, 1, 2, 3, // 1, 2, 3, 4 < 5
            5, 6, 2, 3, 4,
            5, 6, 3, 4, 5,
            5, 6, 4, 5, 6,
            7, 8, 1, 2, 3, // 1, 2, 3, 4, 5 < 7
            7, 8, 2, 3, 4,
            7, 8, 3, 4, 5,
            7, 8, 4, 5, 6,
            7, 8, 5, 6, 7 });
    }
    int _width1 = 2;
    int _width2 = 3;
    shared_ptr<OpIterator> _scan1;
    shared_ptr<OpIterator> _scan2;
    shared_ptr<OpIterator> _eqJoin;
    shared_ptr<OpIterator> _gtJoin;
};
TEST_F(JoinTest, GetTupleDesc) {
    shared_ptr<JoinPredicate> pred = make_shared<JoinPredicate>(0, Predicate::Op::EQUALS, 0);
    shared_ptr<Join> op = make_shared<Join>(pred, _scan1, _scan2);
    shared_ptr<TupleDesc> expected = Utility::getTupleDesc(_width1 + _width2);
    shared_ptr<TupleDesc> actual = op->getTupleDesc();
    EXPECT_TRUE(expected->equals(*actual));
}
TEST_F(JoinTest, Rewind) {
    shared_ptr<JoinPredicate> pred = make_shared<JoinPredicate>(0, Predicate::Op::EQUALS, 0);
    shared_ptr<Join> op = make_shared<Join>(pred, _scan1, _scan2);
    op->open();
    while (op->hasNext()) {
        EXPECT_NO_THROW(op->next());
    }
    EXPECT_TRUE(TestUtil::checkExhausted(*op));
    op->rewind();

    _eqJoin->open();
    Tuple* expected = _eqJoin->next();
    Tuple* actual = op->next();
    EXPECT_TRUE(TestUtil::compareTuples(*expected, *actual));
}
TEST_F(JoinTest, GtJoin) {
    shared_ptr<JoinPredicate> pred = make_shared<JoinPredicate>(0, Predicate::Op::GREATER_THAN, 0);
    shared_ptr<Join> op = make_shared<Join>(pred, _scan1, _scan2);
    op->open();
    _gtJoin->open();
    TestUtil::matchAllTuples(_gtJoin, op);
}
TEST_F(JoinTest, eqJoin) {
    shared_ptr<JoinPredicate> pred = make_shared<JoinPredicate>(0, Predicate::Op::EQUALS, 0);
    shared_ptr<Join> op = make_shared<Join>(pred, _scan1, _scan2);
    op->open();
    _eqJoin->open();
    TestUtil::matchAllTuples(_eqJoin, op);
}