#pragma once
#include"SimpleDbTestBase.h"
#include"HeapPageId.h"
#include"RecordId.h"
using namespace Simpledb;
class RecordIdTest :public SimpleDbTestBase {
protected:
    void SetUp()override {
        auto hpid = make_shared<HeapPageId>(-1, 2);
        auto hpid2 = make_shared<HeapPageId>(-1, 2);
        auto hpid3 = make_shared<HeapPageId>(-2, 2);
        _hrid = make_shared<RecordId>(hpid, 3);
        _hrid2 = make_shared<RecordId>(hpid2, 3);
        _hrid3 = make_shared<RecordId>(hpid, 4);
        _hrid4 = make_shared<RecordId>(hpid3, 3);
    }

    shared_ptr<RecordId> _hrid;
    shared_ptr<RecordId> _hrid2;
    shared_ptr<RecordId> _hrid3;
    shared_ptr<RecordId> _hrid4;
};

TEST_F(RecordIdTest, GetPageId) {
    HeapPageId hpid(-1, 2);
    EXPECT_TRUE(_hrid->getPageId()->equals(hpid));
}

TEST_F(RecordIdTest, Tupleno) {
    EXPECT_EQ(3, _hrid->getTupleNumber());
}

TEST_F(RecordIdTest, Equals) {

    EXPECT_TRUE(_hrid->equals(*_hrid2));
    EXPECT_TRUE(_hrid2->equals(*_hrid));
    EXPECT_FALSE(_hrid->equals(*_hrid3));
    EXPECT_FALSE(_hrid3->equals(*_hrid));
    EXPECT_FALSE(_hrid2->equals(*_hrid4));
    EXPECT_FALSE(_hrid4->equals(*_hrid2));
}

TEST_F(RecordIdTest, HCode) {
    EXPECT_EQ(_hrid->hashCode(), _hrid2->hashCode());
}