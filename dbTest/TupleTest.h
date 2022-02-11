#pragma once
#include"SimpleDbTestBase.h"
#include"Utility.h"
#include"IntField.h"
#include"HeapPage.h"
using namespace Simpledb;
class TupleTest :public SimpleDbTestBase {};
TEST_F(TupleTest, ModifyFields) {
    shared_ptr<TupleDesc> td = Utility::getTupleDesc(2);

    shared_ptr<Tuple> tup = make_shared<Tuple>(td);
    tup->setField(0, make_shared<IntField>(-1));
    tup->setField(1, make_shared<IntField>(0));
    EXPECT_TRUE(tup->getField(0)->equals(IntField(-1)));
    EXPECT_TRUE(tup->getField(1)->equals(IntField(0)));
    
    
    tup->setField(0, make_shared<IntField>(1));
    tup->setField(1, make_shared<IntField>(37));
    EXPECT_TRUE(tup->getField(0)->equals(IntField(1)));
    EXPECT_TRUE(tup->getField(1)->equals(IntField(37)));
}
TEST_F(TupleTest, GetTupleDesc) {
    shared_ptr<TupleDesc> td = Utility::getTupleDesc(5);
    shared_ptr<Tuple> tup = make_shared<Tuple>(td);
    EXPECT_TRUE(tup->getTupleDesc()->equals(*td));
}
TEST_F(TupleTest, ModifyRecordId) {
    shared_ptr<Tuple> tup1 = make_shared<Tuple>(Utility::getTupleDesc(1));
    shared_ptr<HeapPageId> pid1 = make_shared<HeapPageId>(0, 0);
    shared_ptr<RecordId> rid1 = make_shared<RecordId>(pid1, 0);
    tup1->setRecordId(rid1);
    EXPECT_FALSE(rid1->equals(*tup1->getRecordId()));
}
