#pragma once
#include"SimpleDbTestBase.h"
#include"Database.h"
#include"HeapFile.h"
#include"Transaction.h"
#include"Utility.h"
#include"IntField.h"
#include"Insert.h"
#include"SystemTestUtil.h"
using namespace Simpledb;

class AbortEvictionTest :public SimpleDbTestBase {
protected:
	static void insertRow(shared_ptr<HeapFile> f, shared_ptr<Transaction> t) {
        // Create a row to insert
        shared_ptr<TupleDesc> twoIntColumns = Utility::getTupleDesc(2);
        shared_ptr<Tuple> value = make_shared<Tuple>(twoIntColumns);
        value->setField(0,  make_shared<IntField>(-42));
        value->setField(1, make_shared<IntField>(-43));
        shared_ptr<TupleIterator> insertRow = make_shared<TupleIterator>(Utility::getTupleDesc(2), vector<shared_ptr<Tuple>>(1, value));

        // Insert the row
        shared_ptr<Insert> insert = make_shared<Insert>(t->getId(), insertRow, f->getId());
        insert->open();
        Tuple& result = insert->next();
        EXPECT_TRUE(SystemTestUtil::SINGLE_INT_DESCRIPTOR->equals(*(result.getTupleDesc())));
        EXPECT_EQ(1, (dynamic_pointer_cast<IntField>(result.getField(0)))->getValue());
        EXPECT_FALSE(insert->hasNext());
        insert->close();
	}

    static bool findMagicTuple(shared_ptr<HeapFile> f, shared_ptr<Transaction> t) {
    
        shared_ptr<SeqScan> ss = make_shared<SeqScan>(t->getId(), f->getId(), "");
        bool found = false;
        ss->open();
        while (ss->hasNext()) {
            Tuple& v = ss->next();
            int v0 = (dynamic_pointer_cast<IntField>(v.getField(0)))->getValue();
            int v1 = (dynamic_pointer_cast<IntField>(v.getField(1)))->getValue();
            if (v0 == -42 && v1 == -43) {
                EXPECT_FALSE(found);
                found = true;
            }
        }
        ss->close();
        return found;
    }

};


/** Aborts a transaction and ensures that its effects were actually undone.
 * This requires dirty pages to <em>not</em> get flushed to disk.
 */
TEST_F(AbortEvictionTest, TestDoNotEvictDirtyPages) {
    // Allocate a file with ~10 pages of data
    shared_ptr<HeapFile> f = SystemTestUtil::createRandomHeapFile(2, 512 * 10, map<int, int>(), vector<vector<int>>());
    Database::resetBufferPool(2);

    // BEGIN TRANSACTION
    shared_ptr<Transaction> t = make_shared<Transaction>();
    t->start();

    // Insert a new row
    AbortEvictionTest::insertRow(f, t);

    // The tuple must exist in the table
    bool found = AbortEvictionTest::findMagicTuple(f, t);
    EXPECT_TRUE(found);
    // ABORT
    t->transactionComplete(true);

    // A second transaction must not find the tuple
    t = make_shared<Transaction>();
    t->start();
    found = AbortEvictionTest::findMagicTuple(f, t);
    EXPECT_FALSE(found);
    t->commit();
}