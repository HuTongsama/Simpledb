#pragma once
#include"AbortEvictionTestBase.h"


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