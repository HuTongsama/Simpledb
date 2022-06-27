#pragma once
#include"SimpleDbTestBase.h"
#include"TestUtil.h"
using namespace Simpledb;
class TransactionTest
	: public SimpleDbTestBase, public TestUtil::CreateHeapFile {
protected:
	void SetUp()override {
		SimpleDbTestBase::SetUp();
		CreateHeapFile::SetUp();


        // clear all state from the buffer pool
        _bp = Database::resetBufferPool(BufferPool::DEFAULT_PAGES);

        // create a new empty HeapFile and populate it with three pages.
        // we should be able to add 504 tuples on an empty page.
        shared_ptr<TransactionId> tid = make_shared<TransactionId>();
        for (int i = 0; i < 1025; ++i) {
            _empty->insertTuple(tid, Utility::getHeapTuple(i, 2));
        }

        EXPECT_EQ(3, _empty->numPages());

        _p0 = make_shared<HeapPageId>(_empty->getId(), 0);
        _p1 = make_shared<HeapPageId>(_empty->getId(), 1);
        _p2 = make_shared<HeapPageId>(_empty->getId(), 2);
        _tid1 = make_shared<TransactionId>();
        _tid2 = make_shared<TransactionId>();

        // forget about locks associated to tid, so they don't conflict with
        // test cases
        _bp->getPage(tid, _p0, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->getPage(tid, _p1, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->getPage(tid, _p2, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->flushAllPages();
        _bp = Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
	}
    /**
    * Common unit test code for BufferPool.transactionComplete() covering
    * commit and abort. Verify that commit persists changes to disk, and
    * that abort reverts pages to their previous on-disk state.
    */
    void TestTransactionComplete(bool commit) {
        shared_ptr<HeapPage> p = dynamic_pointer_cast<HeapPage>(_bp->getPage(_tid1, _p2, Permissions::READ_WRITE));

        shared_ptr<Tuple> t = Utility::getHeapTuple({ 6, 830 });
        t->setRecordId(make_shared<RecordId>(_p2, 1));

        p->insertTuple(t);
        p->markDirty(true, _tid1);
        _bp->transactionComplete(_tid1, commit);

        // now, flush the buffer pool and access the page again from disk.
        _bp = Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
        p = dynamic_pointer_cast<HeapPage>(_bp->getPage(_tid2, _p2, Permissions::READ_WRITE));
        auto it = p->iterator();
        it->open();
        bool found = false;
        while (it->hasNext()) {
            Tuple& tup = it->next();
            shared_ptr<IntField> f0 = dynamic_pointer_cast<IntField>(tup.getField(0));
            shared_ptr<IntField> f1 = dynamic_pointer_cast<IntField>(tup.getField(1));

            if (f0->getValue() == 6 && f1->getValue() == 830) {
                found = true;
                break;
            }
        }

        EXPECT_EQ(commit, found);
    }

	shared_ptr<PageId> _p0, _p1, _p2;
	shared_ptr<TransactionId> _tid1, _tid2;
	// just so we have a pointer shorter than Database.getBufferPool()
	shared_ptr<BufferPool> _bp;
};


/**
 * Unit test for BufferPool.transactionComplete().
 * Try to acquire locks that would conflict if old locks aren't released
 * during transactionComplete().
 */
TEST_F(TransactionTest, AttemptTransactionTwice) {
    _bp->getPage(_tid1, _p0, Permissions::READ_ONLY);
    _bp->getPage(_tid1, _p1, Permissions::READ_WRITE);
    _bp->transactionComplete(_tid1, true);

    _bp->getPage(_tid2, _p0, Permissions::READ_WRITE);
    _bp->getPage(_tid2, _p0, Permissions::READ_WRITE);
}

/**
 * Unit test for BufferPool.transactionComplete() assuing commit.
 * Verify that a tuple inserted during a committed transaction is durable
 */
TEST_F(TransactionTest, CommitTransaction) {
    TestTransactionComplete(true);
}

/**
 * Unit test for BufferPool.transactionComplete() assuming abort.
 * Verify that a tuple inserted during a committed transaction is durable
 */
TEST_F(TransactionTest, AbortTransaction) {
    TestTransactionComplete(false);
}