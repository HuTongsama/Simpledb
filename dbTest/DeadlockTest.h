#pragma once
#include"SimpleDbTestBase.h"
#include"TestUtil.h"
#include"Transaction.h"
#include<random>
using namespace Simpledb;
class DeadlockTest : public SimpleDbTestBase, public TestUtil::CreateHeapFile {

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
        shared_ptr<PageId> p2 = make_shared<HeapPageId>(_empty->getId(), 2);
        _tid1 = make_shared<TransactionId>();
        _tid2 = make_shared<TransactionId>();
        _rand = default_random_engine(time(0));

        // forget about locks associated to tid, so they don't conflict with
        // test cases
        _bp->getPage(tid, _p0, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->getPage(tid, _p1, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->getPage(tid, p2, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->flushAllPages();
        _bp = Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
	}



	static const int POLL_INTERVAL = 100;
	static const int WAIT_INTERVAL = 200;


	shared_ptr<PageId> _p0, _p1;
	shared_ptr<TransactionId> _tid1, _tid2;
	default_random_engine _rand;
	shared_ptr<BufferPool> _bp;
};