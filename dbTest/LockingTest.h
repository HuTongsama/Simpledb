#pragma once
#include"SimpleDbTestBase.h"
#include"TestUtil.h"
#include"Database.h"
#include<Windows.h>
using namespace Simpledb;

class LockingTest : public SimpleDbTestBase, public TestUtil::CreateHeapFile {

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

        // if this fails, complain to the TA
        EXPECT_EQ(3, _empty->numPages());

        _p0 = make_shared<HeapPageId>(_empty->getId(), 0);
        _p1 = make_shared<HeapPageId>(_empty->getId(), 1);
        shared_ptr<PageId> p2 = make_shared<HeapPageId>(_empty->getId(), 2);
        _tid1 = make_shared<TransactionId>();
        _tid2 = make_shared<TransactionId>();

        // forget about locks associated to tid, so they don't conflict with
        // test cases
        _bp->getPage(tid, _p0, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->getPage(tid, _p1, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->getPage(tid, p2, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->flushAllPages();
        _bp = Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
	}

    /**
    * Generic unit test structure for BufferPool.getPage() assuming locking.
    *
    * @param tid1 the first transaction Id
    * @param pid1 the first page to lock over
    * @param perm1 the type of lock for the first page
    * @param tid2 the second transaction Id
    * @param pid2 the second page to lock over
    * @param perm2 the type of lock for the second page
    * @param expected true if we expect the second acquisition to succeed;
    *   false otherwise
    */
    void metaLockTester(
        shared_ptr<TransactionId> tid1, shared_ptr<PageId> pid1, Permissions perm1,
        shared_ptr<TransactionId> tid2, shared_ptr<PageId> pid2, Permissions perm2,
        bool expected) {

        _bp->getPage(tid1, pid1, perm1);
        grabLock(tid2, pid2, perm2, expected);
    }
    /**
    * Generic unit test structure to grab an additional lock in a new
    * thread.
    *
    * @param tid the transaction Id
    * @param pid the first page to lock over
    * @param perm the type of lock desired
    * @param expected true if we expect the acquisition to succeed;
    *   false otherwise
    */
    void grabLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid, Permissions perm,
        bool expected) {

        TestUtil::LockGrabber t(tid, pid, perm);
        t.start();
        // if we don't have the lock after TIMEOUT, we assume blocking.     
        Sleep(TIMEOUT);
        EXPECT_EQ(expected, t.acquired());
        // TODO(ghuo): yes, stop() is evil, but this is unit test cleanup
        t.stop();     
    }

	static const int TIMEOUT = 100;
	shared_ptr<PageId> _p0, _p1;
	shared_ptr<TransactionId> _tid1, _tid2;
	shared_ptr<BufferPool> _bp;
};

/**
 * Unit test for BufferPool.getPage() assuming locking.
 * Acquires two read locks on the same page.
 */
TEST_F(LockingTest, AcquireReadLocksOnSamePage) {
    metaLockTester(
        _tid1, _p0, Permissions::READ_ONLY,
        _tid2, _p0, Permissions::READ_ONLY, true);
}
/**
 * Unit test for BufferPool.getPage() assuming locking.
 * Acquires a read lock and a write lock on the same page, in that order.
 */
TEST_F(LockingTest, AcquireReadWriteLocksOnSamePage) {
    metaLockTester(
        _tid1, _p0, Permissions::READ_ONLY,
        _tid2, _p0, Permissions::READ_WRITE, false);
}
/**
 * Unit test for BufferPool.getPage() assuming locking.
 * Acquires a write lock and a read lock on the same page, in that order.
 */
TEST_F(LockingTest, AcquireWriteReadLocksOnSamePage) {
    metaLockTester(
        _tid1, _p0, Permissions::READ_WRITE,
        _tid2, _p0, Permissions::READ_ONLY, false);
}

/**
 * Unit test for BufferPool.getPage() assuming locking.
 * Acquires a read lock and a write lock on different pages.
 */
TEST_F(LockingTest, AcquireReadWriteLocksOnTwoPages) {
    metaLockTester(
        _tid1, _p0, Permissions::READ_ONLY,
        _tid2, _p1, Permissions::READ_WRITE, true);
}
/**
 * Unit test for BufferPool.getPage() assuming locking.
 * Acquires write locks on different pages.
 */
TEST_F(LockingTest, AcquireWriteLocksOnTwoPages) {
    metaLockTester(
        _tid1, _p0, Permissions::READ_WRITE,
        _tid2, _p1, Permissions::READ_WRITE, true);
}
/**
 * Unit test for BufferPool.getPage() assuming locking.
 * Acquires read locks on different pages.
 */
TEST_F(LockingTest, AcquireReadLocksOnTwoPages) {
    metaLockTester(
        _tid1, _p0, Permissions::READ_ONLY,
        _tid2, _p1, Permissions::READ_ONLY, true);
}

/**
 * Unit test for BufferPool.getPage() assuming locking.
 * Attempt lock upgrade.
 */
TEST_F(LockingTest, LockUpgrade) {
    metaLockTester(
        _tid1, _p0, Permissions::READ_ONLY,
        _tid1, _p0, Permissions::READ_WRITE, true);
    metaLockTester(
        _tid2, _p1, Permissions::READ_ONLY,
        _tid2, _p1, Permissions::READ_WRITE, true);
}
/**
  * Unit test for BufferPool.getPage() assuming locking.
  * A single transaction should be able to acquire a read lock after it
  * already has a write lock.
 */
TEST_F(LockingTest, AcquireWriteAndReadLocks) {
    metaLockTester(
        _tid1, _p0, Permissions::READ_WRITE,
        _tid1, _p0, Permissions::READ_ONLY, true);
}
/**
 * Unit test for BufferPool.getPage() and BufferPool.releasePage()
 * assuming locking.
 * Acquires read locks on different pages.
 */
TEST_F(LockingTest, AcquireThenRelease) {
    _bp->getPage(_tid1, _p0, Permissions::READ_WRITE);
    _bp->unsafeReleasePage(_tid1, _p0);
    _bp->getPage(_tid2, _p0, Permissions::READ_WRITE);

    _bp->getPage(_tid2, _p1, Permissions::READ_WRITE);
    _bp->unsafeReleasePage(_tid2, _p1);
    _bp->getPage(_tid1, _p1, Permissions::READ_WRITE);
}