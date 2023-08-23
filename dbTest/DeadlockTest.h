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
        _rand = default_random_engine(static_cast<unsigned int>(time(0)));

        // forget about locks associated to tid, so they don't conflict with
        // test cases
        _bp->getPage(tid, _p0, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->getPage(tid, _p1, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->getPage(tid, p2, Permissions::READ_WRITE)->markDirty(true, tid);
        _bp->flushAllPages();
        _bp = Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
	}


    /**
    * Helper method to clean up the syntax of starting a LockGrabber thread.
    * The parameters pass through to the LockGrabber constructor.
    */
    shared_ptr<TestUtil::LockGrabber> startGrabber(shared_ptr<TransactionId> tid,
        shared_ptr<PageId> pid, Permissions perm) {

        shared_ptr<TestUtil::LockGrabber> lg = make_shared<TestUtil::LockGrabber>(tid, pid, perm);
        lg->start();
        
        return lg;
    }


	static const int POLL_INTERVAL = 100;
	static const int WAIT_INTERVAL = 200;


	shared_ptr<PageId> _p0, _p1;
	shared_ptr<TransactionId> _tid1, _tid2;
	default_random_engine _rand;
	shared_ptr<BufferPool> _bp;
};


/**
  * Not-so-unit test to construct a deadlock situation.
  * t1 acquires p0.read; t2 acquires p1.read; t1 attempts p1.write; t2
  * attempts p0.write. Rinse and repeat.
  */
TEST_F(DeadlockTest, TestReadWriteDeadlock) {
    cout << "testReadWriteDeadlock constructing deadlock:" << endl;

    auto lg1Read = startGrabber(_tid1, _p0, Permissions::READ_ONLY);
    auto lg2Read = startGrabber(_tid2, _p1, Permissions::READ_ONLY);

    // allow read locks to acquire
    this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL));
    auto lg1Write = startGrabber(_tid1, _p1, Permissions::READ_WRITE);
    auto lg2Write = startGrabber(_tid2, _p0, Permissions::READ_WRITE);
    while (true) {
        this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL));
        EXPECT_FALSE(lg1Write->acquired() && lg2Write->acquired());
        if (lg1Write->acquired() && !lg2Write->acquired()) break;
        if (!lg1Write->acquired() && lg2Write->acquired()) break;
        if (lg1Write->getError() != "") {
            lg1Read->stop();
            lg1Write->stop();
            _bp->transactionComplete(_tid1);
            this_thread::sleep_for(std::chrono::milliseconds(_rand() % WAIT_INTERVAL));

            _tid1 = make_shared<TransactionId>();
            lg1Read = startGrabber(_tid1, _p0, Permissions::READ_ONLY);
            lg1Write = startGrabber(_tid1, _p1, Permissions::READ_WRITE);
        }
        
        if (lg2Write->getError() != "") {
            lg2Read->stop();
            lg2Write->stop();
            _bp->transactionComplete(_tid2);
            this_thread::sleep_for(std::chrono::milliseconds(_rand() % WAIT_INTERVAL));

            _tid2 = make_shared<TransactionId>();
            lg2Read = startGrabber(_tid2, _p1, Permissions::READ_ONLY);
            lg2Write = startGrabber(_tid2, _p0, Permissions::READ_WRITE);
        }
       
    }

    cout << "testReadWriteDeadlock resolved deadlock" << endl;
}

/**
  * Not-so-unit test to construct a deadlock situation.
  * t1 acquires p0.write; t2 acquires p1.write; t1 attempts p1.write; t2
  * attempts p0.write.
  */
TEST_F(DeadlockTest, TestWriteWriteDeadlock) {
    cout << "testWriteWriteDeadlock constructing deadlock:" << endl;

    auto lg1Write0 = startGrabber(_tid1, _p0, Permissions::READ_WRITE);
    auto lg2Write1 = startGrabber(_tid2, _p1, Permissions::READ_WRITE);

    // allow initial write locks to acquire
    this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL));

    auto lg1Write1 = startGrabber(_tid1, _p1, Permissions::READ_WRITE);
    auto lg2Write0 = startGrabber(_tid2, _p0, Permissions::READ_WRITE);

    while (true) {
        this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL));
        EXPECT_FALSE(lg1Write1->acquired() && lg2Write0->acquired());
        if (lg1Write1->acquired() && !lg2Write0->acquired()) break;
        if (!lg1Write1->acquired() && lg2Write0->acquired()) break;

        if (lg1Write1->getError() != "") {
            lg1Write0->stop();
            lg1Write1->stop();
            _bp->transactionComplete(_tid1);
            this_thread::sleep_for(std::chrono::milliseconds(rand() % WAIT_INTERVAL));

            _tid1 = make_shared<TransactionId>();
            lg1Write0 = startGrabber(_tid1, _p0, Permissions::READ_WRITE);
            lg1Write1 = startGrabber(_tid1, _p1, Permissions::READ_WRITE);
        }

        if (lg2Write0->getError() != "") {
            lg2Write0->stop();
            lg2Write1->stop();
            _bp->transactionComplete(_tid2);
            this_thread::sleep_for(std::chrono::milliseconds(rand() % WAIT_INTERVAL));

            _tid2 = make_shared<TransactionId>();
            lg2Write0 = startGrabber(_tid2, _p1, Permissions::READ_WRITE);
            lg2Write1 = startGrabber(_tid2, _p0, Permissions::READ_WRITE);
        }
    }

    cout << "testWriteWriteDeadlock resolved deadlock" << endl;
}

/**
 * Not-so-unit test to construct a deadlock situation.
 * t1 acquires p0.read; t2 acquires p0.read; t1 attempts to upgrade to
 * p0.write; t2 attempts to upgrade to p0.write
 */
TEST_F(DeadlockTest, TestUpgradeWriteDeadlock) {
    cout << "testUpgradeWriteDeadlock constructing deadlock:" << endl;

    auto lg1Read = startGrabber(_tid1, _p0, Permissions::READ_ONLY);
    auto lg2Read = startGrabber(_tid2, _p0, Permissions::READ_ONLY);

    // allow read locks to acquire
    this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL));

    auto lg1Write = startGrabber(_tid1, _p0, Permissions::READ_WRITE);
    auto lg2Write = startGrabber(_tid2, _p0, Permissions::READ_WRITE);

    while (true) {
        this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL));
        EXPECT_FALSE(lg1Write->acquired() && lg2Write->acquired());
        if (lg1Write->acquired() && !lg2Write->acquired()) break;
        if (!lg1Write->acquired() && lg2Write->acquired()) break;

        if (lg1Write->getError() != "") {
            lg1Read->stop();
            lg1Write->stop();
            _bp->transactionComplete(_tid1);
            this_thread::sleep_for(std::chrono::milliseconds(rand() % WAIT_INTERVAL));
            _tid1 = make_shared<TransactionId>();
            lg1Read = startGrabber(_tid1, _p0, Permissions::READ_ONLY);
            lg1Write = startGrabber(_tid1, _p0, Permissions::READ_WRITE);
        }

        if (lg2Write->getError() != "") {
            lg2Read->stop();
            lg2Write->stop();
            _bp->transactionComplete(_tid2);
            this_thread::sleep_for(std::chrono::milliseconds(rand() % WAIT_INTERVAL));

            _tid2 = make_shared<TransactionId>();
            lg2Read = startGrabber(_tid2, _p0, Permissions::READ_ONLY);
            lg2Write = startGrabber(_tid2, _p0, Permissions::READ_WRITE);
        }
    }
    cout << "testUpgradeWriteDeadlock resolved deadlock" << endl;
}