#pragma once
#include"SimpleDbTestBase.h"
#include"SystemTestUtil.h"
#include"TestUtil.h"
#include"Database.h"
#include<atomic>
class SysTransactionTest : public SimpleDbTestBase {
protected:
    class ModifiableCyclicBarrier {
    public:
        
        ModifiableCyclicBarrier(int parties) {
            
        }
    private:
        class UpdateLatch {
        public:
            UpdateLatch(ModifiableCyclicBarrier* latch, atomic_int* pNextParticipants) {
                _latch = latch;
                _pNextParticipants = pNextParticipants;
            }
            void run() {
                // Reset this barrier if there are threads still running
                if (_pNextParticipants == nullptr || _latch == nullptr)
                    return;
                int participants = *_pNextParticipants;
                if (participants > 0) {
                    _latch->reset(participants);
                }
            }
        private:
            ModifiableCyclicBarrier* _latch;
            atomic_int* _pNextParticipants;

        };

        using CountDownLatch = TestUtil::CountDownLatch;
        using CyclicBarrier = TestUtil::CyclicBarrier<UpdateLatch*>;
        

        void reset(int parties) {
            _nextParticipants = 0;
            _awaitLatch = make_shared<CountDownLatch>(parties);
            _updateLatch = make_shared<UpdateLatch>(this, _nextParticipants);
            _participationLatch = make_shared<CyclicBarrier>(parties, &UpdateLatch::run);
        }



        shared_ptr<CountDownLatch> _awaitLatch;
        shared_ptr<CyclicBarrier> _participationLatch;
        shared_ptr<UpdateLatch> _updateLatch;
        atomic_int _nextParticipants;
    };

	static const int TIMEOUT_MILLIS = 10 * 60 * 1000;

	void validateTransactions(int threads) {
        // Create a table with a single integer value = 0
        map<int, int> columnSpecification;
        columnSpecification.insert(0, 0);
        shared_ptr<DbFile> table = SystemTestUtil::createRandomHeapFile(1, 1, columnSpecification, vector<vector<int>>());

        ModifiableCyclicBarrier latch = new ModifiableCyclicBarrier(threads);
        XactionTester[] list = new XactionTester[threads];
        for (int i = 0; i < list.length; i++) {
            list[i] = new XactionTester(table.getId(), latch);
            list[i].start();
        }

        long stopTestTime = System.currentTimeMillis() + TIMEOUT_MILLIS;
        for (XactionTester tester : list) {
            long timeout = stopTestTime - System.currentTimeMillis();
            if (timeout <= 0) {
                fail("Timed out waiting for transaction to complete");
            }
            try {
                tester.join(timeout);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (tester.isAlive()) {
                fail("Timed out waiting for transaction to complete");
            }

            if (tester.exception != null) {
                // Rethrow any exception from a child thread
                assert tester.exception != null;
                throw new RuntimeException("Child thread threw an exception.", tester.exception);
            }
            assert tester.completed;
        }

        // Check that the table has the correct value
        TransactionId tid = new TransactionId();
        DbFileIterator it = table.iterator(tid);
        it.open();
        Tuple tup = it.next();
        assertEquals(threads, ((IntField)tup.getField(0)).getValue());
        it.close();
        Database.getBufferPool().transactionComplete(tid);
        Database.getBufferPool().flushAllPages();
	}


};