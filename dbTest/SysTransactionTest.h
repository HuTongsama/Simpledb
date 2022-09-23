#pragma once
#include"SimpleDbTestBase.h"
#include"AbortEvictionTestBase.h"
#include"SystemTestUtil.h"
#include"TestUtil.h"
#include"Database.h"
#include"Transaction.h"
#include"Query.h"
#include"Delete.h"
#include"Insert.h"
#include<atomic>
#include<vector>
class SysTransactionTest : public SimpleDbTestBase {
protected:
    class ModifiableCyclicBarrier {
    public:    
        ModifiableCyclicBarrier(int parties) {
            _resetFlag = false;
            _parties = 0;
            reset(parties);
        }
        void await() {
            reset();
            _awaitLatch->countDown();
            _awaitLatch->await();
        }
        void notParticipating() {
            reset();
            _participationLatch->await();
        }
        void stillParticipating() {
            reset();
            _nextParticipants++;
            _participationLatch->await();
        }
    private:
        class UpdateLatch {
        public:
            UpdateLatch(ModifiableCyclicBarrier* latch, atomic_int* pNextParticipants) {
                _latch = latch;
                _pNextParticipants = pNextParticipants;
            }
            void operator()() {
                // Reset this barrier if there are threads still running
                if (_pNextParticipants == nullptr || _latch == nullptr)
                    return;
                int participants = *_pNextParticipants;
                if (participants > 0) {
                    _latch->_parties = participants;
                    _latch->_resetFlag = true;
                }
            }
        private:
            ModifiableCyclicBarrier* _latch;
            atomic_int* _pNextParticipants;

        };

        using CountDownLatch = TestUtil::CountDownLatch;
        using CyclicBarrier = TestUtil::CyclicBarrier<UpdateLatch>;
        
        void reset() {
            lock_guard<mutex> lock(_resetMutex);
            if (_resetFlag)
                reset(_parties);
        }
        void reset(int parties) {
            _nextParticipants = 0;
            _awaitLatch = make_shared<CountDownLatch>(parties);
            _participationLatch = make_shared<CyclicBarrier>(parties, 
                make_shared<UpdateLatch>(this, &_nextParticipants));
            _resetFlag = false;
        }



        shared_ptr<CountDownLatch> _awaitLatch;
        shared_ptr<CyclicBarrier> _participationLatch;
        atomic_int _nextParticipants;
        bool _resetFlag;
        int _parties;
        mutex _resetMutex;
    };

    class XactionTester {
    public:
        XactionTester(size_t tableId, shared_ptr<ModifiableCyclicBarrier> latch) :
            _tableId(tableId), _latch(latch), _isAlive(false), _completed(false) {}
        void start() {
            thread t(&XactionTester::run, this);
            this_thread::sleep_for(chrono::milliseconds(500));
            t.detach();
        }
        void join(int milliseconds = 0) {
            if (milliseconds < 0)
                throw runtime_error("error join parameter");
            unique_lock<mutex> lock(_joinMutex);
            if (_isAlive) {
                if (milliseconds == 0) {
                    _joinCond.wait(lock);
                }
                else {
                    _joinCond.wait_for(lock, std::chrono::milliseconds(milliseconds));
                }
            }
            
        }
        void run() { 
            {
                unique_lock<mutex> lock(_joinMutex);
                _isAlive = true;
            }        
            try {
                // Try to increment the value until we manage to successfully commit
                while (true) {
                    // Wait for all threads to be ready
                    _latch->await();
                    shared_ptr<Transaction> tr = make_shared<Transaction>();
                    try {
                        {
                            lock_guard<mutex> lock(_printMutex);
                            printf("t %s start loop\n", to_string(tr->getId()->getId()).c_str());
                        }
                        tr->start();
                        shared_ptr<SeqScan> ss1 = make_shared<SeqScan>(tr->getId(), _tableId, "");
                        shared_ptr<SeqScan> ss2 = make_shared<SeqScan>(tr->getId(), _tableId, "");

                        // read the value out of the table
                        shared_ptr<Query> q1 = make_shared<Query>(ss1, tr->getId());
                        q1->start();
                        Tuple& tup = q1->next();
                        shared_ptr<IntField> intf = dynamic_pointer_cast<IntField>(tup.getField(0));
                        int i = intf->getValue();

                        // create a Tuple so that Insert can insert this new value
                        // into the table.
                        shared_ptr<Tuple> t = make_shared<Tuple>(SystemTestUtil::SINGLE_INT_DESCRIPTOR);
                        t->setField(0, make_shared<IntField>(i + 1));

                        // sleep to get some interesting thread interleavings
                        this_thread::sleep_for(std::chrono::milliseconds(1));

                        // race the other threads to finish the transaction: one will win
                        q1->close();

                        // delete old values (i.e., just one row) from table
                        shared_ptr<Delete> delOp = make_shared<Delete>(tr->getId(), ss2);

                        shared_ptr<Query> q2 = make_shared<Query>(delOp, tr->getId());

                        q2->start();
                        q2->next();
                        q2->close();

                        // set up a Set with a tuple that is one higher than the old one.
                        vector<shared_ptr<Tuple>> hs;
                        hs.push_back(t);
                        shared_ptr<TupleIterator> ti = make_shared<TupleIterator>(t->getTupleDesc(), hs);

                        // insert this new tuple into the table
                        shared_ptr<Insert> insOp = make_shared<Insert>(tr->getId(), ti, _tableId);
                        shared_ptr<Query> q3 = make_shared<Query>(insOp, tr->getId());
                        q3->start();
                        q3->next();
                        q3->close();

                        tr->commit();
                        break;
                    }
                    catch (const std::exception& e) {
                        //System.out.println("thread " + tr.getId() + " killed");
                        // give someone else a chance: abort the transaction
                        {
                            lock_guard<mutex> lock(_printMutex);
                            printf("t %s throw %s\n", to_string(tr->getId()->getId()).c_str(), e.what());
                        }
                        tr->transactionComplete(true);
                        _latch->stillParticipating();
                    }
                }
                //System.out.println("thread " + id + " done");
            }
            catch (const std::exception& e) {
                // Store exception for the master thread to handle
                _exception = e.what();
            }

            try {
                _latch->notParticipating();
            }
            catch (const std::exception& e) {
                throw e;
            }
            unique_lock<mutex> lock(_joinMutex);
            _completed = true;
            _joinCond.notify_all();
            
        }
        bool isCompleted() {
            return _completed;
        }

        string exception() {
            return _exception;
        }

        bool isAlive() {
            return _isAlive;
        }
        
        
        

    private:
        size_t _tableId;
        shared_ptr<ModifiableCyclicBarrier> _latch;
        condition_variable _joinCond;
        mutex _joinMutex;
        string _exception = "";
        bool _completed = false;
        bool _isAlive;
        mutex _printMutex;
    };

	static const int TIMEOUT_MILLIS = 10 * 60 * 1000;

	void validateTransactions(int threads) {
        // Create a table with a single integer value = 0
        map<int, int> columnSpecification;
        columnSpecification.insert(pair<int,int>(0,0));
        shared_ptr<DbFile> table = SystemTestUtil::createRandomHeapFile(1, 1, columnSpecification, vector<vector<int>>());

        shared_ptr<ModifiableCyclicBarrier> latch =  make_shared<ModifiableCyclicBarrier>(threads);
        vector<shared_ptr<XactionTester>> list;
        for (int i = 0; i < threads; ++i) {
            list.push_back(make_shared<XactionTester>(table->getId(), latch));
            list[i]->start();
        }
        long stopTestTime = clock() + TIMEOUT_MILLIS;
        for (auto& tester : list) {
            long timeout = stopTestTime - clock();
            if (timeout <= 0) {
                FAIL() << "Timed out waiting for transaction to complete" << endl;
            }
            try
            {
                tester->join(timeout);
            }
            catch (const std::exception& e)
            {
                throw e;
            }
            if (tester->isAlive()) {
                FAIL() << "Timed out waiting for transaction to complete" << endl;
            }

            if (tester->exception() != "") {
                throw runtime_error(tester->exception());
            }
            ;
            EXPECT_TRUE(tester->isCompleted());
        }

        // Check that the table has the correct value
        shared_ptr<TransactionId> tid = make_shared<TransactionId>();
        shared_ptr<DbFileIterator> it = table->iterator(tid);
        it->open();
        Tuple& tup = it->next();
        EXPECT_EQ(threads, dynamic_pointer_cast<IntField>(tup.getField(0))->getValue());
        it->close();
        Database::getBufferPool()->transactionComplete(tid);
        Database::getBufferPool()->flushAllPages();
	}

};
TEST_F(SysTransactionTest, TestSingleThread) {
    //validateTransactions(1);
}

TEST_F(SysTransactionTest, TestTwoThreads) { 
    //validateTransactions(2);
}

TEST_F(SysTransactionTest, TestFiveThreads) {
    validateTransactions(5);
}

TEST_F(SysTransactionTest, TestTenThreads) {
    //validateTransactions(10);
}

TEST_F(SysTransactionTest, TestAllDirtyFails) {
    //// Allocate a file with ~10 pages of data
    //shared_ptr<HeapFile> f = SystemTestUtil::createRandomHeapFile(2, 512 * 10, map<int,int>(), vector<vector<int>>());
    //Database::resetBufferPool(1);

    //// BEGIN TRANSACTION
    //shared_ptr<Transaction> t = make_shared<Transaction>();
    //t->start();
    //
    //// Insert a new row
    //AbortEvictionTest::insertRow(f, t);
    //// Scanning the table must fail because it can't evict the dirty page
    //try
    //{
    //    AbortEvictionTest::findMagicTuple(f, t);
    //    FAIL() << "Expected scan to run out of available buffer pages" << endl;
    //}
    //catch (const std::exception&)
    //{
    //    //ignore
    //}
    //t->commit();

}