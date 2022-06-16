#pragma once
#include"TransactionId.h"
#include"Database.h"
#include<memory>
namespace Simpledb {

	class Transaction {

    public:
        Transaction() {
            _tid = make_shared<TransactionId>();
        }

        /** Start the transaction running */
        void start() {
             _started = true;
             try
             {
                 Database::getLogFile()->logXactionBegin(*_tid);
             }
             catch (const std::exception& e)
             {
                 cout << e.what() << endl;
             }
        }

        shared_ptr<TransactionId> getId() {
            return _tid;
        }

        /** Finish the transaction */
        void commit() {
            transactionComplete(false);
        }

        /** Finish the transaction */
        void abort() {
            transactionComplete(true);
        }

        /** Handle the details of transaction commit / abort */
        void transactionComplete(bool abort){

            if (_started) {
                //write abort log record and rollback transaction
                if (abort) {
                    Database::getLogFile()->logAbort(*_tid); //does rollback too
                }

                // Release locks and flush pages if needed
                Database::getBufferPool()->transactionComplete(_tid, !abort); // release locks

                // write commit log record
                if (!abort) {
                    Database::getLogFile()->logCommit(*_tid);
                }

                //setting this here means we could possibly write multiple abort records -- OK?
                _started = false;
            }
        }

    private:
        shared_ptr<TransactionId> _tid;
        mutable bool _started;
	};
}