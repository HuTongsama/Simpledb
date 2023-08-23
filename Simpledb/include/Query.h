#pragma once
#include"Operator.h"
#include"TransactionId.h"
#include"LogicalPlan.h"
namespace Simpledb {
    /**
     * Query is a wrapper class to manage the execution of queries. It takes a query
     * plan in the form of a high level OpIterator (built by initiating the
     * constructors of query plans) and runs it as a part of a specified
     * transaction.
     *
     */
	class Query {
	public:
        shared_ptr<TransactionId> getTransactionId() {
            return _tid;
        }

        void setLogicalPlan(shared_ptr<LogicalPlan> lp) {
            _logicalPlan = lp;
        }

        shared_ptr<LogicalPlan> getLogicalPlan() {
            return _logicalPlan;
        }

        void setPhysicalPlan(shared_ptr<OpIterator> pp) {
            _op = pp;
        }

        shared_ptr<OpIterator> getPhysicalPlan() {
            return _op;
        }

        Query(shared_ptr<TransactionId> t) :_tid(t) {}

        Query(shared_ptr<OpIterator> root, shared_ptr<TransactionId> t)
            :_op(root), _tid(t) {}

        void start();

        shared_ptr<TupleDesc> getOutputTupleDesc() {
            return _op->getTupleDesc();
        }

        bool hasNext() {
            return _op->hasNext();
        }
        /**
         * Returns the next tuple, or throws NoSuchElementException if the iterator
         * is closed.
         *
         * @return The next tuple in the iterator
         * @throws runtime error
         */
        Tuple* next();

        void close() {
            _op->close();
            _started = false;
        }

        void execute();
	private:
		static const long _serialVersionUID = 1L;

		shared_ptr<OpIterator> _op;
		shared_ptr<LogicalPlan> _logicalPlan;
		shared_ptr<TransactionId> _tid;
        bool _started = false;
	};
}