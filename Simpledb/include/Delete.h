#pragma once
#include"Operator.h"
#include"TransactionId.h"
namespace Simpledb {
    /**
     * The delete operator. Delete reads tuples from its child operator and removes
    * them from the table they belong to.
    */
	class Delete :public Operator {
    public:
        /**
         * Constructor specifying the transaction that this delete belongs to as
         * well as the child to read from.
         *
         * @param t
         *            The transaction this delete runs in
         * @param child
         *            The child operator from which to read tuples for deletion
         */
        Delete(shared_ptr<TransactionId> t, shared_ptr<OpIterator> child);
        shared_ptr<TupleDesc> getTupleDesc()override;
        void open()override;
        void close()override;
        void rewind()override;
        vector<shared_ptr<OpIterator>> getChildren()override;
        void setChildren(const vector<shared_ptr<OpIterator>>& children)override;

    protected:
        /**
         * Deletes tuples as they are read from the child operator. Deletes are
         * processed via the buffer pool (which can be accessed via the
         * Database.getBufferPool() method.
         *
         * @return A 1-field tuple containing the number of deleted records.
         * @see Database#getBufferPool
         * @see BufferPool#deleteTuple
         */
        Tuple* fetchNext()override;

    private:
        static long _serialVersionUID;
        shared_ptr<TransactionId> _tid;
        vector<shared_ptr<OpIterator>> _children;
        shared_ptr<Tuple> _result;
        shared_ptr<TupleDesc> _td;
	};
}