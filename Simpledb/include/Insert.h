#pragma once
#include"Operator.h"
#include"TransactionId.h"

namespace Simpledb {
    /**
    * Inserts tuples read from the child operator into the tableId specified in the
    * constructor
    */
	class Insert : public Operator {
    public:
        /**
         * Constructor.
         *
         * @param t
         *            The transaction running the insert.
         * @param child
         *            The child operator from which to read tuples to be inserted.
         * @param tableId
         *            The table in which to insert tuples.
         * @throws DbException
         *             if TupleDesc of child differs from table into which we are to
         *             insert.
         */
        Insert(shared_ptr<TransactionId> t, shared_ptr<OpIterator> child, int tableId);
        shared_ptr<TupleDesc> getTupleDesc()override;
        void open()override;
        void close()override;
        void rewind()override;
        vector<shared_ptr<OpIterator>> getChildren()override;      
        void setChildren(const vector<shared_ptr<OpIterator>>& children)override;

    protected:
        /**
         * Inserts tuples read from child into the tableId specified by the
         * constructor. It returns a one field tuple containing the number of
         * inserted records. Inserts should be passed through BufferPool. An
         * instances of BufferPool is available via Database.getBufferPool(). Note
         * that insert DOES NOT need check to see if a particular tuple is a
         * duplicate before inserting it.
         *
         * @return A 1-field tuple containing the number of inserted records, or
         *         null if called more than once.
         * @see Database#getBufferPool
         * @see BufferPool#insertTuple
         */
        Tuple* fetchNext()override;

    private:
        static long _serialVersionUID;
	};
}