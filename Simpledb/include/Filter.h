#pragma once
#include"Operator.h"
namespace Simpledb {
	class Filter :public Operator {
    public:
        /**
         * Constructor accepts a predicate to apply and a child operator to read
         * tuples to filter from.
         *
         * @param p
         *            The predicate to filter tuples with
         * @param child
         *            The child operator
         */
        Filter(shared_ptr<Predicate> p, shared_ptr<OpIterator> child);
        shared_ptr<Predicate> getPredicate();
        shared_ptr<TupleDesc> getTupleDesc();
        void open()override;
        void close()override;
        void rewind()override;
        vector<shared_ptr<OpIterator>> getChildren()override;
        void setChildren(const vector<shared_ptr<OpIterator>>& children)override;
        
    protected:
        /**
         * AbstractDbIterator.readNext implementation. Iterates over tuples from the
         * child operator, applying the predicate to them and returning those that
         * pass the predicate (i.e. for which the Predicate.filter() returns true.)
         *
         * @return The next tuple that passes the filter, or null if there are no
         *         more tuples
         * @see Predicate#filter
         */
        shared_ptr<Tuple> fetchNext()override;
    private:
        static long _serialVersionUID;
	};
}