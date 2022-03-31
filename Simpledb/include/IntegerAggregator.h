#pragma once
#include"Aggregator.h"
#include"IntField.h"
#include<map>

namespace Simpledb {
	/**
	* Knows how to compute some aggregate over a set of IntFields.
	*/
	class IntegerAggregator : public Aggregator {
    public:
        /**
         * Aggregate constructor
         *
         * @param gbfield
         *            the 0-based index of the group-by field in the tuple, or
         *            NO_GROUPING if there is no grouping
         * @param gbfieldtype
         *            the type of the group by field (e.g., Type.INT_TYPE), or null
         *            if there is no grouping
         * @param afield
         *            the 0-based index of the aggregate field in the tuple
         * @param what
         *            the aggregation operator
         */
        IntegerAggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what);
        /**
         * Merge a new tuple into the aggregate, grouping as indicated in the
         * constructor
         *
         * @param tup
         *            the Tuple containing an aggregate field and a group-by field
         */
        void mergeTupleIntoGroup(Tuple& tup)override;
        /**
         * Create a OpIterator over group aggregate results.
         *
         * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
         *         if using group, or a single (aggregateVal) if no grouping. The
         *         aggregateVal is determined by the type of aggregate specified in
         *         the constructor.
         */
        shared_ptr<OpIterator> iterator()override;
    private:
        void modifyTuple(shared_ptr<Tuple> t, int modifyfield, int64_t groupVal, shared_ptr<IntField> aggregateField);

        static long _serialVersionUID;

	};
}