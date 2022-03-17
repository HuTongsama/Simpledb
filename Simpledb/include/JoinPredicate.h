#pragma once
#include"Tuple.h"
#include"Predicate.h"

namespace Simpledb {
	class JoinPredicate {
	public:
        /**
		* Constructor -- create a new predicate over two fields of two tuples.
		*
		* @param field1
		*            The field index into the first tuple in the predicate
		* @param field2
		*            The field index into the second tuple in the predicate
		* @param op
		*            The operation to apply (as defined in Predicate.Op); either
		*            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
		*            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
		*            Predicate.Op.LESS_THAN_OR_EQ
		* @see Predicate
		*/
		JoinPredicate(int field1, Predicate::Op op, int field2);
		/**
		* Apply the predicate to the two specified tuples. The comparison can be
		* made through Field's compare method.
		*
		* @return true if the tuples satisfy the predicate.
		*/
		bool filter(shared_ptr<Tuple> t1, shared_ptr<Tuple> t2);
		int getField1();
		int getField2();
		Predicate::Op getOperator();
	private:
		static long _serialVersionUID;
	};
}