#pragma once
#include"Predicate.h"
#include"Field.h"

namespace Simpledb {
	class IndexPredicate {
	private:
		static long _serialVersionUID;
		Predicate::Op _op;
		shared_ptr<Field> _fieldvalue;

	public:
        /**
         * Constructor.
         *
         * @param fvalue The value that the predicate compares against.
         * @param op The operation to apply (as defined in Predicate.Op); either
         *   Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN, Predicate.Op.EQUAL,
         *   Predicate.Op.GREATER_THAN_OR_EQ, or Predicate.Op.LESS_THAN_OR_EQ
         * @see Predicate
         */
		IndexPredicate(Predicate::Op op, shared_ptr<Field> fvalue);
        shared_ptr<Field> getField();
        Predicate::Op getOp();
        /** Return true if the fieldvalue in the supplied predicate
            is satisfied by this predicate's fieldvalue and
            operator.
            @param ipd The field to compare against.
        */
        bool equals(IndexPredicate ipd);
	};
}
