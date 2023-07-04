#include"IndexPredicate.h"
namespace Simpledb {
	long IndexPredicate::_serialVersionUID = 1l;
	IndexPredicate::IndexPredicate(Predicate::Op op, shared_ptr<Field> fvalue)
		:_op(op), _fieldvalue(fvalue)
	{
		
	}
	shared_ptr<Field> IndexPredicate::getField()
	{
		return _fieldvalue;
	}
	Predicate::Op IndexPredicate::getOp()
	{
		return _op;
	}
	bool IndexPredicate::equals(IndexPredicate ipd)
	{
		return (_op == ipd._op) && _fieldvalue->equals(*ipd._fieldvalue);
	}
}