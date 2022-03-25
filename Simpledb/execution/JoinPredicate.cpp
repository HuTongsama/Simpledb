#include"JoinPredicate.h"
namespace Simpledb {
	long JoinPredicate::_serialVersionUID = 1l;
	JoinPredicate::JoinPredicate(int field1, Predicate::Op op, int field2)
		:_field1(field1), _op(op), _field2(field2)
	{
	}
	bool JoinPredicate::filter(shared_ptr<Tuple> t1, shared_ptr<Tuple> t2)
	{
		return filter(*t1, *t2);
	}
	bool JoinPredicate::filter(Tuple& t1, Tuple& t2)
	{
		shared_ptr<Field> lhs = t1.getField(_field1);
		shared_ptr<Field> rhs = t2.getField(_field2);
		if (lhs == nullptr || rhs == nullptr)
			return false;
		return lhs->compare(_op, *rhs);
	}
	int JoinPredicate::getField1()
	{
		return _field1;
	}
	int JoinPredicate::getField2()
	{
		return _field2;
	}
	Predicate::Op JoinPredicate::getOperator()
	{
		return _op;
	}
}