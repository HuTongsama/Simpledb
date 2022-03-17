#include"JoinPredicate.h"
namespace Simpledb {
	long JoinPredicate::_serialVersionUID = 1l;
	JoinPredicate::JoinPredicate(int field1, Predicate::Op op, int field2)
	{
	}
	bool JoinPredicate::filter(shared_ptr<Tuple> t1, shared_ptr<Tuple> t2)
	{
		return false;
	}
	int JoinPredicate::getField1()
	{
		return 0;
	}
	int JoinPredicate::getField2()
	{
		return 0;
	}
	Predicate::Op JoinPredicate::getOperator()
	{
		return Predicate::Op();
	}
}