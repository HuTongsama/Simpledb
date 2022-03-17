#include"Predicate.h"
namespace Simpledb {
	Predicate::Predicate(int field, Op op, shared_ptr<Field> operand)
	{
	}
	size_t Predicate::getField()
	{
		return size_t();
	}
	Predicate::Op Predicate::getOp()
	{
		return Op();
	}
	shared_ptr<Field> Predicate::getOperand()
	{
		return shared_ptr<Field>();
	}
	bool Predicate::filter(shared_ptr<Tuple> t)
	{
		return false;
	}
	string Predicate::toString()
	{
		return string();
	}
}