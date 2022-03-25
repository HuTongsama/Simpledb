#include"Predicate.h"
#include"Tuple.h"
namespace Simpledb {
	Predicate::Predicate(int field, Op op, shared_ptr<Field> operand)
	{
		_field = field;
		_op = op;
		_operand = operand;
		//"f = field_id op = op_string operand = operand_string
		_str = "f = " + to_string(_field) + " op = " + opToString(_op) + " operand = " + operand->toString();
	}
	int Predicate::getField()
	{
		return _field;
	}
	Predicate::Op Predicate::getOp()
	{
		return _op;
	}
	shared_ptr<Field> Predicate::getOperand()
	{
		return _operand;
	}
	bool Predicate::filter(shared_ptr<Tuple> t)
	{
		return filter(*t);
	}
	bool Predicate::filter(Tuple& t)
	{
		shared_ptr<Field> lhs = t.getField(_field);
		if (lhs == nullptr)
			return false;
		return lhs->compare(_op, *_operand);
	}
	string Predicate::toString()
	{
		return _str;
	}
}