#include"IntField.h"
#include"Common.h"
namespace Simpledb 
{
	long IntField::_serialVersionUID = 1l;
	vector<unsigned char> IntField::serialize()const
	{
		vector<unsigned char> result = serializeCommonType(_value);
		return result;
	}
	bool IntField::compare(Predicate::Op op, const Field& value)const
	{
		try
		{
			const IntField& iVal = dynamic_cast<const IntField&>(value);
			switch (op)
			{
			case Simpledb::Predicate::Op::EQUALS:
				return _value == iVal._value;
			case Simpledb::Predicate::Op::GREATER_THAN:
				return _value > iVal._value;
			case Simpledb::Predicate::Op::LESS_THAN:
				return _value < iVal._value;
			case Simpledb::Predicate::Op::LESS_THAN_OR_EQ:
				return _value <= iVal._value;
			case Simpledb::Predicate::Op::GREATER_THAN_OR_EQ:
				return _value >= iVal._value;
			case Simpledb::Predicate::Op::LIKE:
				return _value == iVal._value;
			case Simpledb::Predicate::Op::NOT_EQUALS:
				return _value != iVal._value;
			default:
				return false;
			}
			return false;
		}
		catch (const std::exception&)
		{
			return false;
		}
		
	}
	Type::TYPE IntField::getType()const
	{
		return Type::TYPE::INT_TYPE;
	}
	int64_t IntField::hashCode()const
	{
		return _value;
	}
	bool IntField::equals(const Field& feild)const
	{
		try
		{
			const IntField& iVal = dynamic_cast<const IntField&>(feild);
			return iVal._value == _value;
		}
		catch (const std::exception&)
		{
			return false;
		}
		
	}
	string IntField::toString()const
	{
		return to_string(_value);
	}
}