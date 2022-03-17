#include"StringField.h"
#include"Common.h"
namespace Simpledb 
{
	long StringField::_serialVersionUID = 1l;
	StringField::StringField(const string& s, int maxSize)
	{
		_maxSize = maxSize;
		if (s.size() > maxSize) {
			_value = s.substr(0, maxSize);
		}
		else {
			_value = s;
		}
	}
	vector<unsigned char> StringField::serialize()const
	{
		string str = _value;
		int overflow = _maxSize - (int)str.size();
		if (overflow < 0) {
			str = str.substr(0, _maxSize);
		}
		vector<unsigned char> result = serializeCommonType((int)str.size());
		result.insert(result.end(), str.begin(), str.end());
		while (overflow > 0)
		{
			result.push_back(0);
			overflow--;
		}
		return result;
	}
	bool StringField::compare(Predicate::Op op, const Field& value)const
	{
		try
		{
			const StringField& stringField = dynamic_cast<const StringField&>(value);
			int cmpVal = _value.compare(stringField._value);
			switch (op)
			{
			case Simpledb::Predicate::Op::EQUALS:
				return cmpVal == 0;
			case Simpledb::Predicate::Op::GREATER_THAN:
				return cmpVal > 0;
			case Simpledb::Predicate::Op::LESS_THAN:
				return cmpVal < 0;
			case Simpledb::Predicate::Op::LESS_THAN_OR_EQ:
				return cmpVal <= 0;
			case Simpledb::Predicate::Op::GREATER_THAN_OR_EQ:
				return cmpVal >= 0;
			case Simpledb::Predicate::Op::LIKE:
				return _value.find(stringField._value) != string::npos;
			case Simpledb::Predicate::Op::NOT_EQUALS:
				return cmpVal != 0;
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
	shared_ptr<Type> StringField::getType()const
	{
		return String_Type::STRING_TYPE;
	}
	int64_t StringField::hashCode()const
	{
		size_t h = hash<string>{}(_value);
		return h;
	}
	bool StringField::equals(const Field& feild)const
	{
		try
		{
			const StringField& stringField = dynamic_cast<const StringField&>(feild);
			return _value == stringField._value;
		}
		catch (const std::exception&)
		{
			return false;
		}
	}
	string StringField::toString()const
	{
		return _value;
	}
}