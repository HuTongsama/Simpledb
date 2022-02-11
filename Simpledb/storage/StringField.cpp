#include"StringField.h"

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
	void StringField::serialize(ostream& outStream)const
	{
		string str = _value;
		int overflow = _maxSize - (int)str.size();
		if (overflow < 0) {
			str = str.substr(0, _maxSize);
		}
		outStream << str.size();
		outStream << str;
		while (overflow > 0)
		{
			outStream << '\0';
			overflow--;
		}
	}
	bool StringField::compare(Predicate::Op op, const Field& value)const
	{
		try
		{
			const StringField& stringField = dynamic_cast<const StringField&>(value);
			int cmpVal = _value.compare(stringField._value);
			switch (op)
			{
			case Simpledb::Predicate::EQUALS:
				return cmpVal == 0;
			case Simpledb::Predicate::GREATER_THAN:
				return cmpVal > 0;
			case Simpledb::Predicate::LESS_THAN:
				return cmpVal < 0;
			case Simpledb::Predicate::LESS_THAN_OR_EQ:
				return cmpVal <= 0;
			case Simpledb::Predicate::GREATER_THAN_OR_EQ:
				return cmpVal >= 0;
			case Simpledb::Predicate::LIKE:
				return _value.find(stringField._value) != string::npos;
			case Simpledb::Predicate::NOT_EQUALS:
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