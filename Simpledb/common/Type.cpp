#include"Type.h"
#include"IntField.h"
#include"StringField.h"
namespace Simpledb 
{
	int Type::STRING_LEN = 128;

	shared_ptr<Int_Type> Int_Type::INT_TYPE = shared_ptr<Int_Type>(new Int_Type());
	
	shared_ptr<Field> Int_Type::parse(DataStream& ds)
	{
		try
		{
			return make_shared<IntField>(ds.readInt());
		}
		catch (const std::exception&)
		{
			throw runtime_error("parse int failed");
		}
		
	}

	shared_ptr<String_Type> String_Type::STRING_TYPE = shared_ptr<String_Type>(new String_Type());
	
	shared_ptr<Field> String_Type::parse(DataStream& ds)
	{
		try
		{
			int len = ds.readInt();
			char* bytes = new char[len]();
			ds.read(bytes, len);
			ds.skipBytes(STRING_LEN - len);
			shared_ptr<StringField> s = make_shared<StringField>(string(bytes, len), STRING_LEN);
			delete[]bytes;
			return s;
		}
		catch (const std::exception&)
		{
			throw runtime_error("parse string failed");
		}
	}
}