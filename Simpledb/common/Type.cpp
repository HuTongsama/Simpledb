#include"Type.h"
namespace Simpledb 
{
	int Type::STRING_LEN = 128;

	shared_ptr<Int_Type> Int_Type::INT_TYPE = shared_ptr<Int_Type>(new Int_Type());
	
	shared_ptr<Field> Int_Type::parse(istream& instream)
	{
		return shared_ptr<Field>();
	}

	shared_ptr<String_Type> String_Type::STRING_TYPE = shared_ptr<String_Type>(new String_Type());
	
	shared_ptr<Field> String_Type::parse(istream& instream)
	{
		return shared_ptr<Field>();
	}
}