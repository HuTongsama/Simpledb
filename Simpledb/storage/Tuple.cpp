#include"Tuple.h"
#include"IntField.h"
#include"StringField.h"
namespace Simpledb 
{
	long Tuple::_serialVersionUID = 1l;
	Tuple::Tuple(shared_ptr<TupleDesc> td)
		:_pTd(td) {
		auto iter = _pTd->iterator();
		while(iter->hasNext()){
			auto& item = iter->next();
			auto type = item._fieldType->type();
			if (type == Int_Type::INT_TYPE->type()){
				_fields.push_back(make_shared<IntField>(0));
			}
			else if(type == String_Type::STRING_TYPE->type()) {
				_fields.push_back(make_shared<StringField>("",0));
			}
		}
	}
	shared_ptr<TupleDesc> Tuple::getTupleDesc()
	{
		return _pTd;
	}
	shared_ptr<RecordId> Tuple::getRecordId()
	{
		return _pRId;
	}
	void Tuple::setRecordId(shared_ptr<RecordId> rid)
	{
		_pRId = rid;
	}
	void Tuple::setField(int i, shared_ptr<Field> field)
	{
		if (i < 0 || i >= _fields.size())
			throw "error field index";
		_fields[i] = field;
	}
	shared_ptr<Field> Tuple::getField(int i)
	{
		if (i < 0 || i >= _fields.size())
			return nullptr;
		return _fields[i];
	}
	string Tuple::toString()
	{
		size_t sz = _fields.size();
		string result;
		for (size_t i = 0; i < sz; ++i) {
			if (i + 1 == sz) {
				result += _fields[i]->toString();
				break;
			}else {
				result += _fields[i]->toString() + " ";
			}
		}
		return result;
	}

	void Tuple::resetTupleDesc(shared_ptr<TupleDesc> td)
	{
		_pTd = td;
	}
	void Tuple::copyTo(Tuple& dest)
	{
		dest.resetTupleDesc(_pTd);
		dest._fields.clear();
		size_t fields = _pTd->numFields();
		for (int i = 0; i < fields; ++i) {
			auto type = _pTd->getFieldType(i)->type();
			if (type == Type::TYPE::INT_TYPE) {
				dest._fields.push_back(make_shared<IntField>(
					dynamic_pointer_cast<IntField>(_fields[i])->getValue()));
			}
			else if (type == Type::TYPE::STRING_TYPE) {
				dest._fields.push_back(make_shared<StringField>(
					dynamic_pointer_cast<StringField>(_fields[i])->getValue(),Type::STRING_LEN));
			}
		}
	}
	bool Tuple::equals(Tuple& t)
	{
		if (!t.getTupleDesc()->equals(*_pTd))
			return false;
		int numFields = _fields.size();
		for (int id = 0; id < numFields; ++id) {
			auto f1 = t.getField(id);
			auto f2 = _fields[id];
			if (!f1->equals(*f2))
				return false;
		}
		return true;
	}
}