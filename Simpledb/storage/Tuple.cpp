#include"Tuple.h"
#include"IntField.h"
#include"StringField.h"
namespace Simpledb 
{
	long Tuple::_serialVersionUID = 1l;
	Tuple::Tuple(shared_ptr<TupleDesc> td)
		:_pTd(td) , _iter(this) {
		auto& iter = _pTd->iterator();
		while(iter.hasNext()){
			auto& item = iter.next();
			string type = item._fieldType->toString();
			if (type == Int_Type::INT_TYPE->toString()){
				_fields.push_back(make_shared<IntField>(0));
			}
			else if(type == String_Type::STRING_TYPE->toString()) {
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
	Tuple::TupleIter::TupleIter(Tuple* pTuple)
		:_pTuple(pTuple)
	{
	}
	bool Tuple::TupleIter::hasNext()
	{
		if (_pTuple != nullptr && _pTuple->_fields.size() > _position)
			return true;
		return false;
	}
	shared_ptr<Field>& Tuple::TupleIter::next()
	{
		if (_pTuple != nullptr && _position < _pTuple->_fields.size()) {
			shared_ptr<Field>& p = _pTuple->_fields[_position];
			_position++;
			return p;
		}
		throw "no such element";
	}
	void Tuple::TupleIter::remove()
	{
		if (_pTuple != nullptr && _pTuple->_fields.size() > 0) {
			_pTuple->_fields.erase(_pTuple->_fields.begin() + _pTuple->_fields.size() - 1);
		}
	}
}