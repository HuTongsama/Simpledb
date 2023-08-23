#include"TupleDesc.h"
namespace Simpledb {
	long TupleDesc::TDItem::_serialVersionUID = 1l;
	long TupleDesc::_serialVersionUID = 1l;
	//const Iterator<TupleDesc::TDItem>& TupleDesc::iterator()
	//{
	//	// TODO: insert return statement here
	//}
	TupleDesc::TupleDesc(const vector<shared_ptr<Type>>& typeVec, const vector<string>& fieldVec)
	{
		size_t tSz = typeVec.size();
		size_t fSz = fieldVec.size();
		for (int i = 0; i < tSz; ++i)
		{
			string fieldName = i < fSz ? fieldVec[i] : "";
			_tdItems.push_back(TDItem(typeVec[i], fieldName));
		}
	}
	TupleDesc::TupleDesc(const vector<shared_ptr<Type>>& typeVec)
	{
		for (auto& t : typeVec) {
			_tdItems.push_back(TDItem(t,""));
		}
	}
	size_t TupleDesc::numFields()const
	{
		return _tdItems.size();
	}
	string TupleDesc::getFieldName(int i)const
	{
		if (i < 0 || i >= _tdItems.size())
			return string();
		return _tdItems[i]._fieldName;
	}
	shared_ptr<Type> TupleDesc::getFieldType(int i)const
	{
		if (i < 0 || i >= _tdItems.size())
			return nullptr;
		return _tdItems[i]._fieldType;
	}
	int TupleDesc::fieldNameToIndex(const string& name)const
	{
		size_t sz = _tdItems.size();
		for (int i = 0; i < sz; ++i)
		{
			if (name == _tdItems[i]._fieldName)
				return i;
		}
		return -1;
	}
	size_t TupleDesc::getSize()const
	{
		size_t sz = 0;
		for (auto& item : _tdItems) {
			sz += item._fieldType->getLen();
		}
		return sz;
	}
	shared_ptr<TupleDesc> TupleDesc::merge(const TupleDesc& td1, const TupleDesc& td2)
	{
		vector<shared_ptr<Type>> tVec;
		vector<string> fVec;
		size_t sz1 = td1.numFields(), sz2 = td2.numFields();
		for (int i = 0; i < sz1; ++i) {
			tVec.push_back(td1.getFieldType(i));
			fVec.push_back(td1.getFieldName(i));
		}
		for (int i = 0; i < sz2; ++i) {
			tVec.push_back(td2.getFieldType(i));
			fVec.push_back(td2.getFieldName(i));
		}
		return shared_ptr<TupleDesc>(new TupleDesc(tVec,fVec));
	}
	bool TupleDesc::equals(const TupleDesc& td)const
	{
		size_t sz1 = _tdItems.size();
		size_t sz2 = td.numFields();
		if (sz1 != sz2)
			return false;
		for (int i = 0; i < sz1; ++i)
		{
			auto t1 = _tdItems[i]._fieldType->type();
			auto t2 = td.getFieldType(i)->type();
			if (t1 != t2)
				return false;
		}
		return true;
	}
	size_t TupleDesc::hashCode()const
	{
		return size_t();
	}
	string TupleDesc::toString()const
	{
		size_t sz = _tdItems.size();
		string result = "";
		for (int i = 0; i < sz; ++i) {
			result += _tdItems[i]._fieldType->toString() + "(" + _tdItems[i]._fieldName + ")";
			if (i != sz - 1) {
				result += ",";
			}
		}
		return result;
	}
	TupleDesc::TupleDescIter::TupleDescIter(TupleDesc* p)
	{
		_pTd = p;
	}
	bool TupleDesc::TupleDescIter::hasNext()
	{
		if (_pTd != nullptr && _pTd->_tdItems.size() > _position)
			return true;
		return false;
	}
	TupleDesc::TDItem* TupleDesc::TupleDescIter::next()
	{
		if (_pTd != nullptr && _position < _pTd->_tdItems.size()) {
			TDItem& p = _pTd->_tdItems[_position];
			_position++;
			return &p;
		}
		throw runtime_error("no such element");
	}

}