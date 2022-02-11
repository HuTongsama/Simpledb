#include"Tuple.h"
namespace Simpledb 
{
	long Tuple::_serialVersionUID = 1l;
	Tuple::Tuple(shared_ptr<TupleDesc> td)
	{
	}
	const TupleDesc* Tuple::getTupleDesc()
	{
		return nullptr;
	}
	const RecordId* Tuple::getRecordId()
	{
		return nullptr;
	}
	void Tuple::setRecordId(shared_ptr<RecordId> rid)
	{
	}
	void Tuple::setField(int i, shared_ptr<Field> field)
	{
	}
	const Field* Tuple::getField(int i)
	{
		return nullptr;
	}
	string Tuple::toString()
	{
		return string();
	}
	//Iterator<shared_ptr<Field>> Tuple::fields()
	//{
	//	return Iterator<shared_ptr<Field>>();
	//}
	void Tuple::resetTupleDesc(shared_ptr<TupleDesc> td)
	{
	}
}