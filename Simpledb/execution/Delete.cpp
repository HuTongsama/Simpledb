#include"Delete.h"
namespace Simpledb {
	long Delete::_serialVersionUID = 1l;
	Delete::Delete(shared_ptr<TransactionId> t, shared_ptr<OpIterator> child)
	{
	}
	shared_ptr<TupleDesc> Delete::getTupleDesc()
	{
		return shared_ptr<TupleDesc>();
	}
	void Delete::open()
	{
	}
	void Delete::close()
	{
	}
	void Delete::rewind()
	{
	}
	vector<shared_ptr<OpIterator>> Delete::getChildren()
	{
		return vector<shared_ptr<OpIterator>>();
	}
	void Delete::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
	}
	Tuple* Delete::fetchNext()
	{
		return nullptr;
	}
}