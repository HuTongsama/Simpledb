#include"Insert.h"
namespace Simpledb {
	long Insert::_serialVersionUID = 1l;
	Insert::Insert(shared_ptr<TransactionId> t, shared_ptr<OpIterator> child, int tableId)
	{
	}
	shared_ptr<TupleDesc> Insert::getTupleDesc()
	{
		return shared_ptr<TupleDesc>();
	}
	void Insert::open()
	{
	}
	void Insert::close()
	{
	}
	void Insert::rewind()
	{
	}
	vector<shared_ptr<OpIterator>> Insert::getChildren()
	{
		return vector<shared_ptr<OpIterator>>();
	}
	void Insert::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
	}
	Tuple* Insert::fetchNext()
	{
		return nullptr;
	}
}