#include"Delete.h"
#include"Database.h"
#include"IntField.h"
namespace Simpledb {
	long Delete::_serialVersionUID = 1l;
	Delete::Delete(shared_ptr<TransactionId> t, shared_ptr<OpIterator> child)
		:_tid(t)
	{
		_children.push_back(child);
		_td = make_shared<TupleDesc>(vector<shared_ptr<Type>>{ Int_Type::INT_TYPE() });
		_result = nullptr;
	}
	shared_ptr<TupleDesc> Delete::getTupleDesc()
	{
		return _td;
	}
	void Delete::open()
	{
		Operator::open();
		for (auto& child : _children) {
			child->open();
		}
	}
	void Delete::close()
	{
		Operator::close();
		for (auto& child : _children) {
			child->close();
		}
	}
	void Delete::rewind()
	{
		for (auto& child : _children) {
			child->rewind();
		}
	}
	vector<shared_ptr<OpIterator>> Delete::getChildren()
	{
		return _children;
	}
	void Delete::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
		_children = children;
	}
	Tuple* Delete::fetchNext()
	{
		int count = 0;
		shared_ptr<OpIterator> child = _children[0];
		BufferPool* pBufferPool = Database::getBufferPool();
		while (child->hasNext()) {
			Tuple& t = child->next();
			pBufferPool->deleteTuple(_tid, t);
			count++;
		}
		if (nullptr == _result) {
			_result = make_shared<Tuple>(_td);
			_result->setField(0, make_shared<IntField>(count));
			return _result.get();
		}
		else {
			return nullptr;
		}
		
	}
}