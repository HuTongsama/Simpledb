#include"Insert.h"
#include"Database.h"
#include"IntField.h"
namespace Simpledb {
	long Insert::_serialVersionUID = 1l;
	Insert::Insert(shared_ptr<TransactionId> t, shared_ptr<OpIterator> child, size_t tableId)
		:_tid(t), _tableId(tableId)
	{
		_children.push_back(child);
		_td = make_shared<TupleDesc>(vector<shared_ptr<Type>>{ Int_Type::INT_TYPE() });
		shared_ptr<TupleDesc> tableTd = Database::getCatalog()->getTupleDesc(tableId);
		if (!tableTd->equals(*(child->getTupleDesc()))) {
			throw runtime_error("TupleDesc does not match");
		}
		_result = nullptr;
	}
	shared_ptr<TupleDesc> Insert::getTupleDesc()
	{
		return _td;
	}
	void Insert::open()
	{
		Operator::open();
		for (auto& child : _children) {
			child->open();
		}
	}
	void Insert::close()
	{
		Operator::close();
		for (auto& child : _children) {
			child->close();
		}
	}
	void Insert::rewind()
	{
		_result = nullptr;
		for (auto& child : _children) {
			child->rewind();
		}
	}
	vector<shared_ptr<OpIterator>> Insert::getChildren()
	{
		return _children;
	}
	void Insert::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
		_children = children;
	}
	Tuple* Insert::fetchNext()
	{
		shared_ptr<OpIterator> iter = _children[0];
		int count = 0;
		shared_ptr<BufferPool> pBufferPool = Database::getBufferPool();
		while (iter->hasNext()) {
			Tuple& t1 = iter->next();
			shared_ptr<Tuple> t2 = make_shared<Tuple>(_td);
			t1.copyTo(*t2);
			pBufferPool->insertTuple(_tid, _tableId, t2);
			count++;
		}
		if (_result == nullptr) {
			_result = make_shared<Tuple>(_td);
			_result->setField(0, make_shared<IntField>(count));
			return _result.get();
		}
		else {
			return nullptr;
		}
		
	}
}