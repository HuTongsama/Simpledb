#include"Join.h"
namespace Simpledb {
	long Join::_serialVersionUID = 1l;
	Join::Join(shared_ptr<JoinPredicate> p, shared_ptr<OpIterator> child1, shared_ptr<OpIterator> child2)
	{
		_p = p;
		_children = vector<shared_ptr<OpIterator>>{ child1,child2 };
		_td = TupleDesc::merge(*(child1->getTupleDesc()), *(child2->getTupleDesc()));
		_lhs = nullptr;
		_rhs = nullptr;
	}
	shared_ptr<JoinPredicate> Join::getJoinPredicate()
	{
		return _p;
	}
	string Join::getJoinField1Name()
	{
		return _children[0]->getTupleDesc()->getFieldName(_p->getField1());
	}
	string Join::getJoinField2Name()
	{
		return _children[1]->getTupleDesc()->getFieldName(_p->getField2());
	}
	shared_ptr<TupleDesc> Join::getTupleDesc()
	{
		return _td;
	}
	void Join::open()
	{
		Operator::open();
		for (auto& child : _children) {
			child->open();
		}
	}
	void Join::close()
	{
		for (auto& child : _children) {
			child->close();
		}
		Operator::close();
	}
	void Join::rewind()
	{
		_lhs = nullptr;
		_rhs = nullptr;
		for (auto& child : _children) {
			child->rewind();
		}
	}
	vector<shared_ptr<OpIterator>> Join::getChildren()
	{
		return _children;
	}
	void Join::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
		_children = children;
	}
	Tuple* Join::fetchNext()
	{
		Tuple* pNext = nullptr;
		while (_children[0]->hasNext() || _lhs != nullptr){
			if (_lhs == nullptr) {
				_lhs = &(_children[0]->next());
			}
			while (_children[1]->hasNext()) {
				if (_rhs == nullptr) {
					_rhs = &(_children[1]->next());
				}
				if (_p->filter(*_lhs, *_rhs)) {
					_tuple = make_shared<Tuple>(_td);
					size_t sz = 0;
					size_t sz1 = _lhs->getTupleDesc()->numFields();
					size_t sz2 = _rhs->getTupleDesc()->numFields();
					for (size_t i = 0; i < sz1; ++i, ++sz) {
						_tuple->setField(sz, _lhs->getField(i));
					}
					for (size_t i = 0; i < sz2; ++i, ++sz) {
						_tuple->setField(sz, _rhs->getField(i));
					}
					pNext = _tuple.get();
					break;
				}
				else {
					_rhs = nullptr;
				}
			}
			if (!pNext) {
				_children[1]->rewind();
				_lhs = nullptr;
				_rhs = nullptr;
			}
			else{
				_rhs = nullptr;
				break;
			}
		}
		return pNext;
	}
}