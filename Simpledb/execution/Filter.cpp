#include"Filter.h"
namespace Simpledb {
	long Filter::_serialVersionUID = 1l;
	Filter::Filter(shared_ptr<Predicate> p, shared_ptr<OpIterator> child)
	{
		_p = p;
		_children.push_back(child);
		_td = child->getTupleDesc();
	}
	shared_ptr<Predicate> Filter::getPredicate()
	{
		return _p;
	}
	shared_ptr<TupleDesc> Filter::getTupleDesc()
	{
		return _td;
	}
	void Filter::open()
	{
		Operator::open();
		_children[0]->open();
	}
	void Filter::close()
	{
		_children[0]->close();
		Operator::close();
	}
	void Filter::rewind()
	{
		_children[0]->rewind();		
	}
	vector<shared_ptr<OpIterator>> Filter::getChildren()
	{
		return _children;
	}
	void Filter::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
		_children = children;
	}
	Tuple* Filter::fetchNext()
	{
		Tuple* pNext = nullptr;
		while (_children[0]->hasNext()) {
			Tuple& next = _children[0]->next();
			if (_p->filter(next))
			{
				pNext = &next;
				break;
			}
		}
		return pNext;
	}
}