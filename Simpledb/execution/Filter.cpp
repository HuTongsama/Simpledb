#include"Filter.h"
namespace Simpledb {
	long Filter::_serialVersionUID = 1l;
	Filter::Filter(shared_ptr<Predicate> p, shared_ptr<OpIterator> child)
	{
	}
	shared_ptr<Predicate> Filter::getPredicate()
	{
		return shared_ptr<Predicate>();
	}
	shared_ptr<TupleDesc> Filter::getTupleDesc()
	{
		return shared_ptr<TupleDesc>();
	}
	void Filter::open()
	{
	}
	void Filter::close()
	{
	}
	void Filter::rewind()
	{
	}
	vector<shared_ptr<OpIterator>> Filter::getChildren()
	{
		return vector<shared_ptr<OpIterator>>();
	}
	void Filter::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
	}
	shared_ptr<Tuple> Filter::fetchNext()
	{
		return shared_ptr<Tuple>();
	}
}