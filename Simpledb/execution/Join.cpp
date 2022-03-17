#include"Join.h"
namespace Simpledb {
	long Join::_serialVersionUID = 1l;
	Join::Join(shared_ptr<JoinPredicate> p, shared_ptr<OpIterator> child1, shared_ptr<OpIterator> child2)
	{
	}
	shared_ptr<JoinPredicate> Join::getJoinPredicate()
	{
		return shared_ptr<JoinPredicate>();
	}
	string Join::getJoinField1Name()
	{
		return string();
	}
	string Join::getJoinField2Name()
	{
		return string();
	}
	shared_ptr<TupleDesc> Join::getTupleDesc()
	{
		return shared_ptr<TupleDesc>();
	}
	void Join::open()
	{
	}
	void Join::close()
	{
	}
	void Join::rewind()
	{
	}
	vector<shared_ptr<OpIterator>> Join::getChildren()
	{
		return vector<shared_ptr<OpIterator>>();
	}
	void Join::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
	}
	shared_ptr<Tuple> Join::fetchNext()
	{
		return shared_ptr<Tuple>();
	}
}