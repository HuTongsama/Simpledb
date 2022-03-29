#include"Aggregate.h"
namespace Simpledb {
	long Aggregate::_serialVersionUID = 1l;
	Aggregate::Aggregate(shared_ptr<OpIterator> child, int afield, int gfield, Aggregator::Op aop)
	{
	}
	int Aggregate::groupField()
	{
		return 0;
	}
	string Aggregate::groupFieldName()
	{
		return string();
	}
	int Aggregate::aggregateField()
	{
		return 0;
	}
	string Aggregate::aggregateFieldName()
	{
		return string();
	}
	Aggregator::Op Aggregate::aggregateOp()
	{
		return Aggregator::Op();
	}
	void Aggregate::open()
	{
	}
	void Aggregate::rewind()
	{
	}
	shared_ptr<TupleDesc> Aggregate::getTupleDesc()
	{
		return shared_ptr<TupleDesc>();
	}
	void Aggregate::close()
	{
	}
	vector<shared_ptr<OpIterator>> Aggregate::getChildren()
	{
		return vector<shared_ptr<OpIterator>>();
	}
	void Aggregate::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
	}
	Tuple* Aggregate::fetchNext()
	{
		return nullptr;
	}
}