#include"StringAggregator.h"
namespace Simpledb {
	long StringAggregator::_serialVersionUID = 1l;
	StringAggregator::StringAggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what)
	{
	}
	void StringAggregator::mergeTupleIntoGroup(Tuple& tup)
	{
	}
	shared_ptr<OpIterator> StringAggregator::iterator()
	{
		return shared_ptr<OpIterator>();
	}
}