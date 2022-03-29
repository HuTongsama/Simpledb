#include"IntegerAggregator.h"
namespace Simpledb {
	long IntegerAggregator::_serialVersionUID = 1l;
	IntegerAggregator::IntegerAggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what)
	{
	}
	void IntegerAggregator::mergeTupleIntoGroup(Tuple& tup)
	{
	}
	shared_ptr<OpIterator> IntegerAggregator::iterator()
	{
		return shared_ptr<OpIterator>();
	}
}