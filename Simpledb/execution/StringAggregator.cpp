#include"StringAggregator.h"
#include"IntField.h"
#include"StringField.h"
namespace Simpledb {
	long StringAggregator::_serialVersionUID = 1l;
	StringAggregator::StringAggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what)
		:Aggregator(gbfield, gbfieldtype, afield, what)
	{
		if (_op != Aggregator::Op::COUNT) {
			throw runtime_error("error stringAggregator argument");
		}
	}
	void StringAggregator::mergeTupleIntoGroup(Tuple& tup)
	{
		shared_ptr<Tuple> aimTuple = getAimTuple(tup);
		int modifyfield = getModifyfield();
		int64_t gbValue = tup.getField(_gbfield)->hashCode();
		if (_op == Aggregator::Op::COUNT) {
			_groupToCount[gbValue] += 1;
			aimTuple->setField(modifyfield, make_shared<IntField>(_groupToCount[gbValue]));
		}
	}
	shared_ptr<OpIterator> StringAggregator::iterator()
	{
		return make_shared<TupleIterator>(_td, _tuples);
	}
}