#include"IntegerAggregator.h"
#include"IntField.h"
#include"TupleIterator.h"
namespace Simpledb {
	long IntegerAggregator::_serialVersionUID = 1l;
	IntegerAggregator::IntegerAggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what)
		:Aggregator(gbfield, gbfieldtype, afield, what)
	{
	}
	void IntegerAggregator::mergeTupleIntoGroup(Tuple& tup)
	{
		shared_ptr<Tuple> aimTuple = getAimTuple(tup);
		int modifyfield = getModifyfield();
		int64_t gbValue = tup.getField(_gbfield)->hashCode();
		shared_ptr<IntField> aField = dynamic_pointer_cast<IntField>(tup.getField(_afield));
		modifyTuple(aimTuple, modifyfield, gbValue, aField);
	}
	shared_ptr<OpIterator> IntegerAggregator::iterator()
	{
		return make_shared<TupleIterator>(_td, _tuples);
	}
	void IntegerAggregator::modifyTuple(shared_ptr<Tuple> t, int modifyfield, int64_t groupVal, shared_ptr<IntField> aggregateField)
	{
		shared_ptr<IntField> mField = dynamic_pointer_cast<IntField>(t->getField(modifyfield));

		switch (_op)
		{
		case Simpledb::Aggregator::Op::MIN:
			if (aggregateField->compare(Predicate::Op::LESS_THAN, *mField)) {
				t->setField(modifyfield, aggregateField);
			}
			break;
		case Simpledb::Aggregator::Op::MAX:
			if (aggregateField->compare(Predicate::Op::GREATER_THAN, *mField)) {
				t->setField(modifyfield, aggregateField);
			}
			break;
		case Simpledb::Aggregator::Op::COUNT:
			_groupToCount[groupVal] += 1;
			t->setField(modifyfield, make_shared<IntField>(_groupToCount[groupVal]));
			break;
		case Simpledb::Aggregator::Op::AVG:
		{
			int count = _groupToCount[groupVal];
			int avg = (mField->getValue() * count + aggregateField->getValue()) / (count + 1);
			t->setField(modifyfield, make_shared<IntField>(avg));
			_groupToCount[groupVal] += 1;
		}
			break;
		case Simpledb::Aggregator::Op::SUM:
		{
			int sum = mField->getValue() + aggregateField->getValue();
			t->setField(modifyfield, make_shared<IntField>(sum));
		}
			break;
		case Simpledb::Aggregator::Op::SUM_COUNT:
			break;
		case Simpledb::Aggregator::Op::SC_AVG:
			break;
		default:
			break;
		}
	}
}