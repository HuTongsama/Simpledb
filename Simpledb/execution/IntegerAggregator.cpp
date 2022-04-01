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
		int64_t gbValue = _gbfield == Aggregator::NO_GROUPING ?
			Aggregator::NO_GROUPING : tup.getField(_gbfield)->hashCode();
		shared_ptr<IntField> aField = dynamic_pointer_cast<IntField>(tup.getField(_afield));
		modifyTuple(aimTuple, modifyfield, gbValue, aField);
	}
	shared_ptr<OpIterator> IntegerAggregator::iterator()
	{
		if (_op == Simpledb::Aggregator::Op::AVG) {
			size_t sz = _tuples.size();
			if (sz != _gbValues.size())
				throw runtime_error("error tuple size");
			vector<shared_ptr<Tuple>> tuples;
			for (size_t i = 0; i < sz; ++i) {
				shared_ptr<Tuple> t = _tuples[i];				
				shared_ptr<Tuple> t1 = make_shared<Tuple>(t->getTupleDesc());
				t->copyTo(*t1);
				int modifyfield = getModifyfield();
				int sum = dynamic_pointer_cast<IntField>(t1->getField(modifyfield))->getValue();
				int64_t gVal = _gbValues[i];
				int64_t count = _groupToCount[gVal];
				int avg = count != 0 ? sum / count : 0;
				t1->setField(modifyfield, make_shared<IntField>(avg));
				tuples.push_back(t1);
			}
			return make_shared<TupleIterator>(_td, tuples);
		}
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
			_groupToCount[groupVal] += 1;
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