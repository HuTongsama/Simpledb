#include"IntegerAggregator.h"
#include"IntField.h"
#include"TupleIterator.h"
namespace Simpledb {
	long IntegerAggregator::_serialVersionUID = 1l;
	IntegerAggregator::IntegerAggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what)
		:_gbfield(gbfield), _gbfieldtype(gbfieldtype), _afield(afield), _op(what)
	{
	}
	void IntegerAggregator::mergeTupleIntoGroup(Tuple& tup)
	{
		shared_ptr<IntField> gbField1 = dynamic_pointer_cast<IntField>(tup.getField(_gbfield));
		int gbValue1 = gbField1->getValue();
		shared_ptr<Tuple> aimTuple = nullptr;
		if (_gbfield == Aggregator::NO_GROUPING) {
			if (!_tuples.empty()) {
				aimTuple = _tuples[0];
			}
		}
		else {
			for (auto& t : _tuples) {
				shared_ptr<IntField> gbField2 = dynamic_pointer_cast<IntField>(t->getField(_gbfield));
				int gbValue2 = gbField2->getValue();
				if (gbValue1 == gbValue2) {
					aimTuple = t;
					break;
				}
			}
		}
		int modifyfield = _gbfield == Aggregator::NO_GROUPING ? 0 : 1;
		if (aimTuple == nullptr) {
			_groupToCount[gbValue1] = 0;
			if (_gbfield == Aggregator::NO_GROUPING) {
				if (_td == nullptr) {
					 _td = make_shared<TupleDesc>(
							vector<shared_ptr<Type>>{Int_Type::INT_TYPE},
							vector<string>{"Aggregator"});
				}
				
				aimTuple = make_shared<Tuple>(_td);
			}
			else {
				if (_td == nullptr) {
					_td = make_shared<TupleDesc>(
							vector<shared_ptr<Type>>{Int_Type::INT_TYPE, Int_Type::INT_TYPE},
							vector<string>{"Aggregator", tup.getTupleDesc()->getFieldName(_gbfield)});
				}
				aimTuple = make_shared<Tuple>(_td);
				aimTuple->setField(0, gbField1);
			}
			int initVal = 0;
			if (_op == Aggregator::Op::MAX)
				initVal = INT_MIN;
			else if (_op == Aggregator::Op::MIN)
				initVal = INT_MAX;
			aimTuple->setField(modifyfield, make_shared<IntField>(initVal));
			
			_tuples.push_back(aimTuple);
		}
		shared_ptr<IntField> aField = dynamic_pointer_cast<IntField>(tup.getField(_afield));
		modifyTuple(aimTuple, modifyfield, gbValue1, aField);
	}
	shared_ptr<OpIterator> IntegerAggregator::iterator()
	{
		return make_shared<TupleIterator>(_td, _tuples);
	}
	void IntegerAggregator::modifyTuple(shared_ptr<Tuple> t, int modifyfield, int groupVal, shared_ptr<IntField> aggregateField)
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