#include "Aggregator.h"
#include "IntField.h"
namespace Simpledb {
	shared_ptr<Tuple> Aggregator::getAimTuple(Tuple& tup)
	{
		shared_ptr<Field> gbField1 = tup.getField(_gbfield);
		int64_t gbValue1 = gbField1->hashCode();
		shared_ptr<Tuple> aimTuple = nullptr;
		if (_gbfield == Aggregator::NO_GROUPING) {
			if (!_tuples.empty()) {
				aimTuple = _tuples[0];
			}
		}
		else {
			for (auto& t : _tuples) {
				shared_ptr<Field> gbField2 = t->getField(_gbfield);
				int64_t gbValue2 = gbField2->hashCode();
				if (gbValue1 == gbValue2) {
					aimTuple = t;
					break;
				}
			}
		}
		int modifyfield = getModifyfield();
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
		return aimTuple;
	}
}