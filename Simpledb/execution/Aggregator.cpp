#include "Aggregator.h"
#include "IntField.h"
namespace Simpledb {
	Aggregator::Aggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what)
		:_gbfield(gbfield), _gbfieldtype(gbfieldtype), _afield(afield), _op(what)
	{
		if (_gbfield == Aggregator::NO_GROUPING) {

			_td = make_shared<TupleDesc>(
				vector<shared_ptr<Type>>{Int_Type::INT_TYPE()},
				vector<string>{""});
		}
		else {
			_td = make_shared<TupleDesc>(
				vector<shared_ptr<Type>>{_gbfieldtype, Int_Type::INT_TYPE()},
				vector<string>{"", ""});
		}
	}
	shared_ptr<Tuple> Aggregator::getAimTuple(Tuple& tup)
	{
		shared_ptr<Field> gbField1 = tup.getField(_gbfield);
		int64_t gbValue1 = Aggregator::NO_GROUPING;
		if (gbField1 != nullptr) {
			gbValue1 = gbField1->hashCode();
		}
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
						vector<shared_ptr<Type>>{Int_Type::INT_TYPE()},
						vector<string>{""});
				}
				aimTuple = make_shared<Tuple>(_td);
			}
			else {
				if (_td == nullptr) {
					_td = make_shared<TupleDesc>(
						vector<shared_ptr<Type>>{Int_Type::INT_TYPE(), Int_Type::INT_TYPE()},
						vector<string>{"", tup.getTupleDesc()->getFieldName(_gbfield)});
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
			_gbValues.push_back(gbValue1);
		}
		return aimTuple;
	}
}