#include"IntegerAggregator.h"
#include"IntField.h"
namespace Simpledb {
	long IntegerAggregator::_serialVersionUID = 1l;
	IntegerAggregator::IntegerAggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what)
		:_gbfield(gbfield), _gbfieldtype(gbfieldtype), _afield(afield), _op(what)
	{
	}
	void IntegerAggregator::mergeTupleIntoGroup(Tuple& tup)
	{
		shared_ptr<IntField> afield = dynamic_pointer_cast<IntField>(tup.getField(_afield));
		

		if (_gbfield == Aggregator::NO_GROUPING) {
			shared_ptr<TupleDesc> td =
				make_shared<TupleDesc>(
					vector<shared_ptr<Type>>{Int_Type::INT_TYPE},
					vector<string>{Aggregator::opToString(_op)});
			shared_ptr<Tuple> t = make_shared<Tuple>(td);

			t->setField(0, afield);
			if (_tuples.empty()) {				
				_tuples.push_back(t);
				_groupToCount[t->getField(0)->hashCode()] = 1;
			}
			else {
				shared_ptr<Field> f1 = t->getField(0);
				shared_ptr<Field> f2 = _tuples[0]->getField(0);
				switch (_op)
				{
				case Simpledb::Aggregator::Op::MIN:
					if (f1->compare(Predicate::Op::LESS_THAN, *f2)) {
						f1.swap(f2);
					}
					break;
				case Simpledb::Aggregator::Op::MAX:
					if (f1->compare(Predicate::Op::GREATER_THAN, *f2)) {
						f1.swap(f2);
					}
					break;
				case Simpledb::Aggregator::Op::AVG:
					_groupToCount[t->getField(0)->hashCode()]++;
				case Simpledb::Aggregator::Op::SUM:
				{
					int v = dynamic_pointer_cast<IntField>(f1)->getValue() +
						dynamic_pointer_cast<IntField>(f2)->getValue();
					_tuples[0]->setField(0, make_shared<IntField>(v));
				}
					break;
				case Simpledb::Aggregator::Op::COUNT:
					_groupToCount[t->getField(0)->hashCode()]++;
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
		else {
			int64_t groupId = tup.getField(_gbfield)->hashCode();

		}
	}
	shared_ptr<OpIterator> IntegerAggregator::iterator()
	{
		return shared_ptr<OpIterator>();
	}
}