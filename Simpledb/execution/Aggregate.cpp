#include"Aggregate.h"
#include"IntegerAggregator.h"
#include"StringAggregator.h"
namespace Simpledb {
	long Aggregate::_serialVersionUID = 1l;
	Aggregate::Aggregate(shared_ptr<OpIterator> child, int afield, int gfield, Aggregator::Op aop)
		:_afield(afield), _gfield(gfield), _aop(aop)
	{
		_children.push_back(child);
		updateIter();
	}
	int Aggregate::groupField()
	{
		return _gfield;
	}
	string Aggregate::groupFieldName()
	{
		return _td->getFieldName(_gfield);
	}
	int Aggregate::aggregateField()
	{
		return _afield;
	}
	string Aggregate::aggregateFieldName()
	{
		return _td->getFieldName(_afield);
	}
	Aggregator::Op Aggregate::aggregateOp()
	{
		return _aop;
	}
	void Aggregate::open()
	{
		Operator::open();
		_iter->open();
	}
	void Aggregate::rewind()
	{
		_iter->rewind();
	}
	shared_ptr<TupleDesc> Aggregate::getTupleDesc()
	{
		return _td;
	}
	void Aggregate::close()
	{
		Operator::close();
		_iter->close();
	}
	vector<shared_ptr<OpIterator>> Aggregate::getChildren()
	{
		return _children;
	}
	void Aggregate::setChildren(const vector<shared_ptr<OpIterator>>& children)
	{
		_children = children;
		updateIter();
		_iter->open();
	}
	Tuple* Aggregate::fetchNext()
	{
		Tuple* p = nullptr;
		if (_iter->hasNext()) {
			p = _iter->next();
		}
		return p;
	}
	void Aggregate::updateIter()
	{
		shared_ptr<OpIterator> child = _children[0];
		shared_ptr<TupleDesc> td = child->getTupleDesc();
		shared_ptr<Type> aType = td->getFieldType(_afield);
		shared_ptr<Type> gType = nullptr;
		if (_gfield != Aggregator::NO_GROUPING) {
			gType = td->getFieldType(_gfield);
		}
		switch (aType->type())
		{
		case Type::TYPE::INT_TYPE:
			_aggregator = make_shared<IntegerAggregator>(_gfield, gType, _afield, _aop);
			break;
		case Type::TYPE::STRING_TYPE:
			_aggregator = make_shared<StringAggregator>(_gfield, gType, _afield, _aop);
			break;
		default:
			break;
		}
		_children[0]->open();
		while (_children[0]->hasNext()) {
			_aggregator->mergeTupleIntoGroup(*(_children[0]->next()));
		}
		_children[0]->close();
		_iter = _aggregator->iterator();
		_td = _iter->getTupleDesc();
	}
}