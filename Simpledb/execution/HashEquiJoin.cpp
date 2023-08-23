#include"HashEquiJoin.h"
namespace Simpledb {
	HashEquiJoin::HashEquiJoin(shared_ptr<JoinPredicate> p, shared_ptr<OpIterator> child1, shared_ptr<OpIterator> child2)
		:_pred(p), _child1(child1), _child2(child2), _tupPos(0)
	{
		_comboTD = TupleDesc::merge(*child1->getTupleDesc(), *child2->getTupleDesc());
	}
	void HashEquiJoin::open()
	{
		Operator::open();
		_child1->open();
		_child2->open();
		loadMap();
	}
	void HashEquiJoin::close()
	{
		Operator::close();
		_child2->close();
		_child1->close();
		_t1 = nullptr;
		_t2 = nullptr;
		_tupVec.clear();
		_tupPos = 0;
		_result.clear();
		_map.clear();
	}
	void HashEquiJoin::rewind()
	{
		_child1->rewind();
		_child2->rewind();
		_tupVec.clear();
		_tupPos = 0;
		_result.clear();
	}
	Tuple* HashEquiJoin::fetchNext()
	{
		if (_tupPos < _tupVec.size()) {
			shared_ptr<Tuple> t = processList();
			_result.push_back(t);
			return _result.back().get();
		}

		// loop around child2
		while (_child2->hasNext()) {
			Tuple* t = _child2->next();
			_t2 = make_shared<Tuple>(t->getTupleDesc());
			t->copyTo(*_t2);

			// if match, create a combined tuple and fill it with the values
			// from both tuples
			_tupVec = _map[_t2->getField(_pred->getField2())];
			if (_tupVec.empty())
				continue;
			_tupPos = 0;
			shared_ptr<Tuple> t1 = processList();
			_result.push_back(t1);
			return _result.back().get();
		}

		// child2 is done: advance child1
		_child2->rewind();
		if (loadMap()) {
			return fetchNext();
		}

		return nullptr;
	}
	bool HashEquiJoin::loadMap()
	{
		int cnt = 0;
		_map.clear();
		while (_child1->hasNext()) {
			Tuple* t = _child1->next();
			_t1 = make_shared<Tuple>(t->getTupleDesc());
			t->copyTo(*_t1);
			vector<shared_ptr<Tuple>>& list = _map[_t1->getField(_pred->getField1())];
			list.push_back(_t1);
			if (cnt == MAP_SIZE)
				return true;
			cnt += 1;
		}
		return cnt > 0;
	}
	shared_ptr<Tuple> HashEquiJoin::processList()
	{
		_t1 = _tupVec[_tupPos];
		_tupPos++;

		int td1n = _t1->getTupleDesc()->numFields();
		int td2n = _t2->getTupleDesc()->numFields();

		// set fields in combined tuple
		shared_ptr<Tuple> t = make_shared<Tuple>(_comboTD);
		for (int i = 0; i < td1n; i++)
			t->setField(i, _t1->getField(i));
		for (int i = 0; i < td2n; i++)
			t->setField(td1n + i, _t2->getField(i));
		return t;
	}
}