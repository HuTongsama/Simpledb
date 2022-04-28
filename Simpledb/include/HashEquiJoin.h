#pragma once
#include"Operator.h"
#include"JoinPredicate.h"
#include"Tuple.h"
#include<map>
namespace Simpledb {
	/**
	 * The Join operator implements the relational join operation.
	 */
	class HashEquiJoin :public Operator {
	public:
		const static int MAP_SIZE = 20000;
		/**
		 * Constructor. Accepts to children to join and the predicate to join them
		 * on
		 *
		 * @param p
		 *            The predicate to use to join the children
		 * @param child1
		 *            Iterator for the left(outer) relation to join
		 * @param child2
		 *            Iterator for the right(inner) relation to join
		 */
		HashEquiJoin(shared_ptr<JoinPredicate> p, shared_ptr<OpIterator> child1, shared_ptr<OpIterator> child2);
		shared_ptr<JoinPredicate> getJoinPredicate() {
			return _pred;
		}
		shared_ptr<TupleDesc> getTupleDesc() {
			return _comboTD;
		}
		string getJoinField1Name() {
			return _child1->getTupleDesc()->getFieldName(_pred->getField1());
		}
		string getJoinField2Name() {
			return _child2->getTupleDesc()->getFieldName(_pred->getField2());
		}
		void open()override;
		void close()override;
		void rewind()override;
		vector<shared_ptr<OpIterator>> getChildren() {
			return vector<shared_ptr<OpIterator>>{_child1, _child2};
		}
		void setChildren(const vector<shared_ptr<OpIterator>>& children) {
			_child1 = children[0];
			_child2 = children[1];
		}
	protected:
		Tuple* fetchNext()override;
	private:
		bool loadMap();
		/**
		 * Returns the next tuple generated by the join, or null if there are no
		 * more tuples. Logically, this is the next tuple in r1 cross r2 that
		 * satisfies the join predicate. There are many possible implementations;
		 * the simplest is a nested loops join.
		 * <p>
		 * Note that the tuples returned from this particular implementation of Join
		 * are simply the concatenation of joining tuples from the left and right
		 * relation. Therefore, there will be two copies of the join attribute in
		 * the results. (Removing such duplicate columns can be done with an
		 * additional projection operator if needed.)
		 * <p>
		 * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
		 * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
		 *
		 * @return The next matching tuple.
		 * @see JoinPredicate#filter
		 */
		shared_ptr<Tuple> processList();



		static const long _serialVersionUID = 1L;
		shared_ptr<JoinPredicate> _pred;
		shared_ptr<OpIterator> _child1;
		shared_ptr<OpIterator> _child2;
		shared_ptr<TupleDesc> _comboTD;
	    shared_ptr<Tuple> _t1;
		shared_ptr<Tuple> _t2;
		map<shared_ptr<Field>, vector<shared_ptr<Tuple>>> _map;
		vector<shared_ptr<Tuple>> _tupVec;
		size_t _tupPos;
		vector<shared_ptr<Tuple>> _result;

		
	};
}