#pragma once
#include"Operator.h"
#include"JoinPredicate.h"
namespace Simpledb {
	class Join :public Operator {
    public:
        /**
         * Constructor. Accepts two children to join and the predicate to join them
         * on
         *
         * @param p
         *            The predicate to use to join the children
         * @param child1
         *            Iterator for the left(outer) relation to join
         * @param child2
         *            Iterator for the right(inner) relation to join
         */
        Join(shared_ptr<JoinPredicate> p, shared_ptr<OpIterator> child1, shared_ptr<OpIterator> child2);
        shared_ptr<JoinPredicate> getJoinPredicate();
        /**
         * @return
         *       the field name of join field1. Should be quantified by
         *       alias or table name.
         * */
        string getJoinField1Name();
        /**
         * @return
         *       the field name of join field2. Should be quantified by
         *       alias or table name.
         * */
        string getJoinField2Name();
        /**
         * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
         *      implementation logic.
         */
        shared_ptr<TupleDesc> getTupleDesc();
        void open()override;
        void close()override;
        void rewind()override;
        vector<shared_ptr<OpIterator>> getChildren()override;
        void setChildren(const vector<shared_ptr<OpIterator>>& children)override;
    protected:
        /**
         * Returns the next tuple generated by the join, or null if there are no
         * more tuples. Logically, this is the next tuple in r1 cross r2 that
         * satisfies the join predicate. There are many possible implementations;
         * the simplest is a nested loops join.
         * <p>
         * Note that the tuples returned from this particular implementation of Join
         * are simply the concatenation of joining tuples from the left and right
         * relation. Therefore, if an equality predicate is used there will be two
         * copies of the join attribute in the results. (Removing such duplicate
         * columns can be done with an additional projection operator if needed.)
         * <p>
         * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
         * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
         *
         * @return The next matching tuple.
         * @see JoinPredicate#filter
         */
        shared_ptr<Tuple> fetchNext()override;
    private:
        static long _serialVersionUID;
	};
}