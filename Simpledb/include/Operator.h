#pragma once
#include"OpIterator.h"
#include<vector>
using namespace std;
namespace Simpledb {
	class Operator : public OpIterator {
	public:
		Operator()
			:_next(nullptr), _open(false), _estimatedCardinality(0) {
		}
		bool hasNext()override;
		Tuple& next()override;
		/**
		* Closes this iterator. If overridden by a subclass, they should call
		* Operator::close() in order for Operator's internal state to be consistent.
		*/
		void close()override {
			_next = nullptr;
			_open = false;
		}
		void open()override {
			_open = true;
		}
		/**
		* @return return the children DbIterators of this operator. If there is
		*         only one child, return an array of only one element. For join
		*         operators, the order of the children is not important. But they
		*         should be consistent among multiple calls.
		* */
		virtual vector<shared_ptr<OpIterator>> getChildren() = 0;
		/**
		* Set the children(child) of this operator. If the operator has only one
		* child, children[0] should be used. If the operator is a join, children[0]
		* and children[1] should be used.
		*
		*
		* @param children
		*            the DbIterators which are to be set as the children(child) of
		*            this operator
		* */
		virtual void setChildren(const vector<shared_ptr<OpIterator>>& children) = 0;
		int getEstimatedCardinality() {
			return _estimatedCardinality; }
		void setEstimatedCardinality(int card) {
			_estimatedCardinality = card;
		};
	protected:
		/**
		* Returns the next Tuple in the iterator, or nullptr if the iteration is
		* finished. Operator uses this method to implement both <code>next</code>
		* and <code>hasNext</code>.
		*
		* @return the next Tuple in the iterator, or nullptr if the iteration is
		*         finished.
		*/
		virtual Tuple* fetchNext() = 0;
	private:
		static long _serialVersionUID;
		Tuple* _next;
		bool _open;
		int _estimatedCardinality;
	};
}