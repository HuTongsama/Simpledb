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
		void close()override {
			_next = nullptr;
			_open = false;
		}
		void open()override {
			_open = true;
		}
		virtual vector<shared_ptr<OpIterator>> getChildren() = 0;
		virtual void setChildren(const vector<shared_ptr<OpIterator>>& children) = 0;
		int getEstimatedCardinality() {
			return _estimatedCardinality; }
		void setEstimatedCardinality(int card) {
			_estimatedCardinality = card;
		};
	protected:
		virtual shared_ptr<Tuple> fetchNext() = 0;
	private:
		static long _serialVersionUID;
		shared_ptr<Tuple> _next;
		bool _open;
		int _estimatedCardinality;
	};
}