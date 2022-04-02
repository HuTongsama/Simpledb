#pragma once
#include"OpIterator.h"
namespace Simpledb {
	/**
	* Implements a OpIterator by wrapping an Iterable<Tuple>.
	*/
	class TupleIterator : public OpIterator {
    public:
        /**
         * Constructs an iterator from the specified Iterable, and the specified
         * descriptor.
         *
         * @param tuples
         *            The set of tuples to iterate over
         */
        TupleIterator(shared_ptr<TupleDesc> td,const vector<shared_ptr<Tuple>>& tuples);
        void open()override {
            _open = true;
        }
        bool hasNext()override {
            if (!_open)
                return false;
            return (_position < _tuples.size()
                && _tuples[_position] != nullptr);
        }
        Tuple& next()override {
            if (!_open) {
                throw runtime_error("db not open");
            }
            if (_position >= _tuples.size()) {
                throw out_of_range("TupleIterator out of range");
            }
            shared_ptr<Tuple> t = _tuples[_position];
            _position++;
            return *t;
        }
        void rewind()override {
            _position = 0;
        }
        shared_ptr<TupleDesc> getTupleDesc()override {
            return _td;
        }
        void close()override {
            _open = false;
        }
    private:
        static long _serialVersionUID;
        shared_ptr<TupleDesc> _td;
        vector<shared_ptr<Tuple>> _tuples;
        bool _open;
	};
}