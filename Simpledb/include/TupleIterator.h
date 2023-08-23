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
            return (_position < _items.size()
                && _items[_position] != nullptr);
        }
        Tuple* next()override {
            if (!_open) {
                throw runtime_error("db not open");
            }
            return Iterator<Tuple>::next();          
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
        bool _open;
	};
}