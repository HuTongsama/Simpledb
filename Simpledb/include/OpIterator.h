#pragma once
#include"Iterator.h"
#include"Tuple.h"

namespace Simpledb {
	/**
	* OpIterator is the iterator interface that all SimpleDB operators should
	* implement. If the iterator is not open, none of the methods should work,
	* and should throw an exception.  In addition to any
	* resource allocation/deallocation, an open method should call any
	* child iterator open methods, and in a close method, an iterator
	* should call its children's close methods.
	*/
	class OpIterator : public Iterator<Tuple> {
	public:
		/**
		* Opens the iterator. This must be called before any of the other methods.
		* @throws DbException when there are problems opening/accessing the database.
		*/
		virtual void open() = 0;
		/**
		* Resets the iterator to the start.
		* @throws DbException when rewind is unsupported.
		* @throws IllegalStateException If the iterator has not been opened
		*/
		virtual void rewind() = 0;
		/**
		* Returns the TupleDesc associated with this OpIterator.
		* @return the TupleDesc associated with this OpIterator.
		*/
		virtual shared_ptr<TupleDesc> getTupleDesc() = 0;
		/**
		* Closes the iterator. When the iterator is closed, calling next(),
		* hasNext(), or rewind() should fail by throwing IllegalStateException.
		*/
		virtual void close() = 0;
	};
}